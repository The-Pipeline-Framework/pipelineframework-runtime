package org.pipelineframework;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.pipelineframework.orchestrator.DynamoAwaitLifecycleTestStores;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/** Each recovery caller restores new store and runtime instances over the same durable tables. */
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwaitItemContinuationRecoveryIT extends AwaitItemContinuationRecoveryTest {
    private static final String PREFIX = "item_continuation_recovery";
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8")).withServices("dynamodb");
    private DynamoDbClient dynamo;

    @BeforeAll
    void tables() {
        dynamo = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))).build();
    }

    private void createTables(String prefix) {
        table(prefix, "_completion_lifecycle", "pk", Optional.of("sk"), List.of());
        table(prefix, "_execution", "tenant_id", Optional.of("execution_id"), List.of());
        table(prefix, "_execution_key", "tenant_execution_key", Optional.empty(), List.of());
        table(prefix, "_execution_payload", "payload_id", Optional.of("payload_part"), List.of());
        table(prefix, "_unit", "tenant_id", Optional.of("unit_id"), List.of());
        table(prefix, "_interaction_key", "lookup_key", Optional.empty(), List.of());
        table(prefix, "_interaction", "tenant_id", Optional.of("interaction_id"), List.of(
            new Index("await-interaction-pending-by-deadline", "query_deadline_key", "query_deadline_sort", ScalarAttributeType.S),
            new Index("await-interaction-by-unit", "query_unit_key", "query_unit_sort", ScalarAttributeType.S),
            new Index("await-interaction-continuation-work", "query_continuation_key",
                "query_continuation_due_epoch_ms", ScalarAttributeType.N)));
    }

    @Override
    protected Supplier<Stores> stores() {
        // Global due-work queries need isolated durable tables per scenario, including dynamic cases.
        // Fresh callers within the scenario still recover from those same tables.
        String prefix = PREFIX + "_" + UUID.randomUUID().toString().replace("-", "");
        createTables(prefix);
        return () -> new Stores(DynamoAwaitLifecycleTestStores.executionStore(dynamo, prefix),
            org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.interactionStore(dynamo, prefix),
            org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.unitStore(dynamo, prefix));
    }

    private void table(String prefix, String suffix, String hash, Optional<String> range, List<Index> indexes) {
        List<AttributeDefinition> attributes = new ArrayList<>();
        attributes.add(attribute(hash, ScalarAttributeType.S));
        range.ifPresent(value -> attributes.add(attribute(value, ScalarAttributeType.S)));
        List<KeySchemaElement> keys = new ArrayList<>();
        keys.add(key(hash, KeyType.HASH));
        range.ifPresent(value -> keys.add(key(value, KeyType.RANGE)));
        ProvisionedThroughput throughput = ProvisionedThroughput.builder()
            .readCapacityUnits(10L).writeCapacityUnits(10L).build();
        List<GlobalSecondaryIndex> secondary = indexes.stream().map(index -> {
            attributes.add(attribute(index.hash(), ScalarAttributeType.S));
            attributes.add(attribute(index.range(), index.type()));
            return GlobalSecondaryIndex.builder().indexName(index.name())
                .keySchema(key(index.hash(), KeyType.HASH), key(index.range(), KeyType.RANGE))
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .provisionedThroughput(throughput).build();
        }).toList();
        var request = CreateTableRequest.builder().tableName(prefix + suffix).attributeDefinitions(attributes)
            .keySchema(keys).provisionedThroughput(throughput);
        if (!secondary.isEmpty()) {
            request.globalSecondaryIndexes(secondary);
        }
        dynamo.createTable(request.build());
        dynamo.waiter().waitUntilTableExists(value -> value.tableName(prefix + suffix));
    }

    private AttributeDefinition attribute(String name, ScalarAttributeType type) {
        return AttributeDefinition.builder().attributeName(name).attributeType(type).build();
    }
    private KeySchemaElement key(String name, KeyType type) {
        return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }
    private record Index(String name, String hash, String range, ScalarAttributeType type) {}

    @AfterAll
    void close() {
        dynamo.close();
    }
}

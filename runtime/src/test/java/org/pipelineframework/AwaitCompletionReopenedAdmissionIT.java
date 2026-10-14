package org.pipelineframework;

import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
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
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/** The same retained reissue/crash/clock oracles against native physical transactions and fresh wrappers. */
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwaitCompletionReopenedAdmissionIT extends AwaitCompletionReopenedAdmissionTest {
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8")).withServices("dynamodb");
    private DynamoDbClient dynamo;

    @BeforeAll
    void connect() {
        dynamo = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))).build();
    }

    @Override
    protected Scenario scenario() {
        String prefix = "completion_reissue_" + UUID.randomUUID().toString().replace("-", "");
        table(prefix + "_execution", "tenant_id", Optional.of("execution_id"));
        table(prefix + "_execution_key", "tenant_execution_key", Optional.empty());
        table(prefix + "_execution_payload", "payload_id", Optional.of("payload_part"));
        table(prefix + "_interaction", "tenant_id", Optional.of("interaction_id"));
        table(prefix + "_interaction_key", "lookup_key", Optional.empty());
        table(prefix + "_unit", "tenant_id", Optional.of("unit_id"));
        table(prefix + "_completion_lifecycle", "pk", Optional.of("sk"));
        return new Scenario(() -> new AwaitCompletionLifecycleProtocolTest.Stores(
            DynamoAwaitLifecycleTestStores.executionStore(dynamo, prefix),
            org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.interactionStore(dynamo, prefix)),
            () -> org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.unitStore(dynamo, prefix));
    }

    private void table(String name, String hash, Optional<String> range) {
        var attributes = new ArrayList<AttributeDefinition>();
        attributes.add(AttributeDefinition.builder().attributeName(hash).attributeType(ScalarAttributeType.S).build());
        var keys = new ArrayList<KeySchemaElement>();
        keys.add(KeySchemaElement.builder().attributeName(hash).keyType(KeyType.HASH).build());
        range.ifPresent(sort -> {
            attributes.add(AttributeDefinition.builder().attributeName(sort).attributeType(ScalarAttributeType.S).build());
            keys.add(KeySchemaElement.builder().attributeName(sort).keyType(KeyType.RANGE).build());
        });
        dynamo.createTable(request -> request.tableName(name).attributeDefinitions(attributes)
            .keySchema(keys).billingMode(BillingMode.PAY_PER_REQUEST));
        dynamo.waiter().waitUntilTableExists(request -> request.tableName(name));
    }

    @AfterAll
    void close() {
        if (dynamo != null) dynamo.close();
    }
}

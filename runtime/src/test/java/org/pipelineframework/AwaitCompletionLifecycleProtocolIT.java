package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.pipelineframework.awaitable.AwaitCompletionCursor;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.orchestrator.DynamoAwaitLifecycleTestStores;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/** Unchanged protocol oracles against fresh wrappers and actual Dynamo transactions/primary queries. */
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwaitCompletionLifecycleProtocolIT extends AwaitCompletionLifecycleProtocolTest {
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
    protected Supplier<Stores> stores() {
        return fixture().stores();
    }

    private record Fixture(String prefix, Supplier<Stores> stores) {}

    private Fixture fixture() {
        String prefix = "completion_protocol_" + UUID.randomUUID().toString().replace("-", "");
        table(prefix + "_execution", "tenant_id", Optional.of("execution_id"));
        table(prefix + "_execution_key", "tenant_execution_key", Optional.empty());
        table(prefix + "_execution_payload", "payload_id", Optional.of("payload_part"));
        table(prefix + "_interaction", "tenant_id", Optional.of("interaction_id"));
        table(prefix + "_interaction_key", "lookup_key", Optional.empty());
        table(prefix + "_completion_lifecycle", "pk", Optional.of("sk"));
        // Each scenario is isolated; callers within it share the same durable table set.
        return new Fixture(prefix, () -> new Stores(DynamoAwaitLifecycleTestStores.executionStore(dynamo, prefix),
            org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.interactionStore(dynamo, prefix)));
    }

    @Test
    void missingFactTableCannotCommitCompletionWithoutImmutableAdmission() {
        var fixture = fixture();
        long now = System.currentTimeMillis();
        var interaction = imported(fixture.stores().get(), "tenant", "parent", "interaction", "unit", 0,
            AwaitInteractionStatus.WAITING, now, 0);
        removeLifecycleTable(fixture.prefix());
        assertThrows(RuntimeException.class,
            () -> await(fixture.stores().get().interactions().complete(completion(interaction, now + 1))));
        assertEquals(interaction, await(fixture.stores().get().interactions().get("tenant", "interaction")).orElseThrow(),
            "a failed fact transaction must leave the original semantic interaction intact");
        table(fixture.prefix() + "_completion_lifecycle", "pk", Optional.of("sk"));
        assertFalse(await(fixture.stores().get().interactions().complete(completion(interaction, now + 2))).duplicate());
        var admission = fact(fixture.stores().get(), interaction);
        assertEquals(now + 2, admission.firstAdmittedAtEpochMs());
        assertEquals(List.of(admission), enumerate(fixture.stores().get(), "tenant", 1));
    }

    @Test
    void missingAcknowledgementTableCannotReleaseParentWithoutItsImmutableFact() {
        var fixture = fixture();
        long now = System.currentTimeMillis();
        var parent = waiting(fixture.stores().get(), "tenant", "unit", now);
        String unit = parent.awaitUnitId();
        removeLifecycleTable(fixture.prefix());
        assertThrows(RuntimeException.class, () -> await(fixture.stores().get().executions()
            .markAwaitCompleted("tenant", parent.executionId(), unit, 3, now + 1)));
        assertEquals(parent, await(fixture.stores().get().executions().getExecution("tenant", parent.executionId()))
            .orElseThrow(), "a failed ACK transaction must not commit the parent release");
        table(fixture.prefix() + "_completion_lifecycle", "pk", Optional.of("sk"));
        var released = await(fixture.stores().get().executions()
            .markAwaitCompleted("tenant", parent.executionId(), unit, 3, now + 2)).orElseThrow();
        assertEquals(ExecutionStatus.QUEUED, released.status());
        var acknowledgement = ack(fixture.stores().get(), "tenant", parent.executionId(), unit).orElseThrow();
        assertEquals(now + 2, acknowledgement.acknowledgedAtEpochMs());
        assertEquals(released.version(), acknowledgement.parentVersion());
    }

    private void removeLifecycleTable(String prefix) {
        dynamo.deleteTable(request -> request.tableName(prefix + "_completion_lifecycle"));
        dynamo.waiter().waitUntilTableNotExists(request -> request.tableName(prefix + "_completion_lifecycle"));
    }

    @Test
    void filteredExpiredPrimaryRowsStillCarryBoundedProgressToEveryLiveAdmission() {
        Supplier<Stores> factory = stores();
        long now = System.currentTimeMillis();
        long historic = now - 120_000;
        // Use real admission transactions. TTL is deliberately not enabled on these tables,
        // so Dynamo's asynchronous TTL deletion cannot hide evaluated expired OPEN rows.
        for (int item = 0; item < 3; item++) {
            var expired = imported(factory.get(), "tenant", "parent", "expired-" + item, "unit", item,
                AwaitInteractionStatus.WAITING, historic, now / 1000 - 1);
            await(factory.get().interactions().complete(completion(expired, historic + 1)));
        }
        Set<String> expected = Set.of("live-one", "live-two");
        for (String id : expected) {
            var live = imported(factory.get(), "tenant", "parent", id, "unit", 0,
                AwaitInteractionStatus.WAITING, now, 0);
            await(factory.get().interactions().complete(completion(live, now + 1)));
        }
        Optional<AwaitCompletionCursor> cursor = Optional.empty();
        Set<AwaitCompletionCursor> positions = new HashSet<>();
        List<String> actual = new ArrayList<>();
        boolean emptyProgressObserved = false;
        int evaluated = 0;
        do {
            var page = await(factory.get().interactions().findOpenCompletions("tenant", 1, cursor));
            assertTrue(page.evaluatedEntryCount() <= 1);
            evaluated += page.evaluatedEntryCount();
            actual.addAll(page.admissions().stream().map(fact -> fact.interactionId()).toList());
            emptyProgressObserved |= page.admissions().isEmpty() && page.nextCursor().isPresent();
            cursor = page.nextCursor();
            cursor.ifPresent(next -> assertTrue(positions.add(next), "filtered-page cursor must progress"));
            assertTrue(positions.size() <= 5);
        } while (cursor.isPresent());
        assertEquals(5, evaluated, "evaluate each unchanged native OPEN row once, including expired rows");
        assertTrue(emptyProgressObserved, "expiry filtering cannot discard a continuation");
        assertEquals(2, actual.size());
        assertEquals(expected, new HashSet<>(actual));
    }

    private void table(String name, String hash, Optional<String> range) {
        var attributes = new ArrayList<AttributeDefinition>();
        attributes.add(attribute(hash));
        var keys = new ArrayList<KeySchemaElement>();
        keys.add(key(hash, KeyType.HASH));
        range.ifPresent(value -> {
            attributes.add(attribute(value));
            keys.add(key(value, KeyType.RANGE));
        });
        dynamo.createTable(CreateTableRequest.builder().tableName(name).attributeDefinitions(attributes)
            .keySchema(keys).billingMode(BillingMode.PAY_PER_REQUEST).build());
        dynamo.waiter().waitUntilTableExists(value -> value.tableName(name));
    }

    private AttributeDefinition attribute(String name) {
        return AttributeDefinition.builder().attributeName(name).attributeType(ScalarAttributeType.S).build();
    }

    private KeySchemaElement key(String name, KeyType type) {
        return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }

    @AfterAll
    void close() {
        dynamo.close();
    }
}

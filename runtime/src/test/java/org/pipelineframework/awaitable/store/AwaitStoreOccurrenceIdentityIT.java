package org.pipelineframework.awaitable.store;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/** Exact ownership controls over actual conditional Dynamo writes and transactions, without AWS. */
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwaitStoreOccurrenceIdentityIT {
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8")).withServices("dynamodb");
    private DynamoDbClient dynamo;

    @BeforeAll
    void connect() {
        dynamo = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
            .region(Region.of(LOCALSTACK.getRegion())).credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))).build();
    }

    @AfterAll
    void close() {
        dynamo.close();
    }

    private record Fixture(String prefix, PipelineOrchestratorConfig config) {}

    private Fixture fixture() {
        String prefix = "await_identity_" + UUID.randomUUID().toString().replace("-", "");
        table(prefix + "_unit", "tenant_id", "unit_id");
        table(prefix + "_interaction", "tenant_id", "interaction_id");
        table(prefix + "_interaction_key", "lookup_key", "");
        table(prefix + "_completion_lifecycle", "pk", "sk");
        var config = mock(PipelineOrchestratorConfig.class);
        var storage = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(storage);
        when(storage.awaitUnitTable()).thenReturn(prefix + "_unit");
        when(storage.awaitInteractionTable()).thenReturn(prefix + "_interaction");
        when(storage.awaitInteractionKeyTable()).thenReturn(prefix + "_interaction_key");
        when(storage.awaitCompletionLifecycleTable()).thenReturn(prefix + "_completion_lifecycle");
        return new Fixture(prefix, config);
    }

    @Test
    void qualifiedUnitPrimaryMembershipIsImmutableAcrossSealsAndRestart() {
        var fixture = fixture();
        InMemoryAwaitUnitStoreTest.assertPrimarySeal(() -> new DynamoAwaitUnitStore(dynamo, fixture.config()));
    }

    @Test
    void unitCreateAndImportRejectForeignOwnersWithoutReplacingProgress() {
        var fixture = fixture();
        var store = new DynamoAwaitUnitStore(dynamo, fixture.config());
        var original = unit("legacy-unit", "execution", "review", 0, "ONE_TO_ONE", AwaitUnitStatus.COMPLETED);
        store.importRecord(original).await().indefinitely();
        var restarted = new DynamoAwaitUnitStore(dynamo, fixture.config());
        assertEquals(original, restarted.createOrGet(new AwaitUnitCreateCommand("tenant", "legacy-unit", "execution",
            "review", 0, "ONE_TO_ONE", System.currentTimeMillis(), Long.MAX_VALUE)).await().indefinitely());
        for (var conflict : java.util.List.of(
            unit("legacy-unit", "foreign", "review", 0, "ONE_TO_ONE", AwaitUnitStatus.WAITING_EXTERNAL),
            unit("legacy-unit", "execution", "foreign", 0, "ONE_TO_ONE", AwaitUnitStatus.WAITING_EXTERNAL),
            unit("legacy-unit", "execution", "review", 1, "ONE_TO_ONE", AwaitUnitStatus.WAITING_EXTERNAL),
            unit("legacy-unit", "execution", "review", 0, "ONE_TO_MANY", AwaitUnitStatus.WAITING_EXTERNAL))) {
            assertThrows(IllegalStateException.class, () -> restarted.importRecord(conflict).await().indefinitely());
            assertThrows(IllegalStateException.class, () -> restarted.createOrGet(new AwaitUnitCreateCommand("tenant",
                conflict.unitId(), conflict.executionId(), conflict.stepId(), conflict.stepIndex(), conflict.cardinality(),
                System.currentTimeMillis(), Long.MAX_VALUE)).await().indefinitely());
            assertEquals(original, restarted.get("tenant", original.unitId()).await().indefinitely().orElseThrow());
        }
        String qualifiedId = occurrence("initial");
        var qualified = unit(qualifiedId, "execution", "review", 0, "ONE_TO_ONE", AwaitUnitStatus.COMPLETED);
        restarted.importRecord(qualified).await().indefinitely();
        assertThrows(IllegalStateException.class, () -> restarted.importRecord(new AwaitUnitRecord("tenant", qualifiedId,
            "execution", "review", 0, "ONE_TO_ONE", 0, AwaitUnitStatus.WAITING_EXTERNAL, "foreign-primary",
            null, 0, Set.of(), false, 1_000L, 1_000L, 0)).await().indefinitely());
        assertEquals(qualified, restarted.importRecord(new AwaitUnitRecord("tenant", qualifiedId, "execution", "review", 0,
            "ONE_TO_ONE", 0, AwaitUnitStatus.WAITING_EXTERNAL, null, null, 0, Set.of(), false,
            1_000L, 1_000L, 0)).await().indefinitely(), "stale import cannot erase the retained qualified primary");
    }

    @Test
    void legacyConfiguredKeyStillDeduplicatesActiveInteractionAcrossExecutions() {
        var fixture = fixture();
        var store = new DynamoAwaitInteractionStore(dynamo, fixture.config());
        long now = System.currentTimeMillis();
        var first = store.createOrGet(new AwaitCreateCommand("tenant", "execution", "review", 0,
            String.class.getName(), "cause-1", "review:business-key", "corr-execution-1", "request",
            "alice", "finance", "local", "legacy-unit-1", null, now, now + 60_000L, Long.MAX_VALUE))
            .await().indefinitely().record();
        var duplicate = new DynamoAwaitInteractionStore(dynamo, fixture.config()).createOrGet(new AwaitCreateCommand(
            "tenant", "execution-2", "review", 0, String.class.getName(), "cause-2", "review:business-key",
            "corr-execution-2", "different request", "alice", "finance", "local", "legacy-unit-2", null,
            now, now + 60_000L, Long.MAX_VALUE)).await().indefinitely();
        assertTrue(duplicate.duplicate());
        assertEquals(first, duplicate.record());
        assertTrue(store.findByCorrelation("tenant", "corr-execution-2").await().indefinitely().isEmpty());
        var pointers = dynamo.scan(ScanRequest.builder().tableName(fixture.prefix() + "_interaction_key").build()).items();
        assertEquals(2, pointers.size(), "rolled-back legacy duplicate cannot introduce another pointer");
    }

    @Test
    void noExpiryUnitCannotBeOverwrittenWhenCreateInitiallyMissesTheRetainedRow() {
        var fixture = fixture();
        String id = occurrence("initial");
        var original = unit(id, "execution", "review", 0, "ONE_TO_ONE", AwaitUnitStatus.COMPLETED);
        var originalStore = new DynamoAwaitUnitStore(dynamo, fixture.config());
        originalStore.importRecord(original).await().indefinitely();
        assertThrows(IllegalStateException.class, () -> originalStore.importRecord(
            unit(id, "execution", "review", 0, "ONE_TO_MANY", AwaitUnitStatus.WAITING_EXTERNAL)).await().indefinitely());
        // Force the race branch; the actual server, not this read substitute, decides the Put condition.
        var missedRead = mock(DynamoDbClient.class, org.mockito.AdditionalAnswers.delegatesTo(dynamo));
        doReturn(GetItemResponse.builder().build())
            .doAnswer(call -> dynamo.getItem(call.getArgument(0, GetItemRequest.class)))
            .when(missedRead).getItem(any(GetItemRequest.class));
        var store = new DynamoAwaitUnitStore(missedRead, fixture.config());
        assertEquals(original, store.createOrGet(new AwaitUnitCreateCommand("tenant", id, "execution", "review", 0,
            "ONE_TO_ONE", System.currentTimeMillis(), Long.MAX_VALUE)).await().indefinitely());
        assertEquals(original, new DynamoAwaitUnitStore(dynamo, fixture.config()).get("tenant", id).await().indefinitely().orElseThrow());
    }

    @Test
    void sameQualifiedCycleReplaysCompletedInteractionAndNewCauseCreatesFreshOne() {
        var fixture = fixture();
        var store = new DynamoAwaitInteractionStore(dynamo, fixture.config());
        var first = store.createOrGet(command("initial", "idem-initial", "corr-initial")).await().indefinitely().record();
        var completed = store.complete(new AwaitCompletionCommand("tenant", first.interactionId(), null,
            "completion", "approved", "alice", System.currentTimeMillis())).await().indefinitely().record();
        var restarted = new DynamoAwaitInteractionStore(dynamo, fixture.config());
        var replay = restarted.createOrGet(command("initial", "idem-initial", "corr-initial")).await().indefinitely();
        assertTrue(replay.duplicate());
        assertEquals(completed, replay.record());
        assertEquals(completed, restarted.importRecord(first).await().indefinitely(), "existing mutable progress wins over the stale import");
        var fresh = restarted.createOrGet(command("reissue", "idem-reissue", "corr-reissue")).await().indefinitely();
        assertFalse(fresh.duplicate());
        assertNotEquals(first.interactionId(), fresh.record().interactionId());
        assertNotEquals(first.unitId(), fresh.record().unitId());
        assertEquals(completed, restarted.get("tenant", first.interactionId()).await().indefinitely().orElseThrow());
        assertThrows(IllegalStateException.class, () -> restarted.createOrGet(
            command("reissue", "idem-initial", "corr-other")).await().indefinitely());
    }

    @Test
    void qualifiedImportRejectsDuplicateIdsAndPointersAtomically() {
        var fixture = fixture();
        var store = new DynamoAwaitInteractionStore(dynamo, fixture.config());
        var original = store.createOrGet(command("initial", "idem-initial", "corr-initial")).await().indefinitely().record();
        for (var conflict : java.util.List.of(
            interaction(original, original.interactionId(), occurrence("reissue"), 0, original.idempotencyKey(), original.correlationId()),
            interaction(original, original.interactionId(), original.unitId(), 1, original.idempotencyKey(), original.correlationId()),
            interaction(original, original.interactionId(), original.unitId(), 0, "foreign-idem", original.correlationId()),
            interaction(original, original.interactionId(), original.unitId(), 0, original.idempotencyKey(), "foreign-corr"),
            interaction(original, "foreign", original.unitId(), 0, original.idempotencyKey(), "foreign-corr"),
            interaction(original, "foreign", original.unitId(), 0, "foreign-idem", original.correlationId()))) {
            assertThrows(IllegalStateException.class, () -> store.importRecord(conflict).await().indefinitely());
            assertEquals(original, store.get("tenant", original.interactionId()).await().indefinitely().orElseThrow());
            assertEquals(original, store.findByCorrelation("tenant", original.correlationId()).await().indefinitely().orElseThrow());
            assertTrue(store.get("tenant", "foreign").await().indefinitely().isEmpty());
        }
        var retry = store.createOrGet(command("initial", "idem-initial", "corr-initial")).await().indefinitely();
        assertEquals(original, retry.record());
        assertTrue(retry.duplicate());
        var rows = dynamo.scan(ScanRequest.builder().tableName(fixture.prefix() + "_interaction_key").build()).items();
        assertEquals(2, rows.size(), "failed imports must not leave a new correlation or idempotency pointer");
        assertTrue(rows.stream().allMatch(row -> original.interactionId().equals(row.get("interaction_id").s())));
    }

    private String occurrence(String cause) {
        return new AwaitUnitOccurrence("tenant", "execution", "page", "review", 0, cause).unitId();
    }

    private AwaitCreateCommand command(String cause, String idem, String correlation) {
        long now = System.currentTimeMillis();
        return new AwaitCreateCommand("tenant", "execution", "review", 0, String.class.getName(), "causation", idem,
            correlation, "request", "alice", "finance", "local", occurrence(cause), 0, now, now + 60_000L, Long.MAX_VALUE);
    }

    private AwaitUnitRecord unit(String id, String execution, String step, int index, String cardinality, AwaitUnitStatus status) {
        boolean completed = status == AwaitUnitStatus.COMPLETED;
        return new AwaitUnitRecord("tenant", id, execution, step, index, cardinality, 7, status, "original-primary",
            completed ? 1 : null, completed ? 1 : 0, completed ? Set.of("item:0") : Set.of(), completed, 1_000L, 2_000L, 0);
    }

    private AwaitInteractionRecord interaction(AwaitInteractionRecord original, String id, String unit, Integer index, String idem, String correlation) {
        return new AwaitInteractionRecord(original.tenantId(), original.executionId(), original.stepId(), original.stepIndex(),
            original.outputType(), id, correlation, original.causationId(), idem, original.version(), original.status(),
            original.requestPayload(), original.responsePayload(), unit, index, original.actor(), original.assignee(), original.group(),
            original.transportType(), original.transportMetadata(), original.deadlineEpochMs(), original.createdAtEpochMs(),
            original.updatedAtEpochMs(), original.ttlEpochS(), original.transportOutputType());
    }

    private void table(String name, String partition, String sort) {
        var attributes = new java.util.ArrayList<AttributeDefinition>();
        var keys = new java.util.ArrayList<KeySchemaElement>();
        attributes.add(AttributeDefinition.builder().attributeName(partition).attributeType(ScalarAttributeType.S).build());
        keys.add(KeySchemaElement.builder().attributeName(partition).keyType(KeyType.HASH).build());
        if (!sort.isEmpty()) {
            attributes.add(AttributeDefinition.builder().attributeName(sort).attributeType(ScalarAttributeType.S).build());
            keys.add(KeySchemaElement.builder().attributeName(sort).keyType(KeyType.RANGE).build());
        }
        dynamo.createTable(CreateTableRequest.builder().tableName(name).attributeDefinitions(attributes).keySchema(keys)
            .billingMode(BillingMode.PAY_PER_REQUEST).build());
        dynamo.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(name).build());
    }
}

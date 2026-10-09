package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
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

@Testcontainers(disabledWithoutDocker = true)
class DynamoCurrentActivationObservationIT {
    @Container static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
        .withServices("dynamodb");
    private DynamoDbClient client;
    private PipelineOrchestratorConfig config;
    private String table;

    @BeforeEach void setup() {
        client = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint()).region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))).build();
        table = "current-event-" + UUID.randomUUID();
        client.createTable(CreateTableRequest.builder().tableName(table)
            .attributeDefinitions(AttributeDefinition.builder().attributeName("registry_key").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("registry_sort").attributeType(ScalarAttributeType.S).build())
            .keySchema(KeySchemaElement.builder().attributeName("registry_key").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("registry_sort").keyType(KeyType.RANGE).build())
            .billingMode(BillingMode.PAY_PER_REQUEST).build());
        client.waiter().waitUntilTableExists(request -> request.tableName(table));
        config = mock(PipelineOrchestratorConfig.class);
        var dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.releaseTable()).thenReturn(table);
    }

    @AfterEach void close() { client.close(); }

    @Test void sameReleaseTieRegressingLegacyWriteAndHistoricalReplayHaveDistinctCurrentIdentity() {
        var release = CurrentActivationObservationTest.release(PipelineReleaseStatus.REGISTERED);
        fresh().register(release).await().indefinitely();
        var a = fresh().activateOnce(new ActivationOperationCommand("a", release, 3000)).await().indefinitely();
        var b = fresh().activateOnce(new ActivationOperationCommand("b", release, 3000)).await().indefinitely();
        assertNotEquals(a.activationId(), b.activationId());
        var before = rows();
        var current = fresh().currentActivationObservation("tenant", "pipeline").await().indefinitely();
        assertEquals(b.activationId(), current.identifiedEvent().orElseThrow().activationId());
        assertEquals(b.immutableReleaseIdentity(), current.identifiedEvent().orElseThrow().immutableReleaseIdentity());
        assertEquals(before, rows());
        assertEquals(a, fresh().activateOnce(new ActivationOperationCommand("a", release, 9000)).await().indefinitely());
        assertEquals(current, fresh().currentActivationObservation("tenant", "pipeline").await().indefinitely());
        assertEquals(before, rows());
        fresh().activate("tenant", "pipeline", "release", 2000).await().indefinitely();
        before = rows();
        var latest = fresh().currentActivationObservation("tenant", "pipeline").await().indefinitely().identifiedEvent().orElseThrow();
        assertNotEquals(b.activationId(), latest.activationId());
        assertEquals(2000, latest.activatedAtEpochMs());
        assertEquals(head().get("activation_id").s(), latest.activationId());
        assertEquals(before, rows());
        assertEquals(CurrentActivationObservation.State.NONE, fresh().currentActivationObservation("other", "pipeline").await().indefinitely().state());
        assertEquals(CurrentActivationObservation.State.NONE, fresh().currentActivationObservation("tenant", "other").await().indefinitely().state());
        assertEquals(before, rows());
    }

    @Test void legacyUnknownAndNoneAreReadOnlyWithoutInventedIds() {
        var before = rows();
        assertEquals(CurrentActivationObservation.State.NONE, fresh().currentActivationObservation("tenant", "pipeline").await().indefinitely().state());
        assertEquals(before, rows());
        client.putItem(request -> request.tableName(table).item(Map.of("registry_key", av("tenant#pipeline"),
            "registry_sort", av("activation:0000000000000003000:release"), "record_type", av("ACTIVATION"),
            "tenant_id", av("tenant"), "pipeline_id", av("pipeline"), "release_version", av("release"),
            "activated_at_epoch_ms", AttributeValue.builder().n("3000").build())));
        before = rows();
        var current = fresh().currentActivationObservation("tenant", "pipeline").await().indefinitely();
        assertEquals(CurrentActivationObservation.State.LEGACY_UNKNOWN, current.state());
        assertTrue(current.identifiedEvent().isEmpty());
        assertTrue(fresh().getActivationOperation("tenant", "pipeline", "unknown").await().indefinitely().isEmpty());
        assertEquals(before, rows());
    }

    @Test void missingHeadOrImmutableEventFailsClosedWithoutChangingHistoricalReceipt() {
        var release = CurrentActivationObservationTest.release(PipelineReleaseStatus.REGISTERED);
        fresh().register(release).await().indefinitely();
        var receipt = fresh().activateOnce(new ActivationOperationCommand("key", release, 3000)).await().indefinitely();
        var savedHead = head();
        client.deleteItem(request -> request.tableName(table).key(key("activation-head:v1")));
        var before = rows();
        assertThrows(IllegalStateException.class, () -> fresh().currentActivationObservation("tenant", "pipeline").await().indefinitely());
        assertEquals(receipt, fresh().getActivationOperation("tenant", "pipeline", "key").await().indefinitely().orElseThrow());
        assertEquals(before, rows());
        client.putItem(request -> request.tableName(table).item(savedHead));
        client.updateItem(request -> request.tableName(table).key(key("activation-head:v1"))
            .updateExpression("SET activation_id = :bad").expressionAttributeValues(Map.of(":bad", av("wrong-event"))));
        before = rows();
        assertThrows(IllegalStateException.class, () -> fresh().currentActivationObservation("tenant", "pipeline").await().indefinitely());
        assertEquals(before, rows());
        client.putItem(request -> request.tableName(table).item(savedHead));
        var savedEvent = client.getItem(request -> request.tableName(table).key(key("activation-v2:0000000000000000001")).consistentRead(true)).item();
        client.deleteItem(request -> request.tableName(table).key(key("activation-v2:0000000000000000001")));
        before = rows();
        assertThrows(IllegalStateException.class, () -> fresh().currentActivationObservation("tenant", "pipeline").await().indefinitely());
        assertEquals(before, rows());
        client.putItem(request -> request.tableName(table).item(savedEvent));
        client.deleteItem(request -> request.tableName(table).key(key("release:release")));
        before = rows();
        assertThrows(IllegalStateException.class, () -> fresh().currentActivationObservation("tenant", "pipeline").await().indefinitely());
        assertEquals(before, rows());
    }

    @Test void concurrentWritersCurrentIdentityEqualsAuthoritativeHeadNotAnyHistoricalPin() throws InterruptedException {
        var release = CurrentActivationObservationTest.release(PipelineReleaseStatus.REGISTERED);
        fresh().register(release).await().indefinitely();
        var context = Thread.currentThread().getContextClassLoader();
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(4, task -> {
            var thread = new Thread(task, "current-event-concurrency"); thread.setContextClassLoader(context); return thread;
        })) {
            var ready = new java.util.concurrent.CountDownLatch(4);
            var start = new java.util.concurrent.CountDownLatch(1);
            var calls = java.util.stream.IntStream.range(0, 4).mapToObj(index -> java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                ready.countDown();
                try { assertTrue(start.await(20, java.util.concurrent.TimeUnit.SECONDS)); }
                catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IllegalStateException(error); }
                return fresh().activateOnce(new ActivationOperationCommand("key-" + index, release, 3000)).await().indefinitely();
            }, executor)).toList();
            try { assertTrue(ready.await(20, java.util.concurrent.TimeUnit.SECONDS)); } finally { start.countDown(); }
            var receipts = calls.stream().map(java.util.concurrent.CompletableFuture::join).toList();
            assertEquals(4, receipts.stream().map(ActivationOperationReceipt::activationId).distinct().count());
            var before = rows();
            var current = fresh().currentActivationObservation("tenant", "pipeline").await().indefinitely().identifiedEvent().orElseThrow();
            assertEquals(head().get("activation_id").s(), current.activationId());
            assertTrue(receipts.stream().anyMatch(receipt -> receipt.activationId().equals(current.activationId())));
            assertEquals(before, rows());
        }
    }

    private DynamoPipelineReleaseRegistry fresh() { return new DynamoPipelineReleaseRegistry(client, config); }
    private Set<Map<String, AttributeValue>> rows() { return Set.copyOf(client.scan(request -> request.tableName(table).consistentRead(true)).items()); }
    private Map<String, AttributeValue> head() { return client.getItem(request -> request.tableName(table).key(key("activation-head:v1")).consistentRead(true)).item(); }
    private static Map<String, AttributeValue> key(String sort) { return Map.of("registry_key", av("tenant#pipeline"), "registry_sort", av(sort)); }
    private static AttributeValue av(String value) { return AttributeValue.builder().s(value).build(); }
}

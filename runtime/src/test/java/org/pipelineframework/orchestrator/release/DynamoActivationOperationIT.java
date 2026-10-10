package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationTargetException;
import org.junit.jupiter.api.BeforeEach;
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
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;

@Testcontainers(disabledWithoutDocker = true)
class DynamoActivationOperationIT {
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8")).withServices("dynamodb");
    private DynamoDbClient client;
    private PipelineOrchestratorConfig config;
    private String table;

    @BeforeEach
    void createTable() {
        client = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))).build();
        table = "activation-operation-" + UUID.randomUUID();
        client.createTable(CreateTableRequest.builder().tableName(table)
            .attributeDefinitions(
                AttributeDefinition.builder().attributeName("registry_key").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("registry_sort").attributeType(ScalarAttributeType.S).build())
            .keySchema(KeySchemaElement.builder().attributeName("registry_key").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("registry_sort").keyType(KeyType.RANGE).build())
            .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
            .build());
        client.waiter().waitUntilTableExists(request -> request.tableName(table));
        config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.releaseTable()).thenReturn(table);
    }

    @org.junit.jupiter.api.AfterEach
    void closeClient() {
        client.close();
    }

    @Test
    void commonLegacyWriterOrderRetainsActualTimestampDespiteClockRegression() {
        DynamoPipelineReleaseRegistry registry = fresh();
        registry.register(release("release-a")).await().atMost(Duration.ofSeconds(5));
        registry.register(release("release-b")).await().atMost(Duration.ofSeconds(5));
        registry.activate("tenant", "pipeline", "release-a", 3000).await().atMost(Duration.ofSeconds(5));
        registry.activate("tenant", "pipeline", "release-b", 2000).await().atMost(Duration.ofSeconds(5));
        PipelineReleaseRecord active = fresh().active("tenant", "pipeline")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow();
        assertEquals("release-b", active.releaseVersion());
        assertEquals(2000, active.activatedAtEpochMs());
    }

    @Test
    void clockTieIsOrderedByCommonFenceNotReleaseLexicalOrder() {
        fresh().register(release("release-z")).await().indefinitely();
        fresh().register(release("release-a")).await().indefinitely();
        fresh().activate("tenant", "pipeline", "release-z", 3000).await().indefinitely();
        fresh().activate("tenant", "pipeline", "release-a", 3000).await().indefinitely();
        assertEquals("release-a", fresh().active("tenant", "pipeline").await().indefinitely().orElseThrow().releaseVersion());
        assertEquals(2, events());
    }

    @Test
    void lostAcknowledgementThenBAndHistoricalAInquiryNeverReactivates() {
        PipelineReleaseRecord a = release("release-a");
        PipelineReleaseRecord b = release("release-b");
        fresh().register(a).await().indefinitely();
        fresh().register(b).await().indefinitely();
        DynamoDbClient lostAck = boundary(request -> {
            client.transactWriteItems(request);
            throw new IllegalStateException("acknowledgement lost after native Dynamo commit");
        });
        assertThrows(IllegalStateException.class, () -> new DynamoPipelineReleaseRegistry(lostAck, config)
            .activateOnce(new ActivationOperationCommand("operation-a", a, 3000)).await().indefinitely());
        ActivationOperationReceipt original = fresh().getActivationOperation("tenant", "pipeline", "operation-a")
            .await().indefinitely().orElseThrow();
        assertEquals(1, events());
        assertEquals("release-a", fresh().active("tenant", "pipeline").await().indefinitely().orElseThrow().releaseVersion());
        fresh().activateOnce(new ActivationOperationCommand("operation-b", b, 2000)).await().indefinitely();
        var completeRows = rows();
        assertEquals(original, fresh().getActivationOperation("tenant", "pipeline", "operation-a").await().indefinitely().orElseThrow());
        assertEquals(original, fresh().activateOnce(new ActivationOperationCommand("operation-a", a, 9000)).await().indefinitely());
        assertEquals(completeRows, rows());
        assertEquals(2, events());
        assertEquals("release-b", fresh().active("tenant", "pipeline").await().indefinitely().orElseThrow().releaseVersion());
        assertEquals(2000, fresh().active("tenant", "pipeline").await().indefinitely().orElseThrow().activatedAtEpochMs());
        assertThrows(IllegalStateException.class, () -> fresh().activateOnce(new ActivationOperationCommand("operation-a", b, 9000))
            .await().indefinitely());
        assertEquals(completeRows, rows());
    }

    @Test
    void sameKeyConcurrentWritersConvergeOnOneImmutableReceiptAndEvent() throws InterruptedException {
        PipelineReleaseRecord a = release("release-a");
        fresh().register(a).await().indefinitely();
        try (var executor = ownedExecutor(8)) {
            var ready = new java.util.concurrent.CountDownLatch(8);
            var start = new java.util.concurrent.CountDownLatch(1);
            var calls = java.util.stream.IntStream.range(0, 8).mapToObj(index -> CompletableFuture.supplyAsync(() -> {
                ready.countDown();
                try { assertTrue(start.await(20, java.util.concurrent.TimeUnit.SECONDS)); }
                catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IllegalStateException(error); }
                return fresh().activateOnce(new ActivationOperationCommand("same-key", a, 3000 + index)).await().indefinitely();
            }, executor)).toList();
            try { assertTrue(ready.await(20, java.util.concurrent.TimeUnit.SECONDS)); }
            finally { start.countDown(); }
            ActivationOperationReceipt winner = calls.get(0).join();
            calls.forEach(call -> assertEquals(winner, call.join()));
            assertEquals(1, events());
            var completeRows = rows();
            assertEquals(winner, fresh().getActivationOperation("tenant", "pipeline", "same-key").await().indefinitely().orElseThrow());
            assertEquals(completeRows, rows());
        }
    }

    @Test
    void actualTransactionCancellationLeavesNeitherEventHeadNorReceipt() {
        PipelineReleaseRecord a = release("release-a");
        fresh().register(a).await().indefinitely();
        var originalRows = rows();
        DynamoDbClient rejected = boundary(request -> {
            var items = new java.util.ArrayList<>(request.transactItems());
            var check = items.get(0).conditionCheck().toBuilder()
                .conditionExpression("attribute_not_exists(#pk)")
                .expressionAttributeNames(Map.of("#pk", "registry_key"))
                .expressionAttributeValues((Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue>) null).build();
            items.set(0, software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder().conditionCheck(check).build());
            return client.transactWriteItems(request.toBuilder().transactItems(items).build());
        });
        assertThrows(software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException.class, () ->
            new DynamoPipelineReleaseRegistry(rejected, config).activateOnce(new ActivationOperationCommand("rejected", a, 3000))
                .await().indefinitely());
        assertEquals(originalRows, rows());
        assertTrue(fresh().getActivationOperation("tenant", "pipeline", "rejected").await().indefinitely().isEmpty());
        assertTrue(fresh().active("tenant", "pipeline").await().indefinitely().isEmpty());
        assertEquals(originalRows, rows());
    }

    @Test
    void unknownCrossTenantAndPipelineInquiryIsReadOnly() {
        PipelineReleaseRecord a = release("release-a");
        fresh().register(a).await().indefinitely();
        fresh().activateOnce(new ActivationOperationCommand("key", a, 3000)).await().indefinitely();
        var originalRows = rows();
        assertTrue(fresh().getActivationOperation("other", "pipeline", "key").await().indefinitely().isEmpty());
        assertTrue(fresh().getActivationOperation("tenant", "other", "key").await().indefinitely().isEmpty());
        assertTrue(fresh().getActivationOperation("tenant", "pipeline", "unknown").await().indefinitely().isEmpty());
        assertEquals(originalRows, rows());
    }

    @Test
    void legacyHistoryInitializesTheNewWriterWithoutRewritingHistoryOrInventingReceipt() {
        fresh().register(release("release-a")).await().indefinitely();
        fresh().register(release("release-b")).await().indefinitely();
        var legacy = Map.of("registry_key", av("tenant#pipeline"), "registry_sort", av("activation:0000000000000003000:release-a"),
            "record_type", av("ACTIVATION"), "tenant_id", av("tenant"), "pipeline_id", av("pipeline"),
            "release_version", av("release-a"), "activated_at_epoch_ms", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n("3000").build());
        client.putItem(request -> request.tableName(table).item(legacy));
        assertEquals("release-a", fresh().active("tenant", "pipeline").await().indefinitely().orElseThrow().releaseVersion());
        assertTrue(fresh().getActivationOperation("tenant", "pipeline", "legacy").await().indefinitely().isEmpty());
        fresh().activate("tenant", "pipeline", "release-b", 2000).await().indefinitely();
        assertEquals("release-b", fresh().active("tenant", "pipeline").await().indefinitely().orElseThrow().releaseVersion());
        assertTrue(rows().contains(legacy));
        assertEquals(2, events());
        assertEquals(0, rows().stream().filter(row -> "ACTIVATION_OPERATION_V1".equals(row.get("record_type").s())).count());
    }

    @Test
    void unknownHeadOrderingFailsClosedBeforeAnyNewEffect() {
        PipelineReleaseRecord a = release("release-a");
        fresh().register(a).await().indefinitely();
        fresh().activate("tenant", "pipeline", "release-a", 3000).await().indefinitely();
        var head = new java.util.HashMap<>(rows().stream().filter(row -> row.get("registry_sort").s().equals("activation-head:v1"))
            .findFirst().orElseThrow());
        head.put("activation_sequence", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n("99").build());
        client.putItem(request -> request.tableName(table).item(head));
        var corruptRows = rows();
        assertThrows(IllegalStateException.class, () -> fresh().active("tenant", "pipeline").await().indefinitely());
        assertThrows(IllegalStateException.class, () -> fresh().activateOnce(new ActivationOperationCommand("new", a, 4000)).await().indefinitely());
        assertEquals(corruptRows, rows());
        assertTrue(fresh().getActivationOperation("tenant", "pipeline", "new").await().indefinitely().isEmpty());
        assertEquals(corruptRows, rows());
    }

    @Test
    void differentKeysConcurrentWithLegacyWriterAllShareOneOrderedHead() {
        try (var executor = ownedExecutor(5)) {
            PipelineReleaseRecord a = release("release-a");
            fresh().register(a).await().indefinitely();
            var calls = java.util.stream.IntStream.range(0, 5).mapToObj(index -> CompletableFuture.runAsync(() -> {
                if (index == 0) {
                    fresh().activate("tenant", "pipeline", "release-a", 3000).await().indefinitely();
                } else {
                    fresh().activateOnce(new ActivationOperationCommand("key-" + index, a, 3000 - index)).await().indefinitely();
                }
            }, executor)).toList();
            calls.forEach(CompletableFuture::join);
            assertEquals(5, events());
            for (int index = 1; index < 5; index++) {
                assertEquals(3000 - index, fresh().getActivationOperation("tenant", "pipeline", "key-" + index)
                    .await().indefinitely().orElseThrow().activatedAtEpochMs());
            }
            var head = rows().stream().filter(row -> row.get("registry_sort").s().equals("activation-head:v1")).findFirst().orElseThrow();
            assertEquals("5", head.get("activation_sequence").n());
            var originalRows = rows();
            assertEquals(head.get("activated_at_epoch_ms").n(), Long.toString(fresh().active("tenant", "pipeline")
                .await().indefinitely().orElseThrow().activatedAtEpochMs()));
            assertEquals(originalRows, rows());
        }
    }

    @Test
    void missingHeadWithRetainedNewHistoryFailsClosedRatherThanReturningLegacyActive() {
        PipelineReleaseRecord a = release("release-a");
        PipelineReleaseRecord b = release("release-b");
        fresh().register(a).await().indefinitely();
        fresh().register(b).await().indefinitely();
        var legacy = Map.of("registry_key", av("tenant#pipeline"), "registry_sort", av("activation:0000000000000003000:release-a"),
            "record_type", av("ACTIVATION"), "tenant_id", av("tenant"), "pipeline_id", av("pipeline"),
            "release_version", av("release-a"), "activated_at_epoch_ms", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n("3000").build());
        client.putItem(request -> request.tableName(table).item(legacy));
        ActivationOperationReceipt original = fresh().activateOnce(new ActivationOperationCommand("original-b", b, 2000))
            .await().indefinitely();
        assertEquals("release-b", fresh().active("tenant", "pipeline").await().indefinitely().orElseThrow().releaseVersion());
        client.deleteItem(request -> request.tableName(table).key(Map.of("registry_key", av("tenant#pipeline"),
            "registry_sort", av("activation-head:v1"))));
        var remainingRows = rows();
        assertTrue(remainingRows.contains(legacy));
        assertEquals(2, events());
        assertEquals(original, fresh().getActivationOperation("tenant", "pipeline", "original-b").await().indefinitely().orElseThrow());
        assertAll(
            () -> assertThrows(IllegalStateException.class, () -> fresh().active("tenant", "pipeline").await().indefinitely()),
            () -> assertThrows(IllegalStateException.class, () -> fresh().activateOnce(new ActivationOperationCommand("new-a", a, 4000))
                .await().indefinitely()),
            () -> assertEquals(remainingRows, rows()),
            () -> assertTrue(fresh().getActivationOperation("tenant", "pipeline", "new-a").await().indefinitely().isEmpty()),
            () -> assertEquals(original, fresh().getActivationOperation("tenant", "pipeline", "original-b").await().indefinitely().orElseThrow()),
            () -> assertEquals(remainingRows, rows()));
    }

    @Test
    void orderedNamespaceEvidenceAlonePreventsAbsentHeadBootstrapEvenWithUnknownRecordType() {
        PipelineReleaseRecord a = release("release-a");
        fresh().register(a).await().indefinitely();
        ActivationOperationReceipt original = fresh().activateOnce(new ActivationOperationCommand("original", a, 3000))
            .await().indefinitely();
        for (var row : rows()) {
            if (row.get("registry_sort").s().equals("activation-head:v1")
                || row.get("registry_sort").s().startsWith("activation-operation:v1:")) {
                deleteRow(row);
            } else if (row.get("registry_sort").s().startsWith("activation-v2:")) {
                var corrupted = new java.util.HashMap<>(row);
                corrupted.put("record_type", av("UNRECOGNIZED"));
                client.putItem(request -> request.tableName(table).item(corrupted));
            }
        }
        var remainingRows = rows();
        assertTrue(remainingRows.stream().anyMatch(row -> row.get("registry_sort").s().startsWith("activation-v2:")));
        assertThrows(IllegalStateException.class, () -> fresh().active("tenant", "pipeline").await().indefinitely());
        assertThrows(IllegalStateException.class, () -> fresh().activateOnce(new ActivationOperationCommand("new", a, 4000)).await().indefinitely());
        assertTrue(fresh().getActivationOperation("tenant", "pipeline", original.operationKey()).await().indefinitely().isEmpty());
        assertTrue(fresh().getActivationOperation("tenant", "pipeline", "new").await().indefinitely().isEmpty());
        assertEquals(remainingRows, rows());
    }

    @Test
    void receiptNamespaceEvidenceAlonePreventsAbsentHeadBootstrapAndRemainsHistoricallyReadable() {
        PipelineReleaseRecord a = release("release-a");
        fresh().register(a).await().indefinitely();
        ActivationOperationReceipt original = fresh().activateOnce(new ActivationOperationCommand("original", a, 3000))
            .await().indefinitely();
        for (var row : rows()) {
            if (row.get("registry_sort").s().equals("activation-head:v1")
                || row.get("registry_sort").s().startsWith("activation-v2:")) {
                deleteRow(row);
            }
        }
        var remainingRows = rows();
        assertEquals(original, fresh().getActivationOperation("tenant", "pipeline", "original").await().indefinitely().orElseThrow());
        assertThrows(IllegalStateException.class, () -> fresh().active("tenant", "pipeline").await().indefinitely());
        assertThrows(IllegalStateException.class, () -> fresh().activateOnce(new ActivationOperationCommand("new", a, 4000)).await().indefinitely());
        assertTrue(fresh().getActivationOperation("tenant", "pipeline", "new").await().indefinitely().isEmpty());
        assertEquals(original, fresh().getActivationOperation("tenant", "pipeline", "original").await().indefinitely().orElseThrow());
        assertEquals(remainingRows, rows());
    }

    private void deleteRow(Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> row) {
        client.deleteItem(request -> request.tableName(table).key(Map.of("registry_key", row.get("registry_key"),
            "registry_sort", row.get("registry_sort"))));
    }

    private static software.amazon.awssdk.services.dynamodb.model.AttributeValue av(String value) {
        return software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(value).build();
    }

    private java.util.Set<Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue>> rows() {
        return java.util.Set.copyOf(client.scan(ScanRequest.builder().tableName(table).consistentRead(true).build()).items());
    }

    private long events() {
        return rows().stream().filter(row -> "ACTIVATION".equals(row.get("record_type").s())).count();
    }

    private DynamoDbClient boundary(java.util.function.Function<TransactWriteItemsRequest,
        software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse> transaction) {
        return (DynamoDbClient) Proxy.newProxyInstance(DynamoDbClient.class.getClassLoader(), new Class<?>[] { DynamoDbClient.class },
            (proxy, method, args) -> {
                if (method.getName().equals("transactWriteItems") && args != null && args.length == 1
                    && args[0] instanceof TransactWriteItemsRequest request) {
                    return transaction.apply(request);
                }
                try {
                    return method.invoke(client, args);
                } catch (InvocationTargetException failure) {
                    throw failure.getCause();
                }
            });
    }

    private static java.util.concurrent.ExecutorService ownedExecutor(int count) {
        var context = Thread.currentThread().getContextClassLoader();
        return java.util.concurrent.Executors.newFixedThreadPool(count, task -> {
            var thread = new Thread(task, "activation-concurrency");
            thread.setContextClassLoader(context);
            return thread;
        });
    }

    private DynamoPipelineReleaseRegistry fresh() {
        return new DynamoPipelineReleaseRegistry(client, config);
    }

    private static PipelineReleaseRecord release(String version) {
        PipelineReleaseDescriptor descriptor = new PipelineReleaseDescriptor(1, "pipeline", "contract", version,
            "artifact", List.of());
        return new PipelineReleaseRecord("tenant", "pipeline", "contract", version,
            PipelineReleaseStatus.REGISTERED, descriptor, "artifact", "sha256:artifact",
            "file:/artifact.jar", 1, "artifact", null, 1000, 1000, 0);
    }
}

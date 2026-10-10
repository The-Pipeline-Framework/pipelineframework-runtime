package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.release.*;
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
class DynamoExecutionAdmissionIT {
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8")).withServices("dynamodb");
    private DynamoDbClient client;
    private PipelineOrchestratorConfig config;
    private String executions;
    private String keys;
    private String payloads;

    @BeforeEach
    void tables() {
        client = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint()).region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))).build();
        String prefix = "admission-" + UUID.randomUUID();
        executions = prefix + "-execution";
        keys = prefix + "-key";
        payloads = prefix + "-payload";
        table(executions, "tenant_id", "execution_id");
        table(keys, "tenant_execution_key", null);
        table(payloads, "payload_id", "payload_part");
        config = mock(PipelineOrchestratorConfig.class);
        var dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.executionTable()).thenReturn(executions);
        when(dynamo.executionKeyTable()).thenReturn(keys);
        when(dynamo.executionPayloadTable()).thenReturn(payloads);
    }

    @AfterEach
    void close() { client.close(); }

    @Test
    void committedLostAckInquiryAndReplayRemainReadOnlyAfterExecutionExpiry() {
        var command = command(intent("key", "release-a", "original"), 1L);
        var cut = boundary(request -> {
            client.transactWriteItems(request);
            throw new IllegalStateException("lost commit acknowledgement");
        });
        assertThrows(IllegalStateException.class, () -> new DynamoExecutionStateStore(cut, config)
            .createOrGetNativeAdmittedExecution(command, evidence("release-a")).await().indefinitely());
        var receipt = fresh().lookupAdmission("tenant", "pipeline", "key").await().indefinitely().orElseThrow();
        var committed = rows();
        assertEquals(1, scan(executions).size());
        assertEquals(2, scan(keys).size());
        assertEquals(receipt, fresh().inspectExistingAdmission(command.intent()).await().indefinitely().orElseThrow().receipt());
        assertEquals(committed, rows());
        assertTrue(fresh().getExecution("tenant", receipt.executionId()).await().indefinitely().isEmpty());
        assertTrue(scan(executions).isEmpty());
        assertEquals(1, scan(keys).size());
        var retained = rows();
        var replay = fresh().createOrGetNativeAdmittedExecution(command, evidence("release-a")).await().indefinitely();
        assertFalse(replay.newlyCreated());
        assertTrue(replay.creation().isEmpty());
        assertEquals(receipt, replay.receipt());
        assertEquals(retained, rows());
        assertTrue(fresh().lookupAdmission("other-tenant", "pipeline", "key").await().indefinitely().isEmpty());
        assertTrue(fresh().lookupAdmission("tenant", "other-pipeline", "key").await().indefinitely().isEmpty());
        assertTrue(fresh().lookupAdmission("tenant", "pipeline", "unknown").await().indefinitely().isEmpty());
        assertEquals(retained, rows());
    }

    @Test
    void exactRetainedBytesShapeTypeEncodingStreamingAndPinConflictAfterExpiry() {
        var original = intent("key", "release-a", "original");
        var receipt = fresh().createOrGetNativeAdmittedExecution(command(original, 1L), evidence("release-a"))
            .await().indefinitely().receipt();
        assertTrue(fresh().getExecution("tenant", receipt.executionId()).await().indefinitely().isEmpty());
        var retained = rows();
        var variants = List.of(intent("key", "release-a", "changed"), intent("key", "release-b", "original"),
            changed(original, "MULTI", "java.lang.String", "json", false),
            changed(original, "UNI", "java.util.Map", "json", false),
            changed(original, "UNI", "java.lang.String", "other", false),
            changed(original, "UNI", "java.lang.String", "json", true));
        for (var variant : variants) {
            assertThrows(IllegalStateException.class, () -> fresh().inspectExistingAdmission(variant).await().indefinitely());
            assertEquals(retained, rows());
        }
        var changedContract = new ExecutionAdmissionIntent(1, original.tenantId(), original.pipelineId(), original.clientKey(),
            "other-contract", original.releaseVersion(), original.inputShape(), original.payloadTypeId(), original.payloadEncoding(),
            original.inputBytes(), original.outputStreaming());
        assertThrows(IllegalStateException.class, () -> fresh().inspectExistingAdmission(changedContract).await().indefinitely());
        assertEquals(retained, rows());
        var validDifferentEvidence = ExecutionAdmissionReleaseEvidence.snapshot(new PipelineReleaseRecord("tenant", "pipeline", "contract", "release-a",
            PipelineReleaseStatus.REGISTERED, new PipelineReleaseDescriptor(1, "pipeline", "contract", "release-a", "artifact", List.of()),
            "artifact", "sha256:different", "file:/different.jar", 2, "different", null, 1, 1, 0));
        assertNotEquals(evidence("release-a"), validDifferentEvidence);
        var originalCommand = command(original, ttl());
        var differentSnapshotCommand = new ExecutionAdmissionCreateCommand(originalCommand.execution(), original,
            validDifferentEvidence.metadataFingerprint(), validDifferentEvidence.primaryArtifactId(), validDifferentEvidence.primaryArtifactDigest());
        assertThrows(IllegalStateException.class, () -> fresh().createOrGetNativeAdmittedExecution(differentSnapshotCommand,
            validDifferentEvidence).await().indefinitely());
        assertEquals(retained, rows());
        var otherEvidence = evidence("release-a");
        assertThrows(IllegalArgumentException.class, () -> new PipelineReleaseEvidence(otherEvidence.descriptor(), otherEvidence.contract(),
            otherEvidence.primaryArtifactId(), otherEvidence.primaryArtifactDigest(), "file:/changed.jar", 1,
            otherEvidence.primaryArtifactChecksum(), otherEvidence.metadataFingerprint()));
        assertEquals(retained, rows());
    }

    @Test
    void sameKeyRaceCommitsOneExecutionAndConflictingRaceCannotAdoptIt() throws InterruptedException {
        var command = command(intent("key", "release-a", "original"), ttl());
        try (var executor = ownedExecutor(8)) {
            var ready = new java.util.concurrent.CountDownLatch(8);
            var start = new java.util.concurrent.CountDownLatch(1);
            var calls = java.util.stream.IntStream.range(0, 8).mapToObj(index -> CompletableFuture.supplyAsync(() -> {
                ready.countDown();
                try { assertTrue(start.await(20, java.util.concurrent.TimeUnit.SECONDS)); }
                catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IllegalStateException(error); }
                return fresh().createOrGetNativeAdmittedExecution(command, evidence("release-a")).await().indefinitely();
            }, executor)).toList();
            try { assertTrue(ready.await(20, java.util.concurrent.TimeUnit.SECONDS)); }
            finally { start.countDown(); }
            var outcomes = calls.stream().map(CompletableFuture::join).toList();
            assertEquals(1, outcomes.stream().filter(ExecutionAdmissionResult::newlyCreated).count());
            var receipt = outcomes.getFirst().receipt();
            outcomes.forEach(result -> assertEquals(receipt, result.receipt()));
            assertEquals(1, scan(executions).size());
            assertEquals(2, scan(keys).size());
            var retained = rows();
            assertThrows(IllegalStateException.class, () -> fresh().createOrGetNativeAdmittedExecution(
                command(intent("key", "release-b", "changed"), ttl()), evidence("release-b")).await().indefinitely());
            assertEquals(retained, rows());
        }
    }

    @Test
    void conflictingConcurrentFirstWritersHaveOneWinnerAndNeverAdoptDifferentBytes() {
        try (var executor = ownedExecutor(2)) {
            var start = new java.util.concurrent.CountDownLatch(1);
            var inputs = List.of(intent("conflict-race", "release-a", "first"), intent("conflict-race", "release-a", "second"));
            var calls = inputs.stream().map(input -> CompletableFuture.supplyAsync(() -> {
                try {
                    assertTrue(start.await(20, java.util.concurrent.TimeUnit.SECONDS));
                    return fresh().createOrGetNativeAdmittedExecution(command(input, ttl()), evidence("release-a")).await().indefinitely();
                } catch (IllegalStateException conflict) {
                    return conflict;
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }, executor)).toList();
            start.countDown();
            var outcomes = calls.stream().map(CompletableFuture::join).toList();
            assertEquals(1, outcomes.stream().filter(ExecutionAdmissionResult.class::isInstance).count());
            assertEquals(1, outcomes.stream().filter(IllegalStateException.class::isInstance).count());
            var winner = (ExecutionAdmissionResult) outcomes.stream().filter(ExecutionAdmissionResult.class::isInstance).findFirst().orElseThrow();
            assertTrue(winner.newlyCreated());
            assertEquals(1, scan(executions).size());
            assertEquals(2, scan(keys).size());
            var before = rows();
            assertEquals(winner.receipt(), fresh().lookupAdmission("tenant", "pipeline", "conflict-race").await().indefinitely().orElseThrow());
            assertEquals(before, rows());
            for (var input : inputs) {
                if (input.fingerprint().equals(winner.receipt().intentFingerprint())) {
                    assertEquals(winner.receipt(), fresh().inspectExistingAdmission(input).await().indefinitely().orElseThrow().receipt());
                } else {
                    assertThrows(IllegalStateException.class, () -> fresh().inspectExistingAdmission(input).await().indefinitely());
                }
                assertEquals(before, rows());
            }
        }
    }

    @Test
    void atomicCancellationLeavesNoExecutionKeyOrAdmissionButMayLeaveSafeOrphanPayload() {
        var cut = boundary(request -> {
            var items = new java.util.ArrayList<>(request.transactItems());
            var first = items.getFirst().put().toBuilder().conditionExpression("attribute_exists(#tenant)")
                .expressionAttributeNames(Map.of("#tenant", "tenant_id")).build();
            items.set(0, TransactWriteItem.builder().put(first).build());
            return client.transactWriteItems(request.toBuilder().transactItems(items).build());
        });
        var command = command(intent("key", "release-a", "x".repeat(300_000)), ttl());
        assertThrows(TransactionCanceledException.class, () -> new DynamoExecutionStateStore(cut, config)
            .createOrGetNativeAdmittedExecution(command, evidence("release-a")).await().indefinitely());
        assertTrue(scan(executions).isEmpty());
        assertTrue(scan(keys).isEmpty());
        assertFalse(scan(payloads).isEmpty());
        var retained = rows();
        assertTrue(fresh().lookupAdmission("tenant", "pipeline", "key").await().indefinitely().isEmpty());
        assertEquals(retained, rows());
    }

    @Test
    void interruptedManifestWriteLeavesOnlyRealOrphanChunksAndNoAdmission() {
        var transactions = new AtomicInteger();
        var manifests = new AtomicInteger();
        var cut = (DynamoDbClient) Proxy.newProxyInstance(DynamoDbClient.class.getClassLoader(), new Class<?>[] {DynamoDbClient.class},
            (proxy, method, args) -> {
                if (method.getName().equals("transactWriteItems")) transactions.incrementAndGet();
                if (method.getName().equals("putItem") && args[0] instanceof PutItemRequest request
                    && payloads.equals(request.tableName()) && "MANIFEST".equals(request.item().get("payload_part").s())) {
                    manifests.incrementAndGet();
                    throw new IllegalStateException("payload manifest write interrupted");
                }
                try { return method.invoke(client, args); }
                catch (InvocationTargetException error) { throw error.getCause(); }
            });
        var command = command(intent("payload-cut", "release-a", "x".repeat(300_000)), ttl());
        var failure = assertThrows(IllegalStateException.class, () -> new DynamoExecutionStateStore(cut, config)
            .createOrGetNativeAdmittedExecution(command, evidence("release-a")).await().indefinitely());
        assertTrue(failure.getMessage().contains("payload manifest write interrupted"));
        assertEquals(1, manifests.get());
        assertEquals(0, transactions.get());
        assertTrue(scan(executions).isEmpty());
        assertTrue(scan(keys).isEmpty());
        var chunks = scan(payloads);
        assertFalse(chunks.isEmpty());
        assertTrue(chunks.stream().allMatch(row -> !"MANIFEST".equals(row.get("payload_part").s()) && row.get("payload_bytes").b() != null));
        var actualBytes = new java.io.ByteArrayOutputStream();
        chunks.stream().sorted(java.util.Comparator.comparing(row -> row.get("payload_part").s()))
            .forEach(row -> actualBytes.writeBytes(row.get("payload_bytes").b().asByteArray()));
        assertArrayEquals(("\"" + "x".repeat(300_000) + "\"").getBytes(java.nio.charset.StandardCharsets.UTF_8), actualBytes.toByteArray());
        var retained = rows();
        assertTrue(fresh().lookupAdmission("tenant", "pipeline", "payload-cut").await().indefinitely().isEmpty());
        assertTrue(fresh().inspectExistingAdmission(command.intent()).await().indefinitely().isEmpty());
        assertEquals(retained, rows());
    }

    @Test
    void completeItemLimitRejectsBeforePayloadOrTransactionAndBelowLimitHasReadablePayload() {
        int low = 0;
        int high = 410_000;
        while (low + 1 < high) {
            int mid = (low + high) / 2;
            var command = command(intent("boundary", "release-a", "x".repeat(mid)), ttl());
            try {
                NativeAdmissionDynamoCodec.encode(NativeExecutionAdmission.create(command, evidence("release-a"), UUID.randomUUID().toString()));
                low = mid;
            } catch (ExecutionAdmissionTooLargeException error) { high = mid; }
        }
        var before = rows();
        var entries = new AtomicInteger();
        var tracked = boundary(request -> { entries.incrementAndGet(); return client.transactWriteItems(request); });
        var above = command(intent("boundary", "release-a", "x".repeat(high)), ttl());
        assertThrows(ExecutionAdmissionTooLargeException.class, () -> new DynamoExecutionStateStore(tracked, config)
            .createOrGetNativeAdmittedExecution(above, evidence("release-a")).await().indefinitely());
        assertEquals(0, entries.get());
        assertEquals(before, rows());
        var below = command(intent("boundary", "release-a", "x".repeat(low)), ttl());
        var rawItem = NativeAdmissionDynamoCodec.encode(NativeExecutionAdmission.create(below, evidence("release-a"), UUID.randomUUID().toString()));
        long independentBytes = rawItem.entrySet().stream().mapToLong(entry ->
            entry.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + (entry.getValue().s() != null ? entry.getValue().s().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                    : entry.getValue().b().asByteArray().length)).sum();
        assertEquals(low, rawItem.get("admission_input").b().asByteArray().length);
        assertTrue(independentBytes <= 400L * 1024);
        assertTrue(independentBytes + 128 <= 400L * 1024);
        assertTrue(independentBytes + 129 > 400L * 1024);
        var receipt = new DynamoExecutionStateStore(tracked, config).createOrGetNativeAdmittedExecution(below, evidence("release-a"))
            .await().indefinitely().receipt();
        assertEquals(1, entries.get());
        assertEquals(below.execution().inputPayload(), fresh().getExecution("tenant", receipt.executionId())
            .await().indefinitely().orElseThrow().inputPayload());
        assertFalse(scan(payloads).isEmpty());
    }

    @Test
    void legacyCollisionIsNotAdoptedDeletedOrRenewed() {
        var command = command(intent("legacy", "release-a", "original"), 1L);
        var legacy = fresh().createOrGetExecution(command.execution()).await().indefinitely();
        var before = rows();
        assertThrows(IllegalStateException.class, () -> fresh().createOrGetNativeAdmittedExecution(command, evidence("release-a"))
            .await().indefinitely());
        assertTrue(fresh().lookupAdmission("tenant", "pipeline", "legacy").await().indefinitely().isEmpty());
        assertEquals(before, rows());
        assertEquals(legacy.record().executionId(), scan(executions).iterator().next().get("execution_id").s());
        assertFalse(fresh().supportsExecutionAdmission());
    }

    @Test
    void actualDerivedUnicodePartitionKeyBoundRejectsBeforeAnyPayloadOrTransaction() {
        var evidence = evidence("release-a");
        var accepted = command(intent("unicode-key", "release-a", "é😀"), ttl());
        var source = accepted.execution();
        var below = new ExecutionAdmissionCreateCommand(new ExecutionCreateCommand(source.tenantId(), "é".repeat(1000),
            source.pipelineId(), source.contractVersion(), source.releaseVersion(), source.inputPayload(), source.resultShape(),
            source.nowEpochMs(), source.ttlEpochS()), accepted.intent(), evidence.metadataFingerprint(), evidence.primaryArtifactId(), evidence.primaryArtifactDigest());
        var receipt = fresh().createOrGetNativeAdmittedExecution(below, evidence).await().indefinitely().receipt();
        var ordinaryKey = scan(keys).stream().filter(row -> row.containsKey("execution_id")).findFirst().orElseThrow()
            .get("tenant_execution_key").s();
        assertTrue(NativeExecutionAdmission.utf8(ordinaryKey).length <= 2048);
        assertEquals(receipt, fresh().inspectExistingAdmission(below.intent()).await().indefinitely().orElseThrow().receipt());
        var before = rows();
        var rejected = command(intent("unicode-above", "release-a", "x".repeat(300_000)), ttl());
        var original = rejected.execution();
        var above = new ExecutionAdmissionCreateCommand(new ExecutionCreateCommand(original.tenantId(), "é".repeat(1025),
            original.pipelineId(), original.contractVersion(), original.releaseVersion(), original.inputPayload(), original.resultShape(),
            original.nowEpochMs(), original.ttlEpochS()), rejected.intent(), evidence.metadataFingerprint(), evidence.primaryArtifactId(), evidence.primaryArtifactDigest());
        var entries = new AtomicInteger();
        var tracked = boundary(request -> { entries.incrementAndGet(); return client.transactWriteItems(request); });
        var failure = assertThrows(ExecutionAdmissionTooLargeException.class, () -> new DynamoExecutionStateStore(tracked, config)
            .createOrGetNativeAdmittedExecution(above, evidence).await().indefinitely());
        assertTrue(failure.getMessage().contains("partition key"));
        assertEquals(0, entries.get());
        assertEquals(before, rows());
        assertTrue(fresh().lookupAdmission("tenant", "pipeline", "unicode-above").await().indefinitely().isEmpty());
    }

    @Test
    void corruptRetainedActualBytesCannotBeAcceptedUsingReceiptFingerprintAlone() {
        var original = intent("key", "release-a", "original");
        var created = fresh().createOrGetNativeAdmittedExecution(command(original, ttl()), evidence("release-a"))
            .await().indefinitely();
        var admissionKey = Map.of("tenant_execution_key", AttributeValue.builder()
            .s(NativeExecutionAdmission.key("tenant", "pipeline", "key")).build());
        client.updateItem(request -> request.tableName(keys).key(admissionKey).updateExpression("SET #input = :changed")
            .expressionAttributeNames(Map.of("#input", "admission_input"))
            .expressionAttributeValues(Map.of(":changed", AttributeValue.builder()
                .b(software.amazon.awssdk.core.SdkBytes.fromUtf8String("changed")).build())));
        var corrupted = rows();
        assertThrows(IllegalStateException.class, () -> fresh().lookupAdmission("tenant", "pipeline", "key").await().indefinitely());
        assertThrows(IllegalStateException.class, () -> fresh().inspectExistingAdmission(original).await().indefinitely());
        assertEquals(corrupted, rows());
        assertTrue(fresh().getExecution("tenant", created.receipt().executionId()).await().indefinitely().isPresent());
    }

    private DynamoExecutionStateStore fresh() { return new DynamoExecutionStateStore(client, config); }
    private static java.util.concurrent.ExecutorService ownedExecutor(int count) {
        var context = Thread.currentThread().getContextClassLoader();
        return java.util.concurrent.Executors.newFixedThreadPool(count, task -> {
            var thread = new Thread(task, "admission-concurrency");
            thread.setContextClassLoader(context);
            return thread;
        });
    }
    private static long ttl() { return System.currentTimeMillis() / 1000 + 3600; }
    private static ExecutionAdmissionIntent intent(String key, String release, String input) {
        return new ExecutionAdmissionIntent(1, "tenant", "pipeline", key, "contract", release, "UNI", "java.lang.String", "json",
            NativeExecutionAdmission.utf8(input), false);
    }
    private static ExecutionAdmissionIntent changed(ExecutionAdmissionIntent original, String shape, String type, String encoding, boolean streaming) {
        return new ExecutionAdmissionIntent(1, original.tenantId(), original.pipelineId(), original.clientKey(), original.contractVersion(),
            original.releaseVersion(), shape, type, encoding, original.inputBytes(), streaming);
    }
    private static ExecutionAdmissionCreateCommand command(ExecutionAdmissionIntent intent, long ttl) {
        var evidence = evidence(intent.releaseVersion());
        var execution = new ExecutionCreateCommand(intent.tenantId(), "root:" + intent.pipelineId() + ":" + intent.releaseVersion() + ":" + intent.clientKey(),
            intent.pipelineId(), intent.contractVersion(), intent.releaseVersion(), new String(intent.inputBytes(), java.nio.charset.StandardCharsets.UTF_8),
            ExecutionResultShape.SINGLE, System.currentTimeMillis(), ttl);
        return new ExecutionAdmissionCreateCommand(execution, intent, evidence.metadataFingerprint(), evidence.primaryArtifactId(), evidence.primaryArtifactDigest());
    }
    private static PipelineReleaseEvidence evidence(String release) {
        return ExecutionAdmissionReleaseEvidence.snapshot(new PipelineReleaseRecord("tenant", "pipeline", "contract", release,
            PipelineReleaseStatus.REGISTERED, new PipelineReleaseDescriptor(1, "pipeline", "contract", release, "artifact", List.of()),
            "artifact", "sha256:artifact", "file:/artifact.jar", 1, "artifact", null, 1, 1, 0));
    }
    private Set<Map<String, AttributeValue>> scan(String table) {
        return Set.copyOf(client.scan(ScanRequest.builder().tableName(table).consistentRead(true).build()).items());
    }
    private Map<String, Set<Map<String, AttributeValue>>> rows() {
        return Map.of(executions, scan(executions), keys, scan(keys), payloads, scan(payloads));
    }
    private void table(String table, String hash, String range) {
        var attributes = new java.util.ArrayList<AttributeDefinition>();
        var schema = new java.util.ArrayList<KeySchemaElement>();
        attributes.add(AttributeDefinition.builder().attributeName(hash).attributeType(ScalarAttributeType.S).build());
        schema.add(KeySchemaElement.builder().attributeName(hash).keyType(KeyType.HASH).build());
        if (range != null) {
            attributes.add(AttributeDefinition.builder().attributeName(range).attributeType(ScalarAttributeType.S).build());
            schema.add(KeySchemaElement.builder().attributeName(range).keyType(KeyType.RANGE).build());
        }
        client.createTable(CreateTableRequest.builder().tableName(table).attributeDefinitions(attributes).keySchema(schema)
            .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(20L).writeCapacityUnits(20L).build()).build());
        client.waiter().waitUntilTableExists(request -> request.tableName(table));
    }
    private DynamoDbClient boundary(Function<TransactWriteItemsRequest, TransactWriteItemsResponse> action) {
        return (DynamoDbClient) Proxy.newProxyInstance(DynamoDbClient.class.getClassLoader(), new Class<?>[] {DynamoDbClient.class},
            (proxy, method, args) -> {
                if (method.getName().equals("transactWriteItems") && args.length == 1 && args[0] instanceof TransactWriteItemsRequest request) {
                    return action.apply(request);
                }
                try { return method.invoke(client, args); }
                catch (InvocationTargetException error) { throw error.getCause(); }
            });
    }
}

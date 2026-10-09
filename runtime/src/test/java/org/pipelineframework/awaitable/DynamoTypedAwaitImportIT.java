package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.*;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.*;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/** Native typed snapshot/import storage proof, not an external-dispatch or process-restart proof. */
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DynamoTypedAwaitImportIT {
    private static final String PREFIX = "typed_await_import";
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8")).withServices("dynamodb");
    private DynamoDbClient dynamo;
    private InMemoryExecutionStateStore executions;
    private InMemoryPipelineReleaseRegistry releases;
    private AwaitCompletionDescriptorRegistry descriptors;
    private String executionId;

    @BeforeAll
    void tables() {
        dynamo = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))).build();
        table(PREFIX + "_interaction", "tenant_id", "interaction_id");
        table(PREFIX + "_interaction_key", "lookup_key", null);
    }

    @AfterAll
    void close() {
        if (dynamo != null) dynamo.close();
    }

    @BeforeEach
    void pinnedRelease() {
        long now = System.currentTimeMillis();
        executions = new InMemoryExecutionStateStore();
        executionId = executions.createOrGetExecution(new ExecutionCreateCommand(
            "tenant", UUID.randomUUID().toString(), "org.example.await", "contract", "release-1", "input",
            ExecutionResultShape.SINGLE, now, now / 1000 + 3600)).await().indefinitely().record().executionId();
        var contract = new PipelineContractDescriptor(2, "org.example.await", "contract", "contract-hash",
            null, null, null, false, null,
            List.of(new PipelineBundleStepDescriptor(2, "await", "internal", "ONE_TO_ONE",
                "Request", "Decision", null, null, Map.of("transportType", "kafka")),
                new PipelineBundleStepDescriptor(3, "projected-await", "internal", "ONE_TO_ONE",
                    "Request", "ProjectedDecision", null, null, Map.of("transportType", "kafka"))),
            PipelineBundleCapabilities.defaults(),
            Map.of("Request", binding(Request.class, "request-fingerprint"),
                "Decision", binding(Decision.class, "decision-fingerprint"),
                "ProjectedDecision", binding(ProjectedDecision.class, "projected-fingerprint")), "catalog-fingerprint");
        releases = new InMemoryPipelineReleaseRegistry();
        var descriptor = new PipelineReleaseDescriptor(1, "org.example.await", "contract", "release-1", "application",
            List.of(new PipelineReleaseArtifactDescriptor("application", "application-archive", "file:/fixture",
                "sha256:artifact", List.of("await"), List.of("local", "rest", "grpc", "sqs"))));
        releases.register(new PipelineReleaseRecord("tenant", "org.example.await", "contract", "release-1",
            PipelineReleaseStatus.REGISTERED, descriptor, "application", "sha256:artifact", "file:/fixture", 0,
            "checksum", contract, now, now, 0)).await().indefinitely();
        descriptors = new AwaitCompletionDescriptorRegistry();
        descriptors.register(new AwaitCompletionDescriptor("await", Request.class.getName(), Decision.class.getName(),
            "ONE_TO_ONE", Duration.ofMinutes(10), "correlation", "kafka", Map.of(), List.of()));
    }

    @Test
    @SuppressWarnings("unchecked")
    void requestAwareWireCompletionPersistsCanonicalFinalValueAndFreshRuntimeReplaysIt() throws Exception {
        var conversions = new java.util.concurrent.atomic.AtomicInteger();
        var projections = new java.util.concurrent.atomic.AtomicInteger();
        AwaitCompletionProjector<Request, CanonicalChoice, ProjectedDecision> typed =
            new DecisionProjector(projections);
        var descriptor = new AwaitCompletionDescriptor("projected-await", Request.class.getName(),
            ProjectedDecision.class.getName(), "ONE_TO_ONE", Duration.ofMinutes(10), "interactionId", "kafka",
            Map.of(), List.of(), Map.class.getName(), com.google.protobuf.DescriptorProtos.FileDescriptorProto.class.getName(),
            value -> Map.of("id", ((Request) value).id()),
            value -> {
                conversions.incrementAndGet();
                return new CanonicalChoice(((com.google.protobuf.DescriptorProtos.FileDescriptorProto) value).getName());
            }, DecisionProjector.class.getName(),
            (AwaitCompletionProjector<Object, Object, Object>) (AwaitCompletionProjector<?, ?, ?>) typed, true);
        descriptors.register(descriptor);
        var first = completionCoordinator();
        var created = first.createOrGet(descriptor, "tenant", executionId, 3, "cause", new Request("r-1"),
            "reviewer", "review").await().indefinitely().record();

        first.complete(new AwaitCompletionCommand("tenant", created.interactionId(), created.correlationId(),
            "completion-1", Map.of("name", "approved"), "reviewer", System.currentTimeMillis())).await().indefinitely();

        var expected = new ProjectedDecision("r-1", "approved");
        var persisted = store().get("tenant", created.interactionId()).await().indefinitely().orElseThrow();
        assertEquals(AwaitInteractionStatus.COMPLETED, persisted.status());
        assertEquals(expected, assertInstanceOf(ProjectedDecision.class, persisted.responsePayload()));
        var encoded = PipelineJson.mapper().readTree(row(created.interactionId()).get("response_payload_json").s());
        assertEquals("ProjectedDecision", encoded.path("canonicalTypeId").asText());
        assertEquals("projected-fingerprint", encoded.path("typeExpressionFingerprint").asText());
        assertEquals(PipelineJson.mapper().valueToTree(Map.of("requestId", "r-1", "decision", "approved")),
            PipelineJson.mapper().readTree(java.util.Base64.getDecoder().decode(encoded.path("payload").asText())));
        assertEquals(1, conversions.get());
        assertEquals(1, projections.get());

        var restarted = completionCoordinator();
        var fresh = store().get("tenant", created.interactionId()).await().indefinitely().orElseThrow();
        assertEquals(expected, restarted.resumePayload(fresh));
        assertEquals(expected, restarted.resumePayload(fresh));
        assertEquals(1, conversions.get(), "durable canonical replay must not decode the transport twice");
        assertEquals(1, projections.get(), "durable canonical replay must not project twice");
    }

    private AwaitCoordinator completionCoordinator() {
        var coordinator = new AwaitCoordinator();
        coordinator.interactionStores = instances(store());
        coordinator.unitStores = instances(new InMemoryAwaitUnitStore());
        coordinator.descriptorFactory = descriptors;
        coordinator.durablePayloadResolver = resolver();
        coordinator.resumeTokenService = new AwaitResumeTokenService("test-only-resume-token-secret");
        return coordinator;
    }

    private record DecisionProjector(java.util.concurrent.atomic.AtomicInteger projections)
        implements AwaitCompletionProjector<Request, CanonicalChoice, ProjectedDecision> {
        @Override public ProjectedDecision project(Request request, CanonicalChoice completion,
                                                   AwaitCompletionMetadata metadata) {
            projections.incrementAndGet();
            return new ProjectedDecision(request.id(), completion.status());
        }
    }

    @Test
    void nativeDirectSnapshotImportsAndReimportsExistingDispatchedRecord() {
        var canonical = interaction(AwaitInteractionStatus.DISPATCHED, null);
        var snapshot = nativeSnapshot(canonical);
        assertInstanceOf(TypedDurablePayload.class, snapshot.requestPayload());
        AwaitInteractionStore store = store();
        store.importRecord(snapshot).await().indefinitely();
        assertCanonical(canonical, store().get("tenant", canonical.interactionId()).await().indefinitely().orElseThrow());
        var before = row(canonical.interactionId());
        var existing = store().importRecord(snapshot).await().indefinitely();
        assertCanonical(canonical, existing);
        assertEquals(AwaitInteractionStatus.DISPATCHED, existing.status());
        assertEquals(2L, existing.version());
        assertEquals(before, row(canonical.interactionId()));
    }

    @Test
    void directAndJsonEnvelopeSlotsRoundTripThroughFreshStores() throws Exception {
        for (boolean json : List.of(false, true)) {
            var canonical = interaction(AwaitInteractionStatus.COMPLETED, new Decision("approved"));
            var snapshot = nativeSnapshot(canonical);
            if (json) snapshot = PipelineJson.mapper().readValue(
                PipelineJson.mapper().writeValueAsBytes(snapshot), AwaitInteractionRecord.class);
            store().importRecord(snapshot).await().indefinitely();
            assertCanonical(canonical, store().get("tenant", canonical.interactionId()).await().indefinitely().orElseThrow());
            var stored = row(canonical.interactionId());
            assertTrue(stored.get("request_payload_json").s().contains("request-fingerprint"));
            assertTrue(stored.get("response_payload_json").s().contains("decision-fingerprint"));
        }
    }

    @Test
    void canonicalObjectsAndLegacyMapsStillImport() {
        for (boolean legacy : List.of(false, true)) {
            var canonical = interaction(AwaitInteractionStatus.COMPLETED, new Decision("approved"));
            var imported = legacy ? canonical.withPayloadSnapshots(
                Map.of("_tpf_java_class", "untrusted.Request", "_tpf_payload", Map.of("id", "r-1")),
                Map.of("status", "approved")) : canonical;
            store().importRecord(imported).await().indefinitely();
            assertCanonical(canonical, store().get("tenant", canonical.interactionId()).await().indefinitely().orElseThrow());
        }
    }

    @Test
    void mismatchedDirectAndJsonEnvelopesFailClosedWithoutChangingRows() throws Exception {
        for (var slot : AwaitDurablePayloadResolver.Slot.values()) {
            for (String field : List.of("canonicalTypeId", "typeExpressionFingerprint", "catalogFingerprint",
                "encoding", "encodingVersion")) {
                for (boolean json : List.of(false, true)) {
                    var canonical = interaction(AwaitInteractionStatus.COMPLETED, new Decision("approved"));
                    var snapshot = nativeSnapshot(canonical);
                    var envelope = (TypedDurablePayload) (slot == AwaitDurablePayloadResolver.Slot.REQUEST
                        ? snapshot.requestPayload() : snapshot.responsePayload());
                    var wrong = new TypedDurablePayload(
                        field.equals("canonicalTypeId") ? "Wrong" : envelope.canonicalTypeId(),
                        field.equals("typeExpressionFingerprint") ? "wrong" : envelope.typeExpressionFingerprint(),
                        field.equals("catalogFingerprint") ? "wrong" : envelope.catalogFingerprint(),
                        field.equals("encoding") ? "wrong" : envelope.encoding(),
                        field.equals("encodingVersion") ? 2 : envelope.encodingVersion(), envelope.payload());
                    Object payload = json ? PipelineJson.mapper().readValue(
                        PipelineJson.mapper().writeValueAsBytes(wrong), Object.class) : wrong;
                    var rejected = slot == AwaitDurablePayloadResolver.Slot.REQUEST
                        ? snapshot.withPayloadSnapshots(payload, snapshot.responsePayload())
                        : snapshot.withPayloadSnapshots(snapshot.requestPayload(), payload);
                    assertThrows(IllegalStateException.class, () -> store().importRecord(rejected).await().indefinitely(),
                        slot + "/" + field + "/json=" + json);
                    assertTrue(row(canonical.interactionId()).isEmpty(), "fresh bad envelope must not create a row");
                    assertTrue(lookup("idempotency", canonical.stepId() + ":" + canonical.idempotencyKey()).isEmpty());
                    assertTrue(lookup("correlation", canonical.correlationId()).isEmpty());

                    store().importRecord(canonical).await().indefinitely();
                    assertFalse(lookup("idempotency", canonical.stepId() + ":" + canonical.idempotencyKey()).isEmpty());
                    assertFalse(lookup("correlation", canonical.correlationId()).isEmpty());
                    var before = row(canonical.interactionId());
                    assertThrows(IllegalStateException.class, () -> store().importRecord(rejected).await().indefinitely(),
                        "existing/" + slot + "/" + field + "/json=" + json);
                    assertEquals(before, row(canonical.interactionId()));
                    assertCanonical(canonical, store().get("tenant", canonical.interactionId()).await().indefinitely().orElseThrow());
                }
            }
        }
    }

    private AwaitInteractionRecord nativeSnapshot(AwaitInteractionRecord canonical) {
        var source = new InMemoryAwaitInteractionStore();
        source.importRecord(canonical).await().indefinitely();
        var units = new InMemoryAwaitUnitStore();
        long now = System.currentTimeMillis();
        units.createOrGet(new AwaitUnitCreateCommand("tenant", canonical.unitId(), executionId, "await", 2,
            "ONE_TO_ONE", now, now / 1000 + 3600)).await().indefinitely();
        var coordinator = new AwaitCoordinator();
        coordinator.interactionStores = instances(source);
        coordinator.unitStores = instances(units);
        coordinator.durablePayloadResolver = resolver();
        return coordinator.suspensionSnapshot(new AwaitSuspendedException("tenant", executionId, canonical.unitId(), 2))
            .await().indefinitely().interactions().getFirst();
    }

    private AwaitInteractionStore store() {
        return DynamoAwaitLifecycleTestStores.interactionStore(dynamo, PREFIX, resolver());
    }

    private AwaitDurablePayloadResolver resolver() {
        var resolver = new AwaitDurablePayloadResolver();
        resolver.executionStateStore = executions;
        resolver.releaseRegistry = releases;
        resolver.descriptors = descriptors;
        resolver.codec = new JsonDurablePayloadCodec();
        return resolver;
    }

    private AwaitInteractionRecord interaction(AwaitInteractionStatus status, Object response) {
        String id = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        return new AwaitInteractionRecord("tenant", executionId, "await", 2, Decision.class.getName(), id,
            "correlation-" + id, null, "idem-" + id, 2L, status, new Request("r-1"), response, "unit-" + id,
            null, null, null, null, "kafka", Map.of(), now + 600_000, now, now, now / 1000 + 3600,
            Decision.class.getName());
    }

    private void assertCanonical(AwaitInteractionRecord expected, AwaitInteractionRecord actual) {
        assertEquals(expected, actual);
        assertInstanceOf(Request.class, actual.requestPayload());
        if (expected.responsePayload() != null) assertInstanceOf(Decision.class, actual.responsePayload());
    }

    private Map<String, AttributeValue> row(String id) {
        return dynamo.getItem(GetItemRequest.builder().tableName(PREFIX + "_interaction").consistentRead(true)
            .key(Map.of("tenant_id", AttributeValue.builder().s("tenant").build(),
                "interaction_id", AttributeValue.builder().s(id).build())).build()).item();
    }

    private Map<String, AttributeValue> lookup(String kind, String key) {
        return dynamo.getItem(GetItemRequest.builder().tableName(PREFIX + "_interaction_key").consistentRead(true)
            .key(Map.of("lookup_key", AttributeValue.builder()
                .s(kind + ":6:tenant:" + key.length() + ":" + key).build())).build()).item();
    }

    private static Map<String, Object> binding(Class<?> type, String fingerprint) {
        return Map.of("runtimeClass", type.getName(), "definitionFingerprint", fingerprint);
    }

    /** Only CDI candidate enumeration is stubbed; stores, snapshotting, resolver and codec are native. */
    @SuppressWarnings("unchecked")
    private static <T> Instance<T> instances(T value) {
        Instance<T> candidates = mock(Instance.class);
        when(candidates.stream()).thenAnswer(ignored -> java.util.stream.Stream.of(value));
        return candidates;
    }

    private void table(String name, String hash, String range) {
        var attributes = new java.util.ArrayList<AttributeDefinition>();
        var keys = new java.util.ArrayList<KeySchemaElement>();
        attributes.add(AttributeDefinition.builder().attributeName(hash).attributeType(ScalarAttributeType.S).build());
        keys.add(KeySchemaElement.builder().attributeName(hash).keyType(KeyType.HASH).build());
        if (range != null) {
            attributes.add(AttributeDefinition.builder().attributeName(range).attributeType(ScalarAttributeType.S).build());
            keys.add(KeySchemaElement.builder().attributeName(range).keyType(KeyType.RANGE).build());
        }
        dynamo.createTable(CreateTableRequest.builder().tableName(name).attributeDefinitions(attributes).keySchema(keys)
            .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build()).build());
        dynamo.waiter().waitUntilTableExists(request -> request.tableName(name));
    }

    public record Request(String id) {}
    public record Decision(String status) {}
    public record CanonicalChoice(String status) {}
    public record ProjectedDecision(String requestId, String decision) {}
}

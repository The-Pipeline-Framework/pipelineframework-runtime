package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.*;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.connector.*;
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
        table(PREFIX + "_completion_lifecycle", "pk", "sk");
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
                    "Request", "ProjectedDecision", null, null, Map.of("transportType", "kafka")),
                new PipelineBundleStepDescriptor(4, "acknowledged-command", "command", "ONE_TO_ONE",
                    "Request", "AcknowledgedDecision", null, null, Map.of("transportType", "connector-callback"))),
            PipelineBundleCapabilities.defaults(),
            Map.of("Request", binding(Request.class, "request-fingerprint"),
                "Decision", binding(Decision.class, "decision-fingerprint"),
                "ProjectedDecision", binding(ProjectedDecision.class, "projected-fingerprint"),
                "Acknowledgement", binding(Acknowledgement.class, "acknowledgement-fingerprint"),
                "Callback", binding(Callback.class, "callback-fingerprint"),
                "AcknowledgedDecision", binding(AcknowledgedDecision.class, "acknowledged-decision-fingerprint")),
            "catalog-fingerprint");
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

    @Test
    void acknowledgedCommandStagesAndRemovesInvalidEarlyCallbackThenRestoresTypedCompletion() throws Exception {
        var descriptor = commandDescriptor("test.AcknowledgedProjector", Acknowledgement.class.getName(), true);
        descriptors.register(descriptor);
        var first = completionCoordinator();
        var imported = commandInteraction(descriptor);
        store().importRecord(imported).await().indefinitely();
        var token = new AwaitResumeTokenService("test-only-resume-token-secret")
            .sign(imported, imported.createdAtEpochMs());

        var observed = first.complete(callbackCommand(imported, token, "different-run"))
            .await().indefinitely().record();
        assertEquals(AwaitInteractionStatus.COMPLETION_OBSERVED, observed.status());
        assertEquals(Optional.of(new Callback("different-run")), observed.stagedCompletionPayload());
        assertTrue(row(imported.interactionId()).containsKey("staged_completion_json"));
        var stale = store().markDispatched("tenant", imported.interactionId(), imported.version(),
            Map.of(), System.currentTimeMillis()).await().indefinitely();
        assertTrue(stale.isEmpty(), "Dynamo conditional version must reject stale dispatch");

        var rejected = first.settleCommandDispatch(observed, CommandDispatchSettlement.SUCCEEDED,
            Optional.of(new Acknowledgement("trusted-run"))).await().indefinitely().record();
        assertEquals(AwaitInteractionStatus.DISPATCHED, rejected.status());
        assertEquals(Optional.of(new Acknowledgement("trusted-run")), rejected.commandAcknowledgement());
        assertTrue(rejected.stagedCompletionPayload().isEmpty());
        assertFalse(row(imported.interactionId()).containsKey("staged_completion_json"),
            "the rejected early callback must be removed by a real Dynamo update");
        assertTrue(row(imported.interactionId()).containsKey("command_acknowledgement_json"));

        var completed = first.complete(callbackCommand(imported, token, "trusted-run"))
            .await().indefinitely().record();
        assertEquals(AwaitInteractionStatus.COMPLETED, completed.status());
        assertEquals(new AcknowledgedDecision("r-1", "trusted-run"), completed.responsePayload());
        assertTrue(first.complete(callbackCommand(imported, token, "trusted-run"))
            .await().indefinitely().duplicate());
        var stored = row(imported.interactionId());
        for (var field : List.of("request_payload_json", "command_acknowledgement_json",
            "staged_completion_json", "response_payload_json")) {
            assertTrue(stored.get(field).s().contains("catalog-fingerprint"), field);
        }

        var stable = new AwaitCompletionDescriptorRegistry();
        stable.register(commandDescriptor("test.AcknowledgedProjector", Acknowledgement.class.getName(), true));
        var restarted = completionCoordinator(stable);
        var fresh = store(stable).get("tenant", imported.interactionId()).await().indefinitely().orElseThrow();
        assertInstanceOf(Request.class, fresh.requestPayload());
        assertInstanceOf(Acknowledgement.class, fresh.commandAcknowledgement().orElseThrow());
        assertInstanceOf(Callback.class, fresh.stagedCompletionPayload().orElseThrow());
        assertEquals(new AcknowledgedDecision("r-1", "trusted-run"), restarted.resumePayload(fresh));
        var previousLoader = Thread.currentThread().getContextClassLoader();
        var releaseLoader = new RecordingReleaseLoader(previousLoader);
        Thread.currentThread().setContextClassLoader(releaseLoader);
        try {
            var newResolver = resolver(stable);
            assertInstanceOf(Request.class, newResolver.decode(imported, AwaitDurablePayloadResolver.Slot.REQUEST,
                stored.get("request_payload_json").s()));
            assertInstanceOf(Acknowledgement.class, newResolver.decode(imported,
                AwaitDurablePayloadResolver.Slot.COMMAND_ACKNOWLEDGEMENT,
                stored.get("command_acknowledgement_json").s()));
            assertInstanceOf(Callback.class, newResolver.decode(imported,
                AwaitDurablePayloadResolver.Slot.STAGED_COMPLETION,
                stored.get("staged_completion_json").s()));
            assertInstanceOf(AcknowledgedDecision.class, newResolver.decode(imported,
                AwaitDurablePayloadResolver.Slot.RESPONSE,
                stored.get("response_payload_json").s()));
            for (var type : List.of(Request.class, Acknowledgement.class, Callback.class,
                AcknowledgedDecision.class)) {
                assertTrue(releaseLoader.loaded.contains(type.getName()), type.getName());
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
        }
        for (var incompatible : List.of(
            commandDescriptor("test.OtherProjector", Acknowledgement.class.getName(), true),
            commandDescriptor("test.AcknowledgedProjector", Decision.class.getName(), true),
            commandDescriptor("test.AcknowledgedProjector", Acknowledgement.class.getName(), false))) {
            var changed = new AwaitCompletionDescriptorRegistry();
            changed.register(incompatible);
            assertThrows(IllegalStateException.class, () -> completionCoordinator(changed).callbackDescriptor(fresh));
        }
    }

    @Test
    void expiredAcknowledgedCommandCannotBeMutatedByDynamoStore() {
        var descriptor = commandDescriptor("test.AcknowledgedProjector", Acknowledgement.class.getName(), true);
        descriptors.register(descriptor);
        var imported = commandInteraction(descriptor);
        store().importRecord(imported).await().indefinitely();
        dynamo.updateItem(UpdateItemRequest.builder().tableName(PREFIX + "_interaction")
            .key(Map.of("tenant_id", AttributeValue.builder().s("tenant").build(),
                "interaction_id", AttributeValue.builder().s(imported.interactionId()).build()))
            .updateExpression("SET ttl_epoch_s = :expired")
            .expressionAttributeValues(Map.of(":expired", AttributeValue.builder()
                .n(Long.toString(System.currentTimeMillis() / 1000 - 1)).build()))
            .build());
        var before = row(imported.interactionId());
        var next = new AwaitInteractionRecord(imported.tenantId(), imported.executionId(), imported.stepId(),
            imported.stepIndex(), imported.outputType(), imported.interactionId(), imported.correlationId(),
            imported.causationId(), imported.idempotencyKey(), imported.version() + 1,
            AwaitInteractionStatus.COMPLETION_OBSERVED, imported.requestPayload(), imported.responsePayload(),
            imported.unitId(), imported.itemIndex(), imported.actor(), imported.assignee(), imported.group(),
            imported.transportType(), imported.transportMetadata(), imported.deadlineEpochMs(),
            imported.createdAtEpochMs(), System.currentTimeMillis(), imported.ttlEpochS(),
            imported.transportOutputType(), Optional.empty(), Optional.of(new Callback("trusted-run")));
        assertTrue(store().compareAndSetCommandCompletion(imported, next).await().indefinitely().isEmpty());
        assertEquals(before, row(imported.interactionId()));
    }

    private AwaitCoordinator completionCoordinator() {
        return completionCoordinator(descriptors);
    }

    private AwaitCoordinator completionCoordinator(AwaitCompletionDescriptorRegistry registry) {
        var coordinator = new AwaitCoordinator();
        coordinator.interactionStores = instances(store(registry));
        coordinator.unitStores = instances(new InMemoryAwaitUnitStore());
        coordinator.descriptorFactory = registry;
        coordinator.durablePayloadResolver = resolver(registry);
        coordinator.resumeTokenService = new AwaitResumeTokenService("test-only-resume-token-secret");
        return coordinator;
    }

    private AwaitCompletionDescriptor commandDescriptor(String projectorId, String acknowledgementType,
        boolean acknowledgementAware) {
        var operation = new ConnectorOperationIdentity(ConnectorProviderId.of("test.jobs"), "start",
            ConnectorOperationKind.COMMAND, 1);
        var callback = new ConnectorCallbackSelection(ConnectorBindingName.of("jobs"), operation, 1,
            new ConnectorOperationCallbackDescriptor("completed",
                new ConnectorOperationTypeContract(Callback.class.getName(), Optional.empty()), true),
            "test.EndpointResolver", "test.Authenticator");
        if (!acknowledgementAware) {
            return new AwaitCompletionDescriptor("acknowledged-command", Request.class.getName(),
                AcknowledgedDecision.class.getName(), "ONE_TO_ONE", Duration.ofMinutes(10),
                "signedResumeToken", "", Map.of(), List.of(), Request.class.getName(), Callback.class.getName(),
                java.util.function.Function.identity(), java.util.function.Function.identity(), projectorId,
                (request, completion, metadata) -> new AcknowledgedDecision(((Request) request).id(),
                    ((Callback) completion).runId()), true, Optional.of(callback));
        }
        return new AwaitCompletionDescriptor("acknowledged-command", Request.class.getName(),
            AcknowledgedDecision.class.getName(), "ONE_TO_ONE", Duration.ofMinutes(10),
            "signedResumeToken", "", Map.of(), List.of(), Request.class.getName(), Callback.class.getName(),
            java.util.function.Function.identity(), java.util.function.Function.identity(), projectorId,
            null, true, Optional.of(callback), acknowledgementType,
            (request, acknowledgement, completion, metadata) -> {
                var accepted = (Acknowledgement) acknowledgement;
                var observed = (Callback) completion;
                if (!accepted.runId().equals(observed.runId())) {
                    throw new CommandCompletionRejectedException("callback run does not match Command acknowledgement");
                }
                return new AcknowledgedDecision(((Request) request).id(), accepted.runId());
            });
    }

    private AwaitInteractionRecord commandInteraction(AwaitCompletionDescriptor descriptor) {
        var id = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        var metadata = new java.util.HashMap<String, Object>();
        metadata.put("completionMode", "CONNECTOR_CALLBACK");
        metadata.put("callbackSelection", descriptor.callback().orElseThrow().contractMetadata());
        metadata.put("commandCompletionMode", "ACKNOWLEDGEMENT_REQUIRED");
        metadata.put("commandAcknowledgementType", descriptor.commandAcknowledgementType().orElseThrow());
        metadata.put("tpf.await.completion.projector", descriptor.completionProjectorId());
        return new AwaitInteractionRecord("tenant", executionId, descriptor.stepId(), 4,
            descriptor.outputType(), id, "correlation-" + id, "cause-" + id, "idem-" + id,
            0L, AwaitInteractionStatus.DISPATCHING, new Request("r-1"), null, "unit-" + id,
            null, null, null, null, descriptor.transportType(), metadata,
            now + 600_000, now, now, now / 1000 + 3600,
            descriptor.transportOutputType(), Optional.empty(), Optional.empty());
    }

    private AwaitCompletionCommand callbackCommand(AwaitInteractionRecord interaction, String token, String runId) {
        return new AwaitCompletionCommand("tenant", interaction.interactionId(), interaction.correlationId(), token,
            "completion-" + runId, new Callback(runId), "provider", System.currentTimeMillis());
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
    void boundedUnitPagingRetainsImportedMembersSharingThePhysicalIndexSortKey() {
        var store = store();
        String unitId = "paging-" + UUID.randomUUID();
        var first = importedUnitItem("c", "a#b", unitId);
        var second = importedUnitItem("b#c", "a", unitId);
        store.importRecord(first).await().indefinitely();
        store.importRecord(second).await().indefinitely();

        // GSI order is provider-defined. Completeness and a resumable full key are the contract.
        var observed = new java.util.ArrayList<String>();
        java.util.Optional<AwaitInteractionCursor> cursor = java.util.Optional.empty();
        var positions = new java.util.HashSet<String>();
        for (int pageNumber = 0; pageNumber < 3; pageNumber++) {
            var page = store.findByUnitPage("tenant", unitId, 1, cursor).await().indefinitely();
            assertTrue(page.interactions().size() <= 1);
            observed.addAll(page.interactions().stream().map(AwaitInteractionRecord::interactionId).toList());
            cursor = page.nextCursor();
            cursor.ifPresent(value -> assertTrue(positions.add(value.position()), "Cursor must advance"));
            if (cursor.isEmpty()) break;
        }
        assertTrue(cursor.isEmpty(), "A finite unchanged unit must exhaust");
        assertEquals(2, observed.size(), "Each imported identity must occur exactly once");
        assertEquals(java.util.Set.of(first.interactionId(), second.interactionId()), java.util.Set.copyOf(observed));
    }

    private AwaitInteractionRecord importedUnitItem(String interactionId, String causationId, String unitId) {
        var template = interaction(AwaitInteractionStatus.WAITING, null);
        return new AwaitInteractionRecord(template.tenantId(), template.executionId(), template.stepId(),
            template.stepIndex(), template.outputType(), interactionId, "correlation-" + interactionId,
            causationId, "idem-" + interactionId, template.version(), template.status(),
            template.requestPayload(), template.responsePayload(), unitId, 0, template.actor(),
            template.assignee(), template.group(), template.transportType(), template.transportMetadata(),
            template.deadlineEpochMs(), template.createdAtEpochMs(), template.updatedAtEpochMs(),
            template.ttlEpochS(), template.transportOutputType());
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
        return store(descriptors);
    }

    private AwaitInteractionStore store(AwaitCompletionDescriptorRegistry registry) {
        return DynamoAwaitLifecycleTestStores.interactionStore(dynamo, PREFIX, resolver(registry));
    }

    private AwaitDurablePayloadResolver resolver() {
        return resolver(descriptors);
    }

    private AwaitDurablePayloadResolver resolver(AwaitCompletionDescriptorRegistry registry) {
        var resolver = new AwaitDurablePayloadResolver();
        resolver.executionStateStore = executions;
        resolver.releaseRegistry = releases;
        resolver.descriptors = registry;
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
        var throughput = ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build();
        var request = CreateTableRequest.builder().tableName(name).keySchema(keys).provisionedThroughput(throughput);
        if (name.equals(PREFIX + "_interaction")) {
            attributes.add(AttributeDefinition.builder().attributeName("query_unit_key").attributeType(ScalarAttributeType.S).build());
            attributes.add(AttributeDefinition.builder().attributeName("query_unit_sort").attributeType(ScalarAttributeType.S).build());
            request.globalSecondaryIndexes(GlobalSecondaryIndex.builder().indexName("await-interaction-by-unit")
                .keySchema(KeySchemaElement.builder().attributeName("query_unit_key").keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("query_unit_sort").keyType(KeyType.RANGE).build())
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .provisionedThroughput(throughput).build());
        }
        dynamo.createTable(request.attributeDefinitions(attributes).build());
        dynamo.waiter().waitUntilTableExists(waiterRequest -> waiterRequest.tableName(name));
    }

    public record Request(String id) {}
    public record Decision(String status) {}
    public record CanonicalChoice(String status) {}
    public record ProjectedDecision(String requestId, String decision) {}
    public record Acknowledgement(String runId) {}
    public record Callback(String runId) {}
    public record AcknowledgedDecision(String requestId, String runId) {}

    private static final class RecordingReleaseLoader extends ClassLoader {
        private final java.util.Set<String> loaded = new java.util.HashSet<>();
        private RecordingReleaseLoader(ClassLoader parent) { super(parent); }
        @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            loaded.add(name);
            return super.loadClass(name, resolve);
        }
    }
}

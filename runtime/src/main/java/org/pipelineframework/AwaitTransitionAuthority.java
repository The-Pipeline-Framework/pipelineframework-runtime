package org.pipelineframework;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Coordinator admission boundary for the worker Await lifecycle.
 *
 * <p>The worker encodes a request, registers it, acquires a dispatch grant, sends the external
 * request, and reports its outcome. Registration binds the canonical request to an interaction.
 * Claiming changes the interaction version and supplies the grant that a subsequent report must
 * present. Itemized work seals its unit after registering all items. A resume reads the canonical
 * value from that retained unit. Completion and projection run independently in AwaitCoordinator.
 *
 * <p>The parent read authorizes the request. A subsequent parent reclaim fences progress commits
 * while an admitted external request can still finish.
 */
@ApplicationScoped
public class AwaitTransitionAuthority {
    @Inject Instance<ExecutionStateStore> executionStores;
    @Inject Instance<AwaitInteractionStore> interactionStores;
    @Inject PipelineOrchestratorConfig config;
    @Inject PipelineReleaseRegistry releases;
    @Inject AwaitCoordinator coordinator;
    ExecutionStateStore executionStore;
    AwaitInteractionStore interactionStore;
    Clock clock = Clock.systemUTC();
    private static final String GRANT_VERSION = "tpf.await.dispatchGrantVersion";

    public Uni<AwaitCreateResult> register(AwaitRegistrationRequest request) {
        return Uni.createFrom().deferred(() -> authorize(request.scope()).chain(authorized -> {
            PinnedAwaitDescriptor descriptor = PinnedAwaitDescriptor.resolve(authorized.release().contract(), request.stepId());
            AwaitExecutionContext context = context(authorized.execution(), request.stepIndex(), request.invocationPath(),
                org.pipelineframework.telemetry.PipelineTracingSupport.captureCurrentContext());
            descriptor.validatePosition(request.stepIndex(), request.invocationPath());
            validateOccurrence(authorized.execution(), context, request.stepId(), request.identityScope(), request.unitId(),
                request.causationId(), request.itemIndex());
            TypedDurablePayload payload = validateCanonicalPayload(authorized.release().contract(),
                descriptor.canonicalInputTypeId(), request.canonicalRequest());
            return coordinator.createAuthoritativeRegistration(request, descriptor, payload, context).invoke(created -> {
                AwaitInteractionRecord retained = created.record();
                if (!request.scope().tenantId().equals(retained.tenantId())
                    || !request.scope().executionId().equals(retained.executionId())
                    || !request.unitId().equals(retained.unitId()) || !request.stepId().equals(retained.stepId())
                    || request.stepIndex() != retained.stepIndex()
                    || !request.itemIndex().equals(Optional.ofNullable(retained.itemIndex()))
                    || !request.causationId().equals(retained.causationId())
                    || !request.invocationPath().equals(retained.invocationPath())
                    || !samePayload(payload, TypedDurablePayload.fromDurableValue(retained.requestPayload()).orElseThrow(
                        () -> new IllegalStateException("Registered Await request is not a canonical envelope")))) {
                    throw new IllegalStateException("Await registration conflicts with the retained immutable request");
                }
            });
        }));
    }

    /** Returns only the opaque callback token belonging to this validated retained registration. */
    public Uni<AwaitWorkerRegistrationResult> registerForWorker(AwaitRegistrationRequest request) {
        return register(request).map(created -> new AwaitWorkerRegistrationResult(created,
            coordinator.issueRegisteredCallbackToken(created.record())));
    }

    /** Only a successful WAITING-to-DISPATCHING CAS grants a send. A lost reply never grants redispatch. */
    public Uni<AwaitInteractionRecord> claimDispatch(AwaitTransitionScope scope, String interactionId, long expectedVersion) {
        return Uni.createFrom().deferred(() -> {
            if (interactionId == null || interactionId.isBlank() || expectedVersion < 0) {
                throw new IllegalArgumentException("Interaction identity and expected version are required");
            }
            return authorize(scope).chain(authorized -> interactions().getMetadata(scope.tenantId(), interactionId)
                .chain(found -> {
                    AwaitInteractionRecord interaction = found.orElseThrow(() -> new IllegalStateException("Await interaction is absent"));
                    if (!scope.tenantId().equals(interaction.tenantId()) || !scope.executionId().equals(interaction.executionId())
                        || !interactionId.equals(interaction.interactionId()) || interaction.version() != expectedVersion
                        || interaction.status() != AwaitInteractionStatus.WAITING || interaction.deadlineEpochMs() <= clock.millis()) {
                        throw new IllegalStateException("Await dispatch scope, version or status is no longer eligible");
                    }
                    PinnedAwaitDescriptor descriptor = PinnedAwaitDescriptor.resolve(authorized.release().contract(), interaction.stepId());
                    if (!descriptor.outputTypeId().equals(interaction.outputType())
                        || !descriptor.transportOutputTypeId().equals(interaction.transportOutputType())) {
                        throw new IllegalStateException("Retained Await boundary differs from its pinned descriptor");
                    }
                    AwaitExecutionContext context = context(authorized.execution(), interaction.stepIndex(), interaction.invocationPath(), Map.of());
                    descriptor.validatePosition(interaction.stepIndex(), interaction.invocationPath());
                    validateOccurrence(authorized.execution(), context, interaction.stepId(), context.identityScope(), interaction.unitId(),
                        interaction.causationId(), Optional.ofNullable(interaction.itemIndex()));
                    var metadata = new java.util.LinkedHashMap<String, Object>(interaction.transportMetadata());
                    metadata.put(GRANT_VERSION, Math.addExact(expectedVersion, 1));
                    return interactions().markDispatching(scope.tenantId(), interactionId, expectedVersion,
                        Map.copyOf(metadata), clock.millis())
                        .map(claimed -> claimed.orElseThrow(() -> new IllegalStateException("Await dispatch grant was already consumed")));
                }));
        });
    }

    public Uni<AwaitDispatchReportResult> report(AwaitDispatchReportRequest request) {
        return retained(request.scope()).chain(authorized -> interactions().get(request.scope().tenantId(), request.interactionId())
            .chain(found -> {
                var interaction = found.orElseThrow(() -> new IllegalStateException("Await interaction is absent"));
                if (!interaction.interactionId().equals(request.interactionId())
                    || !interaction.tenantId().equals(request.scope().tenantId())
                    || !interaction.executionId().equals(request.scope().executionId())
                    || !(interaction.transportMetadata().get(GRANT_VERSION) instanceof Number grant)
                    || grant.longValue() != request.grantedInteractionVersion()) {
                    throw new IllegalStateException("Dispatch report does not identify its admitted grant");
                }
                PinnedAwaitDescriptor.resolve(authorized.release().contract(), interaction.stepId());
                return coordinator.applyAuthoritativeDispatchReport(interaction, request);
            }));
    }

    public Uni<AwaitUnitRecord> prepareUnit(AwaitWorkerUnitRequest request) {
        return authorize(request.scope()).chain(authorized -> {
            validateUnitRequest(authorized, request);
            return coordinator.prepareAuthoritativeUnit(request,
                PinnedAwaitDescriptor.resolve(authorized.release().contract(), request.stepId()));
        });
    }

    public Uni<AwaitUnitRecord> sealUnit(AwaitUnitSealRequest request) {
        return retainedUnit(request.unit()).chain(unit -> coordinator.findByUnit(unit.tenantId(), unit.unitId())
            .chain(interactions -> {
                var indexes = new java.util.HashSet<Integer>();
                for (var interaction : interactions) {
                    if (interaction.itemIndex() == null || interaction.itemIndex() < 0
                        || interaction.itemIndex() >= request.expectedItemCount() || !indexes.add(interaction.itemIndex())) {
                        throw new IllegalStateException("Await seal differs from admitted item indexes");
                    }
                }
                if (indexes.size() != request.expectedItemCount()) {
                    throw new IllegalStateException("Await seal count differs from admitted interactions");
                }
                return coordinator.reconcileCompletedItemInteractions(unit.tenantId(), unit.unitId(), clock.millis())
                    .chain(() -> coordinator.markDispatchComplete(unit.tenantId(), unit.unitId(), request.expectedItemCount(), clock.millis()));
            }));
    }

    public Uni<TransitionAwaitSuspension> suspension(AwaitWorkerUnitRequest request) {
        return retainedUnit(request).chain(unit -> coordinator.suspensionSnapshot(new AwaitSuspendedException(
            unit.tenantId(), unit.executionId(), unit.unitId(), unit.stepIndex())));
    }

    public Uni<SerializedTransitionPayload> resume(AwaitWorkerUnitRequest request) {
        return retainedUnit(request).chain(unit -> coordinator.loadResumePayload(unit.tenantId(), unit.unitId()))
            .map(value -> new JsonTransitionPayloadCodec().encode(value));
    }

    private Uni<AwaitUnitRecord> retainedUnit(AwaitWorkerUnitRequest request) {
        return retained(request.scope()).chain(authorized -> {
            PinnedAwaitDescriptor.resolve(authorized.release().contract(), request.stepId());
            var occurrence = AwaitUnitOccurrence.fromUnitId(request.unitId()).orElseThrow(() ->
                new IllegalStateException("Remote Await requires retained occurrence identity"));
            if (!occurrence.tenantId().equals(request.scope().tenantId())
                || !occurrence.executionId().equals(request.scope().executionId())
                || !occurrence.identityScope().equals(request.identityScope())
                || !occurrence.stepId().equals(request.stepId()) || occurrence.stepIndex() != request.stepIndex()
                || request.invocationPath().isPresent() && !occurrence.invocationFingerprint().equals(
                    occurrence.withInvocationPath(request.invocationPath().orElseThrow()).invocationFingerprint())) {
                throw new IllegalStateException("Await request differs from retained occurrence identity");
            }
            return coordinator.getUnit(request.scope().tenantId(), request.unitId()).invoke(unit -> {
                if (!unit.unitId().equals(request.unitId()) || !unit.tenantId().equals(request.scope().tenantId()) || !unit.executionId().equals(request.scope().executionId())
                    || !unit.stepId().equals(request.stepId()) || unit.stepIndex() != request.stepIndex()) {
                    throw new IllegalStateException("Await unit differs from admitted occurrence");
                }
            });
        });
    }

    private static void validateUnitRequest(Authorized authorized, AwaitWorkerUnitRequest request) {
        PinnedAwaitDescriptor.resolve(authorized.release().contract(), request.stepId())
            .validatePosition(request.stepIndex(), request.invocationPath());
        var context = context(authorized.execution(), request.stepIndex(), request.invocationPath(), Map.of());
        validateOccurrence(authorized.execution(), context, request.stepId(), request.identityScope(), request.unitId(),
            request.identityScope() + ":" + request.stepIndex(), Optional.empty());
    }

    private Uni<Authorized> retained(AwaitTransitionScope scope) {
        return executions().getExecution(scope.tenantId(), scope.executionId()).chain(found -> {
            var execution = found.orElseThrow(() -> new IllegalStateException("Execution is absent"));
            if (!scope.executionId().equals(execution.executionId())
                || !scope.tenantId().equals(execution.tenantId()) || !scope.pipelineId().equals(execution.pipelineId())
                || !scope.contractVersion().equals(execution.contractVersion()) || !scope.releaseVersion().equals(execution.releaseVersion())) {
                throw new IllegalStateException("Await operation differs from retained execution pin");
            }
            return releases.get(scope.tenantId(), scope.pipelineId(), scope.releaseVersion()).map(foundRelease -> {
                var release = foundRelease.orElseThrow(() -> new IllegalStateException("Pinned Release is absent"));
                if (!scope.tenantId().equals(release.tenantId()) || !scope.pipelineId().equals(release.pipelineId())
                    || !scope.contractVersion().equals(release.contractVersion()) || !scope.releaseVersion().equals(release.releaseVersion())) {
                    throw new IllegalStateException("Await operation differs from retained Release");
                }
                return new Authorized(execution, release);
            });
        });
    }

    private Uni<Authorized> authorize(AwaitTransitionScope scope) {
        Objects.requireNonNull(scope, "scope");
        return executions().getExecution(scope.tenantId(), scope.executionId()).chain(found -> {
            ExecutionRecord<Object, Object> record = found.orElseThrow(() -> new IllegalStateException("Execution is absent"));
            long now = clock.millis();
            if (!scope.tenantId().equals(record.tenantId()) || !scope.executionId().equals(record.executionId())
                || !scope.pipelineId().equals(record.pipelineId()) || !scope.contractVersion().equals(record.contractVersion())
                || !scope.releaseVersion().equals(record.releaseVersion()) || scope.claimedExecutionVersion() != record.version()
                || record.status() != ExecutionStatus.RUNNING || record.leaseOwner() == null || record.leaseOwner().isBlank()
                || record.leaseExpiresEpochMs() <= now || record.ttlEpochS() > 0 && record.ttlEpochS() <= now / 1000
                || !scope.transitionKey().equals(ClaimedSegment.from(record).transitionKey())) {
                throw new IllegalStateException("Await request does not own a live pinned transition claim");
            }
            return releases.get(scope.tenantId(), scope.pipelineId(), scope.releaseVersion()).map(release -> {
                PipelineReleaseRecord pin = release.orElseThrow(() -> new IllegalStateException("Pinned Release is absent"));
                if (!scope.tenantId().equals(pin.tenantId()) || !scope.pipelineId().equals(pin.pipelineId())
                    || !scope.contractVersion().equals(pin.contractVersion()) || !scope.releaseVersion().equals(pin.releaseVersion())) {
                    throw new IllegalStateException("Resolved Release differs from the transition pin");
                }
                return new Authorized(record, pin);
            });
        });
    }

    private static AwaitExecutionContext context(ExecutionRecord<Object, Object> execution, int stepIndex,
        Optional<org.pipelineframework.invocation.PipelineInvocationPath> path, Map<String, Object> trace) {
        if (stepIndex < execution.currentStepIndex()) throw new IllegalArgumentException("Await precedes the claimed segment");
        path.ifPresent(value -> {
            value.requireResumable();
            if (value.frames().isEmpty() && value.stepIndex() >= 0 && value.stepIndex() != stepIndex) {
                throw new IllegalArgumentException("Await root position differs from invocation position");
            }
            if (!value.frames().isEmpty() && value.frames().getFirst().callerStepIndex() != stepIndex) {
                throw new IllegalArgumentException("Await root position differs from invocation continuation");
            }
        });
        return new AwaitExecutionContext(execution.tenantId(), execution.executionId(), stepIndex,
            AwaitContinuationMode.DURABLE_HANDOFF, TerminalOutputOwnership.COORDINATOR, trace,
            execution.pagingState().map(PagedExecutionState::toTransitionContext), execution.awaitOccurrenceCause(), path);
    }

    private static void validateOccurrence(ExecutionRecord<Object, Object> execution, AwaitExecutionContext context,
        String stepId, String identityScope, String unitId, String causation, Optional<Integer> itemIndex) {
        if (execution.awaitOccurrenceCause().isEmpty()) {
            throw new IllegalStateException("Execution has no retained Await occurrence cause");
        }
        String expectedUnit = AwaitCoordinator.unitIdFor(stepId, execution.tenantId(), execution.executionId(),
            context.identityScope(), context.currentStepIndex(), Optional.of(context));
        String expectedCause = context.identityScope() + ":" + context.currentStepIndex() + itemIndex.map(index -> ":" + index).orElse("");
        if (!context.identityScope().equals(identityScope) || !expectedUnit.equals(unitId) || !expectedCause.equals(causation)) {
            throw new IllegalArgumentException("Await occurrence differs from its authorized execution context");
        }
    }

    /** Class-free canonical schema validation; envelope fingerprints alone cannot validate business bytes. */
    public static TypedDurablePayload validateCanonicalPayload(PipelineContractDescriptor contract,
        String expectedType, SerializedTransitionPayload framed) {
        if (!TypedDurablePayload.class.getName().equals(framed.payloadTypeId())
            || !JsonTransitionPayloadCodec.ENCODING.equals(framed.payloadEncoding())) {
            throw new IllegalArgumentException("Await registration requires canonical transition framing");
        }
        try {
            var reader = PipelineJson.mapper().reader().with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .with(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
            TypedDurablePayload payload = reader.forType(TypedDurablePayload.class).readValue(framed.payload());
            String identity = expectedType;
            Map<String, Object> definition = contract.canonicalTypes().get(identity);
            if (definition == null) {
                var matches = contract.canonicalTypes().entrySet().stream()
                    .filter(entry -> expectedType.equals(entry.getValue().get("runtimeClass"))).toList();
                if (matches.size() != 1) throw new IllegalArgumentException("Missing or ambiguous canonical Await input");
                identity = matches.getFirst().getKey(); definition = matches.getFirst().getValue();
            }
            if (!(definition.get("definitionFingerprint") instanceof String fingerprint) || fingerprint.isBlank()) {
                throw new IllegalArgumentException("Canonical definition fingerprint is absent");
            }
            boolean collection = payload.canonicalTypeId().equals("List<" + identity + ">");
            String expression = collection ? HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(("List<" + fingerprint + ">").getBytes(StandardCharsets.UTF_8))) : fingerprint;
            if (!payload.canonicalTypeId().equals(collection ? "List<" + identity + ">" : identity)
                || !payload.typeExpressionFingerprint().equals(expression)
                || !payload.catalogFingerprint().equals(contract.canonicalCatalogFingerprint())
                || !JsonDurablePayloadCodec.ENCODING.equals(payload.encoding())
                || payload.encodingVersion() != JsonDurablePayloadCodec.ENCODING_VERSION) {
                throw new IllegalArgumentException("Await request envelope differs from its pinned canonical boundary");
            }
            com.fasterxml.jackson.databind.JsonNode value = reader.forType(com.fasterxml.jackson.databind.JsonNode.class).readValue(payload.payload());
            var catalogue = CanonicalTypeCatalogue.fromCanonicalTypes(contract.canonicalTypes());
            if (collection) {
                if (!value.isArray()) throw new IllegalArgumentException("Canonical collection requires JSON array");
                for (var item : value) catalogue.validateAndCanonicalize(identity, item.toString());
            } else catalogue.validateAndCanonicalize(identity, value.toString());
            return payload;
        } catch (Exception failure) {
            throw new IllegalArgumentException("Invalid canonical Await registration payload", failure);
        }
    }

    private static boolean samePayload(TypedDurablePayload left, TypedDurablePayload right) {
        return left.canonicalTypeId().equals(right.canonicalTypeId())
            && left.typeExpressionFingerprint().equals(right.typeExpressionFingerprint())
            && left.catalogFingerprint().equals(right.catalogFingerprint()) && left.encoding().equals(right.encoding())
            && left.encodingVersion() == right.encodingVersion() && Arrays.equals(left.payload(), right.payload());
    }

    private ExecutionStateStore executions() {
        if (executionStore != null) return executionStore;
        return executionStores.stream().filter(store -> store.providerName().equalsIgnoreCase(config.stateProvider()))
            .max(java.util.Comparator.comparingInt(ExecutionStateStore::priority)).orElseThrow();
    }

    private AwaitInteractionStore interactions() {
        if (interactionStore != null) return interactionStore;
        return interactionStores.stream().filter(store -> store.providerName().equalsIgnoreCase(config.stateProvider()))
            .max(java.util.Comparator.comparingInt(AwaitInteractionStore::priority)).orElseThrow();
    }

    private record Authorized(ExecutionRecord<Object, Object> execution, PipelineReleaseRecord release) { }
}

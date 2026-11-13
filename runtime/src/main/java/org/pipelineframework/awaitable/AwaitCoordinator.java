package org.pipelineframework.awaitable;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.store.AwaitItemContinuationWorkStore;
import org.pipelineframework.awaitable.store.AwaitInteractionExecutionLookup;
import org.pipelineframework.orchestrator.AwaitItemContinuationCommand;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.admission.AwaitAdmissionReservation;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;
import org.pipelineframework.telemetry.PipelineReplayTelemetry;

/**
 * Coordinates await unit persistence, interaction dispatch, completion admission, and replay payload loading.
 */
@ApplicationScoped
public class AwaitCoordinator {
    private static final Logger LOG = Logger.getLogger(AwaitCoordinator.class);
    private static final String COMPLETION_PROJECTOR_METADATA = "tpf.await.completion.projector";

    @Inject
    Instance<AwaitInteractionStore> interactionStores;

    @Inject
    Instance<AwaitUnitStore> unitStores;

    @Inject
    Instance<AwaitTransportAdapter<?>> adapters;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    AwaitResumeTokenService resumeTokenService;

    @Inject
    AwaitAdmissionCoordinator awaitAdmissionCoordinator;

    @Inject
    AwaitCompletionDescriptorRegistry descriptorFactory;

    @Inject
    AwaitDurablePayloadResolver durablePayloadResolver;

    @Inject
    org.pipelineframework.orchestrator.RegisteredRestWorkerTargets registeredWorkers;

    @Inject
    org.pipelineframework.orchestrator.TransitionPayloadCodec transitionPayloadCodec;

    @Inject
    PipelineReplayTelemetry telemetry;

    @Inject
    AwaitTelemetry awaitTelemetry = AwaitTelemetry.disabled();

    /** Focused Await telemetry seam for runtime collaborators that already own this coordinator. */
    @Inject RemoteAwaitWorkerLifecycle remoteLifecycle;

    private boolean remoteAwait() { return remoteLifecycle != null && remoteLifecycle.enabled(); }

    boolean acceptsRemoteAwaitContext(AwaitExecutionContext context) {
        return remoteAwait() && context != null && context.transitionScope().isPresent();
    }

    public AwaitTelemetry awaitTelemetry() {
        return awaitTelemetry == null ? AwaitTelemetry.disabled() : awaitTelemetry;
    }

    private volatile AwaitInteractionStore resolvedInteractionStore;
    private volatile AwaitUnitStore resolvedUnitStore;
    private final Map<String, AwaitTransportAdapter<?>> resolvedAdapters = new ConcurrentHashMap<>();
    private final Map<String, AwaitCompletionDescriptor> directDescriptors = new ConcurrentHashMap<>();

    public Uni<AwaitCreateResult> createOrGet(
        AwaitCompletionDescriptor descriptor,
        String tenantId,
        String executionId,
        int stepIndex,
        String causationId,
        Object requestPayload,
        String assignee,
        String group
    ) {
        return createOrGet(descriptor, tenantId, executionId, executionId, stepIndex, causationId,
            requestPayload, assignee, group, traceMetadataForCurrentExecution());
    }

    Uni<AwaitCreateResult> createOrGet(
        AwaitCompletionDescriptor descriptor,
        String tenantId,
        String executionId,
        String identityScope,
        int stepIndex,
        String causationId,
        Object requestPayload,
        String assignee,
        String group,
        Map<String, Object> traceMetadata
    ) {
        return createOrGet(descriptor, tenantId, executionId, identityScope, stepIndex, causationId,
            requestPayload, assignee, group, traceMetadata, Optional.ofNullable(AwaitExecutionContextHolder.get()));
    }

    Uni<AwaitCreateResult> createOrGet(
        AwaitCompletionDescriptor descriptor,
        String tenantId,
        String executionId,
        String identityScope,
        int stepIndex,
        String causationId,
        Object requestPayload,
        String assignee,
        String group,
        Map<String, Object> traceMetadata,
        Optional<AwaitExecutionContext> executionContext
    ) {
        var invocationPath = executionContext.flatMap(AwaitExecutionContext::invocationPathForAwait);
        String unitId = unitIdFor(descriptor, tenantId, executionId, identityScope, stepIndex, executionContext);
        if (remoteAwait()) {
            return remoteLifecycle.register(descriptor, executionContext.orElseThrow(), unitId, causationId,
                Optional.empty(), requestPayload, assignee, group).map(AwaitWorkerRegistrationResult::creation);
        }
        return registerDescriptor(descriptor)
            .onItem().transformToUni(registered -> createOrGetUnit(
                registered, tenantId, unitId, executionId, stepIndex)
            .onItem().transformToUni(unit -> createInteraction(
                registered,
                unit.unitId(),
                tenantId,
                executionId,
                identityScope,
                stepIndex,
                causationId,
                requestPayload,
                null,
                assignee,
                group,
                traceMetadata, invocationPath)
                .onItem().transformToUni(created -> unitStore().attachPrimaryInteraction(
                        tenantId,
                        unit.unitId(),
                        created.record().interactionId(),
                        System.currentTimeMillis())
                    .map(attached -> {
                        if (AwaitUnitOccurrence.fromUnitId(unit.unitId()).isPresent()
                            && (attached.isEmpty() || !created.record().interactionId().equals(
                                attached.orElseThrow().primaryInteractionId()))) {
                            throw new IllegalStateException("Await occurrence rejected primary attachment");
                        }
                        return created;
                    }))));
    }

    public Uni<AwaitCreateResult> createOrGetItem(
        AwaitCompletionDescriptor descriptor,
        String tenantId,
        String executionId,
        int stepIndex,
        String causationId,
        Object requestPayload,
        String unitId,
        int itemIndex,
        String assignee,
        String group
    ) {
        return createOrGetItem(descriptor, tenantId, executionId, executionId, stepIndex, causationId,
            requestPayload, unitId, itemIndex, assignee, group, traceMetadataForCurrentExecution());
    }

    Uni<AwaitCreateResult> createOrGetItem(
        AwaitCompletionDescriptor descriptor,
        String tenantId,
        String executionId,
        String identityScope,
        int stepIndex,
        String causationId,
        Object requestPayload,
        String unitId,
        int itemIndex,
        String assignee,
        String group,
        Map<String, Object> traceMetadata
    ) {
        if (remoteAwait()) return remoteLifecycle.register(descriptor,
            Objects.requireNonNull(AwaitExecutionContextHolder.get(), "Await execution context"), unitId, causationId,
            Optional.of(itemIndex), requestPayload, assignee, group).map(AwaitWorkerRegistrationResult::creation);
        return registerDescriptor(descriptor)
            .onItem().transformToUni(registered -> {
                if (itemIndex < 0) {
                    return Uni.createFrom().failure(new IllegalArgumentException("itemIndex must be non-negative"));
                }
                return createOrGetUnit(registered, tenantId, unitId, executionId, stepIndex)
                    .onItem().transformToUni(unit -> createItemInPreparedUnit(
                        registered, unitId, tenantId, executionId, identityScope, stepIndex, causationId, requestPayload,
                        itemIndex, assignee, group, traceMetadata));
            });
    }

    /** Creates the durable unit once before a live itemized source begins concurrent dispatch. */
    public Uni<Void> prepareLiveItemizedUnit(
        AwaitCompletionDescriptor descriptor,
        String tenantId,
        String unitId,
        String executionId,
        int stepIndex
    ) {
        return registerDescriptor(descriptor)
            .onItem().transformToUni(registered -> createOrGetUnit(
                registered, tenantId, unitId, executionId, stepIndex))
            .replaceWithVoid();
    }

    /** Creates one item in a unit that was durably prepared by the live stream before source demand began. */
    public Uni<AwaitCreateResult> createOrGetPreparedItem(
        AwaitCompletionDescriptor descriptor,
        String tenantId,
        String executionId,
        int stepIndex,
        String causationId,
        Object requestPayload,
        String unitId,
        int itemIndex,
        String assignee,
        String group
    ) {
        return createOrGetPreparedItem(descriptor, tenantId, executionId, executionId, stepIndex, causationId,
            requestPayload, unitId, itemIndex, assignee, group, traceMetadataForCurrentExecution());
    }

    Uni<AwaitCreateResult> createOrGetPreparedItem(
        AwaitCompletionDescriptor descriptor,
        String tenantId,
        String executionId,
        String identityScope,
        int stepIndex,
        String causationId,
        Object requestPayload,
        String unitId,
        int itemIndex,
        String assignee,
        String group,
        Map<String, Object> traceMetadata
    ) {
        return registerDescriptor(descriptor)
            .onItem().transformToUni(registered -> {
                if (itemIndex < 0) {
                    return Uni.createFrom().failure(new IllegalArgumentException("itemIndex must be non-negative"));
                }
                return createItemInPreparedUnit(
                    registered, unitId, tenantId, executionId, identityScope, stepIndex, causationId, requestPayload,
                    itemIndex, assignee, group, traceMetadata);
            });
    }

    @SuppressWarnings("unchecked")
    public Uni<AwaitInteractionRecord> dispatch(AwaitCompletionDescriptor descriptor, AwaitInteractionRecord interaction) {
        if (remoteAwait()) {
            AwaitExecutionContext context = Objects.requireNonNull(AwaitExecutionContextHolder.get(), "Await execution context");
            @SuppressWarnings("unchecked") AwaitTransportAdapter<Object> selected = (AwaitTransportAdapter<Object>) adapter(descriptor.transportType());
            return remoteLifecycle.claim(context, interaction).chain(grant -> {
                Object canonical = remoteLifecycle.canonicalRequest(descriptor, grant);
                return selected.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(descriptor, grant,
                        AwaitPayloadSupport.normalize(descriptor.inputToTransport().apply(canonical))))
                    .onFailure().call(failure -> remoteLifecycle.report(context, grant, AwaitDispatchReportKind.FAILED,
                        Map.of(), Optional.ofNullable(failure.getMessage()).filter(value -> !value.isBlank()).or(() -> Optional.of("Dispatch failed")),
                        Optional.empty(), Optional.empty()).replaceWithVoid())
                    .chain(result -> remoteLifecycle.report(context, grant, AwaitDispatchReportKind.DISPATCHED,
                        result.metadata(), Optional.empty(), Optional.empty(), Optional.empty()))
                    .map(AwaitDispatchReportResult::record);
            });
        }
        return registerDescriptor(descriptor)
            .onItem().transformToUni(ignored -> dispatchRegistered(interaction));
    }

    @SuppressWarnings("unchecked")
    private Uni<AwaitInteractionRecord> dispatchRegistered(AwaitInteractionRecord interaction) {
        AwaitCompletionDescriptor registered = descriptorFor(interaction);
        AwaitTransportAdapter<Object> adapter = (AwaitTransportAdapter<Object>) adapter(registered.transportType());
        long nowEpochMs = System.currentTimeMillis();
        return interactionStore().markDispatching(
                interaction.tenantId(),
                interaction.interactionId(),
                interaction.version(),
                nowEpochMs)
            .onItem().transform(optional -> optional.orElseThrow(() ->
                new IllegalStateException("Await interaction dispatch transition lost OCC race: "
                    + interaction.interactionId())))
            .onItem().transformToUni(claimedInteraction -> awaitTelemetry.inProviderDispatchSpan(
                claimedInteraction,
                () -> adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                    registered,
                    claimedInteraction,
                    transportRequestPayload(registered, claimedInteraction))))
                .onFailure().call(failure -> interactionStore().fail(
                    claimedInteraction.tenantId(),
                    claimedInteraction.interactionId(),
                    claimedInteraction.version(),
                    failure.getMessage(),
                    System.currentTimeMillis())
                    .onItem().transformToUni(updated -> updated
                        .map(this::releaseAdmission)
                        .orElseGet(() -> Uni.createFrom().voidItem()))
                    .replaceWithVoid())
                .onItem().transformToUni(result -> interactionStore().markDispatched(
                    claimedInteraction.tenantId(),
                    claimedInteraction.interactionId(),
                    claimedInteraction.version(),
                    dispatchMetadata(claimedInteraction, result.metadata()),
                    System.currentTimeMillis()))
                .onItem().transformToUni(optional -> optional
                    .map(Uni.createFrom()::item)
                    .orElseGet(() -> resolvedAfterDispatchMetadataRace(claimedInteraction)))
                .onItem().invoke(this::recordInteractionDispatched));
    }

    /**
     * Dispatches an interaction owned by an active live stream.  The durable dispatch intent,
     * including the admission-reservation identity needed by a later retry, is committed before
     * sending.  Unlike the conservative handoff path, live dispatch does not synchronously wait
     * for a second post-send metadata transition: a completion may legally race the send and
     * complete a {@code DISPATCHING} interaction.
     */
    @SuppressWarnings("unchecked")
    public Uni<AwaitInteractionRecord> dispatchLive(AwaitCompletionDescriptor descriptor, AwaitInteractionRecord interaction) {
        return registerDescriptor(descriptor)
            .onItem().transformToUni(ignored -> dispatchLiveRegistered(interaction));
    }

    @SuppressWarnings("unchecked")
    private Uni<AwaitInteractionRecord> dispatchLiveRegistered(AwaitInteractionRecord interaction) {
        AwaitCompletionDescriptor registered = descriptorFor(interaction);
        AwaitTransportAdapter<Object> adapter = (AwaitTransportAdapter<Object>) adapter(registered.transportType());
        Uni<AwaitInteractionRecord> intended = interaction.status() == AwaitInteractionStatus.WAITING
            ? interactionStore().markDispatching(
                interaction.tenantId(),
                interaction.interactionId(),
                interaction.version(),
                dispatchMetadata(interaction, Map.of()),
                System.currentTimeMillis())
                .onItem().transform(optional -> optional.orElseThrow(() ->
                    new IllegalStateException("Await interaction live dispatch transition lost OCC race: "
                        + interaction.interactionId())))
            : Uni.createFrom().item(interaction);
        return intended.onItem().transformToUni(dispatching -> awaitTelemetry.inProviderDispatchSpan(
            dispatching,
            () -> adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                registered,
                dispatching,
                transportRequestPayload(registered, dispatching))))
            .onFailure().call(failure -> interactionStore().fail(
                dispatching.tenantId(),
                dispatching.interactionId(),
                dispatching.version(),
                failure.getMessage(),
                System.currentTimeMillis())
                .onItem().transformToUni(updated -> updated
                    .map(this::releaseAdmission)
                    .orElseGet(() -> Uni.createFrom().voidItem()))
                .replaceWithVoid())
            .replaceWith(dispatching)
            .invoke(this::recordInteractionDispatched));
    }

    private Map<String, Object> dispatchMetadata(AwaitInteractionRecord interaction, Map<String, Object> metadata) {
        Map<String, Object> merged = new java.util.LinkedHashMap<>(interaction.transportMetadata());
        if (metadata != null) {
            metadata.forEach((key, value) -> {
                if (!COMPLETION_PROJECTOR_METADATA.equals(key)) {
                    merged.put(key, value);
                }
            });
        }
        Map<String, Object> enriched = awaitAdmissionCoordinator == null ? Map.copyOf(merged)
            : awaitAdmissionCoordinator.dispatchMetadata(interaction, Map.copyOf(merged));
        Object pinnedProjector = interaction.transportMetadata().get(COMPLETION_PROJECTOR_METADATA);
        Map<String, Object> authoritative = new java.util.LinkedHashMap<>(enriched);
        authoritative.remove(COMPLETION_PROJECTOR_METADATA);
        if (pinnedProjector != null) {
            authoritative.put(COMPLETION_PROJECTOR_METADATA, pinnedProjector);
        }
        return Map.copyOf(authoritative);
    }

    /**
     * Resolves whether an await transport can participate in a live pending window.
     *
     * @param descriptor await descriptor
     * @return true when the transport feeds the live completion registry
     */
    public boolean supportsLiveAwaitWindow(AwaitCompletionDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        return !remoteAwait() && adapter(descriptor.transportType()).supportsLiveAwaitWindow(descriptor);
    }

    private Uni<AwaitInteractionRecord> resolvedAfterDispatchMetadataRace(AwaitInteractionRecord claimedInteraction) {
        return interactionStore().get(claimedInteraction.tenantId(), claimedInteraction.interactionId())
            .onItem().transform(optional -> {
                if (optional.isPresent() && optional.get().status().terminal()) {
                    return optional.get();
                }
                throw new IllegalStateException("Await interaction metadata update lost OCC race: "
                    + claimedInteraction.interactionId());
            });
    }

    public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command) {
        AwaitCompletionCommand normalized = normalizeCompletionCommand(command);
        if (durablePayloadResolver != null && durablePayloadResolver.serializedMode()) {
            return completeSerialized(normalized, 8).invoke(this::recordCompletionObservation);
        }
        return resolveForCompletion(normalized)
            .onItem().transformToUni(record -> validateCompletionAdmission(record, normalized)
                .onItem().transformToUni(safeCommand -> record.acknowledgementAwareCommandCallback()
                    ? completeAcknowledgedCommand(record, safeCommand, 8)
                    : Uni.createFrom().item(completionContract(record, safeCommand))
                        .chain(validated -> interactionStore().complete(validated.command()))))
            .invoke(this::recordCompletionObservation);
    }

    private void recordCompletionObservation(AwaitCompletionResult result) {
        if (!result.duplicate() && result.record().status() == AwaitInteractionStatus.COMPLETION_OBSERVED) {
            recordAdmissionLifecycle(AwaitReplayLifecycleEvent.COMPLETION_OBSERVED, result.record());
        }
    }

    private Uni<AwaitCompletionResult> completeSerialized(AwaitCompletionCommand command, int remainingRaces) {
        if (!(interactionStore() instanceof org.pipelineframework.awaitable.spi.FencedAwaitInteractionStore fenced)) {
            return Uni.createFrom().failure(new IllegalStateException("Serialized Await requires fenced completion"));
        }
        if (remainingRaces <= 0) {
            return Uni.createFrom().failure(new IllegalStateException("Await completion exceeded concurrent update limit"));
        }
        return resolveForCompletion(command).chain(record -> validateCompletionAdmission(record, command).chain(admitted -> {
            if (record.acknowledgementAwareCommandCallback()) {
                return completeAcknowledgedSerialized(record, admitted, remainingRaces);
            }
            Uni<AwaitCompletionCommand> projected;
            if (record.status() == AwaitInteractionStatus.COMPLETED
                || record.status() == AwaitInteractionStatus.COMPLETION_OBSERVED) {
                // An authoritative committed completion wins without re-running typed conversion or projection.
                projected = Uni.createFrom().item(admitted);
            } else {
                projected = executeValueOperation(record, admitted, AwaitValueOperation.NORMALIZE_AND_PROJECT_COMPLETION)
                    .map(value -> withResponsePayload(admitted, value));
            }
            return projected.chain(value -> fenced.complete(record, value))
                .chain(committed -> committed.map(Uni.createFrom()::item)
                    .orElseGet(() -> completeSerialized(command, remainingRaces - 1)));
        }));
    }

    private Uni<AwaitCompletionResult> completeAcknowledgedSerialized(AwaitInteractionRecord current,
        AwaitCompletionCommand command, int remainingRaces) {
        return executeValueOperation(current, command, AwaitValueOperation.NORMALIZE_COMPLETION).chain(callback -> {
            if (current.stagedCompletionPayload().isPresent()) {
                if (!PipelineJson.mapper().valueToTree(current.stagedCompletionPayload().orElseThrow())
                    .equals(PipelineJson.mapper().valueToTree(callback))) {
                    return Uni.createFrom().failure(new CommandCompletionRejectedException(
                        "Callback contradicts an admitted completion"));
                }
                if (current.status() == AwaitInteractionStatus.COMPLETION_OBSERVED
                    && current.commandAcknowledgement().isPresent()
                    && Boolean.TRUE.equals(current.transportMetadata().get("completionProjectionPending"))) {
                    return settleAcknowledgedCommand(current, CommandDispatchSettlement.SUCCEEDED,
                        Optional.empty(), remainingRaces)
                        .map(settled -> new AwaitCompletionResult(settled.record(), false));
                }
                return Uni.createFrom().item(new AwaitCompletionResult(current, true));
            }
            if (current.status() == AwaitInteractionStatus.COMPLETED) {
                return Uni.createFrom().failure(new CommandCompletionRejectedException(
                    "Completed interaction has no matching callback evidence"));
            }
            if (current.deadlineEpochMs() <= command.nowEpochMs()) {
                return Uni.createFrom().failure(new AwaitInteractionTerminalException("Await interaction timed out before completion"));
            }
            if (current.status() != AwaitInteractionStatus.DISPATCHING
                && current.status() != AwaitInteractionStatus.DISPATCHED
                && current.status() != AwaitInteractionStatus.COMPLETION_OBSERVED) {
                return Uni.createFrom().failure(new CommandCompletionRejectedException(
                    "Callback cannot be admitted while Command interaction is " + current.status()));
            }
            boolean acknowledged = current.commandAcknowledgement().isPresent();
            Uni<Object> projection = acknowledged
                ? executeValueOperation(current, withResponsePayload(command, callback), AwaitValueOperation.PROJECT_COMPLETION)
                    .map(value -> (Object) value)
                : Uni.createFrom().item(current.responsePayload());
            return projection.chain(response -> {
                AwaitInteractionRecord next = commandState(current,
                    acknowledged ? AwaitInteractionStatus.COMPLETED : AwaitInteractionStatus.COMPLETION_OBSERVED,
                    response, command.actor(), current.transportMetadata(), current.commandAcknowledgement(),
                    Optional.of(callback), command.nowEpochMs());
                return interactionStore().compareAndSetCommandCompletion(current, next)
                    .chain(updated -> updated.map(record -> Uni.createFrom().item(new AwaitCompletionResult(record, false)))
                        .orElseGet(() -> completeSerialized(command, remainingRaces - 1)));
            });
        });
    }

    private Uni<TypedDurablePayload> executeValueOperation(AwaitInteractionRecord record,
        AwaitCompletionCommand command, AwaitValueOperation operation) {
        return executeValueOperation(record, command, operation, record.commandAcknowledgement());
    }

    private Uni<TypedDurablePayload> executeValueOperation(AwaitInteractionRecord record,
        AwaitCompletionCommand command, AwaitValueOperation operation, Optional<Object> acknowledgement) {
        PinnedAwaitDescriptor pinned = durablePayloadResolver.pinnedDescriptor(record);
        var coordinate = durablePayloadResolver.coordinateFor(record);
        AwaitValueOperationRequest request = new AwaitValueOperationRequest(operation, UUID.randomUUID().toString(),
            record.tenantId(), record.executionId(), record.interactionId(), record.unitId(), record.stepId(), record.version(),
            coordinate.pipelineId(), coordinate.contractVersion(), coordinate.releaseVersion(), pinned.completionProjectorId(),
            pinned.callbackContract().map(AwaitValueOperationExecutor::callbackFingerprint),
            Optional.of(transitionPayloadCodec.encode(record.requestPayload())),
            operation == AwaitValueOperation.NORMALIZE_ACKNOWLEDGEMENT
                ? Optional.of(valueOperationSlot(command.responsePayload()))
                : acknowledgement.map(transitionPayloadCodec::encode),
            operation == AwaitValueOperation.NORMALIZE_ACKNOWLEDGEMENT ? Optional.empty()
                : Optional.of(valueOperationSlot(command.responsePayload())),
            command.actor() == null ? "" : command.actor(), command.nowEpochMs());
        return registeredWorkers.executeAwaitValueOperation(request).map(result -> {
            if (!result.matches(request)) throw new IllegalArgumentException("Await value response does not match request");
            Object value = transitionPayloadCodec.decode(result.value());
            TypedDurablePayload envelope = TypedDurablePayload.fromDurableValue(value)
                .orElseThrow(() -> new IllegalArgumentException("Await worker did not return a canonical envelope"));
            return (TypedDurablePayload) durablePayloadResolver.decodeEnvelope(record,
                operation == AwaitValueOperation.NORMALIZE_COMPLETION
                    ? AwaitDurablePayloadResolver.Slot.STAGED_COMPLETION
                    : operation == AwaitValueOperation.NORMALIZE_ACKNOWLEDGEMENT
                        ? AwaitDurablePayloadResolver.Slot.COMMAND_ACKNOWLEDGEMENT
                        : AwaitDurablePayloadResolver.Slot.RESPONSE,
                envelope);
        });
    }

    /**
     * Persists an already authorized worker occurrence using immutable compiled facts.
     * The scoped authority must validate the parent claim, canonical request and occurrence first.
     * This path never resolves an application class or performs an external dispatch.
     */
    public Uni<AwaitCreateResult> createAuthoritativeRegistration(AwaitRegistrationRequest request,
        PinnedAwaitDescriptor descriptor, TypedDurablePayload canonicalRequest, AwaitExecutionContext context) {
        var scope = request.scope();
        String expectedUnit = unitIdFor(descriptor.stepId(), scope.tenantId(), scope.executionId(),
            request.identityScope(), request.stepIndex(), Optional.of(context));
        if (!expectedUnit.equals(request.unitId())) {
            return Uni.createFrom().failure(new IllegalArgumentException("Await proposal differs from execution occurrence"));
        }
        long now = System.currentTimeMillis();
        long deadline = Math.addExact(now, descriptor.timeout().toMillis());
        long ttl = Instant.ofEpochMilli(deadline).plusSeconds(86_400).getEpochSecond();
        String completionScope = AwaitUnitOccurrence.fromUnitId(request.unitId()).isPresent()
            ? request.unitId() : request.identityScope();
        JsonNode body;
        try {
            body = PipelineJson.mapper().readTree(canonicalRequest.payload());
        } catch (java.io.IOException failure) {
            return Uni.createFrom().failure(failure);
        }
        String idempotency = deriveIdempotencyKey(descriptor.stepId(), descriptor.idempotencyKeyFields(),
            scope.executionId(), completionScope, body)
            + (descriptor.callbackContract().isPresent() ? ":step=" + request.stepIndex() : "")
            + request.itemIndex().map(index -> ":item=" + index).orElse("");
        String correlation = deriveCorrelationId(descriptor.stepId(), descriptor.correlationStrategy(),
            scope.tenantId(), completionScope, idempotency);
        var trace = new java.util.LinkedHashMap<String, Object>(context.traceMetadata());
        for (String reserved : List.of("completionMode", "callbackSelection", "commandCompletionMode",
            "commandAcknowledgementType", COMPLETION_PROJECTOR_METADATA)) trace.remove(reserved);
        descriptor.callbackContract().ifPresent(callback -> {
            trace.put("completionMode", "CONNECTOR_CALLBACK");
            trace.put("callbackSelection", callback);
        });
        descriptor.commandAcknowledgementTypeId().ifPresent(type -> {
            trace.put("commandCompletionMode", "ACKNOWLEDGEMENT_REQUIRED");
            trace.put("commandAcknowledgementType", type);
        });
        descriptor.completionProjectorId().ifPresent(projector -> trace.put(COMPLETION_PROJECTOR_METADATA, projector));
        AwaitCreateCommand command = new AwaitCreateCommand(scope.tenantId(), scope.executionId(), descriptor.stepId(),
            request.stepIndex(), descriptor.outputTypeId(), descriptor.transportOutputTypeId(), request.causationId(),
            idempotency, correlation, canonicalRequest, request.assignee().orElse(""), request.group().orElse(""),
            descriptor.transportType(), request.unitId(), request.itemIndex().orElse(null), Map.copyOf(trace),
            now, deadline, ttl).withInvocationPath(request.invocationPath());
        return unitStore().createOrGet(new AwaitUnitCreateCommand(scope.tenantId(), request.unitId(), scope.executionId(),
            descriptor.stepId(), request.stepIndex(), descriptor.cardinality(), now, ttl)).chain(unit -> {
                if (!unit.executionId().equals(scope.executionId()) || !unit.stepId().equals(descriptor.stepId())
                    || unit.stepIndex() != request.stepIndex()) {
                    return Uni.createFrom().failure(new IllegalArgumentException("Await unit belongs to another occurrence"));
                }
                if (AwaitUnitOccurrence.fromUnitId(request.unitId()).isPresent()
                    && (unit.status().terminal() || unit.dispatchComplete())) {
                    return interactionStore().findByCorrelation(scope.tenantId(), correlation).map(found -> found
                        .filter(record -> request.unitId().equals(record.unitId()) && idempotency.equals(record.idempotencyKey()))
                        .map(record -> new AwaitCreateResult(record, true))
                        .orElseThrow(() -> new IllegalStateException("Await occurrence is sealed against new members")));
                }
                Optional<String> endpoint = Optional.empty();
                if (awaitAdmissionCoordinator != null && awaitAdmissionCoordinator.enabled()) {
                    endpoint = descriptor.callbackContract().map(callback -> (String) callback.get("binding"));
                    if (endpoint.isEmpty() && !"interaction-api".equals(descriptor.transportType())) {
                        return Uni.createFrom().failure(new IllegalStateException(
                            "Remote Await endpoint admission requires a trusted endpoint binding"));
                    }
                }
                Uni<Optional<AwaitAdmissionCoordinator.AdmissionLease>> admission = awaitAdmissionCoordinator == null
                    ? Uni.createFrom().item(Optional.empty())
                    : awaitAdmissionCoordinator.acquire(scope.pipelineId(), descriptor.stepId(), endpoint,
                        scope.tenantId(), request.unitId(), request.itemIndex().orElse(null), scope.executionId(), deadline);
                return admission.chain(lease -> interactionStore().createOrGet(command)
                    .chain(created -> bindOrReleaseAdmission(created, lease))
                    .invoke(created -> { if (!created.duplicate()) awaitTelemetry.recordInteractionCreated(created.record()); })
                    .onFailure().call(ignored -> releaseAdmissionAfterDefiniteCreateFailure(lease, scope.tenantId(), correlation)));
            }).chain(created -> {
                if (request.itemIndex().isPresent()) return Uni.createFrom().item(created);
                return unitStore().attachPrimaryInteraction(scope.tenantId(), request.unitId(),
                    created.record().interactionId(), System.currentTimeMillis()).map(attached -> {
                        if (AwaitUnitOccurrence.fromUnitId(request.unitId()).isPresent()
                            && (attached.isEmpty() || !created.record().interactionId()
                                .equals(attached.orElseThrow().primaryInteractionId()))) {
                            throw new IllegalStateException("Await occurrence rejected primary attachment");
                        }
                        return created;
                    });
            });
    }

    /** Registers callback completion before any effect is attempted. */
    private SerializedTransitionPayload valueOperationSlot(Object value) {
        return value instanceof SerializedTransitionPayload payload ? payload : transitionPayloadCodec.encode(value);
    }

    public Uni<AwaitUnitRecord> prepareAuthoritativeUnit(AwaitWorkerUnitRequest request, PinnedAwaitDescriptor descriptor) {
        long now = System.currentTimeMillis();
        long ttl = Instant.ofEpochMilli(now + descriptor.timeout().toMillis()).plusSeconds(86_400).getEpochSecond();
        return unitStore().createOrGet(new AwaitUnitCreateCommand(request.scope().tenantId(), request.unitId(),
            request.scope().executionId(), descriptor.stepId(), request.stepIndex(), descriptor.cardinality(), now, ttl));
    }

    public Uni<AwaitDispatchReportResult> applyAuthoritativeDispatchReport(AwaitInteractionRecord interaction,
        AwaitDispatchReportRequest report) {
        durablePayloadResolver.pinnedDescriptor(interaction);
        if (report.kind() == AwaitDispatchReportKind.COMMAND_SETTLEMENT) {
            if (!interaction.commandCallback()) return Uni.createFrom().failure(new IllegalArgumentException("Interaction is not a Command callback"));
            return settleCommandDispatch(interaction, report.settlement().orElseThrow(),
                report.acknowledgement().map(value -> (Object) value))
                .call(result -> result.record().status().terminal() || result.completedByDispatch()
                    ? recordCompletion(result.record(), System.currentTimeMillis())
                        .chain(() -> releaseAdmission(result.record())) : Uni.createFrom().voidItem())
                .map(result -> new AwaitDispatchReportResult(result.record(), result.completedByDispatch()));
        }
        if (interaction.commandCallback()) return Uni.createFrom().failure(new IllegalArgumentException("Command requires settlement evidence"));
        if (interaction.status() == AwaitInteractionStatus.COMPLETED || interaction.status() == AwaitInteractionStatus.DISPATCHED
            || interaction.status() == AwaitInteractionStatus.TIMED_OUT || interaction.status() == AwaitInteractionStatus.FAILED) {
            return Uni.createFrom().item(new AwaitDispatchReportResult(interaction, false));
        }
        if (interaction.status() != AwaitInteractionStatus.DISPATCHING) {
            return Uni.createFrom().failure(new IllegalStateException("Interaction has no admitted dispatch"));
        }
        long now = System.currentTimeMillis();
        Uni<Optional<AwaitInteractionRecord>> update = report.kind() == AwaitDispatchReportKind.DISPATCHED
            ? interactionStore().markDispatched(interaction.tenantId(), interaction.interactionId(), interaction.version(),
                dispatchMetadata(interaction, report.transportMetadata()), now)
            : interactionStore().fail(interaction.tenantId(), interaction.interactionId(), interaction.version(), report.failureMessage().orElseThrow(), now);
        return update.chain(changed -> {
            if (changed.isEmpty()) return resolvedAfterDispatchMetadataRace(interaction)
                .map(record -> new AwaitDispatchReportResult(record, false));
            var record = changed.orElseThrow();
            if (report.kind() == AwaitDispatchReportKind.FAILED) return releaseAdmission(record)
                .replaceWith(new AwaitDispatchReportResult(record, false));
            recordInteractionDispatched(record);
            return Uni.createFrom().item(new AwaitDispatchReportResult(record, false));
        });
    }

    /** Issues only opaque callback authority for an already registered, pinned interaction. */
    public Optional<String> issueRegisteredCallbackToken(AwaitInteractionRecord record) {
        if (durablePayloadResolver == null || !durablePayloadResolver.serializedMode()) {
            throw new IllegalStateException("Remote callback issuance requires serialized coordination");
        }
        return durablePayloadResolver.pinnedDescriptor(record).callbackContract()
            .map(ignored -> resumeTokenService.sign(record, record.createdAtEpochMs()));
    }

    public Uni<AwaitWorkerRegistrationResult> registerCommandCompletionWithAuthority(
        AwaitCompletionDescriptor descriptor, AwaitExecutionContext context, Object input) {
        if (!remoteAwait()) return registerCommandCompletion(descriptor, context, input)
            .map(created -> new AwaitWorkerRegistrationResult(created, Optional.empty()));
        if (descriptor.callback().isEmpty()) return Uni.createFrom().failure(
            new IllegalStateException("Command completion requires its compiled callback contract"));
        String unit = unitIdFor(descriptor, context.tenantId(), context.executionId(), context.identityScope(),
            context.currentStepIndex(), Optional.of(context));
        return remoteLifecycle.register(descriptor, context, unit,
            context.identityScope() + ":" + context.currentStepIndex(), Optional.empty(), input, "", "");
    }

    public Uni<Optional<AwaitInteractionRecord>> claimCommandDispatch(AwaitInteractionRecord expected,
        AwaitExecutionContext context) {
        return remoteAwait() ? remoteLifecycle.claim(context, expected).map(Optional::of)
            : claimCommandDispatch(expected);
    }

    public Uni<AwaitCreateResult> registerCommandCompletion(AwaitCompletionDescriptor descriptor,
        AwaitExecutionContext context, Object input) {
        if (descriptor.callback().isEmpty() || !interactionStore().supportsCommandCompletion()) {
            return Uni.createFrom().failure(new IllegalStateException("Command callback requires a supporting interaction store"));
        }
        return createOrGet(descriptor, context.tenantId(), context.executionId(), context.identityScope(),
            context.currentStepIndex(),
            context.identityScope() + ":" + context.currentStepIndex(), input, "", "", context.traceMetadata(),
            Optional.of(context))
            .invoke(created -> {
                validatePinnedCompletionProjector(created.record(), descriptor);
                if (created.duplicate() && !PipelineJson.mapper().valueToTree(input)
                    .equals(PipelineJson.mapper().valueToTree(created.record().requestPayload()))) {
                    throw new IllegalStateException("Command completion occurrence has incompatible canonical input");
                }
            });
    }

    public Uni<CommandCompletionSettlementResult> settleCommandDispatch(AwaitInteractionRecord expected,
        CommandDispatchSettlement settlement, Optional<Object> acknowledgement, AwaitExecutionContext context) {
        if (!remoteAwait()) return settleCommandDispatch(expected, settlement, acknowledgement);
        return remoteLifecycle.report(context, expected, AwaitDispatchReportKind.COMMAND_SETTLEMENT, Map.of(),
            Optional.empty(), Optional.of(settlement), acknowledgement)
            .map(result -> new CommandCompletionSettlementResult(result.record(), result.completedByDispatch()));
    }

    public Uni<AwaitUnitRecord> sealRemoteUnit(AwaitExecutionContext context, String unitId, int count) {
        return remoteLifecycle.seal(context, unitId, count);
    }

    public Uni<Object> resumeRemoteUnit(AwaitExecutionContext context, String unitId) {
        return remoteLifecycle.resume(context, unitId);
    }

    public Uni<Optional<AwaitInteractionRecord>> claimCommandDispatch(AwaitInteractionRecord expected) {
        return interactionStore().markDispatching(expected.tenantId(), expected.interactionId(), expected.version(),
            expected.transportMetadata(), System.currentTimeMillis());
    }

    public Uni<CommandCompletionSettlementResult> settleCommandDispatch(AwaitInteractionRecord expected,
        CommandDispatchSettlement settlement) {
        return settleCommandDispatch(expected, settlement, 8);
    }

    public Uni<CommandCompletionSettlementResult> settleCommandDispatch(AwaitInteractionRecord expected,
        CommandDispatchSettlement settlement, Optional<Object> acknowledgement) {
        if (!expected.acknowledgementAwareCommandCallback()) {
            return settleCommandDispatch(expected, settlement);
        }
        return settleAcknowledgedCommand(expected, settlement, acknowledgement, 8);
    }

    private Uni<CommandCompletionSettlementResult> settleAcknowledgedCommand(AwaitInteractionRecord expected,
        CommandDispatchSettlement settlement, Optional<Object> acknowledgement, int remainingRaces) {
        if (remainingRaces == 0) {
            return Uni.createFrom().failure(new IllegalStateException("Command acknowledgement settlement contention"));
        }
        return interactionStore().get(expected.tenantId(), expected.interactionId()).chain(found -> {
            AwaitInteractionRecord current = found.orElseThrow(() ->
                new AwaitInteractionNotFoundException("Command interaction disappeared"));
            boolean serialized = durablePayloadResolver != null && durablePayloadResolver.serializedMode();
            AwaitCompletionDescriptor descriptor = serialized ? null : descriptorFor(current);
            if (serialized) durablePayloadResolver.pinnedDescriptor(current);
            else validateDurableOutputContract(current, descriptor);
            if (current.status().terminal() || current.status() == AwaitInteractionStatus.DISPATCHED
                || current.status() == AwaitInteractionStatus.WAITING) {
                return Uni.createFrom().item(new CommandCompletionSettlementResult(current, false));
            }
            if (current.status() != AwaitInteractionStatus.DISPATCHING
                && current.status() != AwaitInteractionStatus.COMPLETION_OBSERVED) {
                return Uni.createFrom().failure(new IllegalStateException("Invalid Command acknowledgement state"));
            }
            Uni<Optional<Object>> accepted;
            if (current.commandAcknowledgement().isPresent()) {
                accepted = Uni.createFrom().item(current.commandAcknowledgement());
            } else if (settlement == CommandDispatchSettlement.SUCCEEDED && acknowledgement.isPresent()) {
                if (serialized) {
                    AwaitCompletionCommand valueCommand = new AwaitCompletionCommand(current.tenantId(), current.interactionId(),
                        null, null, current.idempotencyKey(), acknowledgement.orElseThrow(), current.actor(), System.currentTimeMillis());
                    accepted = executeValueOperation(current, valueCommand, AwaitValueOperation.NORMALIZE_ACKNOWLEDGEMENT)
                        .map(value -> Optional.<Object>of(value));
                } else {
                    accepted = Uni.createFrom().item(() -> Optional.of(coerceCommandAcknowledgement(descriptor,
                        acknowledgement.orElseThrow())));
                }
            } else {
                accepted = Uni.createFrom().item(Optional.empty());
            }
            return accepted.chain(acceptedAcknowledgement -> {
                Map<String, Object> metadata = new java.util.LinkedHashMap<>(current.transportMetadata());
                boolean hasAcknowledgement = acceptedAcknowledgement.isPresent();
                metadata.put("commandSettlement", hasAcknowledgement ? CommandDispatchSettlement.SUCCEEDED.name() : settlement.name());
                Optional<Object> staged = current.stagedCompletionPayload();
                long now = System.currentTimeMillis();
                Uni<AwaitInteractionRecord> proposal;
                if (current.deadlineEpochMs() > now && hasAcknowledgement && staged.isPresent()) {
                    Uni<Object> projection;
                    if (serialized) {
                        AwaitCompletionCommand valueCommand = new AwaitCompletionCommand(current.tenantId(), current.interactionId(),
                            null, null, current.idempotencyKey(), staged.orElseThrow(), current.actor(), current.updatedAtEpochMs());
                        projection = executeValueOperation(current, valueCommand, AwaitValueOperation.PROJECT_COMPLETION,
                            acceptedAcknowledgement).map(value -> (Object) value);
                    } else {
                        projection = Uni.createFrom().item(() -> projectAcknowledgedCompletion(current, descriptor,
                            acceptedAcknowledgement.orElseThrow(), staged.orElseThrow(), completionMetadata(current)));
                    }
                    proposal = projection.map(response -> {
                        metadata.remove("completionProjectionPending");
                        metadata.put("completionDelivery", "dispatch");
                        return commandState(current, AwaitInteractionStatus.COMPLETED, response, current.actor(), metadata,
                            acceptedAcknowledgement, staged, now);
                    }).onFailure(CommandCompletionRejectedException.class).recoverWithItem(failure -> {
                        metadata.remove("completionProjectionPending");
                        metadata.put("rejectedEarlyCallback", true);
                        return commandState(current, AwaitInteractionStatus.DISPATCHED, current.responsePayload(), current.actor(),
                            metadata, acceptedAcknowledgement, Optional.empty(), now);
                    }).onFailure().recoverWithItem(failure -> {
                        metadata.put("completionProjectionPending", true);
                        return commandState(current, AwaitInteractionStatus.COMPLETION_OBSERVED, current.responsePayload(), current.actor(),
                            metadata, acceptedAcknowledgement, staged, now);
                    });
                } else {
                    AwaitInteractionStatus status;
                    if (current.deadlineEpochMs() <= now) status = AwaitInteractionStatus.TIMED_OUT;
                    else if (hasAcknowledgement) status = AwaitInteractionStatus.DISPATCHED;
                    else if (settlement == CommandDispatchSettlement.AMBIGUOUS) {
                        status = staged.isEmpty() ? AwaitInteractionStatus.DISPATCHED : AwaitInteractionStatus.COMPLETION_OBSERVED;
                    } else if (settlement == CommandDispatchSettlement.RETRYABLE && staged.isEmpty()) {
                        status = AwaitInteractionStatus.WAITING;
                    } else {
                        status = AwaitInteractionStatus.FAILED;
                        if (staged.isPresent()) metadata.put("completionFailure", "contradictory-provider-evidence");
                    }
                    proposal = Uni.createFrom().item(commandState(current, status, current.responsePayload(), current.actor(),
                        metadata, acceptedAcknowledgement, staged, now));
                }
                return proposal.chain(next -> interactionStore().compareAndSetCommandCompletion(current, next))
                    .chain(updated -> updated.map(record -> {
                        if (record.status() == AwaitInteractionStatus.DISPATCHED || record.status() == AwaitInteractionStatus.COMPLETED) {
                            recordInteractionDispatched(record);
                        }
                        return Uni.createFrom().item(new CommandCompletionSettlementResult(record,
                            record.status() == AwaitInteractionStatus.COMPLETED));
                    }).orElseGet(() -> settleAcknowledgedCommand(current, settlement, acknowledgement, remainingRaces - 1)));
            });
        });
    }

    private Uni<CommandCompletionSettlementResult> settleCommandDispatch(AwaitInteractionRecord expected,
        CommandDispatchSettlement settlement, int remainingRaces) {
        if (remainingRaces == 0) {
            return Uni.createFrom().failure(new IllegalStateException("Command completion settlement contention"));
        }
        return interactionStore().get(expected.tenantId(), expected.interactionId())
            .chain(found -> {
                AwaitInteractionRecord current = found.orElseThrow(() -> new AwaitInteractionNotFoundException("Command interaction disappeared"));
                if (current.status().terminal() || current.status() == AwaitInteractionStatus.DISPATCHED
                    || current.status() == AwaitInteractionStatus.WAITING) {
                    return Uni.createFrom().item(new CommandCompletionSettlementResult(current, false));
                }
                return interactionStore().settleCommandDispatch(current, settlement, System.currentTimeMillis())
                    .invoke(updated -> updated.filter(record -> record.status() == AwaitInteractionStatus.DISPATCHED
                            || record.status() == AwaitInteractionStatus.COMPLETED)
                        .ifPresent(this::recordInteractionDispatched))
                    .chain(updated -> updated.map(record -> Uni.createFrom().item(
                            new CommandCompletionSettlementResult(record, record.status() == AwaitInteractionStatus.COMPLETED)))
                        .orElseGet(() -> settleCommandDispatch(current, settlement, remainingRaces - 1)));
            });
    }

    private Uni<AwaitCompletionResult> completeAcknowledgedCommand(AwaitInteractionRecord initial,
        AwaitCompletionCommand command, int remainingRaces) {
        if (remainingRaces == 0) {
            return Uni.createFrom().failure(new IllegalStateException("Command callback admission contention"));
        }
        return interactionStore().get(initial.tenantId(), initial.interactionId()).chain(found -> {
            AwaitInteractionRecord current = found.orElseThrow(() ->
                new AwaitInteractionNotFoundException("Command interaction disappeared"));
            if (current.status().terminal() && current.status() != AwaitInteractionStatus.COMPLETED) {
                return Uni.createFrom().failure(new AwaitInteractionTerminalException(
                    "Await interaction is terminal: " + current.status()));
            }
            AwaitCompletionDescriptor descriptor = descriptorFor(current);
            validateDurableOutputContract(current, descriptor);
            Object callback = descriptor.outputFromTransport().apply(
                coerceTransportPayload(current, command.responsePayload()));
            if (callback == null) {
                return Uni.createFrom().failure(new CommandCompletionRejectedException(
                    "Callback produced no completion payload"));
            }
            if (current.stagedCompletionPayload().isPresent()) {
                boolean same = PipelineJson.mapper().valueToTree(current.stagedCompletionPayload().orElseThrow())
                    .equals(PipelineJson.mapper().valueToTree(callback));
                if (!same) {
                    return Uni.createFrom().failure(new CommandCompletionRejectedException(
                        "Callback contradicts an admitted completion"));
                }
                return Uni.createFrom().item(new AwaitCompletionResult(current, true));
            }
            if (current.status() == AwaitInteractionStatus.COMPLETED) {
                return Uni.createFrom().failure(new CommandCompletionRejectedException(
                    "Completed interaction has no matching callback evidence"));
            }
            if (current.deadlineEpochMs() <= command.nowEpochMs()) {
                return Uni.createFrom().failure(new AwaitInteractionTerminalException(
                    "Await interaction timed out before completion"));
            }
            if (current.status() != AwaitInteractionStatus.DISPATCHING
                && current.status() != AwaitInteractionStatus.DISPATCHED
                && current.status() != AwaitInteractionStatus.COMPLETION_OBSERVED) {
                return Uni.createFrom().failure(new CommandCompletionRejectedException(
                    "Callback cannot be admitted while Command interaction is " + current.status()));
            }
            boolean acknowledged = current.commandAcknowledgement().isPresent();
            Object response;
            try {
                response = acknowledged
                    ? projectAcknowledgedCompletion(current, descriptor, current.commandAcknowledgement().orElseThrow(), callback,
                        new AwaitCompletionMetadata(current.interactionId(), command.actor(),
                            Instant.ofEpochMilli(command.nowEpochMs())))
                    : current.responsePayload();
            } catch (CommandCompletionRejectedException invalidCallback) {
                return Uni.createFrom().failure(new CommandCompletionRejectedException(
                    "Callback does not match the Command acknowledgement"));
            }
            AwaitInteractionRecord next = commandState(current,
                acknowledged ? AwaitInteractionStatus.COMPLETED : AwaitInteractionStatus.COMPLETION_OBSERVED,
                response, command.actor(), current.transportMetadata(), current.commandAcknowledgement(),
                Optional.of(callback), command.nowEpochMs());
            return interactionStore().compareAndSetCommandCompletion(current, next)
                .chain(updated -> updated.map(record -> Uni.createFrom().item(new AwaitCompletionResult(record, false)))
                    .orElseGet(() -> completeAcknowledgedCommand(current, command, remainingRaces - 1)));
        });
    }

    private Object coerceCommandAcknowledgement(AwaitCompletionDescriptor descriptor, Object acknowledgement) {
        try {
            Class<?> expectedType = AwaitPayloadSupport.resolvePayloadClass(descriptor.commandAcknowledgementType().orElseThrow(),
                Thread.currentThread().getContextClassLoader());
            return AwaitPayloadSupport.coercePayload(acknowledgement, expectedType);
        } catch (ClassNotFoundException failure) {
            throw new IllegalStateException("Command acknowledgement type is unavailable", failure);
        }
    }

    private Object projectAcknowledgedCompletion(AwaitInteractionRecord record, AwaitCompletionDescriptor descriptor,
        Object acknowledgement, Object callback, AwaitCompletionMetadata metadata) {
        Object canonicalRequest = restoreCanonicalRequestPayload(descriptor, record.requestPayload());
        Object canonicalAcknowledgement = coerceCommandAcknowledgement(descriptor, acknowledgement);
        Object projected = descriptor.commandCompletionProjector().orElseThrow().project(canonicalRequest,
            canonicalAcknowledgement, callback, metadata);
        return coerceCanonicalPayload(record, projected);
    }

    private static AwaitInteractionRecord commandState(AwaitInteractionRecord current,
        AwaitInteractionStatus status, Object response, String actor, Map<String, Object> metadata,
        Optional<Object> acknowledgement, Optional<Object> stagedCompletion, long nowEpochMs) {
        return new AwaitInteractionRecord(current.tenantId(), current.executionId(), current.stepId(),
            current.stepIndex(), current.outputType(), current.interactionId(), current.correlationId(),
            current.causationId(), current.idempotencyKey(), current.version() + 1, status,
            current.requestPayload(), response, current.unitId(), current.itemIndex(), actor,
            current.assignee(), current.group(), current.transportType(), metadata,
            current.deadlineEpochMs(), current.createdAtEpochMs(), nowEpochMs, current.ttlEpochS(),
            current.transportOutputType(), acknowledgement, stagedCompletion, current.invocationPath());
    }

    private Uni<AwaitCompletionCommand> validateCompletionAdmission(
        AwaitInteractionRecord record,
        AwaitCompletionCommand command
    ) {
        if (record.status().terminal() && record.status() != AwaitInteractionStatus.COMPLETED) {
            return Uni.createFrom().failure(
                new AwaitInteractionTerminalException("Await interaction is terminal: " + record.status()));
        }
        if (command.resumeToken() == null) {
            if (record.commandCallback()) {
                return Uni.createFrom().failure(new AwaitResumeTokenRejectedException("Command callback requires a signed resume token"));
            }
            return Uni.createFrom().item(withResolvedInteractionId(command, record));
        }
        return Uni.createFrom().item(() -> {
                resumeTokenService.validate(command.resumeToken(), record, command.nowEpochMs());
                return withResolvedInteractionId(command, record);
            })
            .runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }

    private static AwaitCompletionCommand withResolvedInteractionId(
        AwaitCompletionCommand command,
        AwaitInteractionRecord record
    ) {
        if (command.interactionId() != null) {
            return command;
        }
        return new AwaitCompletionCommand(
            command.tenantId(),
            record.interactionId(),
            command.correlationId(),
            command.resumeToken(),
            command.idempotencyKey(),
            command.responsePayload(),
            command.actor(),
            command.nowEpochMs());
    }

    public Uni<AwaitUnitRecord> recordCompletion(AwaitInteractionRecord record, long nowEpochMs) {
        if (record.status() == AwaitInteractionStatus.TIMED_OUT) {
            return unitStore().markTerminal(record.tenantId(), record.unitId(), AwaitUnitStatus.TIMED_OUT, nowEpochMs)
                .onItem().transform(optional -> optional.orElseThrow(
                    () -> new IllegalStateException("Await unit not found for timed-out interaction " + record.unitId())))
                .onItem().invoke(unit -> recordUnitTerminal(record, unit));
        }
        if (record.status() == AwaitInteractionStatus.FAILED) {
            return unitStore().markTerminal(record.tenantId(), record.unitId(), AwaitUnitStatus.FAILED, nowEpochMs)
                .onItem().transform(optional -> optional.orElseThrow(
                    () -> new IllegalStateException("Await unit not found for failed interaction " + record.unitId())))
                .onItem().invoke(unit -> recordUnitTerminal(record, unit));
        }
        Uni<Optional<AwaitUnitRecord>> updated = record.itemInteraction()
            ? unitStore().recordItemCompleted(record.tenantId(), record.unitId(), itemCompletionKey(record), nowEpochMs)
            : unitStore().markCompleted(record.tenantId(), record.unitId(), nowEpochMs);
        return updated.onItem().transform(optional -> optional.orElseThrow(
            () -> new IllegalStateException("Await unit not found while recording completion: " + record.unitId())))
            .onItem().invoke(unit -> recordCompletionLifecycle(record, unit));
    }

    /**
     * Rebuilds the conservative itemized-await aggregate from durable interaction facts.
     * This is intentionally used only after a stream has entered the no-live-owner fallback.
     */
    public Uni<Void> reconcileCompletedItemInteractions(String tenantId, String unitId, long nowEpochMs) {
        return findByUnit(tenantId, unitId)
            .onItem().transformToMulti(records -> Multi.createFrom().iterable(records))
            .select().where(record -> record.itemInteraction() && record.status() == AwaitInteractionStatus.COMPLETED)
            .onItem().transformToUniAndConcatenate(record -> recordCompletion(record, nowEpochMs).replaceWithVoid())
            .collect().last()
            .replaceWithVoid();
    }

    private static String itemCompletionKey(AwaitInteractionRecord record) {
        if (AwaitUnitOccurrence.fromUnitId(record.unitId()).isPresent()) return "interaction:" + record.interactionId();
        return record.itemIndex() == null ? record.interactionId() : "item:" + record.itemIndex();
    }

    public Uni<AwaitUnitRecord> markDispatchComplete(String tenantId, String unitId, int expectedItemCount, long nowEpochMs) {
        return unitStore().markDispatchComplete(tenantId, unitId, expectedItemCount, nowEpochMs)
            .onItem().transform(optional -> optional.orElseThrow(
                () -> new IllegalStateException("Await unit not found while completing dispatch: " + unitId)))
            .onItem().invoke(this::recordUnitDispatchComplete);
    }

    public Uni<AwaitUnitRecord> getUnit(String tenantId, String unitId) {
        return unitStore().get(tenantId, unitId)
            .onItem().transform(optional -> optional.orElseThrow(
                () -> new AwaitInteractionNotFoundException("No await unit matches id " + unitId)));
    }

    public Uni<AwaitUnitRecord> recordItemContinuationCompleted(
        String tenantId,
        String unitId,
        int itemIndex,
        long nowEpochMs
    ) {
        return unitStore().recordItemContinuationCompleted(
                tenantId,
                unitId,
                AwaitUnitRecord.continuationCompletionKey(itemIndex),
                nowEpochMs)
            .onItem().transform(optional -> optional.orElseThrow(
                () -> new AwaitInteractionNotFoundException("No await unit matches id " + unitId)));
    }

    public Uni<Object> loadResumePayload(String tenantId, String unitId) {
        return loadResumePayload(tenantId, unitId, false);
    }

    /** Restores control data with a scalar completion before constructing the worker envelope. */
    public Uni<Object> loadResumeInput(String tenantId, String unitId) {
        return loadResumePayload(tenantId, unitId, true);
    }

    private Uni<Object> loadResumePayload(String tenantId, String unitId, boolean retainInvocationPath) {
        return getUnit(tenantId, unitId).onItem().transformToUni(unit -> {
            if (unit.primaryInteractionId() != null) {
                return interactionStore().get(tenantId, unit.primaryInteractionId())
                    .onItem().transform(optional -> optional.orElseThrow(
                        () -> new IllegalStateException("Await interaction not found for primary interaction id "
                            + unit.primaryInteractionId())))
                    .onItem().transform(record -> {
                        Object result = resumePayload(record);
                        return retainInvocationPath && record.invocationPath().isPresent()
                            ? new org.pipelineframework.orchestrator.ExecutionInputSnapshot(
                                org.pipelineframework.orchestrator.ExecutionInputShape.UNI, result,
                                Optional.empty(), record.invocationPath())
                            : result;
                    });
            }
            return interactionStore().findByUnit(tenantId, unitId)
                .onItem().transform(records -> {
                    var completedByItem = records.stream()
                        .filter(record -> record.status() == AwaitInteractionStatus.COMPLETED)
                        .collect(java.util.stream.Collectors.groupingBy(
                            record -> record.itemIndex() == null ? Integer.valueOf(-1) : record.itemIndex()));
                    return completedByItem.entrySet().stream()
                        .sorted(java.util.Map.Entry.comparingByKey())
                        .map(entry -> {
                            var group = entry.getValue();
                            return group.stream()
                                .max(java.util.Comparator.comparing(AwaitInteractionRecord::createdAtEpochMs)
                                    .thenComparing(AwaitInteractionRecord::version))
                                .orElseThrow();
                        })
                                .map(this::resumePayload)
                        .toList();
                });
        });
    }

    public Uni<List<AwaitInteractionRecord>> queryPending(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit) {
        return interactionStore().queryPending(tenantId, assignee, group, stepId, limit);
    }

    public Uni<List<AwaitInteractionRecord>> findByUnit(String tenantId, String unitId) {
        return interactionStore().findByUnit(tenantId, unitId);
    }

    /** Reads one bounded page of the current unit without walking execution history. */
    public Uni<AwaitInteractionPage> findByUnitPage(
        String tenantId, String unitId, int limit,
        Optional<AwaitInteractionCursor> cursor) {
        return interactionStore().findByUnitPage(tenantId, unitId, limit, cursor);
    }

    /** Finds Await interactions by their provider-neutral TPF execution identity. */
    public Uni<List<AwaitInteractionRecord>> findByExecution(
        String tenantId,
        String executionId,
        int limit
    ) {
        AwaitInteractionStore selected = interactionStore();
        if (selected instanceof AwaitInteractionExecutionLookup lookup) {
            return lookup.findByExecution(tenantId, executionId, limit);
        }
        return Uni.createFrom().failure(new IllegalStateException(
            "AwaitInteractionStore(" + selected.providerName()
                + ") does not support execution-scoped Await reconstruction"));
    }

    /** Finds bounded itemized-Await continuation work from the selected durable interaction provider. */
    public Uni<List<AwaitItemContinuationCommand>> findDueItemContinuations(long nowEpochMs, int limit) {
        return itemContinuationWorkStore().findDueItemContinuations(nowEpochMs, limit);
    }

    /** Advances only the mechanical retry projection; Await records remain semantic authority. */
    public Uni<Void> rescheduleItemContinuation(
        AwaitItemContinuationCommand command,
        int nextAttempt,
        long dueEpochMs) {
        return itemContinuationWorkStore().rescheduleItemContinuation(command, nextAttempt, dueEpochMs);
    }

    /** Removes completed mechanical work after the semantic continuation outcome is durable. */
    public Uni<Void> completeItemContinuation(AwaitItemContinuationCommand command) {
        return itemContinuationWorkStore().completeItemContinuation(command);
    }

    /** Reads one interaction for a provider-neutral control-plane checkpoint projection. */
    public Uni<Optional<AwaitInteractionRecord>> getInteraction(String tenantId, String interactionId) {
        return interactionStore().get(tenantId, interactionId);
    }

    public Uni<Optional<AwaitInteractionRecord>> getInteractionByCorrelation(String tenantId, String correlationId) {
        return interactionStore().findByCorrelation(tenantId, correlationId);
    }

    public Uni<Optional<AwaitCompletionAdmission>> getCompletionAdmission(String tenantId, String interactionId) {
        return interactionStore().getCompletionAdmission(tenantId, interactionId);
    }

    public Uni<AwaitCompletionPage> findOpenCompletions(
        String tenantId, int limit, Optional<AwaitCompletionCursor> cursor
    ) {
        return interactionStore().findOpenCompletions(tenantId, limit, cursor);
    }

    /** Caller must first verify the current unit convergence and retained parent-release acknowledgement. */
    public Uni<Void> retireCompletionAdmission(AwaitCompletionAdmission admission) {
        return interactionStore().retireCompletionAdmission(admission);
    }

    private AwaitItemContinuationWorkStore itemContinuationWorkStore() {
        AwaitInteractionStore selected = interactionStore();
        if (selected instanceof AwaitItemContinuationWorkStore workStore) {
            return workStore;
        }
        throw new IllegalStateException(
            "AwaitInteractionStore(" + selected.providerName()
                + ") does not support durable itemized-Await continuation work");
    }

    /** Preloads release-pinned payload metadata before concurrent item dispatch begins. */
    public Uni<Void> preloadDurablePayloads(String tenantId, String executionId) {
        return remoteAwait() || durablePayloadResolver == null
            ? Uni.createFrom().voidItem()
            : durablePayloadResolver.preload(tenantId, executionId);
    }

    public Uni<TransitionAwaitSuspension> suspensionSnapshot(AwaitSuspendedException suspended) {
        return getUnit(suspended.tenantId(), suspended.unitId())
            .onItem().transformToUni(unit -> findByUnit(suspended.tenantId(), suspended.unitId())
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .onItem().transform(interactions -> new TransitionAwaitSuspension(
                    suspended.tenantId(),
                    suspended.executionId(),
                    suspended.unitId(),
                    suspended.stepIndex(),
                    unit,
                    interactions.stream().map(this::transportSafeSnapshot).toList())));
    }

    private AwaitInteractionRecord transportSafeSnapshot(AwaitInteractionRecord interaction) {
        if (durablePayloadResolver != null && durablePayloadResolver.supportsTypedPayloads(interaction)) {
            return interaction.withPayloadSnapshots(
                typedSnapshot(interaction, AwaitDurablePayloadResolver.Slot.REQUEST, interaction.requestPayload()),
                typedSnapshot(interaction, AwaitDurablePayloadResolver.Slot.RESPONSE, interaction.responsePayload()));
        }
        return interaction.withPayloadSnapshots(
            AwaitPayloadSupport.normalize(interaction.requestPayload()),
            AwaitPayloadSupport.normalize(interaction.responsePayload()));
    }

    private TypedDurablePayload typedSnapshot(
        AwaitInteractionRecord interaction,
        AwaitDurablePayloadResolver.Slot slot,
        Object payload
    ) {
        if (payload == null) {
            return null;
        }
        return TypedDurablePayload.fromSerializedBytes(
            durablePayloadResolver.encode(interaction, slot, payload).getBytes(StandardCharsets.UTF_8))
            .orElseThrow(() -> new IllegalStateException("Await typed snapshot encoding failed for " + interaction.interactionId()));
    }

    public boolean requiresAuthoritativeSuspension() {
        return durablePayloadResolver != null && durablePayloadResolver.serializedMode();
    }

    /** A remote worker may identify an admitted Await but cannot import authoritative state. */
    public Uni<TransitionAwaitSuspension> authoritativeSuspension(TransitionAwaitSuspension proposed) {
        return getUnit(proposed.tenantId(), proposed.unitId()).chain(unit -> {
            if (!unit.tenantId().equals(proposed.tenantId()) || !unit.executionId().equals(proposed.executionId())
                || unit.stepIndex() != proposed.stepIndex()) {
                throw new IllegalStateException("Worker suspension differs from admitted Await unit");
            }
            return suspensionSnapshot(new AwaitSuspendedException(proposed.tenantId(), proposed.executionId(),
                proposed.unitId(), proposed.stepIndex())).map(retained -> proposed.pageCompletion()
                    .map(retained::withPageCompletion).orElse(retained));
        });
    }

    public Uni<Void> importSuspension(TransitionAwaitSuspension suspension) {
        if (requiresAuthoritativeSuspension()) {
            if (suspension == null) return Uni.createFrom().failure(new IllegalArgumentException("Await suspension is absent"));
            return authoritativeSuspension(suspension).replaceWithVoid();
        }
        if (suspension == null || suspension.unit() == null) {
            return Uni.createFrom().voidItem();
        }
        List<Uni<Void>> imports = new ArrayList<>();
        imports.add(unitStore().importRecord(suspension.unit()).replaceWithVoid());
        for (AwaitInteractionRecord interaction : suspension.interactions()) {
            imports.add(interactionStore().importRecord(interaction).replaceWithVoid());
        }
        return Uni.join().all(imports).andCollectFailures().replaceWithVoid();
    }

    public Uni<List<AwaitInteractionRecord>> findTimedOut(long nowEpochMs, int limit) {
        return interactionStore().findTimedOut(nowEpochMs, limit);
    }

    public Uni<List<AwaitInteractionRecord>> findDueTimeoutConsequences(long nowEpochMs, int limit) {
        return interactionStore().findDueTimeoutConsequences(nowEpochMs, limit);
    }

    public Uni<Void> completeTimeoutConsequences(AwaitInteractionRecord interaction) {
        return interactionStore().completeTimeoutConsequences(interaction);
    }

    public Uni<Optional<AwaitInteractionRecord>> markTimedOut(AwaitInteractionRecord record, long nowEpochMs) {
        return interactionStore().markTimedOut(record.tenantId(), record.interactionId(), record.version(), nowEpochMs)
            .onItem().transformToUni(updated -> updated
                .map(timedOut -> reconcileTimedOut(timedOut, nowEpochMs).replaceWith(updated))
                .orElseGet(() -> Uni.createFrom().item(Optional.empty())));
    }

    /** Replays consequences of the authoritative timeout winner without changing interaction state. */
    public Uni<Void> reconcileTimedOut(AwaitInteractionRecord record, long nowEpochMs) {
        Objects.requireNonNull(record, "record must not be null");
        if (record.status() != AwaitInteractionStatus.TIMED_OUT) {
            return Uni.createFrom().failure(new IllegalArgumentException("Timeout recovery requires a timed-out interaction"));
        }
        return interactionStore().getMetadata(record.tenantId(), record.interactionId())
            .onItem().transformToUni(current -> {
                var retained = current.orElseThrow(() -> new IllegalStateException("Timed-out interaction is unavailable"));
                if (retained.status() != AwaitInteractionStatus.TIMED_OUT || retained.version() != record.version()
                    || !Objects.equals(retained.tenantId(), record.tenantId())
                    || !Objects.equals(retained.interactionId(), record.interactionId())
                    || !Objects.equals(retained.executionId(), record.executionId())
                    || !Objects.equals(retained.unitId(), record.unitId())) {
                    return Uni.createFrom().failure(new IllegalStateException("Timeout observation no longer matches authority"));
                }
                return getUnit(record.tenantId(), record.unitId())
                    .onItem().transformToUni(unit -> {
                        if (!Objects.equals(unit.tenantId(), record.tenantId())
                            || !Objects.equals(unit.unitId(), record.unitId())
                            || !Objects.equals(unit.executionId(), record.executionId())) {
                            return Uni.createFrom().failure(new IllegalStateException("Timeout unit belongs to another execution"));
                        }
                        if (unit.status().terminal()) {
                            return Uni.createFrom().voidItem();
                        }
                        return unitStore().markTerminal(record.tenantId(), record.unitId(), AwaitUnitStatus.TIMED_OUT, nowEpochMs)
                            .invoke(updated -> {
                                var terminal = updated.orElseThrow(() -> new IllegalStateException("Timeout unit is unavailable"));
                                recordUnitTerminal(record, terminal);
                            }).replaceWithVoid();
                    })
                    .chain(() -> releaseTimeoutAdmission(record));
            });
    }

    private Uni<Void> releaseTimeoutAdmission(AwaitInteractionRecord interaction) {
        if (!admissionEnabled()) {
            return Uni.createFrom().voidItem();
        }
        return awaitAdmissionCoordinator.release(interaction)
            .invoke(released -> {
                if (released) {
                    recordAdmissionLifecycle(AwaitReplayLifecycleEvent.ADMISSION_RELEASED, interaction);
                }
            }).replaceWithVoid();
    }

    private void recordInteractionDispatched(AwaitInteractionRecord record) {
        awaitTelemetry.recordInteractionDispatched(record);
        recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
            AwaitReplayLifecycleEvent.INTERACTION_DISPATCHED,
            record.executionId(),
            record.unitId(),
            record.stepId(),
            record.stepIndex(),
            record.status().name(),
            record.interactionId(),
            record.correlationId(),
            record.transportType(),
            record.itemIndex(),
            null,
            null,
            null));
    }

    private void recordAdmissionLifecycle(String eventName, AwaitInteractionRecord record) {
        recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
            eventName,
            record.executionId(),
            record.unitId(),
            record.stepId(),
            record.stepIndex(),
            record.status().name(),
            record.interactionId(),
            record.correlationId(),
            record.transportType(),
            record.itemIndex(),
            null,
            null,
            null));
    }

    private void recordUnitDispatchComplete(AwaitUnitRecord unit) {
        awaitTelemetry.recordUnitDispatchComplete(unit);
        recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
            AwaitReplayLifecycleEvent.UNIT_DISPATCH_COMPLETE,
            unit.executionId(),
            unit.unitId(),
            unit.stepId(),
            unit.stepIndex(),
            unit.status().name(),
            unit.primaryInteractionId(),
            null,
            null,
            null,
            unit.expectedItemCount(),
            unit.completedItemCount(),
            unit.dispatchComplete()));
    }

    private void recordCompletionLifecycle(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        awaitTelemetry.recordCompletionAdmitted(record);
        if (record.itemInteraction()) {
            awaitTelemetry.recordItemCompleted(record, unit);
            recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
                AwaitReplayLifecycleEvent.UNIT_ITEM_COMPLETED,
                record.executionId(),
                record.unitId(),
                record.stepId(),
                record.stepIndex(),
                unit.status().name(),
                record.interactionId(),
                record.correlationId(),
                record.transportType(),
                record.itemIndex(),
                unit.expectedItemCount(),
                unit.completedItemCount(),
                unit.dispatchComplete()));
        }
        if (unit.status() == AwaitUnitStatus.COMPLETED) {
            recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
                AwaitReplayLifecycleEvent.UNIT_COMPLETED,
                unit.executionId(),
                unit.unitId(),
                unit.stepId(),
                unit.stepIndex(),
                unit.status().name(),
                record.interactionId(),
                record.correlationId(),
                record.transportType(),
                record.itemIndex(),
                unit.expectedItemCount(),
                unit.completedItemCount(),
                unit.dispatchComplete()));
        }
    }

    private void recordUnitTerminal(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        awaitTelemetry.recordUnitTerminal(record, unit);
        recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
            AwaitReplayLifecycleEvent.UNIT_TERMINAL,
            unit.executionId(),
            unit.unitId(),
            unit.stepId(),
            unit.stepIndex(),
            unit.status().name(),
            record.interactionId(),
            record.correlationId(),
            record.transportType(),
            record.itemIndex(),
            unit.expectedItemCount(),
            unit.completedItemCount(),
            unit.dispatchComplete()));
    }

    private void recordAwaitLifecycle(AwaitReplayLifecycleEvent lifecycleEvent) {
        if (telemetry != null) {
            telemetry.recordAwaitLifecycle(lifecycleEvent);
        }
    }

    private Uni<AwaitCreateResult> createInteraction(
        AwaitCompletionDescriptor descriptor,
        String unitId,
        String tenantId,
        String executionId,
        String identityScope,
        int stepIndex,
        String causationId,
        Object requestPayload,
        Integer itemIndex,
        String assignee,
        String group,
        Map<String, Object> traceMetadata
    ) {
        return createInteraction(descriptor, unitId, tenantId, executionId, identityScope, stepIndex, causationId, requestPayload, itemIndex, assignee, group, traceMetadata, Optional.ofNullable(AwaitExecutionContextHolder.get())
                .flatMap(AwaitExecutionContext::invocationPathForAwait));
    }

    private Uni<AwaitCreateResult> createInteraction(
        AwaitCompletionDescriptor descriptor,
        String unitId,
        String tenantId,
        String executionId,
        String identityScope,
        int stepIndex,
        String causationId,
        Object requestPayload,
        Integer itemIndex,
        String assignee,
        String group,
        Map<String, Object> traceMetadata,
        Optional<org.pipelineframework.invocation.PipelineInvocationPath> invocationPath
    ) {
        Object canonicalRequestPayload = restoreCanonicalRequestPayload(descriptor, requestPayload);
        long now = System.currentTimeMillis();
        long deadline = now + descriptor.timeout().toMillis();
        long ttl = Instant.ofEpochMilli(deadline).plusSeconds(86_400).getEpochSecond();
        String completionScope = AwaitUnitOccurrence.fromUnitId(unitId).isPresent() ? unitId : identityScope;
        String idempotencyKey = deriveIdempotencyKey(
            descriptor, executionId, completionScope, canonicalRequestPayload)
            + (descriptor.callback().isPresent() ? ":step=" + stepIndex : "")
            + (itemIndex == null ? "" : ":item=" + itemIndex);
        String correlationId = deriveCorrelationId(descriptor, tenantId, completionScope, idempotencyKey);
        if (AwaitUnitOccurrence.fromUnitId(unitId).isPresent()) {
            return getUnit(tenantId, unitId).onItem().transformToUni(unit -> {
                if (unit.status().terminal() || unit.dispatchComplete()) {
                    return interactionStore().findByCorrelation(tenantId, correlationId)
                        .map(found -> found.filter(record -> unitId.equals(record.unitId())
                            && idempotencyKey.equals(record.idempotencyKey()))
                            .map(record -> new AwaitCreateResult(record, true))
                            .orElseThrow(() -> new IllegalStateException("Await occurrence is sealed against new members")));
                }
                return createInteractionInOpenUnit(descriptor, unitId, tenantId, executionId, stepIndex,
                    causationId, canonicalRequestPayload, itemIndex, assignee, group, traceMetadata,
                    now, deadline, ttl, idempotencyKey, correlationId, invocationPath);
            });
        }
        return createInteractionInOpenUnit(descriptor, unitId, tenantId, executionId, stepIndex,
            causationId, canonicalRequestPayload, itemIndex, assignee, group, traceMetadata,
            now, deadline, ttl, idempotencyKey, correlationId, invocationPath);
    }

    private Uni<AwaitCreateResult> createInteractionInOpenUnit(
        AwaitCompletionDescriptor descriptor, String unitId, String tenantId, String executionId, int stepIndex,
        String causationId, Object canonicalRequestPayload, Integer itemIndex, String assignee, String group,
        Map<String, Object> traceMetadata, long now, long deadline, long ttl, String idempotencyKey, String correlationId,
        Optional<org.pipelineframework.invocation.PipelineInvocationPath> invocationPath
    ) {
        return acquireAdmission(descriptor, tenantId, unitId, itemIndex, executionId, deadline)
            .onItem().transformToUni(lease -> interactionStore().createOrGet(new AwaitCreateCommand(
                tenantId,
                executionId,
                descriptor.stepId(),
                stepIndex,
                descriptor.outputType(),
                descriptor.transportOutputType(),
                causationId,
                idempotencyKey,
                correlationId,
                canonicalRequestPayload,
                assignee,
                group,
                descriptor.transportType(),
                unitId,
                itemIndex,
                completionContractMetadata(descriptor, traceMetadata),
                now,
                deadline,
                ttl).withInvocationPath(invocationPath))
                .onItem().transformToUni(created -> bindOrReleaseAdmission(created, lease))
                .onItem().invoke(created -> {
                    if (!created.duplicate()) {
                    awaitTelemetry.recordInteractionCreated(created.record());
                    }
                })
                .onFailure().call(ignored -> releaseAdmissionAfterDefiniteCreateFailure(lease, tenantId, correlationId)));
    }

    private Uni<AwaitCreateResult> createItemInPreparedUnit(
        AwaitCompletionDescriptor descriptor,
        String unitId,
        String tenantId,
        String executionId,
        String identityScope,
        int stepIndex,
        String causationId,
        Object requestPayload,
        int itemIndex,
        String assignee,
        String group,
        Map<String, Object> traceMetadata
    ) {
        return createInteraction(
            descriptor, unitId, tenantId, executionId, identityScope, stepIndex, causationId, requestPayload,
            itemIndex, assignee, group, traceMetadata);
    }

    private static Object restoreCanonicalRequestPayload(AwaitCompletionDescriptor descriptor, Object requestPayload) {
        try {
            Class<?> canonicalType = AwaitPayloadSupport.resolvePayloadClass(
                descriptor.inputType(), Thread.currentThread().getContextClassLoader());
            return AwaitPayloadSupport.coercePayload(requestPayload, canonicalType);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed restoring await request payload to canonical type "
                    + descriptor.inputType() + " for step " + descriptor.stepId(),
                e);
        }
    }

    /**
     * The durable interaction owns the canonical request. The representation adapter is deliberately
     * applied only at dispatch, so protobuf/JSON transport values cannot become replay inputs.
     */
    private static Object transportRequestPayload(
        AwaitCompletionDescriptor descriptor,
        AwaitInteractionRecord interaction
    ) {
        Object canonical = restoreCanonicalRequestPayload(descriptor, interaction.requestPayload());
        return AwaitPayloadSupport.normalize(descriptor.inputToTransport().apply(canonical));
    }

    /**
     * A retry can acquire a new lease before discovering that its interaction already completed.
     * That lease never represented provider work, so release it instead of retaining it until TTL.
     */
    private Uni<AwaitCreateResult> bindOrReleaseAdmission(
        AwaitCreateResult created,
        Optional<AwaitAdmissionCoordinator.AdmissionLease> lease
    ) {
        if (awaitAdmissionCoordinator == null || lease.isEmpty()) {
            return Uni.createFrom().item(created);
        }
        if (created.duplicate() && created.record().status().terminal()) {
            return awaitAdmissionCoordinator.release(lease.orElseThrow().reservation())
                .onFailure().invoke(failure -> LOG.warnf(
                    failure,
                    "Failed releasing duplicate terminal await admission lease interactionId=%s",
                    created.record().interactionId()))
                .onFailure().recoverWithUni(failure -> Uni.createFrom().voidItem())
                .replaceWith(created);
        }
        bindAdmission(created.record(), lease);
        return Uni.createFrom().item(created);
    }

    /**
     * A failed create may have committed before the client observed the error. Retain the
     * lease unless a durable correlation lookup proves that no interaction exists.
     */
    private Uni<Void> releaseAdmissionAfterDefiniteCreateFailure(
        Optional<AwaitAdmissionCoordinator.AdmissionLease> lease,
        String tenantId,
        String correlationId
    ) {
        if (awaitAdmissionCoordinator == null || lease.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        return interactionStore().findByCorrelation(tenantId, correlationId)
            .onItem().transformToUni(existing -> existing.isEmpty()
                ? awaitAdmissionCoordinator.release(lease.orElseThrow().reservation()).replaceWithVoid()
                : Uni.createFrom().voidItem())
            // An unavailable or failed lookup is ambiguous, so TTL/reconciliation owns recovery.
            .onFailure().recoverWithUni(Uni.createFrom().voidItem());
    }

    private Uni<Optional<AwaitAdmissionCoordinator.AdmissionLease>> acquireAdmission(
        AwaitCompletionDescriptor descriptor,
        String tenantId,
        String unitId,
        Integer itemIndex,
        String executionId,
        long deadlineEpochMs
    ) {
        return awaitAdmissionCoordinator == null
            ? Uni.createFrom().item(Optional.empty())
            : awaitAdmissionCoordinator.acquire(descriptor, tenantId, unitId, itemIndex, executionId, deadlineEpochMs);
    }

    private void bindAdmission(AwaitInteractionRecord interaction, Optional<AwaitAdmissionCoordinator.AdmissionLease> lease) {
        if (awaitAdmissionCoordinator != null) {
            lease.ifPresent(activeLease -> awaitAdmissionCoordinator.bind(interaction, activeLease));
            if (lease.isPresent()) {
                AwaitAdmissionCoordinator.AdmissionLease activeLease = lease.orElseThrow();
                recordAdmissionLifecycle(
                    activeLease.reused() ? AwaitReplayLifecycleEvent.ADMISSION_REUSED : AwaitReplayLifecycleEvent.ADMISSION_ACQUIRED,
                    interaction);
                if (activeLease.reconciledExpired()) {
                    recordAdmissionLifecycle(AwaitReplayLifecycleEvent.ADMISSION_RECONCILED, interaction);
                }
            }
        }
    }

    /**
     * Releases the provider-facing reservation after a durable completion handoff.
     */
    public Uni<Void> releaseAdmission(AwaitInteractionRecord interaction) {
        if (!admissionEnabled()) {
            return Uni.createFrom().voidItem();
        }
        return awaitAdmissionCoordinator == null
            ? Uni.createFrom().voidItem()
            : awaitAdmissionCoordinator.release(interaction)
                .invoke(released -> {
                    if (released) {
                        recordAdmissionLifecycle(AwaitReplayLifecycleEvent.ADMISSION_RELEASED, interaction);
                    }
                })
                .onFailure().invoke(failure -> LOG.warnf(failure,
                    "Await admission release bookkeeping failed interactionId=%s", interaction.interactionId()))
                .onFailure().recoverWithItem(false)
                .replaceWithVoid();
    }

    public boolean admissionEnabled() {
        return awaitAdmissionCoordinator != null && awaitAdmissionCoordinator.enabled();
    }

    private Uni<AwaitUnitRecord> createOrGetUnit(
        AwaitCompletionDescriptor descriptor,
        String tenantId,
        String unitId,
        String executionId,
        int stepIndex
    ) {
        AwaitExecutionContext context = AwaitExecutionContextHolder.get();
        Optional.ofNullable(context).ifPresent(AwaitExecutionContext::invocationPathForAwait);
        if (context != null && context.awaitOccurrenceCause().isPresent()
            && !unitId.equals(unitIdFor(descriptor, tenantId, executionId, context.identityScope(), stepIndex))) {
            return Uni.createFrom().failure(new IllegalStateException("Await unit differs from the execution occurrence"));
        }
        long now = System.currentTimeMillis();
        long ttl = Instant.ofEpochMilli(now + descriptor.timeout().toMillis()).plusSeconds(86_400).getEpochSecond();
        return unitStore().createOrGet(new AwaitUnitCreateCommand(
            tenantId,
            unitId,
            executionId,
            descriptor.stepId(),
            stepIndex,
            descriptor.cardinality(),
            now,
            ttl));
    }

    private AwaitInteractionStore interactionStore() {
        AwaitInteractionStore cached = resolvedInteractionStore;
        if (cached != null) {
            return cached;
        }
        String provider = orchestratorConfig == null ? null : orchestratorConfig.stateProvider();
        synchronized (this) {
            if (resolvedInteractionStore == null) {
                resolvedInteractionStore = interactionStores.stream()
                    .filter(candidate -> provider == null || provider.isBlank() || provider.equalsIgnoreCase(candidate.providerName()))
                    .sorted((left, right) -> {
                        int priorityComparison = Integer.compare(right.priority(), left.priority());
                        if (priorityComparison != 0) {
                            return priorityComparison;
                        }
                        return left.providerName().compareToIgnoreCase(right.providerName());
                    })
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No AwaitInteractionStore provider is available"
                        + (provider == null || provider.isBlank() ? "" : " for provider " + provider)));
            }
            return resolvedInteractionStore;
        }
    }

    private AwaitUnitStore unitStore() {
        AwaitUnitStore cached = resolvedUnitStore;
        if (cached != null) {
            return cached;
        }
        String provider = orchestratorConfig == null ? null : orchestratorConfig.stateProvider();
        synchronized (this) {
            if (resolvedUnitStore == null) {
                resolvedUnitStore = unitStores.stream()
                    .filter(candidate -> provider == null || provider.isBlank() || provider.equalsIgnoreCase(candidate.providerName()))
                    .sorted((left, right) -> {
                        int priorityComparison = Integer.compare(right.priority(), left.priority());
                        if (priorityComparison != 0) {
                            return priorityComparison;
                        }
                        return left.providerName().compareToIgnoreCase(right.providerName());
                    })
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No AwaitUnitStore provider is available"
                        + (provider == null || provider.isBlank() ? "" : " for provider " + provider)));
            }
            return resolvedUnitStore;
        }
    }

    private AwaitTransportAdapter<?> adapter(String type) {
        return resolvedAdapters.computeIfAbsent(type.toLowerCase(java.util.Locale.ROOT), ignored -> resolveAdapter(type));
    }

    private AwaitTransportAdapter<?> resolveAdapter(String type) {
        List<AwaitTransportAdapter<?>> matching = adapters.stream()
            .filter(candidate -> type.equalsIgnoreCase(candidate.type()))
            .toList();
        if (matching.isEmpty()) {
            throw new IllegalStateException("No AwaitTransportAdapter provider is available for type " + type);
        }
        if (matching.size() > 1) {
            String providerInfo = matching.stream()
                .map(candidate -> candidate.getClass().getName())
                .collect(java.util.stream.Collectors.joining(", "));
            throw new IllegalStateException("Ambiguous AwaitTransportAdapter providers for type " + type + ": " + providerInfo);
        }
        return matching.get(0);
    }

    /** Resolves a provider callback without granting completion authority from untrusted lookup hints. */
    public Uni<AwaitInteractionRecord> resolveCallback(String token, long nowEpochMs) {
        var hints = resumeTokenService.lookupHints(token);
        return interactionStore().get(hints.tenantId(), hints.interactionId())
            .map(found -> found.orElseThrow(() -> new AwaitInteractionNotFoundException("Callback unavailable")))
            .invoke(record -> {
                resumeTokenService.validate(token, record, nowEpochMs);
                if (!record.commandCallback()) {
                    throw new AwaitResumeTokenRejectedException("Callback unavailable");
                }
            });
    }

    /** Reuses durable descriptor compatibility checks before ingress authenticates or maps a payload. */
    public AwaitCompletionDescriptor callbackDescriptor(AwaitInteractionRecord record) {
        if (durablePayloadResolver != null && durablePayloadResolver.serializedMode()) {
            throw new UnsupportedOperationException(
                "Typed connector callback ingress is unavailable in serialized Coordinator mode; "
                    + "use the authenticated completion boundary");
        }
        AwaitCompletionDescriptor descriptor = descriptorFor(record);
        validateDurableOutputContract(record, descriptor);
        if (descriptor.callback().isEmpty()) {
            throw new IllegalStateException("Callback descriptor unavailable");
        }
        return descriptor;
    }

    private Uni<AwaitInteractionRecord> resolveForCompletion(AwaitCompletionCommand command) {
        Uni<Optional<AwaitInteractionRecord>> lookup;
        if (command.interactionId() != null) {
            lookup = interactionStore().get(command.tenantId(), command.interactionId());
        } else if (command.correlationId() != null) {
            lookup = interactionStore().findByCorrelation(command.tenantId(), command.correlationId());
        } else {
            lookup = interactionStore().get(command.tenantId(), resumeTokenService.interactionIdHint(command.resumeToken()));
        }
        return lookup.onItem().transform(optional -> optional.orElseThrow(
            () -> new AwaitInteractionNotFoundException("No await interaction matches completion")));
    }

    private AwaitCompletionCommand normalizeCompletionCommand(AwaitCompletionCommand command) {
        return new AwaitCompletionCommand(
            command.tenantId(),
            command.interactionId(),
            command.correlationId(),
            command.resumeToken(),
            command.idempotencyKey(),
            requiresAuthoritativeSuspension()
                && (command.responsePayload() instanceof TypedDurablePayload
                    || command.responsePayload() instanceof SerializedTransitionPayload)
                ? command.responsePayload() : AwaitPayloadSupport.normalize(command.responsePayload()),
            command.actor(),
            command.nowEpochMs());
    }

    /**
     * Decodes one durable await interaction's transport payload and returns its canonical pipeline value.
     *
     * <p>The durable record preserves the canonical contract separately from the transport representation.
     * This method validates both identities against the rebuilt step descriptor before applying the generated
     * transport-to-canonical adapter.</p>
     *
     * @param record completed durable interaction
     * @return canonical pipeline payload
     */
    public Object resumePayload(AwaitInteractionRecord record) {
        if (remoteAwait()) return remoteLifecycle.canonicalCompletion(record);
        if (durablePayloadResolver != null && durablePayloadResolver.serializedMode()) {
            TypedDurablePayload payload = TypedDurablePayload.fromDurableValue(record.responsePayload())
                .orElseThrow(() -> new IllegalStateException("Serialized Await completion has no canonical envelope"));
            return durablePayloadResolver.decodeEnvelope(record, AwaitDurablePayloadResolver.Slot.RESPONSE, payload);
        }
        try {
            AwaitCompletionDescriptor descriptor = descriptorFor(record);
            validateDurableOutputContract(record, descriptor);
            Class<?> canonicalOutputType = AwaitPayloadSupport.resolvePayloadClass(
                record.outputType(),
                Thread.currentThread().getContextClassLoader());
            if (canonicalOutputType.isInstance(record.responsePayload())) {
                return record.responsePayload();
            }
            // Request-aware completion has always persisted its canonical projection. Records created
            // before projector IDs were pinned therefore need the same direct coercion on recovery.
            if (descriptor.requestAwareCompletion()) {
                return coerceCanonicalPayload(record, record.responsePayload());
            }
            return canonicalCompletionPayload(
                record,
                descriptor,
                record.responsePayload(),
                completionMetadata(record));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Failed resolving await canonical output type " + record.outputType()
                    + " for interaction " + record.interactionId() + " stepId=" + record.stepId(),
                e);
        }
    }

    private ValidatedCompletion completionContract(
        AwaitInteractionRecord record,
        AwaitCompletionCommand command
    ) {
        AwaitCompletionDescriptor descriptor = descriptorFor(record);
        validateDurableOutputContract(record, descriptor);
        Object completionPayload = !descriptor.requestAwareCompletion()
            && sameTypeIdentity(record.outputType(), record.transportOutputType())
            ? command.responsePayload()
            : canonicalCompletionPayload(
                record,
                descriptor,
                command.responsePayload(),
                new AwaitCompletionMetadata(
                    record.interactionId(),
                    command.actor(),
                    Instant.ofEpochMilli(command.nowEpochMs())));
        return new ValidatedCompletion(record, withResponsePayload(command, completionPayload), descriptor);
    }

    /**
     * Admits a transport completion at the await boundary and converts it before it crosses the
     * durable canonical boundary. New interaction records therefore persist canonical values;
     * the legacy transport projection remains readable only when restoring older records.
     */
    private Object canonicalCompletionPayload(
        AwaitInteractionRecord record,
        AwaitCompletionDescriptor descriptor,
        Object payload,
        AwaitCompletionMetadata metadata
    ) {
        Object transportPayload = coerceTransportPayload(record, payload);
        Object canonicalPayload;
        if (descriptor.requestAwareCompletion()) {
            Object canonicalRequest = restoreCanonicalRequestPayload(descriptor, record.requestPayload());
            Object canonicalCompletion = descriptor.outputFromTransport().apply(transportPayload);
            canonicalPayload = descriptor.completionProjector().project(canonicalRequest, canonicalCompletion, metadata);
        } else {
            canonicalPayload = descriptor.outputFromTransport().apply(transportPayload);
        }
        return coerceCanonicalPayload(record, canonicalPayload);
    }

    private static AwaitCompletionMetadata completionMetadata(AwaitInteractionRecord record) {
        long completedAtEpochMs = record.updatedAtEpochMs() > 0
            ? record.updatedAtEpochMs()
            : record.createdAtEpochMs();
        return new AwaitCompletionMetadata(
            record.interactionId(),
            record.actor(),
            Instant.ofEpochMilli(completedAtEpochMs));
    }

    private Object coerceTransportPayload(AwaitInteractionRecord record, Object payload) {
        try {
            Class<?> transportOutputType = AwaitPayloadSupport.resolvePayloadClass(
                record.transportOutputType(),
                Thread.currentThread().getContextClassLoader());
            return AwaitPayloadSupport.coercePayload(payload, transportOutputType);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Await transport completion validation failed for interaction " + record.interactionId()
                    + " stepId=" + record.stepId()
                    + ": transportOutputType " + record.transportOutputType() + " is unavailable",
                e);
        }
    }

    private Object coerceCanonicalPayload(AwaitInteractionRecord record, Object payload) {
        try {
            Class<?> canonicalOutputType = AwaitPayloadSupport.resolvePayloadClass(
                record.outputType(),
                Thread.currentThread().getContextClassLoader());
            return AwaitPayloadSupport.coercePayload(payload, canonicalOutputType);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Await canonical completion validation failed for interaction " + record.interactionId()
                    + " stepId=" + record.stepId()
                    + ": outputType " + record.outputType() + " is unavailable",
                e);
        }
    }

    private void validateDurableOutputContract(AwaitInteractionRecord record, AwaitCompletionDescriptor descriptor) {
        if (!sameTypeIdentity(descriptor.outputType(), record.outputType())) {
            throw new IllegalStateException(
                "Await durable-contract compatibility failed for execution " + record.executionId()
                    + " interaction " + record.interactionId()
                    + " stepId=" + record.stepId()
                    + ": durable canonical outputType=" + record.outputType()
                    + " differs from rebuilt descriptor outputType=" + descriptor.outputType());
        }
        if (!sameTypeIdentity(descriptor.transportOutputType(), record.transportOutputType())) {
            throw new IllegalStateException(
                "Await durable transport-contract compatibility failed for execution " + record.executionId()
                    + " interaction " + record.interactionId()
                    + " stepId=" + record.stepId()
                    + ": durable transportOutputType=" + record.transportOutputType()
                    + " differs from rebuilt descriptor transportOutputType=" + descriptor.transportOutputType());
        }
    }

    /**
     * Compares durable type identities by their loadable class when source-form names differ from
     * JVM binary names for nested types. This preserves v2's single protobuf identity while still
     * rejecting a v3 protobuf transport type in place of its canonical domain contract.
     */
    private static boolean sameTypeIdentity(String left, String right) {
        if (left.equals(right)) {
            return true;
        }
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            return AwaitPayloadSupport.resolvePayloadClass(left, classLoader)
                .equals(AwaitPayloadSupport.resolvePayloadClass(right, classLoader));
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }

    private Uni<AwaitCompletionDescriptor> registerDescriptor(AwaitCompletionDescriptor descriptor) {
        return Uni.createFrom().item(() -> {
            AwaitCompletionDescriptor registered = descriptor;
            if (descriptorFactory != null) {
                registered = descriptorFactory.register(registered);
            }
            AwaitCompletionDescriptor existing = directDescriptors.putIfAbsent(registered.stepId(), registered);
            if (existing != null && descriptorFactory == null && !existing.equals(registered)) {
                throw new IllegalStateException(
                    "Conflicting deferred-completion descriptors were registered for step '"
                        + registered.stepId() + "'");
            }
            return existing == null ? registered : existing;
        });
    }

    private AwaitCompletionDescriptor descriptorFor(AwaitInteractionRecord record) {
        try {
            AwaitCompletionDescriptor descriptor;
            if (descriptorFactory != null) {
                descriptor = descriptorFactory.descriptorByStepIdNow(record.stepId());
            } else {
                descriptor = directDescriptors.get(record.stepId());
                if (descriptor == null) {
                    throw durableDescriptorFailure(record, null);
                }
            }
            validatePinnedCompletionProjector(record, descriptor);
            return descriptor;
        } catch (PinnedCompletionProjectorMismatchException e) {
            throw e;
        } catch (RuntimeException e) {
            throw durableDescriptorFailure(record, e);
        }
    }

    private static Map<String, Object> completionContractMetadata(
        AwaitCompletionDescriptor descriptor,
        Map<String, Object> traceMetadata
    ) {
        Map<String, Object> metadata = new java.util.LinkedHashMap<>(traceMetadata);
        metadata.remove("completionMode");
        metadata.remove("callbackSelection");
        metadata.remove("commandCompletionMode");
        metadata.remove("commandAcknowledgementType");
        descriptor.callback().ifPresent(callback -> {
            metadata.put("completionMode", "CONNECTOR_CALLBACK");
            metadata.put("callbackSelection", callback.contractMetadata());
        });
        if (descriptor.acknowledgementAwareCommandCompletion()) {
            metadata.put("commandCompletionMode", "ACKNOWLEDGEMENT_REQUIRED");
            metadata.put("commandAcknowledgementType", descriptor.commandAcknowledgementType().orElseThrow());
        }
        metadata.remove(COMPLETION_PROJECTOR_METADATA);
        if (descriptor.requestAwareCompletion()) {
            metadata.put(COMPLETION_PROJECTOR_METADATA, descriptor.completionProjectorId());
        }
        return Map.copyOf(metadata);
    }

    private static void validatePinnedCompletionProjector(
        AwaitInteractionRecord record,
        AwaitCompletionDescriptor descriptor
    ) {
        if (record.commandCallback() != descriptor.callback().isPresent()
            || (record.commandCallback() && !descriptor.callback().orElseThrow().contractMetadata()
                .equals(record.transportMetadata().get("callbackSelection")))) {
            throw new PinnedCompletionProjectorMismatchException("Await callback contract does not match the pinned interaction");
        }
        if (record.acknowledgementAwareCommandCallback() != descriptor.acknowledgementAwareCommandCompletion()
            || (record.acknowledgementAwareCommandCallback() && !descriptor.commandAcknowledgementType().orElseThrow()
                .equals(record.transportMetadata().get("commandAcknowledgementType")))) {
            throw new PinnedCompletionProjectorMismatchException("Await Command completion mode or acknowledgement type changed");
        }
        if (!descriptor.requestAwareCompletion()) {
            return; // Ignore reserved metadata supplied to legacy non-request-aware interactions.
        }
        Object pinned = record.transportMetadata().get(COMPLETION_PROJECTOR_METADATA);
        if (pinned == null) {
            if (record.acknowledgementAwareCommandCallback()) {
                throw new PinnedCompletionProjectorMismatchException("Await Command completion projector identity is missing");
            }
            return; // Compatibility for interactions created before projector identity was persisted.
        }
        String current = descriptor.completionProjectorId();
        if (!pinned.equals(current)) {
            throw new PinnedCompletionProjectorMismatchException(
                "Await pinned completion projector " + pinned + " is unavailable for interaction "
                    + record.interactionId() + "; current projector is " + current);
        }
    }

    private static final class PinnedCompletionProjectorMismatchException extends IllegalStateException {
        private PinnedCompletionProjectorMismatchException(String message) {
            super(message);
        }
    }

    private static IllegalStateException durableDescriptorFailure(
        AwaitInteractionRecord record,
        Throwable cause
    ) {
        String message = "Await durable-contract resolution failed for execution " + record.executionId()
            + " interaction " + record.interactionId()
            + " stepId=" + record.stepId()
            + ": no AwaitCompletionDescriptor is available";
        return cause == null ? new IllegalStateException(message) : new IllegalStateException(message, cause);
    }

    private record ValidatedCompletion(
        AwaitInteractionRecord record,
        AwaitCompletionCommand command,
        AwaitCompletionDescriptor descriptor
    ) {
    }

    private static AwaitCompletionCommand withResponsePayload(
        AwaitCompletionCommand command,
        Object responsePayload
    ) {
        return new AwaitCompletionCommand(
            command.tenantId(),
            command.interactionId(),
            command.correlationId(),
            command.resumeToken(),
            command.idempotencyKey(),
            responsePayload,
            command.actor(),
            command.nowEpochMs());
    }

    private String deriveCorrelationId(
        AwaitCompletionDescriptor descriptor,
        String tenantId,
        String executionId,
        String idempotencyKey) {
        return deriveCorrelationId(descriptor.stepId(), descriptor.correlationStrategy(), tenantId, executionId, idempotencyKey);
    }

    private static String deriveCorrelationId(String stepId, String strategy, String tenantId,
        String executionId, String idempotencyKey) {
        String basis = tenantId + ":" + executionId + ":" + stepId + ":" + idempotencyKey;
        return switch (strategy) {
            case "interactionId", "signedResumeToken" -> UUID.nameUUIDFromBytes(basis.getBytes(StandardCharsets.UTF_8)).toString();
            default -> UUID.nameUUIDFromBytes((strategy + ":" + basis).getBytes(StandardCharsets.UTF_8)).toString();
        };
    }

    private String deriveIdempotencyKey(
        AwaitCompletionDescriptor descriptor,
        String executionId,
        String identityScope,
        Object requestPayload
    ) {
        return deriveIdempotencyKey(descriptor.stepId(), descriptor.idempotencyKeyFields(), executionId,
            identityScope, requestPayload);
    }

    private static String deriveIdempotencyKey(String stepId, List<String> fields, String executionId,
        String identityScope, Object requestPayload) {
        if (fields.isEmpty()) {
            return identityScope + ":" + stepId;
        }
        JsonNode node = PipelineJson.mapper().valueToTree(requestPayload);
        StringBuilder builder = new StringBuilder();
        if (!identityScope.equals(executionId)) {
            builder.append(identityScope).append(':');
        }
        builder.append(stepId);
        for (String field : fields) {
            builder.append(':').append(field).append('=');
            JsonNode value = node == null ? null : node.get(field);
            builder.append(value == null || value.isNull() ? "<null>" : value.asText());
        }
        return builder.toString();
    }

    private Map<String, Object> traceMetadataForCurrentExecution() {
        AwaitExecutionContext context = AwaitExecutionContextHolder.get();
        if (context != null && !context.traceMetadata().isEmpty()) {
            return context.traceMetadata();
        }
        return awaitTelemetry.captureTraceMetadata();
    }

    static String unitIdFor(AwaitCompletionDescriptor descriptor, String tenantId, String executionId,
        String identityScope, int stepIndex) {
        return unitIdFor(descriptor, tenantId, executionId, identityScope, stepIndex,
            Optional.ofNullable(AwaitExecutionContextHolder.get()));
    }

    private static String unitIdFor(AwaitCompletionDescriptor descriptor, String tenantId, String executionId,
        String identityScope, int stepIndex, Optional<AwaitExecutionContext> executionContext) {
        return unitIdFor(descriptor.stepId(), tenantId, executionId, identityScope, stepIndex, executionContext);
    }

    public static String unitIdFor(String stepId, String tenantId, String executionId,
        String identityScope, int stepIndex, Optional<AwaitExecutionContext> executionContext) {
        return executionContext.filter(context -> context.awaitOccurrenceCause().isPresent()).map(context -> {
            if (!tenantId.equals(context.tenantId()) || !executionId.equals(context.executionId())
                || !identityScope.equals(context.identityScope())) {
                throw new IllegalStateException("Await creation context differs from its semantic owner");
            }
            var occurrence = new AwaitUnitOccurrence(tenantId, executionId, identityScope, stepId,
                stepIndex, context.awaitOccurrenceCause().orElseThrow());
            return context.invocationPath().map(occurrence::withInvocationPath).orElse(occurrence).unitId();
        }).orElseGet(() -> deriveUnitId(tenantId, identityScope, stepId, stepIndex));
    }

    private static String deriveUnitId(String tenantId, String executionId, String stepId, int stepIndex) {
        String basis = tenantId + ":" + executionId + ":" + stepId + ":" + stepIndex;
        return UUID.nameUUIDFromBytes(basis.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
    }
}

package org.pipelineframework.aws.durable;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.orchestrator.AwaitSemanticCheckpoint;
import org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint;
import org.pipelineframework.orchestrator.PipelineControlPlane;

/** Synchronous Lambda shell over bounded reactive {@link PipelineControlPlane} actions. */
public final class AwsDurableControlPlaneActions implements AwsDurableActionInvoker {
    private final PipelineControlPlane controlPlane;
    private final AwsDurableCallbackBindingRepository bindings;
    private final AwsDurableInputDecoder inputDecoder;
    private final ObjectMapper mapper;
    private final Duration actionTimeout;
    private final Duration callbackRetention;
    private final AwsDurableHostTelemetry telemetry;
    private final org.pipelineframework.orchestrator.AwaitItemContinuationDispatcher continuationDispatcher;

    public AwsDurableControlPlaneActions(
        PipelineControlPlane controlPlane,
        AwsDurableCallbackBindingRepository bindings,
        AwsDurableInputDecoder inputDecoder,
        ObjectMapper mapper,
        Duration actionTimeout,
        Duration callbackRetention
    ) {
        this(controlPlane, bindings, inputDecoder, mapper, actionTimeout, callbackRetention, new AwsDurableHostTelemetry());
    }

    public AwsDurableControlPlaneActions(PipelineControlPlane controlPlane, AwsDurableCallbackBindingRepository bindings,
        AwsDurableInputDecoder inputDecoder, ObjectMapper mapper, Duration actionTimeout, Duration callbackRetention,
        org.pipelineframework.orchestrator.AwaitItemContinuationDispatcher continuationDispatcher) {
        this(controlPlane, bindings, inputDecoder, mapper, actionTimeout, callbackRetention,
            new AwsDurableHostTelemetry(), continuationDispatcher);
    }

    AwsDurableControlPlaneActions(
        PipelineControlPlane controlPlane,
        AwsDurableCallbackBindingRepository bindings,
        AwsDurableInputDecoder inputDecoder,
        ObjectMapper mapper,
        Duration actionTimeout,
        Duration callbackRetention,
        AwsDurableHostTelemetry telemetry
    ) {
        this(controlPlane, bindings, inputDecoder, mapper, actionTimeout, callbackRetention, telemetry, null);
    }

    private AwsDurableControlPlaneActions(PipelineControlPlane controlPlane, AwsDurableCallbackBindingRepository bindings,
        AwsDurableInputDecoder inputDecoder, ObjectMapper mapper, Duration actionTimeout, Duration callbackRetention,
        AwsDurableHostTelemetry telemetry, org.pipelineframework.orchestrator.AwaitItemContinuationDispatcher continuationDispatcher) {
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.inputDecoder = Objects.requireNonNull(inputDecoder, "inputDecoder");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.actionTimeout = positive(actionTimeout, "actionTimeout");
        this.callbackRetention = positive(callbackRetention, "callbackRetention");
        this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
        this.continuationDispatcher = continuationDispatcher;
    }

    @Override
    public AwsDurableActionResponse invoke(AwsDurableActionRequest request) {
        Objects.requireNonNull(request, "request");
        return switch (request.operation()) {
            case SUBMIT -> submit(request);
            case STATUS -> status(request);
            case RESULT -> AwsDurableActionResponse.result(json(controlPlane
                .getExecutionResultPayload(request.tenantId(), required(request.executionId(), "executionId"))
                .await().atMost(actionTimeout)));
            case READ_RUNTIME_IDENTITY -> runtimeIdentity(request);
            case REDRIVE -> AwsDurableActionResponse.status(controlPlane.redriveExecution(
                    request.tenantId(),
                    required(request.executionId(), "executionId"),
                    request.expectedVersion().orElseThrow(() -> new IllegalArgumentException("expectedVersion is required")),
                    true,
                    required(request.reason(), "reason"))
                .await().atMost(actionTimeout).status().name());
            case SWEEP -> sweep();
            case PROCESS_AWAIT_CONTINUATION -> itemContinuation(request);
            case QUERY_PENDING_AWAIT -> pendingAwait(request);
            case READ_AWAIT_CHECKPOINT -> awaitCheckpoint(request);
            case READ_EXECUTION_AWAITS -> executionAwaits(request);
            case READ_AWAIT_COMPLETION -> completionObservation(request);
            case QUERY_AWAIT_COMPLETIONS -> completionObservations(request);
            case RECORD_AWAIT_RESUMED -> recordAwaitResumed(request);
            case REGISTER_EXECUTION -> registerExecution(request);
            case REGISTER_CALLBACK -> register(request);
            case BIND_CALLBACK -> bind(request);
            case READ_EXECUTION_LOCATOR -> executionLocator(request);
            case TYPED_RESULT -> typedResult(request);
        };
    }

    /** Only report readiness when the observed runtime matches the requested Release pin. */
    private AwsDurableActionResponse runtimeIdentity(AwsDurableActionRequest request) {
        String pipeline = required(request.pipelineId(), "pipelineId");
        String contract = required(request.contractVersion(), "contractVersion");
        String release = required(request.releaseVersion(), "releaseVersion");
        var observed = controlPlane.runtimeIdentity(request.tenantId()).await().atMost(actionTimeout);
        if (!observed.ready()) {
            throw new IllegalStateException("Observed runtime is not ready");
        }
        if (!pipeline.equals(observed.pipelineId())
            || !contract.equals(observed.contractVersion()) || !release.equals(observed.releaseVersion())) {
            throw new IllegalStateException("Runtime identity differs from the requested Release. "
                + "Observed pipelineId=%s, contractVersion=%s, releaseVersion=%s"
                    .formatted(observed.pipelineId(), observed.contractVersion(), observed.releaseVersion()));
        }
        return AwsDurableActionResponse.result(json(observed));
    }

    private AwsDurableActionResponse completionObservation(AwsDurableActionRequest request) {
        var identity = request.awaitIdentity().orElseThrow(
            () -> new IllegalArgumentException("awaitIdentity is required"));
        var observation = controlPlane.getAwaitCompletionObservation(request.tenantId(), identity.interactionId())
            .await().atMost(actionTimeout);
        telemetry.recordCompletion(observation);
        return AwsDurableActionResponse.result(json(observation));
    }

    private AwsDurableActionResponse completionObservations(AwsDurableActionRequest request) {
        var page = controlPlane.queryAwaitCompletionObservations(
            request.tenantId(), request.completionPageLimit().orElse(100), request.completionCursor())
            .await().atMost(actionTimeout);
        telemetry.recordCompletionPage(request.tenantId(), page);
        return AwsDurableActionResponse.result(json(page));
    }

    private AwsDurableActionResponse recordAwaitResumed(AwsDurableActionRequest request) {
        var identity = request.awaitIdentity().orElseThrow(
            () -> new IllegalArgumentException("awaitIdentity is required"));
        if (!request.tenantId().equals(identity.tenantId())
            || !request.executionId().filter(identity.executionId()::equals).isPresent()
            || request.generation() != identity.generation()) {
            throw new IllegalArgumentException("resume observation does not match its Await identity");
        }
        var checkpoint = driverCheckpoint(request);
        String providerExecutionArn = required(request.providerExecutionArn(), "providerExecutionArn");
        var registration = bindings.findRegistration(identity.tenantId(), identity.executionId(),
            identity.generation(), identity.occurrence());
        if (registration.filter(value -> value.checkpoint().equals(checkpoint)
                && value.providerExecutionArn().equals(providerExecutionArn)
                && value.awaitIdentity().filter(identity::equals).isPresent()).isEmpty()) {
            return AwsDurableActionResponse.bound(false);
        }
        // A superseded driver's authentic historical observation is useful telemetry;
        // it confers no current-generation authority or semantic consequence.
        bindings.recordDriverResumed(identity);
        return AwsDurableActionResponse.bound(true);
    }

    private AwsDurableActionResponse registerExecution(AwsDurableActionRequest request) {
        if (request.idempotencyKey().isPresent()) {
            // Validate the semantic owner's key policy and any existing admission before retaining correlation.
            var existing = request.executionId().isPresent()
                ? Optional.of(controlPlane.getExecutionSemanticCheckpoint(request.tenantId(), request.executionId().orElseThrow())
                    .await().atMost(actionTimeout))
                : controlPlane.getExecutionSemanticCheckpointByClientKey(request.tenantId(), request.idempotencyKey().orElseThrow(),
                    required(request.pipelineId(), "pipelineId"), required(request.releaseVersion(), "releaseVersion"))
                    .await().atMost(actionTimeout);
            existing.ifPresent(semantic -> requireRecoveryRelease(request, semantic));
            return AwsDurableActionResponse.bound(bindings.registerAdmission(
                new org.pipelineframework.aws.durable.model.AwsDurableAdmissionLocator(request.tenantId(),
                    request.idempotencyKey().orElseThrow(), required(request.pipelineId(), "pipelineId"),
                    required(request.contractVersion(), "contractVersion"), required(request.releaseVersion(), "releaseVersion"),
                    request.executionId(), request.generation(), required(request.providerExecutionName(), "providerExecutionName"),
                    required(request.providerExecutionArn(), "providerExecutionArn"))));
        }
        var semantic = controlPlane.getExecutionSemanticCheckpoint(request.tenantId(), required(request.executionId(), "executionId"))
            .await().atMost(actionTimeout);
        requireRecoveryRelease(request, semantic);
        if (!required(request.executionId(), "executionId").equals(semantic.executionId())) {
            throw new IllegalStateException("Recovery locator does not match semantic execution");
        }
        return AwsDurableActionResponse.bound(bindings.registerRecovery(
            new org.pipelineframework.aws.durable.model.AwsDurableRecoveryLocator(
                new org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint(
                    new AwsDurableExecutionCheckpoint(semantic.tenantId(), semantic.executionId(), semantic.pipelineId(),
                        semantic.contractVersion(), semantic.releaseVersion()), request.generation()),
                required(request.providerExecutionName(), "providerExecutionName"), required(request.providerExecutionArn(), "providerExecutionArn"))));
    }

    private void requireRecoveryRelease(AwsDurableActionRequest request,
        org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint semantic) {
        if (!required(request.pipelineId(), "pipelineId").equals(semantic.pipelineId())
            || !required(request.contractVersion(), "contractVersion").equals(semantic.contractVersion())
            || !required(request.releaseVersion(), "releaseVersion").equals(semantic.releaseVersion())) {
            throw new IllegalStateException("Recovery locator does not match semantic pipeline and release");
        }
    }

    private AwsDurableActionResponse submit(AwsDurableActionRequest request) {
        var accepted = controlPlane.executePipelineAsync(
                inputDecoder.decode(required(request.inputJson(), "inputJson")),
                request.tenantId(),
                required(request.idempotencyKey(), "idempotencyKey"),
                false,
                required(request.pipelineId(), "pipelineId"),
                required(request.contractVersion(), "contractVersion"),
                required(request.releaseVersion(), "releaseVersion"))
            .await().atMost(actionTimeout);
        return AwsDurableActionResponse.submitted(new AwsDurableExecutionCheckpoint(
            request.tenantId(), accepted.executionId(), required(request.pipelineId(), "pipelineId"),
            required(request.contractVersion(), "contractVersion"),
            required(request.releaseVersion(), "releaseVersion")), accepted.duplicate());
    }

    private AwsDurableActionResponse pendingAwait(AwsDurableActionRequest request) {
        String executionId = required(request.executionId(), "executionId");
        var selected = controlPlane.getCurrentAwaitSemanticCheckpoint(request.tenantId(), executionId,
            request.awaitSelectionCursor()).await().atMost(actionTimeout);
        return selected.checkpoint()
            .map(checkpoint -> AwsDurableActionResponse.pendingAwait(new AwsDurableAwaitIdentity(
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.interactionId(), checkpoint.correlationId(),
                request.generation())))
            .orElseGet(AwsDurableControlPlaneActions::emptyResponse)
            .withAwaitSelectionCursor(selected.nextCursor());
    }

    private AwsDurableActionResponse status(AwsDurableActionRequest request) {
        String executionId = required(request.executionId(), "executionId");
        String status = controlPlane.getExecutionStatus(request.tenantId(), executionId)
            .await().atMost(actionTimeout).status().name();
        if (!"WAITING_EXTERNAL".equals(status)) {
            return AwsDurableActionResponse.observation(status, Optional.empty());
        }
        var selected = controlPlane.getCurrentAwaitSemanticCheckpoint(request.tenantId(), executionId,
            request.awaitSelectionCursor()).await().atMost(actionTimeout);
        return AwsDurableActionResponse.observation(status, selected.checkpoint().map(this::providerCheckpoint))
            .withAwaitSelectionCursor(selected.nextCursor());
    }

    private AwsDurableAwaitCheckpoint providerCheckpoint(AwaitSemanticCheckpoint checkpoint) {
        return new AwsDurableAwaitCheckpoint(checkpoint.tenantId(), checkpoint.executionId(), checkpoint.interactionId(),
            checkpoint.correlationId(), checkpoint.unitId(), checkpoint.stepId(), checkpoint.status().name(),
            checkpoint.pipelineId(), checkpoint.contractVersion(), checkpoint.releaseVersion());
    }

    private AwsDurableActionResponse sweep() {
        long now = System.currentTimeMillis();
        (continuationDispatcher == null ? controlPlane.sweepOnce(now)
            : controlPlane.sweepOnce(now, continuationDispatcher)).await().atMost(actionTimeout);
        return AwsDurableActionResponse.status("SWEEPED");
    }

    private AwsDurableActionResponse itemContinuation(AwsDurableActionRequest request) {
        org.pipelineframework.orchestrator.AwaitItemContinuationCommand command;
        try {
            command = org.pipelineframework.config.pipeline.PipelineJson.mapper().readValue(
                required(request.inputJson(), "inputJson"), org.pipelineframework.orchestrator.AwaitItemContinuationCommand.class);
        } catch (java.io.IOException malformed) {
            throw new IllegalArgumentException("Invalid item continuation command", malformed);
        }
        if (!request.tenantId().equals(command.tenantId())
            || !required(request.executionId(), "executionId").equals(command.executionId())) {
            throw new IllegalArgumentException("Item continuation command does not match its execution scope");
        }
        var semantic = controlPlane.getExecutionSemanticCheckpoint(command.tenantId(), command.executionId())
            .await().atMost(actionTimeout);
        requireRecoveryRelease(request, semantic);
        if (!semantic.tenantId().equals(command.tenantId()) || !semantic.executionId().equals(command.executionId())) {
            throw new IllegalStateException("Item continuation semantic checkpoint does not match command scope");
        }
        var current = new org.pipelineframework.orchestrator.AwaitItemContinuationCommand(command.tenantId(),
            command.executionId(), command.unitId(), command.interactionId(), command.itemIndex(), command.attempt(),
            System.currentTimeMillis());
        return AwsDurableActionResponse.result(canonicalJson(controlPlane.processAwaitItemContinuation(current)
            .await().atMost(actionTimeout)));
    }

    private static String canonicalJson(Object value) {
        try { return org.pipelineframework.config.pipeline.PipelineJson.mapper().writeValueAsString(value); }
        catch (java.io.IOException failure) { throw new IllegalStateException("Cannot serialize item continuation result", failure); }
    }

    private AwsDurableActionResponse awaitCheckpoint(AwsDurableActionRequest request) {
        AwsDurableAwaitIdentity identity = request.awaitIdentity()
            .orElseThrow(() -> new IllegalArgumentException("awaitIdentity is required"));
        AwaitSemanticCheckpoint checkpoint = controlPlane
            .getAwaitSemanticCheckpoint(identity.tenantId(), identity.interactionId())
            .await().atMost(actionTimeout)
            .orElseThrow(() -> new IllegalStateException("Await semantic checkpoint is not available"));
        return AwsDurableActionResponse.awaitCheckpoint(new AwsDurableAwaitCheckpoint(
            checkpoint.tenantId(), checkpoint.executionId(), checkpoint.interactionId(),
            checkpoint.correlationId(), checkpoint.unitId(), checkpoint.stepId(), checkpoint.status().name(),
            checkpoint.pipelineId(), checkpoint.contractVersion(), checkpoint.releaseVersion()));
    }

    private AwsDurableActionResponse executionAwaits(AwsDurableActionRequest request) {
        String executionId = required(request.executionId(), "executionId");
        return AwsDurableActionResponse.awaitCheckpoints(controlPlane
            .getAwaitSemanticCheckpoints(request.tenantId(), executionId, 100)
            .await().atMost(actionTimeout).stream()
            .map(checkpoint -> new AwsDurableAwaitCheckpoint(
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.interactionId(),
                checkpoint.correlationId(), checkpoint.unitId(), checkpoint.stepId(), checkpoint.status().name(),
                checkpoint.pipelineId(), checkpoint.contractVersion(), checkpoint.releaseVersion()))
            .toList());
    }

    private AwsDurableActionResponse register(AwsDurableActionRequest request) {
        AwsDurableDriverCheckpoint checkpoint = driverCheckpoint(request);
        var identity = request.awaitIdentity()
            .orElseThrow(() -> new IllegalArgumentException("callback registration requires a pinned Await identity"));
        var semantic = controlPlane.getAwaitSemanticCheckpoint(identity.tenantId(), identity.interactionId())
            .await().atMost(actionTimeout).orElseThrow(() -> new IllegalStateException("pinned Await is unavailable"));
        if (!checkpoint.tenantId().equals(identity.tenantId())
            || !checkpoint.executionId().equals(identity.executionId()) || checkpoint.generation() != identity.generation()
            || !identity.tenantId().equals(semantic.tenantId())
            || !identity.interactionId().equals(semantic.interactionId())
            || !identity.executionId().equals(semantic.executionId())
            || !identity.correlationId().equals(semantic.correlationId())
            || !checkpoint.pipelineId().equals(semantic.pipelineId())
            || !checkpoint.contractVersion().equals(semantic.contractVersion())
            || !checkpoint.releaseVersion().equals(semantic.releaseVersion())) {
            throw new IllegalArgumentException("callback registration does not match the semantic Await checkpoint");
        }
        long now = System.currentTimeMillis();
        boolean created = bindings.register(new AwsDurableCallbackRegistration(
            checkpoint,
            required(request.providerExecutionName(), "providerExecutionName"),
            required(request.providerExecutionArn(), "providerExecutionArn"),
            required(request.providerCallbackId(), "providerCallbackId"),
            now,
            Instant.ofEpochMilli(now).plus(callbackRetention.toSeconds(), ChronoUnit.SECONDS).getEpochSecond(),
            request.awaitIdentity()));
        return AwsDurableActionResponse.bound(created);
    }

    private AwsDurableActionResponse bind(AwsDurableActionRequest request) {
        AwsDurableAwaitIdentity identity = request.awaitIdentity()
            .orElseThrow(() -> new IllegalArgumentException("awaitIdentity is required"));
        AwsDurableCallbackRegistration registration = bindings.findRegistration(
                identity.tenantId(), identity.executionId(), identity.generation(), identity.occurrence())
            .orElseThrow(() -> new IllegalStateException("callback registration is not available"));
        return AwsDurableActionResponse.bound(bindings.bind(registration.bind(identity)));
    }

    private static AwsDurableDriverCheckpoint driverCheckpoint(AwsDurableActionRequest request) {
        return new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint(
            request.tenantId(),
            required(request.executionId(), "executionId"),
            required(request.pipelineId(), "pipelineId"),
            required(request.contractVersion(), "contractVersion"),
            required(request.releaseVersion(), "releaseVersion")), request.generation());
    }

    private String json(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("TPF result could not be serialized", exception);
        }
    }

    /** Reads retained execution correlation after authorizing its exact semantic execution pin. */
    private AwsDurableActionResponse executionLocator(AwsDurableActionRequest request) {
        var pin = driverCheckpoint(request);
        readPinnedExecution(pin);
        return AwsDurableActionResponse.result(json(bindings.findCurrentRecovery(pin).orElse(null)));
    }

    /** Checks the driver generation around the result read, after authorizing the execution pin. */
    private AwsDurableActionResponse typedResult(AwsDurableActionRequest request) {
        var pin = driverCheckpoint(request);
        var semantic = readPinnedExecution(pin);
        requireCurrentRecovery(pin);
        var result = controlPlane.getExecutionTypedResult(semantic).await().atMost(actionTimeout);
        requireCurrentRecovery(pin);
        return AwsDurableActionResponse.result(json(result));
    }

    /** Rejects absent or superseded driver generations. */
    private void requireCurrentRecovery(AwsDurableDriverCheckpoint pin) {
        if (bindings.findCurrentRecovery(pin).isEmpty()) {
            throw new IllegalStateException("Typed result requires a current execution locator");
        }
    }

    /** Applies the existing status-read admission and verifies all coordinates before returning evidence. */
    private ExecutionSemanticCheckpoint readPinnedExecution(AwsDurableDriverCheckpoint pin) {
        var semantic = controlPlane.getExecutionSemanticCheckpoint(pin.tenantId(), pin.executionId())
            .await().atMost(actionTimeout);
        if (!pin.tenantId().equals(semantic.tenantId()) || !pin.executionId().equals(semantic.executionId())
            || !pin.pipelineId().equals(semantic.pipelineId()) || !pin.contractVersion().equals(semantic.contractVersion())
            || !pin.releaseVersion().equals(semantic.releaseVersion())) {
            throw new IllegalStateException("Semantic execution differs from the requested Release pin");
        }
        return semantic;
    }

    private static String required(java.util.Optional<String> value, String name) {
        return value.filter(candidate -> !candidate.isBlank())
            .orElseThrow(() -> new IllegalArgumentException(name + " is required"));
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static AwsDurableActionResponse emptyResponse() {
        return new AwsDurableActionResponse(
            Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            java.util.List.of(), false);
    }
}

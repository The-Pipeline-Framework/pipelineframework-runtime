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
import org.pipelineframework.orchestrator.PipelineControlPlane;

/** Synchronous Lambda shell over bounded reactive {@link PipelineControlPlane} actions. */
public final class AwsDurableControlPlaneActions implements AwsDurableActionInvoker {
    private final PipelineControlPlane controlPlane;
    private final AwsDurableCallbackBindingRepository bindings;
    private final AwsDurableInputDecoder inputDecoder;
    private final ObjectMapper mapper;
    private final Duration actionTimeout;
    private final Duration callbackRetention;

    public AwsDurableControlPlaneActions(
        PipelineControlPlane controlPlane,
        AwsDurableCallbackBindingRepository bindings,
        AwsDurableInputDecoder inputDecoder,
        ObjectMapper mapper,
        Duration actionTimeout,
        Duration callbackRetention
    ) {
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.inputDecoder = Objects.requireNonNull(inputDecoder, "inputDecoder");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.actionTimeout = positive(actionTimeout, "actionTimeout");
        this.callbackRetention = positive(callbackRetention, "callbackRetention");
    }

    @Override
    public AwsDurableActionResponse invoke(AwsDurableActionRequest request) {
        Objects.requireNonNull(request, "request");
        return switch (request.operation()) {
            case SUBMIT -> submit(request);
            case STATUS -> AwsDurableActionResponse.status(controlPlane
                .getExecutionStatus(request.tenantId(), required(request.executionId(), "executionId"))
                .await().atMost(actionTimeout).status().name());
            case RESULT -> AwsDurableActionResponse.result(json(controlPlane
                .getExecutionResultPayload(request.tenantId(), required(request.executionId(), "executionId"))
                .await().atMost(actionTimeout)));
            case REDRIVE -> AwsDurableActionResponse.status(controlPlane.redriveExecution(
                    request.tenantId(),
                    required(request.executionId(), "executionId"),
                    request.expectedVersion().orElseThrow(() -> new IllegalArgumentException("expectedVersion is required")),
                    true,
                    required(request.reason(), "reason"))
                .await().atMost(actionTimeout).status().name());
            case SWEEP -> sweep();
            case QUERY_PENDING_AWAIT -> pendingAwait(request);
            case READ_AWAIT_CHECKPOINT -> awaitCheckpoint(request);
            case READ_EXECUTION_AWAITS -> executionAwaits(request);
            case REGISTER_CALLBACK -> register(request);
            case BIND_CALLBACK -> bind(request);
        };
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
        return controlPlane
            .getAwaitSemanticCheckpoints(request.tenantId(), executionId, 100)
            .await().atMost(actionTimeout).stream()
            .filter(checkpoint -> !checkpoint.status().terminal())
            .findFirst()
            .map(checkpoint -> AwsDurableActionResponse.pendingAwait(new AwsDurableAwaitIdentity(
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.interactionId(), checkpoint.correlationId(),
                request.generation())))
            .orElseGet(AwsDurableControlPlaneActions::emptyResponse);
    }

    private AwsDurableActionResponse sweep() {
        controlPlane.sweepOnce(System.currentTimeMillis()).await().atMost(actionTimeout);
        return AwsDurableActionResponse.status("SWEEPED");
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
        long now = System.currentTimeMillis();
        boolean created = bindings.register(new AwsDurableCallbackRegistration(
            checkpoint,
            required(request.providerExecutionName(), "providerExecutionName"),
            required(request.providerExecutionArn(), "providerExecutionArn"),
            required(request.providerCallbackId(), "providerCallbackId"),
            now,
            Instant.ofEpochMilli(now).plus(callbackRetention.toSeconds(), ChronoUnit.SECONDS).getEpochSecond()));
        return AwsDurableActionResponse.bound(created);
    }

    private AwsDurableActionResponse bind(AwsDurableActionRequest request) {
        AwsDurableAwaitIdentity identity = request.awaitIdentity()
            .orElseThrow(() -> new IllegalArgumentException("awaitIdentity is required"));
        AwsDurableCallbackRegistration registration = bindings.findRegistration(
                identity.tenantId(), identity.executionId(), identity.generation())
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

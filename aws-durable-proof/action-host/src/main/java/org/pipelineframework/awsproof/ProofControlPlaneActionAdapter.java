package org.pipelineframework.awsproof;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.orchestrator.PipelineControlPlane;

/** Adapts the proof transport onto the existing TPF action contract. */
@ApplicationScoped
final class ProofControlPlaneActionAdapter {
    private static final Duration ACTION_TIMEOUT = Duration.ofSeconds(30);

    @Inject
    PipelineControlPlane controlPlane;

    @Inject
    ObjectMapper mapper;

    @Inject
    ProofFaultInjector faults;

    @Inject
    ProofReleaseCatalog releaseCatalog;

    AwsDurableActionResponse handle(AwsDurableActionRequest request) {
        return switch (request.operation()) {
            case SUBMIT -> submit(request);
            case STATUS -> status(request);
            case RESULT -> result(request);
            case REDRIVE -> redrive(request);
            case SWEEP -> sweep();
            case QUERY_PENDING_AWAIT -> pendingAwait(request);
            case READ_AWAIT_CHECKPOINT -> awaitCheckpoint(request);
            case READ_EXECUTION_AWAITS -> executionAwaits(request);
            case REGISTER_CALLBACK, BIND_CALLBACK -> throw new IllegalArgumentException(
                request.operation() + " is an AWS Durable host operation");
        };
    }

    private AwsDurableActionResponse submit(AwsDurableActionRequest request) {
        try {
            faults.failIfArmed("submit-before-tpf-commit", required(request.idempotencyKey(), "idempotencyKey"));
            String pipelineId = required(request.pipelineId(), "pipelineId");
            String contractVersion = required(request.contractVersion(), "contractVersion");
            String releaseVersion = required(request.releaseVersion(), "releaseVersion");
            ProofPipelineInput input = mapper.readValue(
                required(request.inputJson(), "inputJson"), ProofPipelineInput.class);
            var accepted = releaseCatalog.ensureRegistered(
                    request.tenantId(), pipelineId, contractVersion, releaseVersion)
                .onItem().transformToUni(ignored -> controlPlane.executePipelineAsync(
                    input,
                    request.tenantId(),
                    required(request.idempotencyKey(), "idempotencyKey"),
                    false,
                    pipelineId,
                    contractVersion,
                    releaseVersion))
                .await().atMost(ACTION_TIMEOUT);
            faults.failIfArmed("submit-after-tpf-commit", accepted.executionId());
            return AwsDurableActionResponse.submitted(
                new AwsDurableExecutionCheckpoint(
                    request.tenantId(),
                    accepted.executionId(),
                    pipelineId,
                    contractVersion,
                    releaseVersion),
                accepted.duplicate());
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("inputJson must contain a valid proof pipeline input", exception);
        }
    }

    private AwsDurableActionResponse status(AwsDurableActionRequest request) {
        var status = controlPlane.getExecutionStatus(
                request.tenantId(), required(request.executionId(), "executionId"))
            .await().atMost(ACTION_TIMEOUT);
        return AwsDurableActionResponse.status(status.status().name());
    }

    private AwsDurableActionResponse result(AwsDurableActionRequest request) {
        Object payload = Objects.requireNonNull(controlPlane.getExecutionResultPayload(
                request.tenantId(), required(request.executionId(), "executionId"))
            .await().atMost(ACTION_TIMEOUT), "result payload");
        try {
            return AwsDurableActionResponse.result(mapper.writeValueAsString(payload));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("TPF result payload could not be serialized", exception);
        }
    }

    private AwsDurableActionResponse redrive(AwsDurableActionRequest request) {
        var result = controlPlane.redriveExecution(
                request.tenantId(),
                required(request.executionId(), "executionId"),
                request.expectedVersion().orElseThrow(() ->
                    new IllegalArgumentException("expectedVersion is required")),
                true,
                required(request.reason(), "reason"))
            .await().atMost(ACTION_TIMEOUT);
        return AwsDurableActionResponse.status(result.status().name());
    }

    private AwsDurableActionResponse sweep() {
        controlPlane.sweepOnce(System.currentTimeMillis()).await().atMost(ACTION_TIMEOUT);
        return AwsDurableActionResponse.status("SWEEPED");
    }

    private AwsDurableActionResponse pendingAwait(AwsDurableActionRequest request) {
        String executionId = required(request.executionId(), "executionId");
        return controlPlane.getAwaitSemanticCheckpoints(request.tenantId(), executionId, 100)
            .await().atMost(ACTION_TIMEOUT).stream()
            .filter(checkpoint -> !checkpoint.status().terminal())
            .findFirst()
            .map(checkpoint -> AwsDurableActionResponse.pendingAwait(new AwsDurableAwaitIdentity(
                checkpoint.tenantId(),
                checkpoint.executionId(),
                checkpoint.interactionId(),
                checkpoint.correlationId(),
                request.generation())))
            .orElseGet(() -> new AwsDurableActionResponse(
                Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                java.util.List.of(), false));
    }

    private AwsDurableActionResponse awaitCheckpoint(AwsDurableActionRequest request) {
        AwsDurableAwaitIdentity identity = request.awaitIdentity()
            .orElseThrow(() -> new IllegalArgumentException("awaitIdentity is required"));
        return controlPlane.getAwaitSemanticCheckpoint(identity.tenantId(), identity.interactionId())
            .await().atMost(ACTION_TIMEOUT)
            .map(checkpoint -> AwsDurableActionResponse.awaitCheckpoint(new AwsDurableAwaitCheckpoint(
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.interactionId(),
                checkpoint.correlationId(), checkpoint.unitId(), checkpoint.stepId(), checkpoint.status().name(),
                checkpoint.pipelineId(), checkpoint.contractVersion(), checkpoint.releaseVersion())))
            .orElseThrow(() -> new IllegalStateException("Await semantic checkpoint is not available"));
    }

    private AwsDurableActionResponse executionAwaits(AwsDurableActionRequest request) {
        return AwsDurableActionResponse.awaitCheckpoints(controlPlane
            .getAwaitSemanticCheckpoints(
                request.tenantId(), required(request.executionId(), "executionId"), 100)
            .await().atMost(ACTION_TIMEOUT).stream()
            .map(checkpoint -> new AwsDurableAwaitCheckpoint(
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.interactionId(),
                checkpoint.correlationId(), checkpoint.unitId(), checkpoint.stepId(), checkpoint.status().name(),
                checkpoint.pipelineId(), checkpoint.contractVersion(), checkpoint.releaseVersion()))
            .toList());
    }

    private static String required(Optional<String> value, String name) {
        return value.orElseThrow(() -> new IllegalArgumentException(name + " is required"));
    }
}

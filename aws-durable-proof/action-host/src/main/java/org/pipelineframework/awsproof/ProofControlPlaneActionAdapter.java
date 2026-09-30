package org.pipelineframework.awsproof;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awsproof.model.ProofActionRequest;
import org.pipelineframework.awsproof.model.ProofActionResponse;
import org.pipelineframework.awsproof.model.ProofAwaitIdentity;
import org.pipelineframework.awsproof.model.ProofExecutionCheckpoint;
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

    ProofActionResponse handle(ProofActionRequest request) {
        return switch (request.operation()) {
            case SUBMIT -> submit(request);
            case STATUS -> status(request);
            case RESULT -> result(request);
            case REDRIVE -> redrive(request);
            case SWEEP -> sweep();
            case QUERY_PENDING_AWAIT -> pendingAwait(request);
            case REGISTER_CALLBACK, BIND_CALLBACK -> throw new IllegalArgumentException(
                request.operation() + " is an AWS Durable host operation");
        };
    }

    private ProofActionResponse submit(ProofActionRequest request) {
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
            return ProofActionResponse.submitted(
                new ProofExecutionCheckpoint(
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

    private ProofActionResponse status(ProofActionRequest request) {
        var status = controlPlane.getExecutionStatus(
                request.tenantId(), required(request.executionId(), "executionId"))
            .await().atMost(ACTION_TIMEOUT);
        return ProofActionResponse.status(status.status().name());
    }

    private ProofActionResponse result(ProofActionRequest request) {
        Object payload = Objects.requireNonNull(controlPlane.getExecutionResultPayload(
                request.tenantId(), required(request.executionId(), "executionId"))
            .await().atMost(ACTION_TIMEOUT), "result payload");
        try {
            return ProofActionResponse.result(mapper.writeValueAsString(payload));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("TPF result payload could not be serialized", exception);
        }
    }

    private ProofActionResponse redrive(ProofActionRequest request) {
        var result = controlPlane.redriveExecution(
                request.tenantId(),
                required(request.executionId(), "executionId"),
                request.expectedVersion().orElseThrow(() ->
                    new IllegalArgumentException("expectedVersion is required")),
                true,
                required(request.reason(), "reason"))
            .await().atMost(ACTION_TIMEOUT);
        return ProofActionResponse.status(result.status().name());
    }

    private ProofActionResponse sweep() {
        controlPlane.sweepOnce(System.currentTimeMillis()).await().atMost(ACTION_TIMEOUT);
        return ProofActionResponse.status("SWEEPED");
    }

    private ProofActionResponse pendingAwait(ProofActionRequest request) {
        String executionId = required(request.executionId(), "executionId");
        Optional<AwaitInteractionRecord> pending = controlPlane.queryPendingAwaitInteractions(
                request.tenantId(), "", "", "", 100)
            .await().atMost(ACTION_TIMEOUT).stream()
            .filter(record -> executionId.equals(record.executionId()))
            .findFirst();
        return pending
            .map(record -> ProofActionResponse.pendingAwait(new ProofAwaitIdentity(
                record.tenantId(),
                record.executionId(),
                record.interactionId(),
                record.correlationId(),
                request.generation())))
            .orElseGet(() -> new ProofActionResponse(
                Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty(), false));
    }

    private static String required(Optional<String> value, String name) {
        return value.orElseThrow(() -> new IllegalArgumentException(name + " is required"));
    }
}

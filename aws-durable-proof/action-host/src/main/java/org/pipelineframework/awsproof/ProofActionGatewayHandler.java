package org.pipelineframework.awsproof;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.awaitable.AwaitCompletionDescriptorRegistry;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awsproof.model.ProofActionRequest;
import org.pipelineframework.awsproof.model.ProofActionResponse;
import org.pipelineframework.awsproof.model.ProofAwaitIdentity;
import org.pipelineframework.awsproof.model.ProofBindingStatus;
import org.pipelineframework.awsproof.model.ProofCallbackBinding;
import org.pipelineframework.awsproof.model.ProofCallbackRegistration;
import org.pipelineframework.awsproof.model.ProofExecutionCheckpoint;
import org.pipelineframework.orchestrator.PipelineControlPlane;

/** One synchronous Lambda invocation maps to one existing bounded TPF action. */
@Named("proof-action-gateway")
@ApplicationScoped
public final class ProofActionGatewayHandler implements RequestHandler<ProofActionRequest, ProofActionResponse> {
    private static final Duration ACTION_TIMEOUT = Duration.ofSeconds(30);

    @Inject
    PipelineControlPlane controlPlane;

    @Inject
    ProofCallbackBindingRepository bindings;

    @Inject
    ProofWakeupService wakeups;

    @Inject
    ObjectMapper mapper;

    @Inject
    ProofFaultInjector faults;

    @Inject
    ProofReleaseCatalog releaseCatalog;

    @Inject
    AwaitCompletionDescriptorRegistry descriptorRegistry;

    @Inject
    ProofAwaitDescriptorFactory descriptorFactory;

    @Override
    public ProofActionResponse handleRequest(ProofActionRequest request, Context context) {
        Objects.requireNonNull(request, "request");
        descriptorRegistry.register(descriptorFactory.create());
        return switch (request.operation()) {
            case SUBMIT -> submit(request);
            case STATUS -> status(request);
            case RESULT -> result(request);
            case REDRIVE -> redrive(request);
            case SWEEP -> sweep();
            case QUERY_PENDING_AWAIT -> pendingAwait(request);
            case REGISTER_CALLBACK -> registerCallback(request);
            case BIND_CALLBACK -> bindCallback(request);
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
            return ProofActionResponse.submitted(new ProofExecutionCheckpoint(
                request.tenantId(),
                accepted.executionId(),
                accepted.duplicate(),
                pipelineId,
                contractVersion,
                releaseVersion,
                request.generation()));
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
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), false));
    }

    private ProofActionResponse bindCallback(ProofActionRequest request) {
        ProofAwaitIdentity identity = request.awaitIdentity()
            .orElseThrow(() -> new IllegalArgumentException("awaitIdentity is required"));
        long now = Instant.now().toEpochMilli();
        ProofCallbackBinding binding = new ProofCallbackBinding(
            identity,
            required(request.providerExecutionName(), "providerExecutionName"),
            required(request.providerExecutionArn(), "providerExecutionArn"),
            required(request.providerCallbackId(), "providerCallbackId"),
            ProofBindingStatus.OPEN,
            now,
            Instant.ofEpochMilli(now).plus(400, ChronoUnit.DAYS).getEpochSecond());
        faults.failIfArmed("bind-before-provider-binding", identity.executionId());
        boolean created = bindings.bind(binding);
        faults.failIfArmed("bind-after-provider-binding", identity.executionId());
        wakeups.wake(identity);
        return ProofActionResponse.bound(created);
    }

    private ProofActionResponse registerCallback(ProofActionRequest request) {
        ProofExecutionCheckpoint checkpoint = new ProofExecutionCheckpoint(
            request.tenantId(),
            required(request.executionId(), "executionId"),
            true,
            required(request.pipelineId(), "pipelineId"),
            required(request.contractVersion(), "contractVersion"),
            required(request.releaseVersion(), "releaseVersion"),
            request.generation());
        long now = Instant.now().toEpochMilli();
        ProofCallbackRegistration registration = new ProofCallbackRegistration(
            checkpoint,
            required(request.providerExecutionName(), "providerExecutionName"),
            required(request.providerExecutionArn(), "providerExecutionArn"),
            required(request.providerCallbackId(), "providerCallbackId"),
            now,
            Instant.ofEpochMilli(now).plus(400, ChronoUnit.DAYS).getEpochSecond());
        faults.failIfArmed("bind-before-provider-binding", checkpoint.executionId());
        boolean created = bindings.register(registration);
        faults.failIfArmed("bind-after-provider-binding", checkpoint.executionId());
        return ProofActionResponse.bound(created);
    }

    private static String required(Optional<String> value, String name) {
        return value.orElseThrow(() -> new IllegalArgumentException(name + " is required"));
    }
}

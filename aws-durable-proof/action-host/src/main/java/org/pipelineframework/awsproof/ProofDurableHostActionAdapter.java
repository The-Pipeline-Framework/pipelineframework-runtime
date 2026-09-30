package org.pipelineframework.awsproof;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.awsproof.model.ProofActionRequest;
import org.pipelineframework.awsproof.model.ProofActionResponse;
import org.pipelineframework.awsproof.model.ProofAwaitIdentity;
import org.pipelineframework.awsproof.model.ProofBindingStatus;
import org.pipelineframework.awsproof.model.ProofCallbackBinding;
import org.pipelineframework.awsproof.model.ProofCallbackRegistration;
import org.pipelineframework.awsproof.model.ProofDriverCheckpoint;
import org.pipelineframework.awsproof.model.ProofExecutionCheckpoint;

/** Owns AWS Durable callback mechanics without admitting TPF semantic transitions. */
@ApplicationScoped
final class ProofDurableHostActionAdapter {
    @Inject
    ProofCallbackBindingRepository bindings;

    @Inject
    ProofWakeupService wakeups;

    @Inject
    ProofFaultInjector faults;

    ProofActionResponse handle(ProofActionRequest request) {
        return switch (request.operation()) {
            case REGISTER_CALLBACK -> registerCallback(request);
            case BIND_CALLBACK -> bindCallback(request);
            case SUBMIT, STATUS, RESULT, REDRIVE, SWEEP, QUERY_PENDING_AWAIT ->
                throw new IllegalArgumentException(request.operation() + " is a TPF control-plane operation");
        };
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
        ProofDriverCheckpoint checkpoint = new ProofDriverCheckpoint(
            new ProofExecutionCheckpoint(
                request.tenantId(),
                required(request.executionId(), "executionId"),
                required(request.pipelineId(), "pipelineId"),
                required(request.contractVersion(), "contractVersion"),
                required(request.releaseVersion(), "releaseVersion")),
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

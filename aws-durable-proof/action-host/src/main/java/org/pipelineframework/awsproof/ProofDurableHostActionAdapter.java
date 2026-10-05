package org.pipelineframework.awsproof;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableBindingStatus;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;

/** Owns AWS Durable callback mechanics without admitting TPF semantic transitions. */
@ApplicationScoped
final class ProofDurableHostActionAdapter {
    @Inject
    ProofCallbackBindingRepository bindings;

    @Inject
    ProofWakeupService wakeups;

    @Inject
    ProofFaultInjector faults;

    AwsDurableActionResponse handle(AwsDurableActionRequest request) {
        return switch (request.operation()) {
            case REGISTER_CALLBACK -> registerCallback(request);
            case BIND_CALLBACK -> bindCallback(request);
            case SUBMIT, STATUS, RESULT, REDRIVE, SWEEP, QUERY_PENDING_AWAIT, READ_AWAIT_CHECKPOINT,
                READ_EXECUTION_AWAITS ->
                throw new IllegalArgumentException(request.operation() + " is a TPF control-plane operation");
        };
    }

    private AwsDurableActionResponse bindCallback(AwsDurableActionRequest request) {
        AwsDurableAwaitIdentity identity = request.awaitIdentity()
            .orElseThrow(() -> new IllegalArgumentException("awaitIdentity is required"));
        long now = Instant.now().toEpochMilli();
        AwsDurableCallbackBinding binding = new AwsDurableCallbackBinding(
            identity,
            required(request.providerExecutionName(), "providerExecutionName"),
            required(request.providerExecutionArn(), "providerExecutionArn"),
            required(request.providerCallbackId(), "providerCallbackId"),
            AwsDurableBindingStatus.OPEN,
            now,
            Instant.ofEpochMilli(now).plus(400, ChronoUnit.DAYS).getEpochSecond());
        faults.failIfArmed("bind-before-provider-binding", identity.executionId());
        boolean created = bindings.bind(binding);
        faults.failIfArmed("bind-after-provider-binding", identity.executionId());
        wakeups.wake(identity);
        return AwsDurableActionResponse.bound(created);
    }

    private AwsDurableActionResponse registerCallback(AwsDurableActionRequest request) {
        AwsDurableDriverCheckpoint checkpoint = new AwsDurableDriverCheckpoint(
            new AwsDurableExecutionCheckpoint(
                request.tenantId(),
                required(request.executionId(), "executionId"),
                required(request.pipelineId(), "pipelineId"),
                required(request.contractVersion(), "contractVersion"),
                required(request.releaseVersion(), "releaseVersion")),
            request.generation());
        long now = Instant.now().toEpochMilli();
        AwsDurableCallbackRegistration registration = new AwsDurableCallbackRegistration(
            checkpoint,
            required(request.providerExecutionName(), "providerExecutionName"),
            required(request.providerExecutionArn(), "providerExecutionArn"),
            required(request.providerCallbackId(), "providerCallbackId"),
            now,
            Instant.ofEpochMilli(now).plus(400, ChronoUnit.DAYS).getEpochSecond());
        faults.failIfArmed("bind-before-provider-binding", checkpoint.executionId());
        boolean created = bindings.register(registration);
        faults.failIfArmed("bind-after-provider-binding", checkpoint.executionId());
        return AwsDurableActionResponse.bound(created);
    }

    private static String required(Optional<String> value, String name) {
        return value.orElseThrow(() -> new IllegalArgumentException(name + " is required"));
    }
}

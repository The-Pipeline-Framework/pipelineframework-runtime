package org.pipelineframework.aws.durable;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackSignal;
import software.amazon.awssdk.services.lambda.model.CallbackTimeoutException;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;

/** Generation-fenced, TPF-first callback wake-up protocol. */
public final class AwsDurableWakeupService {
    private final AwsDurableHostTelemetry telemetry;
    private final AwsDurableAwaitCheckpointReader checkpoints;
    private final AwsDurableCallbackBindingRepository bindings;
    private final AwsDurableCallbackClient callbacks;
    private final AwsDurableReplacementStarter replacements;

    public AwsDurableWakeupService(
        AwsDurableAwaitCheckpointReader checkpoints,
        AwsDurableCallbackBindingRepository bindings,
        AwsDurableCallbackClient callbacks,
        AwsDurableReplacementStarter replacements
    ) {
        this(checkpoints, bindings, callbacks, replacements, new AwsDurableHostTelemetry());
    }

    AwsDurableWakeupService(AwsDurableAwaitCheckpointReader checkpoints,
        AwsDurableCallbackBindingRepository bindings, AwsDurableCallbackClient callbacks,
        AwsDurableReplacementStarter replacements, AwsDurableHostTelemetry telemetry) {
        this.telemetry = Objects.requireNonNull(telemetry);
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.callbacks = Objects.requireNonNull(callbacks, "callbacks");
        this.replacements = Objects.requireNonNull(replacements, "replacements");
    }

    public AwsDurableWakeupDisposition wake(AwsDurableAwaitIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        try {
            return wakeObserved(identity);
        } catch (RuntimeException retryRequired) {
            observe(identity, AwsDurableHostTelemetry.Outcome.TRANSIENT_FAILURE, AwsDurableWakeupDisposition.RETRY);
            throw retryRequired;
        }
    }

    private AwsDurableWakeupDisposition wakeObserved(AwsDurableAwaitIdentity identity) {
        Optional<AwsDurableAwaitCheckpoint> checkpoint = checkpoints.read(
            identity.tenantId(), identity.interactionId());
        if (checkpoint.isEmpty()) {
            return observe(identity, AwsDurableHostTelemetry.Outcome.CHECKPOINT_MISSING, AwsDurableWakeupDisposition.RETRY);
        }
        AwsDurableAwaitCheckpoint semantic = checkpoint.orElseThrow();
        if (!matches(identity, semantic)) {
            return observe(identity, AwsDurableHostTelemetry.Outcome.IDENTITY_MISMATCH, AwsDurableWakeupDisposition.ACKNOWLEDGE);
        }
        if (!"COMPLETED".equals(semantic.status())) {
            return observe(identity, AwsDurableHostTelemetry.Outcome.SEMANTIC_NOT_COMPLETED, AwsDurableWakeupDisposition.ACKNOWLEDGE);
        }
        Optional<AwsDurableCallbackBinding> binding = bindings.find(
            identity.tenantId(), identity.interactionId(), identity.generation(), identity.occurrence());
        if (bindings.isSuperseded(identity.tenantId(), identity.executionId(), identity.generation())) {
            Optional<AwsDurableCallbackBinding> obsolete = binding.or(() -> bindings.findRegistration(
                identity.tenantId(), identity.executionId(), identity.generation(), identity.occurrence()).map(value -> value.bind(identity)));
            return observe(identity, AwsDurableHostTelemetry.Outcome.STALE,
                obsolete.isPresent() && replacements.startReplacement(obsolete.orElseThrow())
                    ? AwsDurableWakeupDisposition.ACKNOWLEDGE : AwsDurableWakeupDisposition.RETRY);
        }
        if (binding.isEmpty()) {
            return observe(identity, AwsDurableHostTelemetry.Outcome.BINDING_MISSING, AwsDurableWakeupDisposition.RETRY);
        }
        AwsDurableCallbackBinding current = binding.orElseThrow();
        if (!bindings.isCurrentGeneration(identity.tenantId(), identity.executionId(), identity.generation())) {
            return observe(identity, AwsDurableHostTelemetry.Outcome.GENERATION_UNCONFIRMED, AwsDurableWakeupDisposition.RETRY);
        }
        Optional<AwsDurableCallbackBinding> latest = bindings.findLatest(
            identity.tenantId(), identity.interactionId());
        if (latest.isPresent()
            && !latest.orElseThrow().awaitIdentity().equals(identity)) {
            return observe(identity, AwsDurableHostTelemetry.Outcome.STALE, AwsDurableWakeupDisposition.ACKNOWLEDGE);
        }
        if (bindings.delivered(current)) {
            return observe(identity, AwsDurableHostTelemetry.Outcome.ALREADY_DELIVERED, AwsDurableWakeupDisposition.ACKNOWLEDGE);
        }
        try {
            if (bindings.isSuperseded(identity.tenantId(), identity.executionId(), identity.generation())) {
                return observe(identity, AwsDurableHostTelemetry.Outcome.STALE, replacements.startReplacement(current)
                    ? AwsDurableWakeupDisposition.ACKNOWLEDGE : AwsDurableWakeupDisposition.RETRY);
            }
            long acceptedAtEpochMs = callbacks.sendSuccessObserved(current, new AwsDurableCallbackSignal(
                identity.tenantId(), identity.executionId(), identity.interactionId(),
                identity.correlationId(), identity.generation(), semantic.status(), identity.occurrence()));
            bindings.recordCallbackAccepted(current, acceptedAtEpochMs);
            bindings.recordDelivered(current, "CALLBACK_ACCEPTED");
            return observe(identity, AwsDurableHostTelemetry.Outcome.CALLBACK_ACCEPTED, AwsDurableWakeupDisposition.ACKNOWLEDGE);
        } catch (ResourceNotFoundException | CallbackTimeoutException uncertain) {
            return reconcileUncertain(current);
        } catch (RuntimeException transientFailure) {
            return observe(identity, AwsDurableHostTelemetry.Outcome.TRANSIENT_FAILURE, AwsDurableWakeupDisposition.RETRY);
        }
    }

    private AwsDurableWakeupDisposition reconcileUncertain(AwsDurableCallbackBinding binding) {
        return switch (callbacks.state(binding)) {
            case SUCCEEDED -> {
                bindings.recordDelivered(binding, "PROVIDER_STATE_SUCCEEDED");
                yield observe(binding.awaitIdentity(), AwsDurableHostTelemetry.Outcome.PROVIDER_SUCCEEDED, AwsDurableWakeupDisposition.ACKNOWLEDGE);
            }
            case CLOSED -> observe(binding.awaitIdentity(), AwsDurableHostTelemetry.Outcome.PROVIDER_CLOSED,
                replacements.startReplacement(binding) ? AwsDurableWakeupDisposition.ACKNOWLEDGE : AwsDurableWakeupDisposition.RETRY);
            case OPEN -> observe(binding.awaitIdentity(), AwsDurableHostTelemetry.Outcome.PROVIDER_OPEN, AwsDurableWakeupDisposition.RETRY);
            case UNKNOWN -> observe(binding.awaitIdentity(), AwsDurableHostTelemetry.Outcome.PROVIDER_UNKNOWN, AwsDurableWakeupDisposition.RETRY);
        };
    }

    private AwsDurableWakeupDisposition observe(AwsDurableAwaitIdentity identity,
        AwsDurableHostTelemetry.Outcome outcome, AwsDurableWakeupDisposition disposition) {
        telemetry.record(AwsDurableHostTelemetry.Stage.WAKEUP, outcome,
            identity.tenantId().length() + ":" + identity.tenantId() + identity.executionId(), identity.generation());
        return disposition;
    }

    private static boolean matches(
        AwsDurableAwaitIdentity identity,
        AwsDurableAwaitCheckpoint checkpoint
    ) {
        return identity.tenantId().equals(checkpoint.tenantId())
            && identity.executionId().equals(checkpoint.executionId())
            && identity.interactionId().equals(checkpoint.interactionId())
            && identity.correlationId().equals(checkpoint.correlationId());
    }
}

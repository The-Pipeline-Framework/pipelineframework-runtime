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
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.callbacks = Objects.requireNonNull(callbacks, "callbacks");
        this.replacements = Objects.requireNonNull(replacements, "replacements");
    }

    public AwsDurableWakeupDisposition wake(AwsDurableAwaitIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        Optional<AwsDurableAwaitCheckpoint> checkpoint = checkpoints.read(
            identity.tenantId(), identity.interactionId());
        if (checkpoint.isEmpty()) {
            return AwsDurableWakeupDisposition.RETRY;
        }
        AwsDurableAwaitCheckpoint semantic = checkpoint.orElseThrow();
        if (!matches(identity, semantic) || !"COMPLETED".equals(semantic.status())) {
            return AwsDurableWakeupDisposition.ACKNOWLEDGE;
        }
        Optional<AwsDurableCallbackBinding> binding = bindings.find(
            identity.tenantId(), identity.interactionId(), identity.generation());
        if (binding.isEmpty()) {
            return AwsDurableWakeupDisposition.RETRY;
        }
        AwsDurableCallbackBinding current = binding.orElseThrow();
        Optional<AwsDurableCallbackBinding> latest = bindings.findLatest(
            identity.tenantId(), identity.interactionId());
        if (latest.isPresent()
            && latest.orElseThrow().awaitIdentity().generation() != identity.generation()) {
            return AwsDurableWakeupDisposition.ACKNOWLEDGE;
        }
        if (bindings.delivered(current)) {
            return AwsDurableWakeupDisposition.ACKNOWLEDGE;
        }
        try {
            callbacks.sendSuccess(current, new AwsDurableCallbackSignal(
                identity.tenantId(), identity.executionId(), identity.interactionId(),
                identity.correlationId(), identity.generation(), semantic.status()));
            bindings.recordDelivered(current, "CALLBACK_ACCEPTED");
            return AwsDurableWakeupDisposition.ACKNOWLEDGE;
        } catch (ResourceNotFoundException | CallbackTimeoutException uncertain) {
            return reconcileUncertain(current);
        } catch (RuntimeException transientFailure) {
            return AwsDurableWakeupDisposition.RETRY;
        }
    }

    private AwsDurableWakeupDisposition reconcileUncertain(AwsDurableCallbackBinding binding) {
        return switch (callbacks.state(binding)) {
            case SUCCEEDED -> {
                bindings.recordDelivered(binding, "PROVIDER_STATE_SUCCEEDED");
                yield AwsDurableWakeupDisposition.ACKNOWLEDGE;
            }
            case CLOSED -> replacements.startReplacement(binding)
                ? AwsDurableWakeupDisposition.ACKNOWLEDGE
                : AwsDurableWakeupDisposition.RETRY;
            case OPEN, UNKNOWN -> AwsDurableWakeupDisposition.RETRY;
        };
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

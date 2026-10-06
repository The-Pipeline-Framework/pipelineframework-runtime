package org.pipelineframework.awsproof;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitCompletionDescriptorRegistry;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.store.DynamoAwaitInteractionStore;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackSignal;
import software.amazon.awssdk.services.lambda.model.CallbackTimeoutException;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;

@ApplicationScoped
final class ProofWakeupService {
    private static final Logger LOG = Logger.getLogger(ProofWakeupService.class);
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(15);

    @Inject
    DynamoAwaitInteractionStore awaitStore;

    @Inject
    ProofCallbackBindingRepository bindings;

    @Inject
    ProofDurableCallbackClient callbacks;

    @Inject
    ProofFaultInjector faults;

    @Inject
    ProofDriverRecoveryService recovery;

    @Inject
    AwaitCompletionDescriptorRegistry descriptorRegistry;

    @Inject
    ProofAwaitDescriptorFactory descriptorFactory;

    ProofWakeupDisposition wake(AwsDurableAwaitIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        descriptorRegistry.register(descriptorFactory.create());
        var interaction = awaitStore.get(identity.tenantId(), identity.interactionId())
            .await().atMost(READ_TIMEOUT);
        if (interaction.isEmpty()) {
            return ProofWakeupDisposition.RETRY;
        }
        var record = interaction.orElseThrow();
        if (!record.executionId().equals(identity.executionId())
            || !record.correlationId().equals(identity.correlationId())) {
            return ProofWakeupDisposition.ACKNOWLEDGE;
        }
        if (record.status() != AwaitInteractionStatus.COMPLETED) {
            return ProofWakeupDisposition.ACKNOWLEDGE;
        }
        var binding = bindings.find(identity.tenantId(), identity.interactionId(), identity.generation());
        if (binding.isEmpty()) {
            return ProofWakeupDisposition.RETRY;
        }
        AwsDurableCallbackBinding current = binding.orElseThrow();
        Optional<AwsDurableCallbackBinding> latest = bindings.findLatest(identity.tenantId(), identity.interactionId());
        if (latest.isPresent()
            && latest.orElseThrow().awaitIdentity().generation() != identity.generation()) {
            return ProofWakeupDisposition.ACKNOWLEDGE;
        }
        if (bindings.delivered(current)) {
            return ProofWakeupDisposition.ACKNOWLEDGE;
        }
        AwsDurableCallbackSignal signal = new AwsDurableCallbackSignal(
            identity.tenantId(),
            identity.executionId(),
            identity.interactionId(),
            identity.correlationId(),
            identity.generation(),
            record.status().name());
        try {
            faults.failIfArmed("wakeup-before-provider-callback", identity.executionId());
            callbacks.sendSuccess(current, signal);
            faults.failIfArmed("wakeup-after-provider-callback", identity.executionId());
            bindings.recordDelivered(current, "CALLBACK_ACCEPTED");
            return ProofWakeupDisposition.ACKNOWLEDGE;
        } catch (ResourceNotFoundException | CallbackTimeoutException uncertain) {
            LOG.warnf(uncertain,
                "Provider callback outcome is uncertain for execution=%s interaction=%s generation=%d",
                identity.executionId(), identity.interactionId(), identity.generation());
            return reconcileUncertain(current);
        } catch (RuntimeException transientFailure) {
            LOG.warnf(transientFailure,
                "Provider callback delivery will retry for execution=%s interaction=%s generation=%d",
                identity.executionId(), identity.interactionId(), identity.generation());
            return ProofWakeupDisposition.RETRY;
        }
    }

    private ProofWakeupDisposition reconcileUncertain(AwsDurableCallbackBinding binding) {
        return switch (callbacks.state(binding)) {
            case SUCCEEDED -> {
                bindings.recordDelivered(binding, "PROVIDER_STATE_SUCCEEDED");
                yield ProofWakeupDisposition.ACKNOWLEDGE;
            }
            case CLOSED -> {
                yield recovery.recoverClosed(binding.providerExecutionArn())
                    ? ProofWakeupDisposition.ACKNOWLEDGE
                    : ProofWakeupDisposition.RETRY;
            }
            case OPEN, UNKNOWN -> ProofWakeupDisposition.RETRY;
        };
    }
}

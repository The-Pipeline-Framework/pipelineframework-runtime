package org.pipelineframework.aws.durable;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import software.amazon.awssdk.services.lambda.model.CallbackTimeoutException;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;

/** Settles disposable callback state only after a terminal semantic observation. */
public final class AwsDurableTerminalCallbackSettler {
    private final PipelineControlPlane controlPlane;
    private final AwsDurableCallbackBindingRepository bindings;
    private final AwsDurableCallbackClient callbacks;
    private final Duration timeout;

    public AwsDurableTerminalCallbackSettler(PipelineControlPlane controlPlane,
        AwsDurableCallbackBindingRepository bindings, AwsDurableCallbackClient callbacks, Duration timeout) {
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.callbacks = Objects.requireNonNull(callbacks, "callbacks");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("timeout must be positive");
    }

    public AwsDurableWakeupDisposition settleExecution(String tenantId, String executionId) {
        return bindings.findLatestRegistration(tenantId, executionId)
            .flatMap(this::settle).orElse(AwsDurableWakeupDisposition.ACKNOWLEDGE);
    }

    public Optional<AwsDurableWakeupDisposition> settle(AwsDurableCallbackRegistration registration) {
        var execution = registration.checkpoint().execution();
        var status = controlPlane.getExecutionSemanticCheckpoint(execution.tenantId(), execution.executionId())
            .await().atMost(timeout);
        if (!execution.tenantId().equals(status.tenantId())
            || !execution.executionId().equals(status.executionId())
            || !execution.pipelineId().equals(status.pipelineId())
            || !execution.contractVersion().equals(status.contractVersion())
            || !execution.releaseVersion().equals(status.releaseVersion())) {
            throw new IllegalStateException("semantic execution observation does not match registration");
        }
        String terminal = status.status().name();
        if (!java.util.Set.of("FAILED", "DLQ", "REMOTE_OUTCOME_UNKNOWN").contains(terminal)) {
            return Optional.empty();
        }
        var identity = registration.awaitIdentity()
            .orElseThrow(() -> new IllegalStateException("terminal settlement requires a pinned registration"));
        if (bindings.isSuperseded(identity.tenantId(), identity.executionId(), identity.generation())) {
            return Optional.of(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        }
        if (!bindings.isCurrentGeneration(identity.tenantId(), identity.executionId(), identity.generation())) {
            return Optional.of(AwsDurableWakeupDisposition.RETRY);
        }
        var binding = registration.bind(identity);
        if (bindings.delivered(binding)) return Optional.of(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        try {
            bindings.bind(binding);
            // The provider API cannot join the generation transaction; fence again immediately before delivery.
            if (!bindings.isCurrentGeneration(identity.tenantId(), identity.executionId(), identity.generation())) {
                return Optional.of(AwsDurableWakeupDisposition.RETRY);
            }
            callbacks.sendFailure(binding, terminal);
            bindings.recordDelivered(binding, "TERMINAL_CALLBACK_ACCEPTED");
            return Optional.of(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        } catch (AwsDurableStaleGenerationException stale) {
            return Optional.of(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        } catch (ResourceNotFoundException | CallbackTimeoutException uncertain) {
            return Optional.of(switch (callbacks.state(binding)) {
                case SUCCEEDED, CLOSED -> {
                    bindings.recordDelivered(binding, "TERMINAL_PROVIDER_CALLBACK_CLOSED");
                    yield AwsDurableWakeupDisposition.ACKNOWLEDGE;
                }
                case OPEN, UNKNOWN -> AwsDurableWakeupDisposition.RETRY;
            });
        } catch (RuntimeException transientFailure) {
            return Optional.of(AwsDurableWakeupDisposition.RETRY);
        }
    }
}

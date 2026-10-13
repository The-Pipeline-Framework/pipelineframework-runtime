package org.pipelineframework.aws.durable;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;

/** Starts a fenced Durable generation from TPF's reconstructable semantic checkpoint. */
public final class AwsDurableReplacementService implements AwsDurableReplacementStarter {
    private final AwsDurableHostTelemetry telemetry;
    private final AwsDurableAwaitCheckpointReader checkpoints;
    private final AwsDurableExecutionStarter starter;
    private final AwsDurableCallbackBindingRepository bindings;

    public AwsDurableReplacementService(
        AwsDurableAwaitCheckpointReader checkpoints,
        AwsDurableExecutionStarter starter,
        AwsDurableCallbackBindingRepository bindings
    ) {
        this(checkpoints, starter, bindings, new AwsDurableHostTelemetry());
    }

    AwsDurableReplacementService(AwsDurableAwaitCheckpointReader checkpoints,
        AwsDurableExecutionStarter starter, AwsDurableCallbackBindingRepository bindings,
        AwsDurableHostTelemetry telemetry) {
        this.telemetry = Objects.requireNonNull(telemetry);
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.starter = Objects.requireNonNull(starter, "starter");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
    }

    @Override
    public boolean startReplacement(AwsDurableCallbackBinding binding) {
        Objects.requireNonNull(binding, "binding");
        try {
            return startObserved(binding);
        } catch (RuntimeException retryRequired) {
            observe(binding, AwsDurableHostTelemetry.Outcome.FAILED, false);
            throw retryRequired;
        }
    }

    private boolean startObserved(AwsDurableCallbackBinding binding) {
        Optional<AwsDurableAwaitCheckpoint> checkpoint = checkpoints.read(
            binding.awaitIdentity().tenantId(), binding.awaitIdentity().interactionId());
        if (checkpoint.isEmpty()) {
            return observe(binding, AwsDurableHostTelemetry.Outcome.CHECKPOINT_MISSING, false);
        }
        AwsDurableAwaitCheckpoint semantic = checkpoint.orElseThrow();
        if (!binding.awaitIdentity().tenantId().equals(semantic.tenantId())
            || !binding.awaitIdentity().interactionId().equals(semantic.interactionId())
            || !binding.awaitIdentity().executionId().equals(semantic.executionId())
            || !binding.awaitIdentity().correlationId().equals(semantic.correlationId())) {
            return observe(binding, AwsDurableHostTelemetry.Outcome.IDENTITY_MISMATCH, false);
        }
        AwsDurableExecutionInput input = AwsDurableExecutionInput.resume(
            binding.awaitIdentity(), semantic.pipelineId(), semantic.contractVersion(), semantic.releaseVersion());
        var claim = new AwsDurableDriverCheckpoint(
            new AwsDurableExecutionCheckpoint(
                semantic.tenantId(), semantic.executionId(), semantic.pipelineId(),
                semantic.contractVersion(), semantic.releaseVersion()), input.generation());
        if (bindings.claimGeneration(claim) == AwsDurableGenerationAdmission.STALE) {
            return observe(binding, AwsDurableHostTelemetry.Outcome.STALE, true); // A later generation owns recovery.
        }
        var response = starter.pinnedToExecution(binding.providerExecutionArn()).start(input);
        boolean started = response.statusCode() >= 200 && response.statusCode() < 300;
        return observe(binding, started ? AwsDurableHostTelemetry.Outcome.STARTED : AwsDurableHostTelemetry.Outcome.REJECTED, started);
    }
    private boolean observe(AwsDurableCallbackBinding binding, AwsDurableHostTelemetry.Outcome outcome, boolean result) {
        var identity = binding.awaitIdentity();
        telemetry.record(AwsDurableHostTelemetry.Stage.REPLACEMENT, outcome,
            identity.tenantId().length() + ":" + identity.tenantId() + identity.executionId(), identity.generation());
        return result;
    }
}

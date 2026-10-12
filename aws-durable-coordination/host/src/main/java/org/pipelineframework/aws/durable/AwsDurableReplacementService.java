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
    private final AwsDurableAwaitCheckpointReader checkpoints;
    private final AwsDurableExecutionStarter starter;
    private final AwsDurableCallbackBindingRepository bindings;

    public AwsDurableReplacementService(
        AwsDurableAwaitCheckpointReader checkpoints,
        AwsDurableExecutionStarter starter,
        AwsDurableCallbackBindingRepository bindings
    ) {
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.starter = Objects.requireNonNull(starter, "starter");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
    }

    @Override
    public boolean startReplacement(AwsDurableCallbackBinding binding) {
        Objects.requireNonNull(binding, "binding");
        Optional<AwsDurableAwaitCheckpoint> checkpoint = checkpoints.read(
            binding.awaitIdentity().tenantId(), binding.awaitIdentity().interactionId());
        if (checkpoint.isEmpty()) {
            return false;
        }
        AwsDurableAwaitCheckpoint semantic = checkpoint.orElseThrow();
        if (!binding.awaitIdentity().tenantId().equals(semantic.tenantId())
            || !binding.awaitIdentity().interactionId().equals(semantic.interactionId())
            || !binding.awaitIdentity().executionId().equals(semantic.executionId())
            || !binding.awaitIdentity().correlationId().equals(semantic.correlationId())) {
            return false;
        }
        AwsDurableExecutionInput input = AwsDurableExecutionInput.resume(
            binding.awaitIdentity(), semantic.pipelineId(), semantic.contractVersion(), semantic.releaseVersion());
        var claim = new AwsDurableDriverCheckpoint(
            new AwsDurableExecutionCheckpoint(
                semantic.tenantId(), semantic.executionId(), semantic.pipelineId(),
                semantic.contractVersion(), semantic.releaseVersion()), input.generation());
        if (bindings.claimGeneration(claim) == AwsDurableGenerationAdmission.STALE) {
            return true; // A later generation already owns recovery; do not start an obsolete replacement.
        }
        var response = starter.pinnedToExecution(binding.providerExecutionArn()).start(input);
        return response.statusCode() >= 200 && response.statusCode() < 300;
    }
}

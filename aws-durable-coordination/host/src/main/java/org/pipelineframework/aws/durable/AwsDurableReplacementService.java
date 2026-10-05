package org.pipelineframework.aws.durable;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;

/** Starts a fenced Durable generation from TPF's reconstructable semantic checkpoint. */
public final class AwsDurableReplacementService implements AwsDurableReplacementStarter {
    private final AwsDurableAwaitCheckpointReader checkpoints;
    private final AwsDurableExecutionStarter starter;

    public AwsDurableReplacementService(
        AwsDurableAwaitCheckpointReader checkpoints,
        AwsDurableExecutionStarter starter
    ) {
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.starter = Objects.requireNonNull(starter, "starter");
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
        if (!binding.awaitIdentity().executionId().equals(semantic.executionId())
            || !binding.awaitIdentity().correlationId().equals(semantic.correlationId())) {
            return false;
        }
        var response = starter.start(AwsDurableExecutionInput.resume(
            binding.awaitIdentity(), semantic.pipelineId(), semantic.contractVersion(), semantic.releaseVersion()));
        return response.statusCode() >= 200 && response.statusCode() < 300;
    }
}

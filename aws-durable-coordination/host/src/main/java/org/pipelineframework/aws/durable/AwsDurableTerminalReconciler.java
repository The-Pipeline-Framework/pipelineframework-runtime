package org.pipelineframework.aws.durable;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionStatusPoll;
import org.pipelineframework.orchestrator.PipelineControlPlane;

/** Targeted reconstruction from semantic execution state, independent of callback rows. */
public final class AwsDurableTerminalReconciler {
    private final AwsDurableCallbackBindingRepository bindings;
    private final PipelineControlPlane controlPlane;
    private final AwsDurableExecutionStarter starter;
    private final AwsDurableProviderExecutionInspector providerExecutions;
    private final Duration timeout;

    public AwsDurableTerminalReconciler(AwsDurableCallbackBindingRepository bindings,
        PipelineControlPlane controlPlane, AwsDurableExecutionStarter starter,
        AwsDurableProviderExecutionInspector providerExecutions, Duration timeout) {
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane");
        this.starter = Objects.requireNonNull(starter, "starter");
        this.providerExecutions = Objects.requireNonNull(providerExecutions, "providerExecutions");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("timeout must be positive");
    }

    public boolean reconcile(String providerExecutionArn) {
        var providerState = providerExecutions.inspect(providerExecutionArn);
        if (providerState == AwsDurableProviderExecutionState.ACTIVE || providerState == AwsDurableProviderExecutionState.SUCCEEDED) {
            return false;
        }
        var locator = bindings.findRecoveryByProviderExecutionArn(providerExecutionArn)
            .orElseThrow(() -> new IllegalStateException("execution recovery locator is not visible; retry targeted reconciliation"));
        var original = locator.checkpoint();
        var semantic = controlPlane.getExecutionSemanticCheckpoint(original.tenantId(), original.executionId()).await().atMost(timeout);
        if (!original.tenantId().equals(semantic.tenantId()) || !original.executionId().equals(semantic.executionId())
            || !original.pipelineId().equals(semantic.pipelineId()) || !original.contractVersion().equals(semantic.contractVersion())
            || !original.releaseVersion().equals(semantic.releaseVersion())) {
            throw new IllegalStateException("recovery locator does not match semantic execution identity and release");
        }
        if (new AwsDurableExecutionStatusPoll(semantic.status().name()).terminal()) return false;
        // Validate the retained numeric target before fencing the old generation.
        var pinnedStarter = starter.pinnedToExecution(providerExecutionArn);
        long generation = Math.addExact(original.generation(), 1);
        var checkpoint = new AwsDurableExecutionCheckpoint(semantic.tenantId(), semantic.executionId(),
            semantic.pipelineId(), semantic.contractVersion(), semantic.releaseVersion());
        if (bindings.claimGeneration(new AwsDurableDriverCheckpoint(checkpoint, generation)) == AwsDurableGenerationAdmission.STALE) {
            return false;
        }
        var input = new AwsDurableExecutionInput(semantic.tenantId(), semantic.executionId(), semantic.pipelineId(),
            semantic.contractVersion(), semantic.releaseVersion(), "{}", Optional.of(semantic.executionId()), generation);
        var response = pinnedStarter.start(input);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("replacement admission is unresolved; retry reconciliation");
        }
        return true;
    }
}

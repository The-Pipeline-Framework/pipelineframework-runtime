package org.pipelineframework.aws.durable;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionStatusPoll;
import org.pipelineframework.orchestrator.PipelineControlPlane;

/** Targeted reconstruction from semantic execution state, independent of callback rows. */
public final class AwsDurableTerminalReconciler {
    private final AwsDurableHostTelemetry telemetry;
    private final AwsDurableCallbackBindingRepository bindings;
    private final PipelineControlPlane controlPlane;
    private final AwsDurableExecutionStarter starter;
    private final AwsDurableProviderExecutionInspector providerExecutions;
    private final Duration timeout;

    public AwsDurableTerminalReconciler(AwsDurableCallbackBindingRepository bindings,
        PipelineControlPlane controlPlane, AwsDurableExecutionStarter starter,
        AwsDurableProviderExecutionInspector providerExecutions, Duration timeout) {
        this(bindings, controlPlane, starter, providerExecutions, timeout, new AwsDurableHostTelemetry());
    }

    AwsDurableTerminalReconciler(AwsDurableCallbackBindingRepository bindings,
        PipelineControlPlane controlPlane, AwsDurableExecutionStarter starter,
        AwsDurableProviderExecutionInspector providerExecutions, Duration timeout, AwsDurableHostTelemetry telemetry) {
        this.telemetry = Objects.requireNonNull(telemetry);
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane");
        this.starter = Objects.requireNonNull(starter, "starter");
        this.providerExecutions = Objects.requireNonNull(providerExecutions, "providerExecutions");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("timeout must be positive");
    }

    public boolean reconcile(String providerExecutionArn) {
        try {
            return reconcileObserved(providerExecutionArn);
        } catch (RuntimeException retryRequired) {
            telemetry.record(AwsDurableHostTelemetry.Stage.REPAIR, AwsDurableHostTelemetry.Outcome.FAILED, providerExecutionArn, 0);
            throw retryRequired;
        }
    }

    private boolean reconcileObserved(String providerExecutionArn) {
        OptionalLong firstObservedAt = OptionalLong.empty();
        try {
            firstObservedAt = bindings.recordRepairFirstObservedAt(providerExecutionArn);
        } catch (RuntimeException unavailable) {
            // This retained timestamp is an observation only; its storage cannot block repair.
        }
        telemetry.recordRepairAge(providerExecutionArn, firstObservedAt);
        var providerState = providerExecutions.inspect(providerExecutionArn);
        if (providerState == AwsDurableProviderExecutionState.ACTIVE || providerState == AwsDurableProviderExecutionState.SUCCEEDED) {
            telemetry.record(AwsDurableHostTelemetry.Stage.REPAIR,
                providerState == AwsDurableProviderExecutionState.ACTIVE ? AwsDurableHostTelemetry.Outcome.PROVIDER_ACTIVE
                    : AwsDurableHostTelemetry.Outcome.PROVIDER_SUCCEEDED, providerExecutionArn, 0);
            return false;
        }
        var retained = bindings.findRecoveryByProviderExecutionArn(providerExecutionArn);
        org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint semantic;
        AwsDurableDriverCheckpoint original;
        if (retained.isPresent()) {
            original = retained.orElseThrow().checkpoint();
            semantic = controlPlane.getExecutionSemanticCheckpoint(original.tenantId(), original.executionId()).await().atMost(timeout);
        } else {
            var admission = bindings.findAdmissionByProviderExecutionArn(providerExecutionArn)
                .orElseThrow(() -> new IllegalStateException("execution recovery locator is not visible; retry targeted reconciliation"));
            semantic = admission.executionId().isPresent()
                ? controlPlane.getExecutionSemanticCheckpoint(admission.tenantId(), admission.executionId().orElseThrow()).await().atMost(timeout)
                : controlPlane.getExecutionSemanticCheckpointByClientKey(admission.tenantId(), admission.clientKey()).await().atMost(timeout)
                    .orElseThrow(() -> new IllegalStateException("semantic admission is not visible; retry without resubmission"));
            if (!admission.pipelineId().equals(semantic.pipelineId())
                || !admission.contractVersion().equals(semantic.contractVersion())
                || !admission.releaseVersion().equals(semantic.releaseVersion())) {
                throw new IllegalStateException("admission locator does not match semantic pipeline and release");
            }
            original = new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint(semantic.tenantId(), semantic.executionId(),
                semantic.pipelineId(), semantic.contractVersion(), semantic.releaseVersion()), admission.generation());
        }
        if (!original.tenantId().equals(semantic.tenantId()) || !original.executionId().equals(semantic.executionId())
            || !original.pipelineId().equals(semantic.pipelineId()) || !original.contractVersion().equals(semantic.contractVersion())
            || !original.releaseVersion().equals(semantic.releaseVersion())) {
            throw new IllegalStateException("recovery locator does not match semantic execution identity and release");
        }
        if (new AwsDurableExecutionStatusPoll(semantic.status().name()).terminal()) {
            telemetry.record(AwsDurableHostTelemetry.Stage.REPAIR, AwsDurableHostTelemetry.Outcome.SEMANTIC_TERMINAL, providerExecutionArn, original.generation());
            return false;
        }
        // Validate the retained numeric target before fencing the old generation.
        var pinnedStarter = starter.pinnedToExecution(providerExecutionArn);
        if (retained.isEmpty()) {
            // ADMISSION predates semantic SUBMIT. Only the admitted semantic identity above
            // can establish its original claim; a callback row or provider history cannot.
            // A replay may already be fenced; the successor admission below decides whether
            // this deterministic replacement is still current and needs its start replayed.
            bindings.claimGeneration(original);
        }
        long generation = Math.addExact(original.generation(), 1);
        var checkpoint = new AwsDurableExecutionCheckpoint(semantic.tenantId(), semantic.executionId(),
            semantic.pipelineId(), semantic.contractVersion(), semantic.releaseVersion());
        if (bindings.claimGeneration(new AwsDurableDriverCheckpoint(checkpoint, generation)) == AwsDurableGenerationAdmission.STALE) {
            telemetry.record(AwsDurableHostTelemetry.Stage.REPAIR, AwsDurableHostTelemetry.Outcome.STALE, providerExecutionArn, generation);
            return false;
        }
        var input = new AwsDurableExecutionInput(semantic.tenantId(), semantic.executionId(), semantic.pipelineId(),
            semantic.contractVersion(), semantic.releaseVersion(), "{}", Optional.of(semantic.executionId()), generation);
        var response = pinnedStarter.start(input);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("replacement admission is unresolved; retry reconciliation");
        }
        telemetry.record(AwsDurableHostTelemetry.Stage.REPAIR, AwsDurableHostTelemetry.Outcome.STARTED, providerExecutionArn, generation);
        return true;
    }
}

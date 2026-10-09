package org.pipelineframework.orchestrator;

import java.util.Optional;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.release.PipelineReleaseEvidence;

/** Native admission authority retaining the complete verified Release snapshot, not only its fingerprint. */
public interface NativeExecutionAdmissionStore {
    Uni<Optional<ExecutionAdmissionResult>> inspectExistingAdmission(ExecutionAdmissionIntent intent);

    Uni<Optional<ExecutionAdmissionReceipt>> lookupAdmission(String tenantId, String pipelineId, String clientKey);

    Uni<ExecutionAdmissionResult> createOrGetNativeAdmittedExecution(
        ExecutionAdmissionCreateCommand command, PipelineReleaseEvidence evidence);
}

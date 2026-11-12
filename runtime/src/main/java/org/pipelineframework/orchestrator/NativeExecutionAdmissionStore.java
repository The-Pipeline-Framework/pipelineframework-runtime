package org.pipelineframework.orchestrator;

import java.util.Optional;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.release.PipelineReleaseEvidence;

/** Native admission authority retaining the complete verified Release snapshot, not only its fingerprint. */
public interface NativeExecutionAdmissionStore {
    /** Uses the exact admission scope for paired executions, independently of ordinary submission policy. */
    static String pairedExecutionKey(ExecutionAdmissionIntent intent) {
        return NativeExecutionAdmission.key(intent.tenantId(), intent.pipelineId(), intent.clientKey());
    }

    Uni<Optional<ExecutionAdmissionResult>> inspectExistingAdmission(ExecutionAdmissionIntent intent);

    /** Reads the immutable initial input associated with an already observed native execution. */
    Uni<Optional<Object>> getInitialExecutionInput(ExecutionRecord<Object, Object> execution);

    Uni<Optional<ExecutionAdmissionReceipt>> lookupAdmission(String tenantId, String pipelineId, String clientKey);

    Uni<ExecutionAdmissionResult> createOrGetNativeAdmittedExecution(
        ExecutionAdmissionCreateCommand command, PipelineReleaseEvidence evidence);
}

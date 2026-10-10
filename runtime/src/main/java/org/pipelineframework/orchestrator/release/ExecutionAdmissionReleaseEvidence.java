package org.pipelineframework.orchestrator.release;

/** Server-owned snapshot of the already verified registered Release for native execution admission. */
public final class ExecutionAdmissionReleaseEvidence {
    private ExecutionAdmissionReleaseEvidence() { }

    public static PipelineReleaseEvidence snapshot(PipelineReleaseRecord release) {
        return PipelineReleaseEvidence.from(release);
    }
}

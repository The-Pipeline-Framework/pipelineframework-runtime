package org.pipelineframework.orchestrator;

/** A retained admission or its paired worker command cannot fit its size limit. */
public final class ExecutionAdmissionTooLargeException extends IllegalArgumentException {
    public ExecutionAdmissionTooLargeException() {
        super("Complete retained execution admission exceeds the DynamoDB item limit");
    }

    public ExecutionAdmissionTooLargeException(String message) {
        super(message);
    }
}

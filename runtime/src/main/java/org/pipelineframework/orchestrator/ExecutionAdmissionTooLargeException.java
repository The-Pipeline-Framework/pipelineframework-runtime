package org.pipelineframework.orchestrator;

/** The complete retained native Dynamo admission row cannot fit in the provider's atomic item limit. */
public final class ExecutionAdmissionTooLargeException extends IllegalArgumentException {
    public ExecutionAdmissionTooLargeException() {
        super("Complete retained execution admission exceeds the DynamoDB item limit");
    }

    public ExecutionAdmissionTooLargeException(String message) {
        super(message);
    }
}

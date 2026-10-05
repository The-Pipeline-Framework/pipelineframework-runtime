package org.pipelineframework.aws.durable.model;

public enum AwsDurableOperation {
    SUBMIT,
    STATUS,
    RESULT,
    REDRIVE,
    SWEEP,
    QUERY_PENDING_AWAIT,
    READ_AWAIT_CHECKPOINT,
    READ_EXECUTION_AWAITS,
    REGISTER_CALLBACK,
    BIND_CALLBACK
}

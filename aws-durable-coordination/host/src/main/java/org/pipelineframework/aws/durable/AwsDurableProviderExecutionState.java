package org.pipelineframework.aws.durable;

enum AwsDurableProviderExecutionState {
    ACTIVE,
    SUCCEEDED,
    REPLACEABLE,
    MISSING
}

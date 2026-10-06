package org.pipelineframework.aws.durable;

import java.util.Objects;

import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.ExecutionStatus;
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionRequest;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;

final class LambdaAwsDurableProviderExecutionInspector implements AwsDurableProviderExecutionInspector {
    private final LambdaClient lambda;

    LambdaAwsDurableProviderExecutionInspector(LambdaClient lambda) {
        this.lambda = Objects.requireNonNull(lambda, "lambda");
    }

    @Override
    public AwsDurableProviderExecutionState inspect(String providerExecutionArn) {
        try {
            ExecutionStatus status = lambda.getDurableExecution(GetDurableExecutionRequest.builder()
                .durableExecutionArn(providerExecutionArn)
                .build()).status();
            return switch (status) {
                case RUNNING -> AwsDurableProviderExecutionState.ACTIVE;
                case SUCCEEDED -> AwsDurableProviderExecutionState.SUCCEEDED;
                case FAILED, STOPPED, TIMED_OUT -> AwsDurableProviderExecutionState.REPLACEABLE;
                case UNKNOWN_TO_SDK_VERSION -> throw new IllegalStateException(
                    "Unsupported Durable execution status for " + providerExecutionArn);
            };
        } catch (ResourceNotFoundException expiredOrDeleted) {
            return AwsDurableProviderExecutionState.MISSING;
        }
    }
}

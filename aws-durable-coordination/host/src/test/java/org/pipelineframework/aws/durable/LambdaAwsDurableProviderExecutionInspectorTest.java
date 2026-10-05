package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.ExecutionStatus;
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionRequest;
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionResponse;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;

class LambdaAwsDurableProviderExecutionInspectorTest {
    private final LambdaClient lambda = mock(LambdaClient.class);
    private final LambdaAwsDurableProviderExecutionInspector inspector =
        new LambdaAwsDurableProviderExecutionInspector(lambda);

    @Test
    void mapsProviderStatusesToReplacementDecisions() {
        assertThat(inspect(ExecutionStatus.RUNNING)).isEqualTo(AwsDurableProviderExecutionState.ACTIVE);
        assertThat(inspect(ExecutionStatus.SUCCEEDED)).isEqualTo(AwsDurableProviderExecutionState.SUCCEEDED);
        assertThat(inspect(ExecutionStatus.FAILED)).isEqualTo(AwsDurableProviderExecutionState.REPLACEABLE);
        assertThat(inspect(ExecutionStatus.STOPPED)).isEqualTo(AwsDurableProviderExecutionState.REPLACEABLE);
        assertThat(inspect(ExecutionStatus.TIMED_OUT)).isEqualTo(AwsDurableProviderExecutionState.REPLACEABLE);
    }

    @Test
    void treatsExpiredOrDeletedProviderHistoryAsMissing() {
        when(lambda.getDurableExecution(any(GetDurableExecutionRequest.class)))
            .thenThrow(ResourceNotFoundException.builder().message("gone").build());

        assertThat(inspector.inspect("arn:aws:lambda:eu-west-1:123:function:coordinator:1/execution/expired"))
            .isEqualTo(AwsDurableProviderExecutionState.MISSING);
    }

    @Test
    void rejectsUnknownFutureProviderStatuses() {
        when(lambda.getDurableExecution(any(GetDurableExecutionRequest.class)))
            .thenReturn(GetDurableExecutionResponse.builder()
                .status(ExecutionStatus.UNKNOWN_TO_SDK_VERSION)
                .build());

        assertThatThrownBy(() -> inspector.inspect("arn:aws:lambda:eu-west-1:123:function:coordinator:1/execution/new"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Unsupported Durable execution status");
    }

    private AwsDurableProviderExecutionState inspect(ExecutionStatus status) {
        when(lambda.getDurableExecution(any(GetDurableExecutionRequest.class)))
            .thenReturn(GetDurableExecutionResponse.builder().status(status).build());
        return inspector.inspect("arn:aws:lambda:eu-west-1:123:function:coordinator:1/execution/test");
    }
}

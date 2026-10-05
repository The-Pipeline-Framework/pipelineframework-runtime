package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

class AwsDurableExecutionStarterTest {

    @Test
    void invokesNumberedAliasWithStableDurableExecutionName() {
        LambdaClient lambda = mock(LambdaClient.class);
        when(lambda.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().statusCode(202).build());
        AwsDurableExecutionInput input = new AwsDurableExecutionInput(
            "tenant", "stable-key", "pipeline", "contract", "release", "{}", Optional.empty(), 1);

        var response = new AwsDurableExecutionStarter(lambda, new ObjectMapper(), "durable", "live")
            .start(input);

        assertThat(response.statusCode()).isEqualTo(202);
        ArgumentCaptor<InvokeRequest> request = ArgumentCaptor.forClass(InvokeRequest.class);
        verify(lambda).invoke(request.capture());
        assertThat(request.getValue().functionName()).isEqualTo("durable");
        assertThat(request.getValue().qualifier()).isEqualTo("live");
        assertThat(request.getValue().durableExecutionName()).isEqualTo(response.durableExecutionName());
    }
}

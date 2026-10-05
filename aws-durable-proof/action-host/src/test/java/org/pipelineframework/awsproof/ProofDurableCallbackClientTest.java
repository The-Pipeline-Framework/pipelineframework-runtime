package org.pipelineframework.awsproof;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awsproof.model.ProofAwaitIdentity;
import org.pipelineframework.awsproof.model.ProofBindingStatus;
import org.pipelineframework.awsproof.model.ProofCallbackBinding;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionHistoryRequest;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProofDurableCallbackClientTest {
    @Test
    void expiredProviderHistoryIsClassifiedAsClosedForReplacementRecovery() {
        LambdaClient lambda = mock(LambdaClient.class);
        when(lambda.getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class)))
            .thenThrow(ResourceNotFoundException.builder().message("history expired").build());
        ProofDurableCallbackClient client = new ProofDurableCallbackClient();
        client.lambda = lambda;
        client.mapper = new ObjectMapper();
        client.faults = mock(ProofFaultInjector.class);

        assertThat(client.state(binding())).isEqualTo(ProofProviderCallbackState.CLOSED);
    }

    private static ProofCallbackBinding binding() {
        return new ProofCallbackBinding(
            new ProofAwaitIdentity("tenant-1", "execution-1", "interaction-1", "correlation-1", 1),
            "durable-name-1",
            "arn:aws:lambda:us-east-2:111122223333:function:proof:1/durable-execution/one",
            "callback-1",
            ProofBindingStatus.OPEN,
            1L,
            999_999L);
    }
}

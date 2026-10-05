package org.pipelineframework.aws.durable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackSignal;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.Event;
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionHistoryRequest;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;
import software.amazon.awssdk.services.lambda.model.SendDurableExecutionCallbackSuccessRequest;

/** AWS callback and history client. Provider history is mechanical state, never semantic authority. */
public final class AwsDurableCallbackClient {
    private final LambdaClient lambda;
    private final ObjectMapper mapper;

    public AwsDurableCallbackClient(LambdaClient lambda, ObjectMapper mapper) {
        this.lambda = Objects.requireNonNull(lambda, "lambda");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    public void sendSuccess(AwsDurableCallbackBinding binding, AwsDurableCallbackSignal signal) {
        try {
            lambda.sendDurableExecutionCallbackSuccess(SendDurableExecutionCallbackSuccessRequest.builder()
                .callbackId(binding.providerCallbackId())
                .result(SdkBytes.fromUtf8String(mapper.writeValueAsString(signal)))
                .build());
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("callback signal could not be serialized", exception);
        }
    }

    public AwsDurableProviderCallbackState state(AwsDurableCallbackBinding binding) {
        try {
            return AwsDurableHistory.callbackState(history(binding.providerExecutionArn()), binding.providerCallbackId());
        } catch (ResourceNotFoundException unavailable) {
            return AwsDurableProviderCallbackState.CLOSED;
        }
    }

    private List<Event> history(String executionArn) {
        List<Event> events = new ArrayList<>();
        String marker = "";
        do {
            var request = GetDurableExecutionHistoryRequest.builder()
                .durableExecutionArn(executionArn)
                .includeExecutionData(true)
                .maxItems(1000);
            if (!marker.isBlank()) {
                request.marker(marker);
            }
            var response = lambda.getDurableExecutionHistory(request.build());
            events.addAll(response.events());
            marker = Optional.ofNullable(response.nextMarker()).orElse("");
        } while (!marker.isBlank());
        return List.copyOf(events);
    }
}

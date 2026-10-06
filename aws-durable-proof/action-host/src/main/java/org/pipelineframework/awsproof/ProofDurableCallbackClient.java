package org.pipelineframework.awsproof;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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

@ApplicationScoped
class ProofDurableCallbackClient {
    @Inject
    LambdaClient lambda;

    @Inject
    ObjectMapper mapper;

    @Inject
    ProofFaultInjector faults;

    void sendSuccess(AwsDurableCallbackBinding binding, AwsDurableCallbackSignal signal) {
        try {
            faults.failIfArmed("callback-api-throttled", binding.awaitIdentity().executionId());
            lambda.sendDurableExecutionCallbackSuccess(SendDurableExecutionCallbackSuccessRequest.builder()
                .callbackId(binding.providerCallbackId())
                .result(SdkBytes.fromUtf8String(mapper.writeValueAsString(signal)))
                .build());
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("proof callback signal could not be serialized", exception);
        }
    }

    ProofProviderCallbackState state(AwsDurableCallbackBinding binding) {
        try {
            faults.failIfArmed("provider-history-unavailable", binding.awaitIdentity().executionId());
            List<Event> events = history(binding.providerExecutionArn());
            return ProofDurableHistory.callbackState(events, binding.providerCallbackId());
        } catch (ResourceNotFoundException | ProofInjectedFaultException unavailable) {
            // Provider history is disposable mechanical state. A replacement generation is safe:
            // TPF retains semantic identity and generation fencing prevents the old callback from
            // acquiring authority if the provider response was merely lost or delayed.
            return ProofProviderCallbackState.CLOSED;
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

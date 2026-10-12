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
    private static final int MAX_HISTORY_PAGES = 100;

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

    public void sendFailure(AwsDurableCallbackBinding binding, String terminalStatus) {
        lambda.sendDurableExecutionCallbackFailure(
            software.amazon.awssdk.services.lambda.model.SendDurableExecutionCallbackFailureRequest.builder()
                .callbackId(binding.providerCallbackId())
                .error(error -> error.errorType("TPF_TERMINAL_" + terminalStatus)
                    .errorMessage("TPF execution is terminal; this callback cannot admit completion"))
                .build());
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
        for (int page = 0; page < MAX_HISTORY_PAGES; page++) {
            var request = GetDurableExecutionHistoryRequest.builder()
                .durableExecutionArn(executionArn)
                .includeExecutionData(true)
                .maxItems(1000);
            if (!marker.isBlank()) {
                request.marker(marker);
            }
            var response = lambda.getDurableExecutionHistory(request.build());
            String nextMarker = Optional.ofNullable(response.nextMarker()).orElse("");
            if (!nextMarker.isBlank() && nextMarker.equals(marker)) {
                throw new IllegalStateException(
                    "Durable history inspection received a repeated page marker; retry reconciliation");
            }
            events.addAll(response.events());
            if (nextMarker.isBlank()) {
                return List.copyOf(events);
            }
            marker = nextMarker;
        }
        // An incomplete history cannot establish whether a callback is still open or closed.
        throw new IllegalStateException("Durable history inspection exceeded its page budget; retry reconciliation");
    }
}

package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableBindingStatus;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.CallbackStartedDetails;
import software.amazon.awssdk.services.lambda.model.Event;
import software.amazon.awssdk.services.lambda.model.EventType;
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionHistoryRequest;
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionHistoryResponse;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;
import software.amazon.awssdk.services.lambda.model.SendDurableExecutionCallbackSuccessRequest;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;

class AwsDurableCallbackClientTest {
    private final LambdaClient lambda = mock(LambdaClient.class);
    private final AwsDurableCallbackClient client = new AwsDurableCallbackClient(lambda, new ObjectMapper());

    @Test
    void sendsFailureWithMechanicalErrorAndNoCompletionPayload() {
        client.sendFailure(binding(), "FAILED");
        var request = org.mockito.ArgumentCaptor.forClass(
            software.amazon.awssdk.services.lambda.model.SendDurableExecutionCallbackFailureRequest.class);
        verify(lambda).sendDurableExecutionCallbackFailure(request.capture());
        assertThat(request.getValue().callbackId()).isEqualTo(binding().providerCallbackId());
        assertThat(request.getValue().error().errorType()).isEqualTo("TPF_TERMINAL_FAILED");
        assertThat(request.getValue().error().errorData()).isNull();
        verify(lambda, org.mockito.Mockito.never()).sendDurableExecutionCallbackSuccess(
            any(software.amazon.awssdk.services.lambda.model.SendDurableExecutionCallbackSuccessRequest.class));
    }

    @Test
    void followsMarkersThroughAnEmptyPageBeforeClassifyingSuccess() {
        when(lambda.getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class)))
            .thenReturn(page(List.of(started()), "second"), page(List.of(), "third"),
                GetDurableExecutionHistoryResponse.builder().events(terminal(EventType.CALLBACK_SUCCEEDED)).build());

        assertThat(client.state(binding())).isEqualTo(AwsDurableProviderCallbackState.SUCCEEDED);

        var requests = org.mockito.ArgumentCaptor.forClass(GetDurableExecutionHistoryRequest.class);
        verify(lambda, times(3)).getDurableExecutionHistory(requests.capture());
        assertThat(requests.getAllValues().get(0).marker()).isNull();
        assertThat(requests.getAllValues().get(1).marker()).isEqualTo("second");
        assertThat(requests.getAllValues().get(2).marker()).isEqualTo("third");
        assertThat(requests.getAllValues()).allSatisfy(request -> {
            assertThat(request.durableExecutionArn()).isEqualTo(binding().providerExecutionArn());
            assertThat(request.maxItems()).isBetween(1, 1000);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    void rejectsANonAdvancingMarkerOnTheFirstRepeatedResponse(int advancingPages) {
        AtomicInteger calls = new AtomicInteger();
        when(lambda.getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class)))
            .thenAnswer(ignored -> {
                int call = calls.incrementAndGet();
                return page(call == 1 ? List.of(started()) : List.of(),
                    "page-" + Math.min(call, advancingPages));
            });

        assertThatThrownBy(() -> client.state(binding()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("repeated page marker")
            .hasMessageContaining("retry reconciliation");
        assertThat(calls).hasValue(advancingPages + 1);

        var requests = org.mockito.ArgumentCaptor.forClass(GetDurableExecutionHistoryRequest.class);
        verify(lambda, times(advancingPages + 1)).getDurableExecutionHistory(requests.capture());
        assertThat(requests.getAllValues().get(0).marker()).isNull();
        for (int page = 1; page <= advancingPages; page++) {
            assertThat(requests.getAllValues().get(page).marker()).isEqualTo("page-" + page);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " "})
    void blankNextMarkerCompletesHistoryAfterAnAdvancingPage(String finalMarker) {
        when(lambda.getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class)))
            .thenReturn(page(List.of(started()), "next-page"),
                page(List.of(terminal(EventType.CALLBACK_SUCCEEDED)), finalMarker));

        assertThat(client.state(binding())).isEqualTo(AwsDurableProviderCallbackState.SUCCEEDED);
        verify(lambda, times(2)).getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class));
    }

    @Test
    void openAndUnrelatedCallbacksAreNotSuccessfulDeliveryEvidence() {
        when(lambda.getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class)))
            .thenReturn(GetDurableExecutionHistoryResponse.builder().events(started()).build());
        assertThat(client.state(binding())).isEqualTo(AwsDurableProviderCallbackState.OPEN);
        when(lambda.getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class)))
            .thenReturn(GetDurableExecutionHistoryResponse.builder().build());
        assertThat(client.state(binding())).isEqualTo(AwsDurableProviderCallbackState.UNKNOWN);
    }

    @Test
    void unavailableHistoryIsClosedButThrottlingRemainsRetryable() {
        when(lambda.getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class)))
            .thenThrow(ResourceNotFoundException.builder().message("expired").build());
        assertThat(client.state(binding())).isEqualTo(AwsDurableProviderCallbackState.CLOSED);
        when(lambda.getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class)))
            .thenThrow(TooManyRequestsException.builder().message("retry later").build());
        assertThatThrownBy(() -> client.state(binding())).isInstanceOf(TooManyRequestsException.class);
    }

    @Test
    void incompleteHistoryCannotExceedTheInspectionBudgetOrBeClassifiedAsOpen() {
        AtomicInteger pageNumber = new AtomicInteger();
        when(lambda.getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class)))
            .thenAnswer(ignored -> {
                int number = pageNumber.incrementAndGet();
                // Terminate even on the defective implementation: no timeout-based hang oracle.
                return number <= 100
                    ? page(List.of(started()), "page-" + number)
                    : GetDurableExecutionHistoryResponse.builder().build();
            });

        assertThatThrownBy(() -> client.state(binding()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("history inspection");
        assertThat(pageNumber).hasValue(100);
    }

    @Test
    void exhaustedHistoryKeepsTheStreamRecordRetryableWithoutDeliveryOrReplacement() {
        var current = binding();
        var identity = current.awaitIdentity();
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var replacements = mock(AwsDurableReplacementStarter.class);
        var actions = mock(AwsDurableActionInvoker.class);
        when(bindings.findLatestRegistration("tenant-a", "execution-a")).thenReturn(Optional.of(
            new AwsDurableCallbackRegistration(
                new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint(
                    "tenant-a", "execution-a", "pipeline-a", "contract-a", "release-a"), 1),
                current.providerExecutionName(), current.providerExecutionArn(), current.providerCallbackId(),
                100, 1000)));
        when(bindings.isCurrentGeneration("tenant-a", "execution-a", 1)).thenReturn(true);
        when(bindings.find("tenant-a", "interaction-a", 1, 1)).thenReturn(Optional.of(current));
        when(bindings.findLatest("tenant-a", "interaction-a")).thenReturn(Optional.of(current));
        AwsDurableAwaitCheckpointReader checkpoints = (tenant, interaction) -> Optional.of(
            new AwsDurableAwaitCheckpoint(tenant, "execution-a", interaction, "correlation-a", "unit-a", "await-a",
                "COMPLETED", "pipeline-a", "contract-a", "release-a"));
        var wakeups = new AwsDurableWakeupService(checkpoints, bindings, client, replacements);
        var processor = new AwsDurableAwaitStreamProcessor(bindings, actions, wakeups, "arn:await-stream", "arn:binding-stream",
            ignored -> checkpoints.read(identity.tenantId(), identity.interactionId()), ignoredRegistration -> Optional.empty());
        doThrow(ResourceNotFoundException.builder().message("uncertain callback delivery").build())
            .when(lambda).sendDurableExecutionCallbackSuccess(any(SendDurableExecutionCallbackSuccessRequest.class));
        AtomicInteger pageNumber = new AtomicInteger();
        when(lambda.getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class)))
            .thenAnswer(ignored -> pageNumber.incrementAndGet() <= 100
                ? page(List.of(started()), "more-" + pageNumber.get())
                : GetDurableExecutionHistoryResponse.builder().build());
        var record = new DynamodbEvent.DynamodbStreamRecord();
        StreamRecord stream = new StreamRecord();
        stream.setSequenceNumber("sequence-a");
        stream.setNewImage(Map.of(
            "tenant_id", new AttributeValue().withS(identity.tenantId()),
            "execution_id", new AttributeValue().withS(identity.executionId()),
            "interaction_id", new AttributeValue().withS(identity.interactionId()),
            "correlation_id", new AttributeValue().withS(identity.correlationId()),
            "status", new AttributeValue().withS("COMPLETED")));
        record.setEventSourceARN("arn:await-stream");
        record.setEventName("MODIFY");
        record.setDynamodb(stream);
        var event = new DynamodbEvent();
        event.setRecords(List.of(record));

        var response = new AwsDurableAwaitStreamBatchAdapter().handle(event, processor::process);

        assertThat(response.getBatchItemFailures()).extracting(item -> item.getItemIdentifier())
            .containsExactly("sequence-a");
        assertThat(pageNumber).hasValue(100);
        verify(bindings, never()).recordDelivered(any(), any());
        verifyNoInteractions(replacements, actions);
    }

    private static GetDurableExecutionHistoryResponse page(List<Event> events, String marker) {
        return GetDurableExecutionHistoryResponse.builder().events(events).nextMarker(marker).build();
    }

    private static Event started() {
        return Event.builder().eventType(EventType.CALLBACK_STARTED).id("operation-a")
            .callbackStartedDetails(CallbackStartedDetails.builder().callbackId("callback-a").build()).build();
    }

    private static Event terminal(EventType type) {
        return Event.builder().eventType(type).id("operation-a").build();
    }

    private static AwsDurableCallbackBinding binding() {
        return new AwsDurableCallbackBinding(
            new AwsDurableAwaitIdentity("tenant-a", "execution-a", "interaction-a", "correlation-a", 1),
            "provider-a", "arn:aws:lambda:us-east-2:123456789012:function:driver:1/durable-execution/a/b",
            "callback-a", AwsDurableBindingStatus.OPEN, 100, 1000);
    }
}

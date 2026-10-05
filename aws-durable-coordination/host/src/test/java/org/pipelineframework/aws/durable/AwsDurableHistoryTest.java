package org.pipelineframework.aws.durable;

import java.util.List;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.lambda.model.CallbackStartedDetails;
import software.amazon.awssdk.services.lambda.model.Event;
import software.amazon.awssdk.services.lambda.model.EventResult;
import software.amazon.awssdk.services.lambda.model.EventType;
import software.amazon.awssdk.services.lambda.model.StepSucceededDetails;

import static org.assertj.core.api.Assertions.assertThat;

class AwsDurableHistoryTest {
    @Test
    void classifiesOpenSucceededAndClosedCallbacksFromPublicHistory() {
        Event started = callback(EventType.CALLBACK_STARTED, "op-1", "callback-1");

        assertThat(AwsDurableHistory.callbackState(List.of(started), "callback-1"))
            .isEqualTo(AwsDurableProviderCallbackState.OPEN);
        assertThat(AwsDurableHistory.callbackState(List.of(
                started, event(EventType.CALLBACK_SUCCEEDED, "op-1")), "callback-1"))
            .isEqualTo(AwsDurableProviderCallbackState.SUCCEEDED);
        assertThat(AwsDurableHistory.callbackState(List.of(
                started, event(EventType.CALLBACK_TIMED_OUT, "op-1")), "callback-1"))
            .isEqualTo(AwsDurableProviderCallbackState.CLOSED);
    }

    @Test
    void closesAnUnfinishedCallbackWhenTheDurableExecutionIsTerminal() {
        assertThat(AwsDurableHistory.callbackState(List.of(
                callback(EventType.CALLBACK_STARTED, "op-1", "callback-1"),
                event(EventType.EXECUTION_STOPPED, "execution")), "callback-1"))
            .isEqualTo(AwsDurableProviderCallbackState.CLOSED);
    }

    @Test
    void findsOnlyAnOpenNamedCallbackForBindingReconstruction() {
        Event first = callback(EventType.CALLBACK_STARTED, "op-1", "callback-1");
        Event replacement = callback(EventType.CALLBACK_STARTED, "op-2", "callback-2");
        List<Event> events = List.of(first, event(EventType.CALLBACK_FAILED, "op-1"), replacement);

        assertThat(AwsDurableHistory.openCallback(events, "await-completion-callback"))
            .contains(replacement);
    }

    @Test
    void reportsUnknownWhenTheCallbackIdIsNotPresent() {
        assertThat(AwsDurableHistory.callbackState(List.of(), "missing"))
            .isEqualTo(AwsDurableProviderCallbackState.UNKNOWN);
    }

    @Test
    void readsAReplaySafeSemanticCheckpointFromACompletedStep() {
        Event checkpoint = Event.builder()
            .eventType(EventType.STEP_SUCCEEDED)
            .name("record-tpf-await")
            .stepSucceededDetails(StepSucceededDetails.builder()
                .result(EventResult.builder().payload("{\"executionId\":\"exec-1\"}").build())
                .build())
            .build();

        assertThat(AwsDurableHistory.successfulStepPayload(List.of(checkpoint), "record-tpf-await"))
            .contains("{\"executionId\":\"exec-1\"}");
    }

    private static Event callback(EventType type, String id, String callbackId) {
        return Event.builder()
            .eventType(type)
            .id(id)
            .name("await-completion-callback")
            .callbackStartedDetails(CallbackStartedDetails.builder().callbackId(callbackId).build())
            .build();
    }

    private static Event event(EventType type, String id) {
        return Event.builder().eventType(type).id(id).build();
    }
}

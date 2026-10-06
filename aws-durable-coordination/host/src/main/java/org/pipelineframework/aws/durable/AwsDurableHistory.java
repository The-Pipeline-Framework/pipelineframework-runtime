package org.pipelineframework.aws.durable;

import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.services.lambda.model.Event;
import software.amazon.awssdk.services.lambda.model.EventType;

/** Pure interpretation of the public durable-execution history API. */
public final class AwsDurableHistory {
    private AwsDurableHistory() {
    }

    public static AwsDurableProviderCallbackState callbackState(List<Event> events, String callbackId) {
        Optional<Event> started = events.stream()
            .filter(event -> event.eventType() == EventType.CALLBACK_STARTED)
            .filter(event -> event.callbackStartedDetails() != null)
            .filter(event -> callbackId.equals(event.callbackStartedDetails().callbackId()))
            .findFirst();
        if (started.isEmpty()) {
            return AwsDurableProviderCallbackState.UNKNOWN;
        }
        String operationId = started.orElseThrow().id();
        Optional<EventType> terminal = events.stream()
            .filter(event -> operationId.equals(event.id()))
            .map(Event::eventType)
            .filter(AwsDurableHistory::isTerminalCallback)
            .findFirst();
        if (terminal.isPresent()) {
            return terminal.orElseThrow() == EventType.CALLBACK_SUCCEEDED
                ? AwsDurableProviderCallbackState.SUCCEEDED
                : AwsDurableProviderCallbackState.CLOSED;
        }
        return events.stream().map(Event::eventType).anyMatch(AwsDurableHistory::isTerminalExecution)
            ? AwsDurableProviderCallbackState.CLOSED
            : AwsDurableProviderCallbackState.OPEN;
    }

    public static Optional<Event> openCallback(List<Event> events, String operationName) {
        return events.stream()
            .filter(event -> event.eventType() == EventType.CALLBACK_STARTED)
            .filter(event -> operationName.equals(event.name()))
            .filter(event -> event.callbackStartedDetails() != null)
            .filter(event -> callbackOperationIsOpen(events, event.id()))
            .findFirst();
    }

    public static Optional<String> successfulStepPayload(List<Event> events, String operationName) {
        return events.stream()
            .filter(event -> event.eventType() == EventType.STEP_SUCCEEDED)
            .filter(event -> operationName.equals(event.name()))
            .filter(event -> event.stepSucceededDetails() != null)
            .filter(event -> event.stepSucceededDetails().result() != null)
            .map(event -> event.stepSucceededDetails().result().payload())
            .filter(payload -> payload != null && !payload.isBlank())
            .findFirst();
    }

    private static boolean callbackOperationIsOpen(List<Event> events, String operationId) {
        return events.stream()
            .filter(event -> operationId.equals(event.id()))
            .map(Event::eventType)
            .noneMatch(AwsDurableHistory::isTerminalCallback);
    }

    private static boolean isTerminalCallback(EventType type) {
        return type == EventType.CALLBACK_SUCCEEDED
            || type == EventType.CALLBACK_FAILED
            || type == EventType.CALLBACK_TIMED_OUT;
    }

    private static boolean isTerminalExecution(EventType type) {
        return type == EventType.EXECUTION_SUCCEEDED
            || type == EventType.EXECUTION_FAILED
            || type == EventType.EXECUTION_TIMED_OUT
            || type == EventType.EXECUTION_STOPPED;
    }
}

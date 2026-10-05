package org.pipelineframework.awsproof;

import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.services.lambda.model.Event;
import software.amazon.awssdk.services.lambda.model.EventType;

/** Pure interpretation of the public durable-execution history API. */
final class ProofDurableHistory {
    private ProofDurableHistory() {
    }

    static ProofProviderCallbackState callbackState(List<Event> events, String callbackId) {
        Optional<Event> started = events.stream()
            .filter(event -> event.eventType() == EventType.CALLBACK_STARTED)
            .filter(event -> event.callbackStartedDetails() != null)
            .filter(event -> callbackId.equals(event.callbackStartedDetails().callbackId()))
            .findFirst();
        if (started.isEmpty()) {
            return ProofProviderCallbackState.UNKNOWN;
        }
        String operationId = started.orElseThrow().id();
        Optional<EventType> terminal = events.stream()
            .filter(event -> operationId.equals(event.id()))
            .map(Event::eventType)
            .filter(ProofDurableHistory::isTerminalCallback)
            .findFirst();
        if (terminal.isPresent()) {
            return terminal.orElseThrow() == EventType.CALLBACK_SUCCEEDED
                ? ProofProviderCallbackState.SUCCEEDED
                : ProofProviderCallbackState.CLOSED;
        }
        return events.stream().map(Event::eventType).anyMatch(ProofDurableHistory::isTerminalExecution)
            ? ProofProviderCallbackState.CLOSED
            : ProofProviderCallbackState.OPEN;
    }

    static Optional<Event> openCallback(List<Event> events, String operationName) {
        return events.stream()
            .filter(event -> event.eventType() == EventType.CALLBACK_STARTED)
            .filter(event -> operationName.equals(event.name()))
            .filter(event -> event.callbackStartedDetails() != null)
            .filter(event -> callbackOperationIsOpen(events, event.id()))
            .findFirst();
    }

    static Optional<String> successfulStepPayload(List<Event> events, String operationName) {
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
            .noneMatch(ProofDurableHistory::isTerminalCallback);
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

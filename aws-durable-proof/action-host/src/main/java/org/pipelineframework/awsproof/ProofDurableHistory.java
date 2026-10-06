package org.pipelineframework.awsproof;

import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.services.lambda.model.Event;

/** Pure interpretation of the public durable-execution history API. */
final class ProofDurableHistory {
    private ProofDurableHistory() {
    }

    static ProofProviderCallbackState callbackState(List<Event> events, String callbackId) {
        return ProofProviderCallbackState.valueOf(
            org.pipelineframework.aws.durable.AwsDurableHistory.callbackState(events, callbackId).name());
    }

    static Optional<Event> openCallback(List<Event> events, String operationName) {
        return org.pipelineframework.aws.durable.AwsDurableHistory.openCallback(events, operationName);
    }

    static Optional<String> successfulStepPayload(List<Event> events, String operationName) {
        return org.pipelineframework.aws.durable.AwsDurableHistory.successfulStepPayload(events, operationName);
    }
}

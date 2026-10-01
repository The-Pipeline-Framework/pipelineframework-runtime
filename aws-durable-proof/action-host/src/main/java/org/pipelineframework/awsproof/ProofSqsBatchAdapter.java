package org.pipelineframework.awsproof;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.SqsInboundMessage;
import org.pipelineframework.orchestrator.SqsMessageDisposition;

@ApplicationScoped
final class ProofSqsBatchAdapter {
    @Inject
    ProofFaultInjector faults;

    SQSBatchResponse handle(
        String scope,
        SQSEvent event,
        Function<SqsInboundMessage, Uni<SqsMessageDisposition>> action,
        Duration timeout
    ) {
        List<SQSBatchResponse.BatchItemFailure> failures = new ArrayList<>();
        for (SQSEvent.SQSMessage record : event.getRecords()) {
            String messageId = Optional.ofNullable(record.getMessageId()).orElse("");
            SqsInboundMessage message = new SqsInboundMessage(
                messageId.isBlank() ? Optional.empty() : Optional.of(messageId),
                Optional.ofNullable(record.getBody()));
            try {
                faults.failIfArmed(scope + "-before-action", messageId);
                if (action.apply(message).await().atMost(timeout) == SqsMessageDisposition.RETRY) {
                    failures.add(new SQSBatchResponse.BatchItemFailure(messageId));
                } else {
                    faults.failIfArmed(scope + "-after-action", messageId);
                }
            } catch (RuntimeException failure) {
                failures.add(new SQSBatchResponse.BatchItemFailure(messageId));
            }
        }
        return new SQSBatchResponse(List.copyOf(failures));
    }
}

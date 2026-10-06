package org.pipelineframework.aws.durable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.SqsInboundMessage;
import org.pipelineframework.orchestrator.SqsMessageDisposition;

/** Adapts an AWS SQS event batch onto one of TPF's bounded message actions. */
public final class AwsDurableSqsBatchAdapter {
    public SQSBatchResponse handle(
        SQSEvent event,
        Function<SqsInboundMessage, Uni<SqsMessageDisposition>> action,
        Duration timeout
    ) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(action, "action");
        Objects.requireNonNull(timeout, "timeout");
        List<SQSBatchResponse.BatchItemFailure> failures = new ArrayList<>();
        for (SQSEvent.SQSMessage record : event.getRecords()) {
            String messageId = Optional.ofNullable(record.getMessageId()).orElse("");
            SqsInboundMessage message = new SqsInboundMessage(
                messageId.isBlank() ? Optional.empty() : Optional.of(messageId),
                Optional.ofNullable(record.getBody()));
            try {
                if (action.apply(message).await().atMost(timeout) == SqsMessageDisposition.RETRY) {
                    failures.add(new SQSBatchResponse.BatchItemFailure(messageId));
                }
            } catch (RuntimeException failure) {
                failures.add(new SQSBatchResponse.BatchItemFailure(messageId));
            }
        }
        return new SQSBatchResponse(List.copyOf(failures));
    }
}

package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.SqsMessageDisposition;

class AwsDurableSqsBatchAdapterTest {
    @Test
    void returnsOnlyRetryAndFailedRecordsForPartialBatchRedelivery() {
        SQSEvent.SQSMessage acknowledged = message("ack", "one");
        SQSEvent.SQSMessage retried = message("retry", "two");
        SQSEvent.SQSMessage failed = message("failed", "three");
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(acknowledged, retried, failed));

        var response = new AwsDurableSqsBatchAdapter().handle(event, message -> switch (message.messageId().orElse("")) {
            case "ack" -> Uni.createFrom().item(SqsMessageDisposition.ACKNOWLEDGE);
            case "retry" -> Uni.createFrom().item(SqsMessageDisposition.RETRY);
            default -> Uni.createFrom().failure(new IllegalStateException("transient"));
        }, Duration.ofSeconds(1));

        assertThat(response.getBatchItemFailures())
            .extracting(item -> item.getItemIdentifier())
            .containsExactly("retry", "failed");
    }

    private static SQSEvent.SQSMessage message(String id, String body) {
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setMessageId(id);
        message.setBody(body);
        return message;
    }
}

package org.pipelineframework.awsproof;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.SqsMessageDisposition;

class ProofSqsBatchAdapterTest {
    @Test
    void onlyRetryDispositionsBecomePartialBatchFailures() {
        ProofSqsBatchAdapter adapter = new ProofSqsBatchAdapter();
        adapter.faults = new ProofFaultInjector();
        SQSEvent event = event(message("ack", "valid"), message("retry", "transient"));

        var response = adapter.handle(
            "work",
            event,
            message -> Uni.createFrom().item(
                message.body().filter("valid"::equals).isPresent()
                    ? SqsMessageDisposition.ACKNOWLEDGE
                    : SqsMessageDisposition.RETRY),
            Duration.ofSeconds(1));

        assertThat(response.getBatchItemFailures())
            .extracting(com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure::getItemIdentifier)
            .containsExactly("retry");
    }

    @Test
    void synchronousActionFailuresRemainRetryable() {
        ProofSqsBatchAdapter adapter = new ProofSqsBatchAdapter();
        adapter.faults = new ProofFaultInjector();

        var response = adapter.handle(
            "work",
            event(message("failed", "payload")),
            message -> {
                throw new IllegalStateException("boom");
            },
            Duration.ofSeconds(1));

        assertThat(response.getBatchItemFailures())
            .extracting(com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure::getItemIdentifier)
            .containsExactly("failed");
    }

    private static SQSEvent event(SQSEvent.SQSMessage... messages) {
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(messages));
        return event;
    }

    private static SQSEvent.SQSMessage message(String id, String body) {
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setMessageId(id);
        message.setBody(body);
        return message;
    }
}

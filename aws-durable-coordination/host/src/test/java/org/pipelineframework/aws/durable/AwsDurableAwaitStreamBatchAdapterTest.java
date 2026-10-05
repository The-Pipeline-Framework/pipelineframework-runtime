package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;

class AwsDurableAwaitStreamBatchAdapterTest {
    @Test
    void returnsOnlyRetryAndFailedRecordsForPartialBatchRedelivery() {
        DynamodbEvent event = new DynamodbEvent();
        event.setRecords(List.of(record("ack"), record("retry"), record("failed"), record("ignored")));
        var response = new AwsDurableAwaitStreamBatchAdapter().handle(
            event,
            record -> switch (record.getDynamodb().getSequenceNumber()) {
                case "failed" -> throw new IllegalStateException("decode failed");
                case "ignored" -> Optional.empty();
                default -> Optional.of(new AwsDurableAwaitIdentity(
                    "tenant",
                    "execution",
                    record.getDynamodb().getSequenceNumber(),
                    "correlation",
                    1));
            },
            identity -> "retry".equals(identity.interactionId())
                ? AwsDurableWakeupDisposition.RETRY
                : AwsDurableWakeupDisposition.ACKNOWLEDGE);

        assertThat(response.getBatchItemFailures())
            .extracting(item -> item.getItemIdentifier())
            .containsExactly("retry", "failed");
    }

    private static DynamodbEvent.DynamodbStreamRecord record(String sequence) {
        DynamodbEvent.DynamodbStreamRecord record = new DynamodbEvent.DynamodbStreamRecord();
        StreamRecord stream = new StreamRecord();
        stream.setSequenceNumber(sequence);
        record.setDynamodb(stream);
        return record;
    }
}

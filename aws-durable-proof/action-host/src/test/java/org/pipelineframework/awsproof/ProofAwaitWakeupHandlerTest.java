package org.pipelineframework.awsproof;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProofAwaitWakeupHandlerTest {
    @Test
    void partialBatchFailureUsesTheDynamoStreamSequenceNumber() {
        StreamRecord stream = new StreamRecord();
        stream.setSequenceNumber("123456789");
        DynamodbEvent.DynamodbStreamRecord record = new DynamodbEvent.DynamodbStreamRecord();
        record.setEventID("event-id-is-not-the-checkpoint");
        record.setDynamodb(stream);

        assertThat(ProofAwaitWakeupHandler.itemIdentifier(record)).isEqualTo("123456789");
    }
}

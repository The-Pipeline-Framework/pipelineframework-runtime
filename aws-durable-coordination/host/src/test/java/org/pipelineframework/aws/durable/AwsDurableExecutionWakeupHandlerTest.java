package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import org.junit.jupiter.api.Test;

class AwsDurableExecutionWakeupHandlerTest {
    @Test
    void keysLocateSettlementWhileUntrustedImagesCannotSelectState() {
        var handler = handler();
        var settler = handler.services.terminalCallbacks();
        when(settler.settleExecution("tenant", "execution")).thenReturn(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        var record = record();
        record.getDynamodb().setNewImage(Map.of("status", string("FAILED"), "execution_id", string("wrong")));
        assertThat(handler.handleRequest(event(record), mock(Context.class)).getBatchItemFailures()).isEmpty();
        verify(settler).settleExecution("tenant", "execution");
    }

    @Test
    void wrongSourceMalformedKeysAndSemanticReadFailuresPreserveSequenceForRetry() {
        var handler = handler();
        var record = record();
        record.setEventSourceARN("other-stream");
        assertRetry(handler, record);
        record.setEventSourceARN("execution-stream");
        record.getDynamodb().setKeys(Map.of("pk", string("tenant#execution")));
        assertRetry(handler, record);
        verifyNoInteractions(handler.services.terminalCallbacks());
        record = record();
        when(handler.services.terminalCallbacks().settleExecution("tenant", "execution"))
            .thenThrow(new IllegalStateException("store unavailable"));
        assertRetry(handler, record);
    }

    @Test
    void removeDoesNotAdmitSemanticFailure() {
        var handler = handler();
        var record = record();
        record.setEventName("REMOVE");
        assertThat(handler.handleRequest(event(record), mock(Context.class)).getBatchItemFailures()).isEmpty();
        verifyNoInteractions(handler.services.terminalCallbacks());
    }

    private static AwsDurableExecutionWakeupHandler handler() {
        var handler = new AwsDurableExecutionWakeupHandler("execution-stream");
        handler.services = mock(AwsDurableHostServices.class);
        when(handler.services.terminalCallbacks()).thenReturn(mock(AwsDurableTerminalCallbackSettler.class));
        return handler;
    }

    private static void assertRetry(AwsDurableExecutionWakeupHandler handler, DynamodbEvent.DynamodbStreamRecord record) {
        assertThat(handler.handleRequest(event(record), mock(Context.class)).getBatchItemFailures())
            .extracting(failure -> failure.getItemIdentifier()).containsExactly("sequence");
    }

    private static DynamodbEvent event(DynamodbEvent.DynamodbStreamRecord record) {
        var event = new DynamodbEvent(); event.setRecords(List.of(record)); return event;
    }

    private static DynamodbEvent.DynamodbStreamRecord record() {
        var record = new DynamodbEvent.DynamodbStreamRecord();
        record.setEventSourceARN("execution-stream"); record.setEventName("MODIFY");
        var stream = new StreamRecord(); stream.setSequenceNumber("sequence");
        stream.setKeys(Map.of("tenant_id", string("tenant"), "execution_id", string("execution")));
        record.setDynamodb(stream); return record;
    }

    private static AttributeValue string(String value) { var attribute = new AttributeValue(); attribute.setS(value); return attribute; }
}

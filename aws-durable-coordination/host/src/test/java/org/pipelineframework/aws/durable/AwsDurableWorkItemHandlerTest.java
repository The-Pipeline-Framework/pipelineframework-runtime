package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.orchestrator.CoordinationHost;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.SqsInboundMessage;
import org.pipelineframework.orchestrator.SqsMessageDisposition;
import org.pipelineframework.orchestrator.SqsWorkItemAction;

class AwsDurableWorkItemHandlerTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nativeBatchPreservesPerMessageRetry(boolean configured) {
        var action = mock(SqsWorkItemAction.class);
        when(action.handle(any())).thenReturn(Uni.createFrom().item(SqsMessageDisposition.ACKNOWLEDGE),
            Uni.createFrom().item(SqsMessageDisposition.RETRY));
        var handler = new AwsDurableWorkItemHandler();
        handler.action = action;
        if (configured) {
            handler.config = mock(PipelineOrchestratorConfig.class);
            when(handler.config.coordinationHost()).thenReturn(CoordinationHost.NATIVE);
        }
        var first = new SQSEvent.SQSMessage();
        first.setMessageId("first"); first.setBody("first-body");
        var second = new SQSEvent.SQSMessage();
        second.setMessageId("second"); second.setBody("second-body");
        var event = new SQSEvent(); event.setRecords(List.of(first, second));

        var result = handler.handleRequest(event, mock(Context.class));

        assertThat(result.getBatchItemFailures()).extracting(failure -> failure.getItemIdentifier()).containsExactly("second");
        var messages = ArgumentCaptor.forClass(SqsInboundMessage.class);
        verify(action, times(2)).handle(messages.capture());
        assertThat(messages.getAllValues()).containsExactly(
            new SqsInboundMessage(Optional.of("first"), Optional.of("first-body")),
            new SqsInboundMessage(Optional.of("second"), Optional.of("second-body")));
    }
}

package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import io.smallrye.mutiny.Uni;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.SqsMessageDisposition;
import org.pipelineframework.orchestrator.SqsWorkItemAction;

class AwsDurableWorkItemHandlerTest {
    @Test
    void invocationScopedWorkSelectsPortableActionAndPreservesPerMessageRetry() {
        var action = mock(SqsWorkItemAction.class);
        when(action.handlePortable(any())).thenReturn(Uni.createFrom().item(SqsMessageDisposition.ACKNOWLEDGE),
            Uni.createFrom().item(SqsMessageDisposition.RETRY));
        var handler = new AwsDurableWorkItemHandler();
        handler.action = action;
        var first = new SQSEvent.SQSMessage();
        first.setMessageId("first"); first.setBody("first-body");
        var second = new SQSEvent.SQSMessage();
        second.setMessageId("second"); second.setBody("second-body");
        var event = new SQSEvent(); event.setRecords(List.of(first, second));

        var result = handler.handleRequest(event, mock(Context.class));

        assertThat(result.getBatchItemFailures()).extracting(failure -> failure.getItemIdentifier()).containsExactly("second");
        verify(action, times(2)).handlePortable(any());
        verify(action, never()).handle(any());
    }
}

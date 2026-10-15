package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.CoordinatorSweepResult;
import org.pipelineframework.orchestrator.PipelineControlPlane;

class AwsDurableDueWorkHandlerTest {
    @Test
    void delayedAndDuplicateTicksUseFreshHostTimeWithoutExecutionOrProviderState() {
        var controlPlane = mock(PipelineControlPlane.class);
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(1000L, 2000L);
        when(controlPlane.sweepOnce(anyLong())).thenAnswer(invocation ->
            Uni.createFrom().item(new CoordinatorSweepResult(invocation.getArgument(0), 50, 1, 2)));
        var handler = new AwsDurableDueWorkHandler(controlPlane, clock);
        // Deliberately misleading payload: the delivery cannot choose time, tenant or execution.
        var event = event("{\"time\":0,\"tenantId\":\"other\",\"executionId\":\"other\"}");
        var context = mock(Context.class);

        assertThat(handler.handleRequest(event, context).getBatchItemFailures()).isEmpty();
        assertThat(handler.handleRequest(event, context).getBatchItemFailures()).isEmpty();

        verify(controlPlane).sweepOnce(1000L);
        verify(controlPlane).sweepOnce(2000L);
        verifyNoMoreInteractions(controlPlane);
    }

    @Test
    void failedTickIsRedeliveredAndLaterTickRetriesTheSameSemanticAction() {
        var controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.sweepOnce(1000L))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("store unavailable")))
            .thenReturn(Uni.createFrom().item(new CoordinatorSweepResult(1000L, 50, 0, 1)));
        var handler = new AwsDurableDueWorkHandler(controlPlane,
            Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC));
        var event = event("tick");
        var context = mock(Context.class);

        assertThat(handler.handleRequest(event, context).getBatchItemFailures())
            .extracting(failure -> failure.getItemIdentifier()).containsExactly("tick-id");
        assertThat(handler.handleRequest(event, context).getBatchItemFailures()).isEmpty();
        verify(controlPlane, times(2)).sweepOnce(1000L);
    }

    @Test
    void synchronousSetupFailureAlsoLeavesTheTickRetryable() {
        var controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.sweepOnce(anyLong())).thenThrow(new IllegalStateException("not initialised"));
        var response = new AwsDurableDueWorkHandler(controlPlane).handleRequest(event("tick"), mock(Context.class));
        assertThat(response.getBatchItemFailures()).extracting(failure -> failure.getItemIdentifier())
            .containsExactly("tick-id");
    }

    private static SQSEvent event(String body) {
        var message = new SQSEvent.SQSMessage();
        message.setMessageId("tick-id");
        message.setBody(body);
        var event = new SQSEvent();
        event.setRecords(List.of(message));
        return event;
    }
}

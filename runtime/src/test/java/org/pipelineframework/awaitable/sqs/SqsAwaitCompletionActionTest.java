package org.pipelineframework.awaitable.sqs;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitInteractionNotFoundException;
import org.pipelineframework.awaitable.AwaitTelemetry;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.SqsInboundMessage;
import org.pipelineframework.orchestrator.SqsMessageDisposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SqsAwaitCompletionActionTest {

    @Test
    void retriesMalformedAndMissingBodies() {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        AwaitTelemetry telemetry = mock(AwaitTelemetry.class);
        SqsAwaitCompletionAction action = new SqsAwaitCompletionAction(executionService, telemetry);

        assertEquals(SqsMessageDisposition.RETRY,
            action.handle(message("bad", "{bad-json"), Duration.ofSeconds(1)).await().indefinitely());
        assertEquals(SqsMessageDisposition.RETRY,
            action.handle(new SqsInboundMessage(Optional.of("missing"), Optional.empty()), Duration.ofSeconds(1))
                .await().indefinitely());

        verifyNoInteractions(executionService, telemetry);
    }

    @Test
    void acknowledgesSuccessfulCompletion() throws Exception {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        when(executionService.completeAwaitInteraction(any(AwaitCompletionCommand.class)))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(null, false)));
        SqsAwaitCompletionAction action = new SqsAwaitCompletionAction(executionService, AwaitTelemetry.disabled());

        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            action.handle(message("success", completionJson()), Duration.ofSeconds(1)).await().indefinitely());

        verify(executionService).completeAwaitInteraction(argThat(command ->
            command.tenantId().equals("tenant-1")
                && command.interactionId().equals("interaction-1")
                && command.correlationId().equals("corr-1")));
    }

    @Test
    void acknowledgesDeterministicFailureAndRetriesTransientFailureAndTimeout() throws Exception {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        AwaitTelemetry telemetry = mock(AwaitTelemetry.class);
        SqsAwaitCompletionAction action = new SqsAwaitCompletionAction(executionService, telemetry);
        when(executionService.completeAwaitInteraction(any(AwaitCompletionCommand.class)))
            .thenReturn(Uni.createFrom().failure(new AwaitInteractionNotFoundException("missing")))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("store down")))
            .thenReturn(Uni.createFrom().nothing());

        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            action.handle(message("deterministic", completionJson()), Duration.ofSeconds(1)).await().indefinitely());
        assertEquals(SqsMessageDisposition.RETRY,
            action.handle(message("transient", completionJson()), Duration.ofSeconds(1)).await().indefinitely());
        assertEquals(SqsMessageDisposition.RETRY,
            action.handle(message("timeout", completionJson()), Duration.ofMillis(10)).await().indefinitely());

        verify(telemetry).recordDroppedCompletion("sqs", "not_found");
    }

    private static SqsInboundMessage message(String id, String body) {
        return new SqsInboundMessage(Optional.of(id), Optional.of(body));
    }

    private static String completionJson() throws Exception {
        return PipelineJson.mapper().writeValueAsString(new SqsAwaitCompletionEnvelope(
            "tenant-1",
            "interaction-1",
            "corr-1",
            "resume-token",
            "idem-1",
            Map.of("status", "Completed"),
            "provider"));
    }
}

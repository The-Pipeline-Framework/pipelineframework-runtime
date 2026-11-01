package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

class SqsWorkItemActionTest {

    @Test
    void portableDeliveryAcknowledgesMalformedAndMissingBodiesWithoutExecuting() {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        SqsWorkItemAction action = new SqsWorkItemAction(executionService);
        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            action.handlePortable(message("malformed", "{bad-json")).await().indefinitely());
        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            action.handlePortable(new SqsInboundMessage(Optional.of("missing"), Optional.empty())).await().indefinitely());
        verifyNoInteractions(executionService);
    }

    @Test
    void invocationScopedDeliveryUsesPortableProcessingInsteadOfResidentProcessing() throws Exception {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        ExecutionWorkItem item = new ExecutionWorkItem("tenant-a", "portable");
        when(executionService.processPortableExecutionWorkItem(item)).thenReturn(Uni.createFrom().voidItem());
        SqsWorkItemAction action = new SqsWorkItemAction(executionService);
        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            action.handlePortable(message("portable", PipelineJson.mapper().writeValueAsString(item)))
                .await().indefinitely());
        verify(executionService).processPortableExecutionWorkItem(item);
        verify(executionService, never()).processExecutionWorkItem(item);
    }

    @Test
    void portableDeliveryRetriesFailuresAndTimeouts() throws Exception {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        ExecutionWorkItem failed = new ExecutionWorkItem("tenant-a", "portable-failed");
        ExecutionWorkItem timeout = new ExecutionWorkItem("tenant-a", "portable-timeout");
        when(executionService.processPortableExecutionWorkItem(failed))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("unavailable")));
        when(executionService.processPortableExecutionWorkItem(timeout)).thenReturn(Uni.createFrom().nothing());
        SqsWorkItemAction action = new SqsWorkItemAction(executionService, Duration.ofMillis(10));
        for (ExecutionWorkItem item : java.util.List.of(failed, timeout))
            assertEquals(SqsMessageDisposition.RETRY,
                action.handlePortable(message("portable", PipelineJson.mapper().writeValueAsString(item)))
                    .await().indefinitely());
    }

    @Test
    void acknowledgesMalformedAndMissingBodies() {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        SqsWorkItemAction action = new SqsWorkItemAction(executionService);

        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            action.handle(message("malformed", "{bad-json")).await().indefinitely());
        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            action.handle(new SqsInboundMessage(Optional.of("missing"), Optional.empty())).await().indefinitely());
        verifyNoInteractions(executionService);
    }

    @Test
    void acknowledgesSuccessfullyProcessedWorkItem() throws Exception {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        ExecutionWorkItem workItem = new ExecutionWorkItem("tenant-a", "exec-1");
        when(executionService.processExecutionWorkItem(workItem)).thenReturn(Uni.createFrom().voidItem());
        SqsWorkItemAction action = new SqsWorkItemAction(executionService);

        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            action.handle(message("message-1", PipelineJson.mapper().writeValueAsString(workItem)))
                .await().indefinitely());

        verify(executionService).processExecutionWorkItem(workItem);
    }

    @Test
    void retriesProcessingFailureAndTimeout() throws Exception {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        ExecutionWorkItem failed = new ExecutionWorkItem("tenant-a", "failed");
        ExecutionWorkItem timedOut = new ExecutionWorkItem("tenant-a", "timed-out");
        when(executionService.processExecutionWorkItem(failed))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("boom")));
        when(executionService.processExecutionWorkItem(timedOut)).thenReturn(Uni.createFrom().nothing());
        SqsWorkItemAction action = new SqsWorkItemAction(executionService, Duration.ofMillis(10));

        assertEquals(SqsMessageDisposition.RETRY,
            action.handle(message("failed", PipelineJson.mapper().writeValueAsString(failed)))
                .await().indefinitely());
        assertEquals(SqsMessageDisposition.RETRY,
            action.handle(message("timed-out", PipelineJson.mapper().writeValueAsString(timedOut)))
                .await().indefinitely());
    }

    private static SqsInboundMessage message(String id, String body) {
        return new SqsInboundMessage(Optional.of(id), Optional.of(body));
    }
}

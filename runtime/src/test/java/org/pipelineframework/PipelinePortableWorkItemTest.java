package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.PipelineTransitionWorkerSelector;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

class PipelinePortableWorkItemTest {
    @Test
    void portableLocalFallbackInvokesPortableTransitionThroughTheSameControlPlane() {
        var service = spy(new PipelineExecutionService());
        service.controlPlane = mock(PipelineControlPlane.class);
        service.transitionWorkerSelector = mock(PipelineTransitionWorkerSelector.class);
        when(service.transitionWorkerSelector.select(any())).thenAnswer(call -> call.getArgument(0));
        var item = new ExecutionWorkItem("tenant", "portable");
        var command = mock(TransitionCommandEnvelope.class);
        var result = mock(TransitionResultEnvelope.class);
        doReturn(Uni.createFrom().item(result)).when(service).executePortableTransition(command);
        when(service.controlPlane.processExecutionWorkItem(eq(item), any(), any())).thenReturn(Uni.createFrom().voidItem());

        service.processPortableExecutionWorkItem(item).await().indefinitely();

        var worker = ArgumentCaptor.forClass(PipelineTransitionWorker.class);
        verify(service.controlPlane).processExecutionWorkItem(eq(item), worker.capture(), any());
        assertSame(result, worker.getValue().executeTransition(command).await().indefinitely());
        verify(service).executePortableTransition(command);
        verify(service, never()).executeTransition(command);
    }

    @Test
    void portableDeliveryKeepsConfiguredRemoteWorkerSelection() {
        var service = new PipelineExecutionService();
        service.controlPlane = mock(PipelineControlPlane.class);
        service.transitionWorkerSelector = mock(PipelineTransitionWorkerSelector.class);
        var remote = mock(PipelineTransitionWorker.class);
        var item = new ExecutionWorkItem("tenant", "remote");
        when(service.transitionWorkerSelector.select(any())).thenReturn(remote);
        when(service.controlPlane.processExecutionWorkItem(eq(item), eq(remote), any())).thenReturn(Uni.createFrom().voidItem());

        service.processPortableExecutionWorkItem(item).await().indefinitely();

        verify(service.controlPlane).processExecutionWorkItem(eq(item), eq(remote), any());
    }
}

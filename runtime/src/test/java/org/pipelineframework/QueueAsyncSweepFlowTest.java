package org.pipelineframework;

import java.util.List;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.CoordinatorSweepResult;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueAsyncSweepFlowTest {

  private QueueAsyncSweepFlow flow;

  @Mock
  private PipelineOrchestratorConfig orchestratorConfig;

  @Mock
  private ExecutionStateStore executionStateStore;

  @Mock
  private WorkDispatcher workDispatcher;

  @Mock
  private AwaitTimeoutFlow awaitTimeoutFlow;

  @BeforeEach
  void setUp() {
    lenient().when(orchestratorConfig.sweepLimit()).thenReturn(100);
    flow = new QueueAsyncSweepFlow(
        orchestratorConfig,
        executionStateStore,
        workDispatcher,
        awaitTimeoutFlow);
  }

  @Test
  void sweepRunsTimeoutsThenEnqueuesEveryDueExecution() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100)).thenReturn(Uni.createFrom().item(2));
    when(executionStateStore.findDueExecutions(1000L, 100)).thenReturn(Uni.createFrom().item(List.of(
        record("tenant-b", "exec-b", 20L),
        record("tenant-b", "exec-a", 10L),
        record("tenant-a", "exec-c", 10L))));
    when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

    CoordinatorSweepResult result = flow.sweepOnce(1000L).await().indefinitely();

    assertEquals(new CoordinatorSweepResult(1000L, 100, 2, 3), result);

    InOrder order = inOrder(awaitTimeoutFlow, executionStateStore, workDispatcher);
    order.verify(awaitTimeoutFlow).sweepTimedOut(1000L, 100);
    order.verify(executionStateStore).findDueExecutions(1000L, 100);
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-a", "exec-c"));
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-b", "exec-a"));
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-b", "exec-b"));
  }

  @Test
  void emptyDueBatchDoesNotDispatch() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100)).thenReturn(Uni.createFrom().item(0));
    when(executionStateStore.findDueExecutions(1000L, 100)).thenReturn(Uni.createFrom().item(List.of()));

    CoordinatorSweepResult result = flow.sweepOnce(1000L).await().indefinitely();

    assertEquals(new CoordinatorSweepResult(1000L, 100, 0, 0), result);
    verify(workDispatcher, never()).enqueueNow(any());
  }

  @Test
  void enqueueFailuresAreAggregatedAfterEveryDueItemIsAttempted() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100)).thenReturn(Uni.createFrom().item(0));
    when(executionStateStore.findDueExecutions(1000L, 100)).thenReturn(Uni.createFrom().item(List.of(
        record("tenant-a", "exec-a"),
        record("tenant-b", "exec-b"),
        record("tenant-c", "exec-c"))));
    when(workDispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-a")))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("dispatcher down")));
    when(workDispatcher.enqueueNow(new ExecutionWorkItem("tenant-b", "exec-b")))
        .thenReturn(Uni.createFrom().voidItem());
    when(workDispatcher.enqueueNow(new ExecutionWorkItem("tenant-c", "exec-c")))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("dispatcher still down")));

    IllegalStateException error = assertThrows(
        IllegalStateException.class,
        () -> flow.sweepOnce(1000L).await().indefinitely());

    assertEquals("Failed to re-dispatch 2 due executions", error.getMessage());
    assertTrue(error.getCause().getMessage().contains("exec-a"));
    assertEquals(1, error.getSuppressed().length);
    InOrder order = inOrder(workDispatcher);
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-a", "exec-a"));
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-b", "exec-b"));
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-c", "exec-c"));
  }

  @Test
  void timeoutFailurePreventsDueExecutionQuery() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("timeout store down")));

    IllegalStateException error = assertThrows(
        IllegalStateException.class,
        () -> flow.sweepOnce(1000L).await().indefinitely());

    assertEquals("timeout store down", error.getMessage());
    verify(executionStateStore, never()).findDueExecutions(anyLong(), anyInt());
  }

  @Test
  void dueExecutionQueryFailurePreventsDispatch() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100)).thenReturn(Uni.createFrom().item(1));
    when(executionStateStore.findDueExecutions(1000L, 100))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("execution store down")));

    IllegalStateException error = assertThrows(
        IllegalStateException.class,
        () -> flow.sweepOnce(1000L).await().indefinitely());

    assertEquals("execution store down", error.getMessage());
    verify(workDispatcher, never()).enqueueNow(any());
  }

  @Test
  void coordinatorSweepResultRejectsInvalidValues() {
    assertThrows(IllegalArgumentException.class, () -> new CoordinatorSweepResult(-1L, 1, 0, 0));
    assertThrows(IllegalArgumentException.class, () -> new CoordinatorSweepResult(0L, 0, 0, 0));
    assertThrows(IllegalArgumentException.class, () -> new CoordinatorSweepResult(0L, 1, -1, 0));
    assertThrows(IllegalArgumentException.class, () -> new CoordinatorSweepResult(0L, 1, 0, -1));
  }

  @Test
  void sweepRejectsInvalidInvocationValuesBeforeEffects() {
    assertThrows(IllegalArgumentException.class, () -> flow.sweepOnce(-1L).await().indefinitely());
    verifyNoInteractions(awaitTimeoutFlow, executionStateStore, workDispatcher);

    when(orchestratorConfig.sweepLimit()).thenReturn(0);

    assertThrows(IllegalArgumentException.class, () -> flow.sweepOnce(0L).await().indefinitely());
    verifyNoInteractions(awaitTimeoutFlow, executionStateStore, workDispatcher);
  }

  private static ExecutionRecord<Object, Object> record(String tenantId, String executionId) {
    return record(tenantId, executionId, 1L);
  }

  private static ExecutionRecord<Object, Object> record(String tenantId, String executionId, long nextDueEpochMs) {
    return new ExecutionRecord<>(
        tenantId,
        executionId,
        executionId + "-key",
        ExecutionResultShape.SINGLE,
        ExecutionStatus.WAIT_RETRY,
        1L,
        2,
        1,
        null,
        0L,
        nextDueEpochMs,
        null,
        "input",
        null,
        null,
        null,
        null,
        1L,
        1L,
        99999999L);
  }
}

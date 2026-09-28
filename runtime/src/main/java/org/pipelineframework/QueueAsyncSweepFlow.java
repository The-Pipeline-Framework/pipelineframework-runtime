package org.pipelineframework;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.CoordinatorSweepResult;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;

class QueueAsyncSweepFlow {

  private final PipelineOrchestratorConfig orchestratorConfig;
  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final AwaitTimeoutFlow awaitTimeoutFlow;

  QueueAsyncSweepFlow(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitTimeoutFlow awaitTimeoutFlow) {
    this.orchestratorConfig = Objects.requireNonNull(orchestratorConfig, "orchestratorConfig must not be null");
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.workDispatcher = Objects.requireNonNull(workDispatcher, "workDispatcher must not be null");
    this.awaitTimeoutFlow = Objects.requireNonNull(awaitTimeoutFlow, "awaitTimeoutFlow must not be null");
  }

  Uni<CoordinatorSweepResult> sweepOnce(long nowEpochMs) {
    if (nowEpochMs < 0) {
      return Uni.createFrom().failure(new IllegalArgumentException("nowEpochMs must not be negative"));
    }
    int limit = orchestratorConfig.sweepLimit();
    if (limit <= 0) {
      return Uni.createFrom().failure(new IllegalArgumentException("sweep limit must be positive"));
    }
    return awaitTimeoutFlow.sweepTimedOut(nowEpochMs, limit)
        .chain(timedOutAwaitCount -> executionStateStore.findDueExecutions(nowEpochMs, limit)
            .onItem().transform(DueExecutionDispatchPlan::from)
            .onItem().transformToUni(plan -> dispatchDueExecutions(plan)
                .onItem().transform(dispatchedExecutionCount -> new CoordinatorSweepResult(
                    nowEpochMs,
                    limit,
                    timedOutAwaitCount,
                    dispatchedExecutionCount))));
  }

  private Uni<Integer> dispatchDueExecutions(DueExecutionDispatchPlan plan) {
    if (plan.empty()) {
      return Uni.createFrom().item(0);
    }
    return Multi.createFrom().iterable(plan.workItems())
        .onItem().transformToUniAndConcatenate(this::enqueueDueExecution)
        .collect().asList()
        .onItem().transformToUni(this::failIfAnyDispatchFailed)
        .replaceWith(plan.workItems().size());
  }

  private Uni<Optional<Throwable>> enqueueDueExecution(ExecutionWorkItem item) {
    return workDispatcher.enqueueNow(item)
        .replaceWith(Optional.<Throwable>empty())
        .onFailure().recoverWithItem(failure -> Optional.of(new IllegalStateException(
            "Failed to re-dispatch due execution " + item.executionId(),
            failure)));
  }

  private Uni<Void> failIfAnyDispatchFailed(List<Optional<Throwable>> results) {
    List<Throwable> failures = results.stream()
        .flatMap(Optional::stream)
        .toList();
    if (failures.isEmpty()) {
      return Uni.createFrom().voidItem();
    }
    if (failures.size() == 1) {
      return Uni.createFrom().failure(failures.get(0));
    }
    IllegalStateException combined = new IllegalStateException(
        "Failed to re-dispatch " + failures.size() + " due executions",
        failures.get(0));
    failures.stream().skip(1).forEach(combined::addSuppressed);
    return Uni.createFrom().failure(combined);
  }
}

package org.pipelineframework;

import java.util.Objects;
import java.util.function.Supplier;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

class AwaitTimeoutFlow {

  private final AwaitCoordinator awaitCoordinator;
  private final ExecutionStateStore executionStateStore;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;

  AwaitTimeoutFlow(
      AwaitCoordinator awaitCoordinator,
      ExecutionStateStore executionStateStore,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger) {
    this.awaitCoordinator = Objects.requireNonNull(awaitCoordinator, "awaitCoordinator must not be null");
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.segmentBoundaryLedger =
        Objects.requireNonNull(segmentBoundaryLedger, "segmentBoundaryLedger must not be null");
  }

  Uni<Integer> sweepTimedOut(long nowEpochMs, int limit) {
    if (limit <= 0) {
      return Uni.createFrom().item(0);
    }
    int batchLimit = Math.min(limit, 100);
    return awaitCoordinator.findDueTimeoutConsequences(nowEpochMs, batchLimit)
        .onItem().transformToUni(retained -> Multi.createFrom().iterable(retained)
            .onItem().transformToUniAndConcatenate(record -> finishTimeout(record, nowEpochMs))
            .collect().asList()
            .onItem().transformToUni(counts -> {
              int remaining = batchLimit - retained.size();
              int repaired = counts.stream().mapToInt(Integer::intValue).sum();
              if (remaining <= 0) {
                return Uni.createFrom().item(repaired);
              }
              return awaitCoordinator.findTimedOut(nowEpochMs, remaining)
                  .onItem().transform(records -> TimedOutAwaitInteractionsPlan.from(records, remaining))
                  .onItem().transformToUni(plan -> Multi.createFrom().iterable(plan.interactions())
                      .onItem().transformToUniAndConcatenate(record -> admitTimeout(record, nowEpochMs))
                      .collect().asList()
                      .onItem().transform(admitted -> repaired + admitted.stream().mapToInt(Integer::intValue).sum()));
            }));
  }

  private Uni<Integer> admitTimeout(AwaitInteractionRecord interaction, long nowEpochMs) {
    return awaitCoordinator.markTimedOut(interaction, nowEpochMs)
        .onItem().transformToUni(updated -> updated
            .map(record -> finishTimeout(record, nowEpochMs))
            .orElseGet(() -> Uni.createFrom().item(0)));
  }

  private Uni<Integer> finishTimeout(AwaitInteractionRecord record, long nowEpochMs) {
    return awaitCoordinator.reconcileTimedOut(record, nowEpochMs)
        .chain(() -> segmentBoundaryLedger.get().recordInteractionTimedOutRequired(record, record.updatedAtEpochMs()))
        .chain(() -> executionStateStore.getExecution(record.tenantId(), record.executionId()))
        .onItem().transform(parent -> {
          if (parent.isEmpty()) {
            throw new IllegalStateException("Timeout parent is unavailable");
          }
          var execution = parent.orElseThrow();
          if (!Objects.equals(execution.tenantId(), record.tenantId())
              || !Objects.equals(execution.executionId(), record.executionId())) {
            throw new IllegalStateException("Timeout parent belongs to another execution");
          }
          return AwaitTimeoutPlan.from(record, parent);
        })
        .onItem().transformToUni(plan -> plan.failParent()
            ? failParent(plan, nowEpochMs)
            : recoverFailureFact(plan))
        .chain(() -> awaitCoordinator.completeTimeoutConsequences(record))
        .replaceWith(1);
  }

  private Uni<Void> recoverFailureFact(AwaitTimeoutPlan plan) {
    var parent = plan.parent().orElseThrow();
    if (parent.status() == ExecutionStatus.FAILED
        && Objects.equals(parent.errorCode(), plan.errorCode())
        && Objects.equals(parent.errorMessage(), plan.errorMessage())) {
      return segmentBoundaryLedger.get().recordRunFailedRequired(parent,
          plan.errorCode(), plan.errorMessage(), parent.updatedAtEpochMs());
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> failParent(AwaitTimeoutPlan plan, long nowEpochMs) {
    var parent = plan.parent().orElseThrow();
    return executionStateStore.markTerminalFailure(
            parent.tenantId(), parent.executionId(), parent.version(), ExecutionStatus.FAILED,
            plan.transitionKey().orElseThrow(), plan.errorCode(), plan.errorMessage(), nowEpochMs)
        .onItem().transformToUni(updated -> updated
            .map(failed -> segmentBoundaryLedger.get().recordRunFailedRequired(
                failed, plan.errorCode(), plan.errorMessage(), nowEpochMs))
            .orElseGet(() -> Uni.createFrom().failure(new IllegalStateException("Timeout parent changed before failure admission"))))
        .replaceWithVoid();
  }
}

package org.pipelineframework;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiFunction;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

/** Claims an execution and maintains its lease while the segment owns the transition. */
final class QueueAsyncExecutionLease {
  private static final Logger QUEUE_TRACE = Logger.getLogger("org.pipelineframework.diagnostics.queue");

  private final ExecutionStateStore executionStateStore;
  private final PipelineOrchestratorConfig orchestratorConfig;
  private final String queueWorkerId;

  QueueAsyncExecutionLease(
      ExecutionStateStore executionStateStore, PipelineOrchestratorConfig orchestratorConfig, String queueWorkerId) {
    this.orchestratorConfig = Objects.requireNonNull(orchestratorConfig, "orchestratorConfig must not be null");
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.queueWorkerId = queueWorkerId == null || queueWorkerId.isBlank() ? "worker-local" : queueWorkerId;
  }

  Uni<Void> run(ExecutionWorkItem workItem, BiFunction<ClaimedSegment, Long, Uni<Void>> work) {
    long now = System.currentTimeMillis();
    return executionStateStore.claimLease(
            workItem.tenantId(),
            workItem.executionId(),
            queueWorkerId,
            now,
            orchestratorConfig.leaseMs())
        .onItem().transformToUni(claimed -> claimed
            .map(record -> {
              ClaimedSegment segment = ClaimedSegment.from(record);
              QUEUE_TRACE.debugf("event=execution_lease_claimed executionId=%s transition=%s version=%d expiresEpochMs=%d",
                  record.executionId(), segment.transitionKey(), record.version(), record.leaseExpiresEpochMs());
              return keepLeaseAlive(segment, work.apply(segment, now));
            })
            .orElseGet(() -> Uni.createFrom().voidItem()));
  }

  private Uni<Void> keepLeaseAlive(ClaimedSegment segment, Uni<Void> work) {
    long leaseMs = orchestratorConfig.leaseMs();
    Duration renewalInterval = Duration.ofMillis(Math.max(1L, leaseMs / 3L));
    return Uni.join().first(work, renewLeaseUntilLost(segment, renewalInterval)).toTerminate();
  }

  private Uni<Void> renewLeaseUntilLost(ClaimedSegment segment, Duration renewalInterval) {
    return Uni.createFrom().voidItem()
        .onItem().delayIt().by(renewalInterval)
        .onItem().transformToUni(ignored -> {
          ExecutionRecord<Object, Object> record = segment.record();
          long nowEpochMs = System.currentTimeMillis();
          return executionStateStore.renewLease(
                  record.tenantId(),
                  record.executionId(),
                  record.version(),
                  queueWorkerId,
                  nowEpochMs,
                  orchestratorConfig.leaseMs())
              .onFailure().invoke(failure -> QUEUE_TRACE.debugf(failure,
                  "event=execution_lease_renewal_error executionId=%s transition=%s version=%d",
                  record.executionId(), segment.transitionKey(), record.version()))
              .onItem().invoke(renewed -> QUEUE_TRACE.debugf(
                  "event=execution_lease_renewal executionId=%s transition=%s version=%d renewed=%s elapsedMs=%d",
                  record.executionId(), segment.transitionKey(), record.version(), renewed.isPresent(),
                  System.currentTimeMillis() - nowEpochMs))
              .onItem().transformToUni(renewed -> renewed.isPresent()
                  ? renewLeaseUntilLost(segment, renewalInterval)
                  : continueAfterCommittedTransitionOrFail(segment));
        });
  }

  private Uni<Void> continueAfterCommittedTransitionOrFail(ClaimedSegment segment) {
    ExecutionRecord<Object, Object> claimed = segment.record();
    return executionStateStore.getExecution(claimed.tenantId(), claimed.executionId())
        .onItem().transformToUni(current -> {
          boolean committed = current.filter(record -> transitionWasCommitted(segment, record)).isPresent();
          QUEUE_TRACE.debugf("event=execution_lease_renewal_reconciled executionId=%s transition=%s committed=%s",
              claimed.executionId(), segment.transitionKey(), committed);
          if (committed) {
            // The state commit legitimately fences subsequent renewals. Keep this branch pending so
            // post-commit ledger work can finish and the actual work branch determines the outcome.
            return Uni.createFrom().nothing();
          }
          return Uni.createFrom().failure(new IllegalStateException(
              "Execution lease ownership was lost for execution " + claimed.executionId()));
        });
  }

  private static boolean transitionWasCommitted(
      ClaimedSegment segment,
      ExecutionRecord<Object, Object> current) {
    ExecutionRecord<Object, Object> claimed = segment.record();
    return current.version() > claimed.version()
        && current.status() != ExecutionStatus.RUNNING
        && segment.transitionKey().equals(current.lastTransitionKey());
  }
}

package org.pipelineframework;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.PagedExecutionState;
import org.pipelineframework.orchestrator.PagedTransitionCompletion;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;

/**
 * Interprets immutable segment commit plans against the existing projection stores.
 */
class SegmentCommitEffects {

  private static final Logger LOG = Logger.getLogger(SegmentCommitEffects.class);

  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final DeadLetterPublisher deadLetterPublisher;
  private final AwaitCoordinator awaitCoordinator;
  private final ExecutionFailureHandler executionFailureHandler;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;
  private final AwaitContinuations awaitContinuations;
  private final TerminalPublicationBoundary terminalPublicationBoundary;
  private final Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder;

  SegmentCommitEffects(
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      DeadLetterPublisher deadLetterPublisher,
      AwaitCoordinator awaitCoordinator,
      ExecutionFailureHandler executionFailureHandler,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger,
      AwaitContinuations awaitContinuations,
      TerminalPublicationBoundary terminalPublicationBoundary,
      Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder) {
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.workDispatcher = Objects.requireNonNull(workDispatcher, "workDispatcher must not be null");
    this.deadLetterPublisher = Objects.requireNonNull(deadLetterPublisher, "deadLetterPublisher must not be null");
    this.awaitCoordinator = Objects.requireNonNull(awaitCoordinator, "awaitCoordinator must not be null");
    this.executionFailureHandler = Objects.requireNonNull(executionFailureHandler, "executionFailureHandler must not be null");
    this.segmentBoundaryLedger = Objects.requireNonNull(segmentBoundaryLedger, "segmentBoundaryLedger must not be null");
    this.awaitContinuations = Objects.requireNonNull(awaitContinuations, "awaitContinuations must not be null");
    this.terminalPublicationBoundary = Objects.requireNonNull(
        terminalPublicationBoundary,
        "terminalPublicationBoundary must not be null");
    this.lifecycleRecorder = Objects.requireNonNull(lifecycleRecorder, "lifecycleRecorder must not be null");
  }

  Uni<Void> commit(
      SegmentCommitPlan plan,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return switch (plan) {
      case CompletedSegment completed -> commitCompleted(completed);
      case SuspendedSegment suspended -> commitSuspended(suspended, itemContinuationHandler);
      case FailedSegment failed -> fail(failed.segment(), failed.failure());
    };
  }

  Uni<Void> fail(ClaimedSegment segment, Throwable failure) {
    return executionFailureHandler.handleExecutionFailure(
        segment.record(),
        segment.transitionKey(),
        failure,
        executionStateStore,
        workDispatcher,
        deadLetterPublisher);
  }

  private Uni<Void> commitCompleted(CompletedSegment completed) {
    ClaimedSegment segment = completed.segment();
    long nowEpochMs = System.currentTimeMillis();
    return segmentBoundaryLedger.get()
        .recordSegmentCompleted(segment.record(), segment.transitionKey(), completed.result(), nowEpochMs)
        .chain(() -> terminalPublicationBoundary.publishBeforeSuccess(completed, nowEpochMs))
        .chain(() -> completed.result().pageCompletion()
            .filter(page -> !page.exhausted())
            .map(page -> advancePage(completed, page, nowEpochMs))
            .orElseGet(() -> completeExecution(completed, nowEpochMs)));
  }

  private Uni<Void> completeExecution(CompletedSegment completed, long nowEpochMs) {
    ClaimedSegment segment = completed.segment();
    return executionStateStore.markSucceeded(
            segment.record().tenantId(),
            segment.record().executionId(),
            segment.record().version(),
            segment.transitionKey(),
            completed.outputItems(),
            nowEpochMs)
        .onItem().transformToUni(updated -> updated
            .map(succeeded -> segmentBoundaryLedger.get().recordRunSucceeded(
                succeeded,
                completed.outputItems(),
                nowEpochMs))
            .orElseGet(() -> Uni.createFrom().failure(successCommitFailure(completed))))
        .replaceWithVoid();
  }

  private Uni<Void> advancePage(
      CompletedSegment completed,
      PagedTransitionCompletion completion,
      long nowEpochMs) {
    ClaimedSegment segment = completed.segment();
    PagedExecutionState current = segment.record().pagingState().orElseThrow(() ->
        new IllegalStateException("worker returned page completion for an unpaged execution"));
    String checkpoint = completion.nextCheckpoint().orElseThrow(() ->
        new IllegalStateException("non-exhausted page did not return a successor checkpoint"));
    PagedExecutionState successor = current.successor(checkpoint);
    return executionStateStore.advancePage(
            segment.record().tenantId(),
            segment.record().executionId(),
            segment.record().version(),
            segment.transitionKey(),
            successor,
            nowEpochMs)
        .onItem().transformToUni(updated -> updated
            .map(queued -> workDispatcher.enqueueNow(new ExecutionWorkItem(
                queued.tenantId(), queued.executionId())))
            .orElseGet(() -> reconcilePageCommit(completed, successor)))
        .replaceWithVoid();
  }

  private Uni<Void> reconcilePageCommit(
      CompletedSegment completed,
      PagedExecutionState successor) {
    ClaimedSegment segment = completed.segment();
    return executionStateStore.getExecution(
            segment.record().tenantId(), segment.record().executionId())
        .onItem().transformToUni(current -> current
            .filter(record -> pageAlreadyCommitted(record, successor, segment.transitionKey()))
            .map(ignored -> Uni.createFrom().voidItem())
            .orElseGet(() -> Uni.createFrom().failure(pageCommitFailure(completed, successor))));
  }

  private static boolean pageAlreadyCommitted(
      ExecutionRecord<Object, Object> current,
      PagedExecutionState successor,
      String transitionKey) {
    return current.pagingState().filter(state ->
        state.pageIndex() > successor.pageIndex()
            || (state.equals(successor) && transitionKey.equals(current.lastTransitionKey())))
        .isPresent();
  }

  private IllegalStateException pageCommitFailure(
      CompletedSegment completed,
      PagedExecutionState successor) {
    return new IllegalStateException(
        "Failed to commit page " + (successor.pageIndex() - 1) + " for execution "
            + completed.segment().record().executionId()
            + " (expectedVersion=" + completed.segment().record().version()
            + ", transitionKey=" + completed.segment().transitionKey() + ")");
  }

  private Uni<Void> commitSuspended(
      SuspendedSegment suspended,
      AwaitItemContinuationHandler itemContinuationHandler) {
    ClaimedSegment segment = suspended.segment();
    long nowEpochMs = System.currentTimeMillis();
    return executionStateStore.markWaitingExternal(
            segment.record().tenantId(),
            segment.record().executionId(),
            segment.record().version(),
            segment.transitionKey(),
            suspended.suspension().unitId(),
            suspended.suspension().stepIndex(),
            nowEpochMs)
        .onItem().transformToUni(updated -> {
          if (updated.isEmpty()) {
            return Uni.createFrom().failure(waitingExternalFailure(suspended));
          }
          return awaitCoordinator.importSuspension(suspended.suspension())
              .chain(() -> afterWaitingExternal(suspended, updated.get(), itemContinuationHandler, nowEpochMs));
        });
  }

  private Uni<Void> afterWaitingExternal(
      SuspendedSegment suspended,
      ExecutionRecord<Object, Object> waiting,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    ClaimedSegment segment = suspended.segment();
    LOG.infof(
        "Execution %s persisted WAITING_EXTERNAL at stepIndex=%d awaitUnitId=%s awaitInteractions=%d",
        segment.record().executionId(),
        suspended.suspension().stepIndex(),
        suspended.suspension().unitId(),
        suspended.suspension().interactions().size());
    lifecycleRecorder.accept(new AwaitReplayLifecycleEvent(
        AwaitReplayLifecycleEvent.EXECUTION_WAITING,
        segment.record().executionId(),
        suspended.suspension().unitId(),
        null,
        suspended.suspension().stepIndex(),
        ExecutionStatus.WAITING_EXTERNAL.name(),
        null,
        null,
        null,
        null,
        null,
        null,
        null));
    return segmentBoundaryLedger.get()
        .recordSegmentSuspended(segment.record(), segment.transitionKey(), suspended.suspension(), nowEpochMs)
        .chain(() -> awaitContinuations.afterParentWaiting(
            waiting,
            suspended.suspension(),
            nowEpochMs,
            itemContinuationHandler));
  }

  private IllegalStateException waitingExternalFailure(SuspendedSegment suspended) {
    return new IllegalStateException(
        "Failed to persist WAITING_EXTERNAL state for execution "
            + suspended.segment().record().executionId()
            + " at step "
            + suspended.suspension().stepIndex()
            + " (expectedVersion="
            + suspended.segment().record().version()
            + ", awaitUnitId="
            + suspended.suspension().unitId()
            + ")");
  }

  private IllegalStateException successCommitFailure(CompletedSegment completed) {
    return new IllegalStateException(
        "Failed to persist SUCCEEDED state for execution "
            + completed.segment().record().executionId()
            + " (expectedVersion="
            + completed.segment().record().version()
            + ", transitionKey="
            + completed.segment().transitionKey()
            + ")");
  }
}

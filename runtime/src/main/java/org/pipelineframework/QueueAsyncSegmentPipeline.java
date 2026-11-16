package org.pipelineframework;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.OptionalInt;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitThrowableSupport;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.ControlPlaneTransitionAdmission;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

/**
 * Reactive queue-async segment flow: claim, run, plan, and commit one synchronous segment.
 */
class QueueAsyncSegmentPipeline {

  private static final Logger LOG = Logger.getLogger(QueueAsyncSegmentPipeline.class);

  private final QueueAsyncExecutionLease executionLease;
  private final WorkDispatcher workDispatcher;
  private final AwaitCoordinator awaitCoordinator;
  private final TransitionWorkerExecutor transitionWorkerExecutor;
  private final ControlPlaneAdmissionPolicy admissionPolicy;
  private final Supplier<TransitionPayloadCodec> payloadCodec;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;
  private final Supplier<Duration> saturatedDelay;
  private final Function<ExecutionRecord<?, ?>, OptionalInt> pipelineStepCount;
  private final SegmentCommitEffects segmentCommitEffects;

  QueueAsyncSegmentPipeline(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitCoordinator awaitCoordinator,
      TransitionWorkerExecutor transitionWorkerExecutor,
      ControlPlaneAdmissionPolicy admissionPolicy,
      Supplier<TransitionPayloadCodec> payloadCodec,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger,
      Supplier<Duration> saturatedDelay,
      Function<ExecutionRecord<?, ?>, OptionalInt> pipelineStepCount,
      SegmentCommitEffects segmentCommitEffects,
      String queueWorkerId) {
    this.executionLease = new QueueAsyncExecutionLease(executionStateStore, orchestratorConfig, queueWorkerId);
    this.workDispatcher = Objects.requireNonNull(workDispatcher, "workDispatcher must not be null");
    this.awaitCoordinator = Objects.requireNonNull(awaitCoordinator, "awaitCoordinator must not be null");
    this.transitionWorkerExecutor = Objects.requireNonNull(transitionWorkerExecutor, "transitionWorkerExecutor must not be null");
    this.admissionPolicy = Objects.requireNonNull(admissionPolicy, "admissionPolicy must not be null");
    this.payloadCodec = Objects.requireNonNull(payloadCodec, "payloadCodec must not be null");
    this.segmentBoundaryLedger = Objects.requireNonNull(segmentBoundaryLedger, "segmentBoundaryLedger must not be null");
    this.saturatedDelay = Objects.requireNonNull(saturatedDelay, "saturatedDelay must not be null");
    this.pipelineStepCount = Objects.requireNonNull(pipelineStepCount, "pipelineStepCount must not be null");
    this.segmentCommitEffects = Objects.requireNonNull(segmentCommitEffects, "segmentCommitEffects must not be null");
  }

  Uni<Void> process(
      ExecutionWorkItem workItem,
      PipelineTransitionWorker worker,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return Uni.createFrom().deferred(() -> {
      Optional<TransitionWorkerExecutor.TransitionAdmission> admission = transitionWorkerExecutor.tryAdmit();
      if (admission.isEmpty()) {
        return workDispatcher.enqueueDelayed(workItem, saturatedDelay.get());
      }
      TransitionWorkerExecutor.TransitionAdmission transitionPermit = admission.get();
      ControlPlaneTransitionAdmission tenantAdmission = admissionPolicy.admitTransition(admissionRequest(workItem));
      if (!tenantAdmission.decision().allowed()) {
        transitionPermit.close();
        return handleDeniedAdmission(workItem, tenantAdmission);
      }
      return executionLease.run(workItem,
          (segment, claimedAt) -> runClaimed(segment, worker, itemContinuationHandler, claimedAt))
          .onTermination().invoke(() -> {
            transitionPermit.close();
            tenantAdmission.permit().close();
          });
    });
  }

  private Uni<Void> handleDeniedAdmission(
      ExecutionWorkItem workItem,
      ControlPlaneTransitionAdmission tenantAdmission) {
    if (ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED
        .equals(tenantAdmission.decision().errorCode())) {
      return workDispatcher.enqueueDelayed(workItem, saturatedDelay.get());
    }
    LOG.warnf(
        "Skipping async execution work item tenant=%s executionId=%s: %s",
        workItem.tenantId(),
        workItem.executionId(),
        tenantAdmission.decision().reason());
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> runClaimed(
      ClaimedSegment segment,
      PipelineTransitionWorker worker,
      AwaitItemContinuationHandler itemContinuationHandler,
      long claimedAtEpochMs) {
    ExecutionRecord<Object, Object> record = segment.record();
    LOG.infof(
        "Claimed async execution %s tenant=%s stepIndex=%d attempt=%d resultShape=%s awaitUnitId=%s",
        record.executionId(),
        record.tenantId(),
        record.currentStepIndex(),
        record.attempt(),
        record.resultShape(),
        record.awaitUnitId() == null ? "<none>" : record.awaitUnitId());
    return segmentBoundaryLedger.get()
        .recordSegmentAttemptStarted(record, segment.transitionKey(), claimedAtEpochMs)
        .chain(() -> executeTransition(segment, worker))
        .onItem().transform(result -> SegmentCommitPlan.from(segment, result, payloadCodec.get()))
        .onItem().transformToUni(plan -> segmentCommitEffects.commit(plan, itemContinuationHandler))
        .onFailure(AwaitThrowableSupport::containsAwaitSuspension)
        .recoverWithUni(failure -> suspendedPlan(segment, failure)
            .onItem().transformToUni(plan -> segmentCommitEffects.commit(plan, itemContinuationHandler)))
        .onFailure().recoverWithUni(failure -> segmentCommitEffects.fail(segment, failure));
  }

  private Uni<TransitionResultEnvelope> executeTransition(
      ClaimedSegment segment,
      PipelineTransitionWorker worker) {
    boolean atRootEnd = isCoordinatorTerminalMaterialization(segment);
    if (atRootEnd && !segment.resumesFromAwait()) {
      return Uni.createFrom().item(withRetainedPageCompletion(
          segment, TransitionResultEnvelope.completedTerminalInputPassthrough()));
    }
    return inputPayload(segment).chain(payload -> {
      // The root cursor can be at the end while an invoked definition still has work.
      if (atRootEnd && !(payload instanceof ExecutionInputSnapshot snapshot
          && snapshot.invocationPath().filter(path -> !path.frames().isEmpty()).isPresent())) {
        Object output = payload instanceof ExecutionInputSnapshot snapshot ? snapshot.payload() : payload;
        return Uni.createFrom().item(withRetainedPageCompletion(segment,
            TransitionResultEnvelope.completedInProcess(terminalOutputItems(segment, output))));
      }
      return transitionWorkerExecutor.execute(worker, segment.transitionCommand(payload, payloadCodec.get()))
          .chain(result -> normalizeTerminalAwaitResult(segment, result));
    });
  }

  private TransitionResultEnvelope withRetainedPageCompletion(
      ClaimedSegment segment, TransitionResultEnvelope result) {
    return segment.record().pagingState()
        .map(page -> result.withPageCompletion(page.suspendedCompletion().orElseThrow(() ->
            new IllegalStateException("a terminal paged segment requires retained page completion"))))
        .orElse(result);
  }

  private Uni<TransitionResultEnvelope> normalizeTerminalAwaitResult(
      ClaimedSegment segment, TransitionResultEnvelope result) {
    if (result != null && result.outcome() == org.pipelineframework.orchestrator.TransitionWorkerOutcome.WAITING_EXTERNAL
        && awaitCoordinator.requiresAuthoritativeSuspension()) {
      return awaitCoordinator.authoritativeSuspension(result.awaitSuspension()).map(TransitionResultEnvelope::waiting);
    }
    if (result == null || !result.terminalInputPassthrough() || !segment.resumesFromAwait()) {
      return Uni.createFrom().item(result);
    }
    if (awaitCoordinator.requiresAuthoritativeSuspension()) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Serialized Await requires the worker to return a canonical terminal result"));
    }
    // The worker establishes the terminal cursor using its generated execution order.
    // The authoritative completed Await value, not the execution's original input, is the result.
    return awaitCoordinator.loadResumePayload(segment.record().tenantId(), segment.record().awaitUnitId())
        .map(payload -> {
          TransitionResultEnvelope completed = TransitionResultEnvelope.completedInProcess(
              terminalOutputItems(segment, payload));
          return result.pageCompletion().map(completed::withPageCompletion).orElse(completed);
        });
  }

  private boolean isCoordinatorTerminalMaterialization(ClaimedSegment segment) {
    if (segment.record().resultShape() != ExecutionResultShape.MATERIALIZED_MULTI
        && !segment.resumesFromAwait()) {
      return false;
    }
    OptionalInt localCount = pipelineStepCount.apply(segment.record());
    return localCount.isPresent() && segment.record().currentStepIndex() == localCount.getAsInt();
  }
  private static List<?> terminalOutputItems(ClaimedSegment segment, Object payload) {
    if (segment.record().resultShape() == ExecutionResultShape.SINGLE) {
      return List.of(payload);
    }
    if (payload instanceof Iterable<?> items) {
      return java.util.stream.StreamSupport
          .stream(items.spliterator(), false).toList();
    }
    return List.of(payload);
  }
  private Uni<Object> inputPayload(ClaimedSegment segment) {
    if (!segment.resumesFromAwait()) {
      return Uni.createFrom().item(segment.record().inputPayload());
    }
    return awaitCoordinator.loadResumeInput(segment.record().tenantId(), segment.record().awaitUnitId());
  }

  private Uni<SegmentCommitPlan> suspendedPlan(ClaimedSegment segment, Throwable failure) {
    return awaitCoordinator.suspensionSnapshot(AwaitThrowableSupport.extractAwaitSuspension(failure))
        .onItem().transform(suspension -> new SuspendedSegment(segment, suspension));
  }

  private ControlPlaneAdmissionRequest admissionRequest(ExecutionWorkItem workItem) {
    return new ControlPlaneAdmissionRequest(
        workItem.tenantId(),
        ControlPlaneAdmissionOperation.PROCESS_WORK_ITEM,
        null,
        null,
        workItem.executionId(),
        "worker-dispatch",
        true);
  }
}

package org.pipelineframework;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitTelemetry;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.AwaitItemContinuationCommand;
import org.pipelineframework.orchestrator.AwaitItemContinuationResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;

class ItemizedAwaitContinuationFlow {

  private static final int MAX_ATTEMPTS = 3;
  private static final long RETRY_BASE_MS = 100L;

  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final AwaitCoordinator awaitCoordinator;
  private final TransitionWorkerExecutor transitionWorkerExecutor;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;
  private final Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder;
  private final AwaitContinuationPlanner planner;
  private final ItemContinuationClaims claims;
  private final Supplier<TransitionPayloadCodec> payloadCodec;

  ItemizedAwaitContinuationFlow(
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitCoordinator awaitCoordinator,
      TransitionWorkerExecutor transitionWorkerExecutor,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger,
      Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder,
      AwaitContinuationPlanner planner,
      ItemContinuationClaims claims) {
    this(
        executionStateStore,
        workDispatcher,
        awaitCoordinator,
        transitionWorkerExecutor,
        segmentBoundaryLedger,
        lifecycleRecorder,
        planner,
        claims,
        org.pipelineframework.orchestrator.JsonTransitionPayloadCodec::new);
  }

  ItemizedAwaitContinuationFlow(
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitCoordinator awaitCoordinator,
      TransitionWorkerExecutor transitionWorkerExecutor,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger,
      Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder,
      AwaitContinuationPlanner planner,
      ItemContinuationClaims claims,
      Supplier<TransitionPayloadCodec> payloadCodec) {
    this.executionStateStore = executionStateStore;
    this.workDispatcher = workDispatcher;
    this.awaitCoordinator = awaitCoordinator;
    this.transitionWorkerExecutor = transitionWorkerExecutor;
    this.segmentBoundaryLedger = segmentBoundaryLedger;
    this.lifecycleRecorder = lifecycleRecorder;
    this.planner = planner;
    this.claims = claims;
    this.payloadCodec = payloadCodec;
  }

  Uni<Void> afterRecordedCompletion(
      AwaitInteractionRecord record,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    if (record == null || unit == null || !unit.dispatchComplete()) {
      telemetry().recordEarlyCompletionHeld(record, unit);
      return Uni.createFrom().voidItem();
    }
    Uni<Optional<ExecutionRecord<Object, Object>>> parentLookup =
        executionStateStore.getExecution(record.tenantId(), record.executionId());
    if (parentLookup == null) {
      telemetry().recordEarlyCompletionHeld(record, unit);
      return Uni.createFrom().voidItem();
    }
    return parentLookup
        .onItem().transform(parent -> planner.afterRecordedCompletion(record, unit, parent))
        .onItem().transformToUni(plan -> interpretAfterRecordedCompletion(plan, itemContinuationHandler, nowEpochMs));
  }

  Uni<Void> afterParentWaiting(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int suspendedStepIndex,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    AwaitContinuationPlan plan = planner.afterParentWaiting(parent, unit, suspendedStepIndex);
    if (plan instanceof AwaitContinuationPlan.DispatchItemContinuations dispatch) {
      Uni<Void> dispatched = dispatchCompletedItemContinuations(
          dispatch.parent(),
          dispatch.unit(),
          itemContinuationHandler,
          nowEpochMs);
      return unit.status() == AwaitUnitStatus.COMPLETED
          ? dispatched.chain(() -> itemContinuationHandler.releaseAwaitParentIfReady(
              dispatch.parent(),
              dispatch.unit(),
              dispatch.nextStepIndex(),
              nowEpochMs))
          : dispatched;
    }
    return Uni.createFrom().voidItem();
  }

  Uni<Void> captureOutput(
      AwaitInteractionRecord interaction,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      ExecutionInputSnapshot continuationInput,
      List<?> segmentOutputs,
      long nowEpochMs) {
    if (!AwaitContinuationPlanner.usesItemContinuations(interaction, unit)) {
      return Uni.createFrom().voidItem();
    }
    return executionStateStore.getExecution(interaction.tenantId(), interaction.executionId())
        .onItem().transformToUni(parent -> {
          if (parent.isEmpty()) {
            return Uni.createFrom().voidItem();
          }
          AwaitContinuationPlan.RecordItemOutput plan =
              (AwaitContinuationPlan.RecordItemOutput) planner.recordItemOutput(
                  parent.get(),
                  unit,
                  interaction,
                  aggregateStepIndex,
                  continuationInput,
                  segmentOutputs);
          return captureOutput(plan, nowEpochMs);
        });
  }

  Uni<Void> releaseParentIfReady(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      long nowEpochMs) {
    if (parent == null || unit == null || !AwaitContinuationPlanner.usesItemContinuations(unit)
        || !unit.dispatchComplete() || unit.expectedItemCount() == null) {
      return Uni.createFrom().voidItem();
    }
    return awaitCoordinator.getUnit(parent.tenantId(), unit.unitId())
        .onItem().transformToUni(currentUnit -> {
          if (!allExternalItemCompletionsDurablyCompleted(currentUnit)) {
            return Uni.createFrom().voidItem();
          }
          if (!currentUnit.hasContinuationCompletionFacts()
              || allItemContinuationsDurablyCompleted(currentUnit)) {
            return releaseParentFromDurableChildren(parent, currentUnit, aggregateStepIndex, nowEpochMs);
          }
          if (!claims.claimReconciliation(parent, currentUnit)) {
            return Uni.createFrom().voidItem();
          }
          return reconcileMissingContinuationFacts(parent, currentUnit, aggregateStepIndex, nowEpochMs)
              .onTermination().invoke(() -> claims.releaseReconciliation(parent, currentUnit));
        });
  }

  private static boolean allItemContinuationsDurablyCompleted(AwaitUnitRecord unit) {
    return allExternalItemCompletionsDurablyCompleted(unit)
        && unit.hasContinuationCompletionFacts()
        && unit.completedContinuationItemCount() >= unit.expectedItemCount();
  }

  private static boolean allExternalItemCompletionsDurablyCompleted(AwaitUnitRecord unit) {
    return unit != null
        && unit.dispatchComplete()
        && unit.expectedItemCount() != null
        && unit.completedItemCount() >= unit.expectedItemCount();
  }

  private Uni<Void> reconcileMissingContinuationFacts(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      long nowEpochMs) {
    return executionStateStore.getExecutionsByKey(parent.tenantId(), childExecutionKeys(parent, unit))
        .onItem().transformToUni(children -> recordMissingContinuationFacts(
            parent, unit, children, 0, nowEpochMs)
            .onItem().transformToUni(reconciled -> allItemContinuationsDurablyCompleted(reconciled)
                ? releaseParentFromChildren(parent, reconciled, aggregateStepIndex, children, nowEpochMs)
                : Uni.createFrom().voidItem()));
  }

  private Uni<AwaitUnitRecord> recordMissingContinuationFacts(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      List<Optional<ExecutionRecord<Object, Object>>> children,
      int itemIndex,
      long nowEpochMs) {
    if (itemIndex >= children.size()) {
      return Uni.createFrom().item(unit);
    }
    Optional<ExecutionRecord<Object, Object>> child = children.get(itemIndex);
    if (child.isPresent() && child.get().status() == ExecutionStatus.SUCCEEDED
        && !unit.hasContinuationCompletionFact(itemIndex)) {
      return awaitCoordinator.recordItemContinuationCompleted(
              parent.tenantId(), unit.unitId(), itemIndex, nowEpochMs)
          .onItem().transformToUni(updated -> recordMissingContinuationFacts(
              parent, updated, children, itemIndex + 1, nowEpochMs));
    }
    return recordMissingContinuationFacts(parent, unit, children, itemIndex + 1, nowEpochMs);
  }

  private Uni<Void> releaseParentFromDurableChildren(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      long nowEpochMs) {
    return executionStateStore.getExecutionsByKey(parent.tenantId(), childExecutionKeys(parent, unit))
        .onItem().transformToUni(children -> releaseParentFromChildren(
            parent, unit, aggregateStepIndex, children, nowEpochMs));
  }

  private Uni<Void> releaseParentFromChildren(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      List<Optional<ExecutionRecord<Object, Object>>> children,
      long nowEpochMs) {
    AwaitContinuationPlan plan = planner.releaseItemizedParent(
        parent, unit, aggregateStepIndex, children, payloadCodec.get());
    if (plan instanceof AwaitContinuationPlan.ReleaseItemizedParent release) {
      return releaseParent(release.release(), nowEpochMs);
    }
    return Uni.createFrom().voidItem();
  }

  private static List<String> childExecutionKeys(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit) {
    List<String> keys = new java.util.ArrayList<>(unit.expectedItemCount());
    for (int index = 0; index < unit.expectedItemCount(); index++) {
      keys.add(ItemContinuationKey.from(parent, unit, index).childExecutionKey());
    }
    return List.copyOf(keys);
  }

  private Uni<Void> interpretAfterRecordedCompletion(
      AwaitContinuationPlan plan,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    if (plan instanceof AwaitContinuationPlan.HoldCompletion held) {
      telemetry().recordEarlyCompletionHeld(held.interaction(), held.unit());
      return Uni.createFrom().voidItem();
    }
    if (plan instanceof AwaitContinuationPlan.DispatchItemContinuations dispatch) {
      return dispatchCompletedItemContinuations(
          dispatch.parent(),
          dispatch.unit(),
          itemContinuationHandler,
          nowEpochMs);
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> dispatchCompletedItemContinuations(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    if (parent == null || unit == null || !AwaitContinuationPlanner.usesItemContinuations(unit) || !unit.dispatchComplete()
        || itemContinuationHandler == AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER) {
      return Uni.createFrom().voidItem();
    }
    return awaitCoordinator.findByUnit(parent.tenantId(), unit.unitId())
        .onItem().transformToMulti(records -> Multi.createFrom().iterable(records))
        .select().where(record -> record.itemInteraction()
            && record.itemIndex() != null
            && record.status() == AwaitInteractionStatus.COMPLETED)
        .onItem().transformToUniAndConcatenate(record -> processToTerminal(
            command(record, 1, nowEpochMs), record, unit, itemContinuationHandler))
        .collect().last()
        .replaceWithVoid();
  }

  Uni<AwaitItemContinuationResult> processOne(
      AwaitItemContinuationCommand command,
      AwaitItemContinuationHandler itemContinuationHandler,
      long saturatedDelayMs) {
    if (itemContinuationHandler == null
        || itemContinuationHandler == AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER) {
      return Uni.createFrom().item(AwaitItemContinuationResult.notReady(command));
    }
    return awaitCoordinator.getUnit(command.tenantId(), command.unitId())
        .onItem().transformToUni(unit -> awaitCoordinator.findByUnit(command.tenantId(), command.unitId())
            .onItem().transformToUni(records -> matchingInteraction(command, records)
                .map(record -> processResolved(command, record, unit, itemContinuationHandler, saturatedDelayMs))
                .orElseGet(() -> Uni.createFrom().item(AwaitItemContinuationResult.notReady(command)))))
        .onItem().transformToUni(result -> recordWorkDisposition(result, saturatedDelayMs));
  }

  Uni<Void> sweepDue(
      long nowEpochMs,
      int limit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long saturatedDelayMs) {
    return awaitCoordinator.findDueItemContinuations(nowEpochMs, limit)
        .onItem().transformToMulti(commands -> Multi.createFrom().iterable(commands))
        .onItem().transformToUniAndConcatenate(command -> processOne(
            command, itemContinuationHandler, saturatedDelayMs))
        .collect().last()
        .replaceWithVoid();
  }

  private Uni<AwaitItemContinuationResult> recordWorkDisposition(
      AwaitItemContinuationResult result,
      long saturatedDelayMs) {
    return switch (result.disposition()) {
      case COMPLETED, ALREADY_COMPLETED, TERMINAL_FAILURE -> awaitCoordinator
          .completeItemContinuation(result.command())
          .replaceWith(result);
      case NOT_READY -> awaitCoordinator.rescheduleItemContinuation(
              result.command(),
              result.command().attempt(),
              result.command().nowEpochMs() + Math.max(1L, saturatedDelayMs))
          .replaceWith(result);
      case RETRY -> awaitCoordinator.rescheduleItemContinuation(
              result.command(),
              result.failureMessage().isPresent()
                  ? result.command().attempt() + 1
                  : result.command().attempt(),
              result.retryAtEpochMs())
          .replaceWith(result);
    };
  }

  private Uni<AwaitItemContinuationResult> processResolved(
      AwaitItemContinuationCommand command,
      AwaitInteractionRecord record,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long saturatedDelayMs) {
    planner.validateItemIndex(record, unit);
    if (!AwaitContinuationPlanner.usesItemContinuations(record, unit)) {
      return Uni.createFrom().item(AwaitItemContinuationResult.alreadyCompleted(command));
    }
    if (unit.hasContinuationCompletionFact(command.itemIndex())) {
      return Uni.createFrom().item(AwaitItemContinuationResult.alreadyCompleted(command));
    }
    ItemContinuationKey key = new ItemContinuationKey(
        record.tenantId(), record.executionId(), record.executionId(), record.unitId(), record.itemIndex());
    if (!claims.claimDispatch(key)) {
      return Uni.createFrom().item(AwaitItemContinuationResult.notReady(command));
    }
    Optional<TransitionWorkerExecutor.TransitionAdmission> admission = transitionWorkerExecutor.tryAdmit();
    if (admission.isEmpty()) {
      claims.releaseDispatch(key);
      return Uni.createFrom().item(AwaitItemContinuationResult.retry(
          command, command.nowEpochMs() + Math.max(1L, saturatedDelayMs), null));
    }
    TransitionWorkerExecutor.TransitionAdmission permit = admission.orElseThrow();
    return executionStateStore.getExecution(record.tenantId(), record.executionId())
        .onItem().transformToUni(parent -> {
          if (!planner.itemContinuationReady(parent, unit)) {
            return Uni.createFrom().item(AwaitItemContinuationResult.notReady(command));
          }
          Uni<Void> continuation = itemContinuationHandler.continueAwaitItem(
              record, unit, record.stepIndex() + 1, parent, command.nowEpochMs());
          return continuation == null
              ? Uni.createFrom().item(AwaitItemContinuationResult.completed(command))
              : continuation.replaceWith(AwaitItemContinuationResult.completed(command));
        })
        .onFailure().recoverWithUni(failure -> command.attempt() < MAX_ATTEMPTS
            ? Uni.createFrom().item(AwaitItemContinuationResult.retry(
                command, command.nowEpochMs() + retryDelayMs(command.attempt()), failure))
            : failParentAfterContinuationError(
                new AwaitContinuationPlan.FailParent(record, failure, command.attempt()),
                command.nowEpochMs())
                .replaceWith(AwaitItemContinuationResult.terminalFailure(command, failure)))
        .onItem().invoke(result -> {
          if (!result.successful()) {
            claims.releaseDispatch(key);
          }
        })
        .onFailure().invoke(() -> claims.releaseDispatch(key))
        .onTermination().invoke(permit::close);
  }

  private Uni<AwaitItemContinuationResult> processToTerminal(
      AwaitItemContinuationCommand command,
      AwaitInteractionRecord record,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler handler) {
    return processResolved(command, record, unit, handler, 1L)
        .onItem().transformToUni(result -> {
          if (result.disposition() != org.pipelineframework.orchestrator.AwaitItemContinuationDisposition.RETRY) {
            return Uni.createFrom().item(result);
          }
          if (result.failureMessage().isEmpty()) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Await item continuation worker capacity is saturated"));
          }
          return processToTerminal(
              command.nextAttempt(result.retryAtEpochMs()), record, unit, handler);
        });
  }

  private static AwaitItemContinuationCommand command(
      AwaitInteractionRecord record,
      int attempt,
      long nowEpochMs) {
    return new AwaitItemContinuationCommand(
        record.tenantId(),
        record.executionId(),
        record.unitId(),
        record.interactionId(),
        record.itemIndex(),
        attempt,
        nowEpochMs);
  }

  private static Optional<AwaitInteractionRecord> matchingInteraction(
      AwaitItemContinuationCommand command,
      List<AwaitInteractionRecord> records) {
    return records.stream()
        .filter(record -> command.executionId().equals(record.executionId()))
        .filter(record -> command.interactionId().equals(record.interactionId()))
        .filter(record -> record.itemIndex() != null && command.itemIndex() == record.itemIndex())
        .filter(record -> record.status() == AwaitInteractionStatus.COMPLETED)
        .findFirst();
  }

  private Uni<Void> captureOutput(
      AwaitContinuationPlan.RecordItemOutput plan,
      long nowEpochMs) {
    return executionStateStore.getExecutionByKey(plan.interaction().tenantId(), plan.key().childExecutionKey())
        .onItem().transformToUni(existing -> {
          if (existing.isPresent() && existing.get().status() == ExecutionStatus.SUCCEEDED) {
            return recordItemContinuationAndRelease(plan, nowEpochMs);
          }
          long ttl = plan.parent().ttlEpochS();
          ExecutionCreateCommand create = new ExecutionCreateCommand(
              plan.interaction().tenantId(),
              plan.key().childExecutionKey(),
              plan.parent().pipelineId(),
              plan.parent().contractVersion(),
              plan.parent().releaseVersion(),
              plan.continuationInput(),
              ExecutionResultShape.MATERIALIZED_MULTI,
              Optional.of(plan.interaction().outputType()),
              plan.parent().currentStepIndex() + 1,
              nowEpochMs,
              ttl);
          return executionStateStore.createOrGetExecution(create)
              .onItem().transformToUni(created -> handleCreatedChild(plan, created, nowEpochMs));
        });
  }

  private Uni<Void> handleCreatedChild(
      AwaitContinuationPlan.RecordItemOutput plan,
      CreateExecutionResult created,
      long nowEpochMs) {
    ExecutionRecord<Object, Object> child = created.record();
    if (created.duplicate() && child.status() == ExecutionStatus.SUCCEEDED) {
      return recordItemContinuationAndRelease(plan, nowEpochMs);
    }
    String transitionKey = "await-item-continuation:" + plan.unit().unitId() + ":" + plan.interaction().itemIndex();
    return executionStateStore.markSucceeded(
            child.tenantId(),
            child.executionId(),
            child.version(),
            transitionKey,
            plan.segmentOutputs(),
            nowEpochMs)
        .onItem().transformToUni(updated -> handleChildSuccessUpdate(plan, updated, child, nowEpochMs));
  }

  private Uni<Void> handleChildSuccessUpdate(
      AwaitContinuationPlan.RecordItemOutput plan,
      Optional<ExecutionRecord<Object, Object>> updated,
      ExecutionRecord<Object, Object> child,
      long nowEpochMs) {
    if (updated.isPresent()) {
      return recordItemContinuationAndRelease(plan, nowEpochMs);
    }
    return executionStateStore.getExecutionByKey(plan.parent().tenantId(), child.executionKey())
        .onItem().transformToUni(current -> {
          if (current.isPresent() && current.get().status() == ExecutionStatus.SUCCEEDED) {
            return recordItemContinuationAndRelease(plan, nowEpochMs);
          }
          if (current.isPresent() && isPendingChildMaterialization(current.get())) {
            ExecutionRecord<Object, Object> refreshed = current.get();
            return executionStateStore.markSucceeded(
                    refreshed.tenantId(),
                    refreshed.executionId(),
                    refreshed.version(),
                    "await-item-continuation:" + plan.unit().unitId() + ":" + plan.interaction().itemIndex(),
                    plan.segmentOutputs(),
                    nowEpochMs)
                .onItem().transformToUni(retried -> retried.isPresent()
                    ? recordItemContinuationAndRelease(plan, nowEpochMs)
                    : executionStateStore.getExecutionByKey(plan.parent().tenantId(), child.executionKey())
                        .onItem().transformToUni(latest -> {
                          if (latest.isPresent() && latest.get().status() == ExecutionStatus.SUCCEEDED) {
                            return recordItemContinuationAndRelease(plan, nowEpochMs);
                          }
                          return childSuccessNotAdmitted(child.executionKey());
                        }));
          }
          return childSuccessNotAdmitted(child.executionKey());
        });
  }

  private static Uni<Void> childSuccessNotAdmitted(String childExecutionKey) {
    return Uni.createFrom().failure(new IllegalStateException(
        "Await item continuation child success was not admitted and child is not already SUCCEEDED: "
            + childExecutionKey));
  }

  private static boolean isPendingChildMaterialization(ExecutionRecord<Object, Object> child) {
    return child.status() == ExecutionStatus.QUEUED || child.status() == ExecutionStatus.RUNNING;
  }

  private Uni<Void> recordItemContinuationSegment(
      AwaitContinuationPlan.RecordItemOutput plan,
      long nowEpochMs) {
    return segmentBoundaryLedger.get().recordContinuationSegmentCreated(
        plan.parent(),
        plan.unit(),
        plan.key().segmentId(),
        plan.interaction().stepIndex() + 1,
        plan.aggregateStepIndex(),
        plan.continuationInput(),
        nowEpochMs);
  }

  private Uni<Void> recordItemContinuationAndRelease(
      AwaitContinuationPlan.RecordItemOutput plan,
      long nowEpochMs) {
    Integer itemIndex = plan.interaction().itemIndex();
    if (itemIndex == null) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Itemized await continuation requires an item index for unit " + plan.unit().unitId()));
    }
    return recordItemContinuationSegment(plan, nowEpochMs)
        .chain(() -> awaitCoordinator.recordItemContinuationCompleted(
            plan.parent().tenantId(),
            plan.unit().unitId(),
            itemIndex,
            nowEpochMs))
        .onItem().transformToUni(currentUnit -> releaseParentIfReady(
            plan.parent(), currentUnit, plan.aggregateStepIndex(), nowEpochMs));
  }

  private Uni<Void> releaseParent(
      ItemizedParentRelease release,
      long nowEpochMs) {
    return executionStateStore.markAwaitItemContinuationsCompleted(
            release.parent().tenantId(),
            release.parent().executionId(),
            release.unit().unitId(),
            release.aggregateStepIndex(),
            release.resumePayload(),
            nowEpochMs)
        .onItem().transformToUni(updated -> updated
            .map(released -> recordReleaseAndEnqueue(released, release, nowEpochMs))
            .orElseGet(() -> enqueueAlreadyReleasedParent(release, nowEpochMs)));
  }

  private Uni<Void> enqueueAlreadyReleasedParent(
      ItemizedParentRelease release,
      long nowEpochMs) {
    return executionStateStore.getExecution(release.parent().tenantId(), release.parent().executionId())
        .onItem().transformToUni(current -> {
          if (current.isEmpty() || !releasedForStep(current.get(), release.aggregateStepIndex())) {
            return Uni.createFrom().voidItem();
          }
          return recordReleaseAndEnqueue(current.get(), release, nowEpochMs);
        });
  }

  private Uni<Void> recordReleaseAndEnqueue(
      ExecutionRecord<Object, Object> released,
      ItemizedParentRelease release,
      long nowEpochMs) {
    return segmentBoundaryLedger.get().recordContinuationSegmentCreated(
            released,
            release.unit(),
            SegmentBoundaryLedger.segmentId(released),
            release.aggregateStepIndex(),
            -1,
            release.resumePayload(),
            nowEpochMs)
        .chain(() -> workDispatcher.enqueueNow(new ExecutionWorkItem(
            released.tenantId(),
            released.executionId())))
        .invoke(() -> {
          claims.clearDispatches(release.unit());
          lifecycleRecorder.accept(new AwaitReplayLifecycleEvent(
              AwaitReplayLifecycleEvent.RESUME_RELEASED,
              released.executionId(),
              release.unit().unitId(),
              release.unit().stepId(),
              release.unit().stepIndex(),
              released.status().name(),
              null,
              null,
              null,
              null,
              release.unit().expectedItemCount(),
              release.unit().completedItemCount(),
              release.unit().dispatchComplete()));
          telemetry().recordResumeReleased(release.unit());
        })
        .replaceWithVoid();
  }

  private Uni<Void> failParentAfterContinuationError(
      AwaitContinuationPlan.FailParent plan,
      long nowEpochMs) {
    AwaitInteractionRecord record = plan.interaction();
    return executionStateStore.getExecution(record.tenantId(), record.executionId())
        .onItem().transformToUni(parent -> {
          if (parent.isEmpty() || parent.get().status().terminal()) {
            return Uni.createFrom().item(Optional.<ExecutionRecord<Object, Object>>empty());
          }
          ExecutionRecord<Object, Object> parentRecord = parent.get();
          return executionStateStore.markTerminalFailure(
              parentRecord.tenantId(),
              parentRecord.executionId(),
              parentRecord.version(),
              ExecutionStatus.FAILED,
              "await-item-continuation-failed:" + record.unitId() + ":" + record.itemIndex(),
              "AWAIT_ITEM_CONTINUATION_FAILED",
              "Await item continuation failed after " + plan.attempt() + " attempts: " + plan.failure().getMessage(),
              nowEpochMs);
        })
        .replaceWithVoid();
  }

  private AwaitTelemetry telemetry() {
    AwaitTelemetry telemetry = awaitCoordinator == null ? null : awaitCoordinator.awaitTelemetry();
    return telemetry == null ? AwaitTelemetry.disabled() : telemetry;
  }

  private static long retryDelayMs(int attempt) {
    int boundedAttempt = Math.max(1, Math.min(attempt, MAX_ATTEMPTS));
    return RETRY_BASE_MS << (boundedAttempt - 1);
  }

  private static boolean releasedForStep(
      ExecutionRecord<Object, Object> record,
      int stepIndex) {
    return record != null
        && record.status() == ExecutionStatus.QUEUED
        && record.currentStepIndex() == stepIndex;
  }
}

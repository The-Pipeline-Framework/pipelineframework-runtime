package org.pipelineframework;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.function.IntSupplier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.config.pipeline.PipelineOrderResourceLoader;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitLiveCompletionRegistry;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.CoordinatorSweepResult;
import org.pipelineframework.orchestrator.AwaitItemContinuationCommand;
import org.pipelineframework.orchestrator.AwaitItemContinuationResult;
import org.pipelineframework.orchestrator.AwaitSemanticCheckpoint;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;
import org.pipelineframework.telemetry.PipelineReplayTelemetry;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRedriveResult;
import org.pipelineframework.orchestrator.ExecutionRedriveIntent;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShapeResolver;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.release.LocalPipelineReleaseActivation;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;

/**
 * Coordinates queue-mode orchestration lifecycle and provider interactions.
 */
@ApplicationScoped
class QueueAsyncCoordinator {

  private static final Logger LOG = Logger.getLogger(QueueAsyncCoordinator.class);
  private static final AwaitItemContinuationHandler NOOP_ITEM_CONTINUATION_HANDLER =
      AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER;

  @Inject
  PipelineOrchestratorConfig orchestratorConfig;

  @Inject
  Instance<ExecutionStateStore> executionStateStores;

  @Inject
  Instance<WorkDispatcher> workDispatchers;

  @Inject
  Instance<DeadLetterPublisher> deadLetterPublishers;

  @Inject
  ExecutionInputPolicy executionInputPolicy;

  @Inject
  ExecutionFailureHandler executionFailureHandler;

  @Inject
  ExecutionResultShapeResolver executionResultShapeResolver;

  @Inject
  CheckpointPublicationService checkpointPublicationService;

  @Inject
  ObjectPublishCompletionService objectPublishCompletionService;

  @Inject
  AwaitCoordinator awaitCoordinator;

  @Inject
  AwaitLiveCompletionRegistry awaitLiveCompletionRegistry;

  @Inject
  TransitionWorkerExecutor transitionWorkerExecutor;

  @Inject
  TransitionPayloadCodec transitionPayloadCodec;

  @Inject
  org.pipelineframework.orchestrator.RegisteredRestWorkerTargets registeredTargets;

  @Inject
  PipelineReleaseIdentityResolver releaseIdentityResolver;

  @Inject
  LocalPipelineReleaseActivation localReleaseActivation;

  @Inject
  PipelineReleaseRegistry releaseRegistry;

  @Inject
  ControlPlaneAdmissionPolicy controlPlaneAdmissionPolicy;

  @Inject
  SegmentBoundaryLedger segmentBoundaryLedger;

  private volatile TransitionPayloadCodec fallbackTransitionPayloadCodec;
  private volatile PipelineReleaseIdentityResolver fallbackReleaseIdentityResolver;
  private volatile ControlPlaneAdmissionPolicy fallbackAdmissionPolicy;
  private volatile AwaitContinuations awaitContinuations;
  private volatile ExecutionReadModel executionReadModel;
  private volatile QueueAsyncSubmissionFlow submissionFlow;
  private volatile QueueAsyncRedriveFlow redriveFlow;
  private volatile QueueAsyncSweepFlow sweepFlow;
  private final CachedIntSupplier pipelineStepCount = new CachedIntSupplier(this::loadPipelineStepCount);

  @Inject
  PipelineReplayTelemetry telemetry;

  volatile ExecutionStateStore executionStateStore;
  volatile WorkDispatcher workDispatcher;
  volatile DeadLetterPublisher deadLetterPublisher;
  private final String queueWorkerId = "worker-" + UUID.randomUUID();

  private volatile boolean queueProvidersInitialized;

  synchronized void initializeQueueMode() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return;
    }
    initializeQueueProviders();
  }

  Uni<RunAsyncAcceptedDto> executePipelineAsync(
      Object input,
      String tenantId,
      String idempotencyKey,
      boolean outputStreaming) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return submissionFlow().submit(input, tenantId, idempotencyKey, outputStreaming);
  }

  Uni<RunAsyncAcceptedDto> executePipelineAsync(
      Object input,
      String tenantId,
      String idempotencyKey,
      boolean outputStreaming,
      String pipelineId,
      String contractVersion,
      String releaseVersion) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return submissionFlow().submit(
        input,
        tenantId,
        idempotencyKey,
        outputStreaming,
        pipelineId,
        contractVersion,
        releaseVersion);
  }

  Uni<ExecutionStatusDto> getExecutionStatus(String tenantId, String executionId) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return executionReadModel().getExecutionStatus(tenantId, executionId);
  }

  <T> Uni<T> getExecutionResult(String tenantId, String executionId, Class<?> outputType, boolean outputStreaming) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return executionReadModel().getExecutionResult(tenantId, executionId, outputType, outputStreaming);
  }

  Uni<ExecutionRedriveResult> redriveExecution(
      String tenantId,
      String executionId,
      Long expectedVersion,
      boolean allowFailed,
      String reason) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return redriveFlow().redrive(tenantId, executionId, expectedVersion, allowFailed, reason);
  }

  Uni<ExecutionRedriveResult> redriveExecution(
      String tenantId,
      String executionId,
      Long expectedVersion,
      boolean allowFailed,
      ExecutionRedriveIntent intent,
      String reason) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return redriveFlow().redrive(tenantId, executionId, expectedVersion, allowFailed, intent, reason);
  }

  Uni<ExecutionRedriveResult> redriveExecution(
      String tenantId,
      String executionId,
      Long expectedVersion,
      boolean allowFailed,
      ExecutionRedriveIntent intent,
      String targetCommandId,
      String reason) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return redriveFlow().redrive(
        tenantId, executionId, expectedVersion, allowFailed, intent, targetCommandId, reason);
  }

  Uni<Object> getExecutionResultPayload(String tenantId, String executionId) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return executionReadModel().getExecutionResultPayload(tenantId, executionId);
  }

  Uni<Void> processExecutionWorkItem(ExecutionWorkItem workItem, PipelineTransitionWorker worker) {
    return processExecutionWorkItem(workItem, worker, NOOP_ITEM_CONTINUATION_HANDLER);
  }

  Uni<Void> processExecutionWorkItem(
      ExecutionWorkItem workItem,
      PipelineTransitionWorker worker,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return Uni.createFrom().deferred(() -> {
      if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC || workItem == null) {
        return Uni.createFrom().voidItem();
      }
      ensureQueueModeReady();
      if (worker == null) {
        return Uni.createFrom().failure(new IllegalArgumentException("PipelineTransitionWorker must not be null"));
      }
      if (org.pipelineframework.orchestrator.RegisteredRestWorkerTargets.enabled(orchestratorConfig)) {
        return executionStateStore.getExecution(workItem.tenantId(), workItem.executionId())
            .chain(execution -> execution.map(registeredTargets::validateExecutionPin)
                .orElseGet(() -> Uni.createFrom().voidItem()))
            .chain(() -> segmentPipeline().process(workItem, worker, itemContinuationHandler));
      }
      return segmentPipeline().process(workItem, worker, itemContinuationHandler);
    });
  }

  Uni<CoordinatorSweepResult> sweepOnce(long nowEpochMs) {
    return sweepOnce(nowEpochMs, NOOP_ITEM_CONTINUATION_HANDLER);
  }

  Uni<CoordinatorSweepResult> sweepOnce(
      long nowEpochMs,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return Uni.createFrom().deferred(() -> {
      if (!ensureQueueModeReady()) {
        return Uni.createFrom().failure(queueModeDisabledException());
      }
      int limit = orchestratorConfig.sweepLimit();
      Uni<CoordinatorSweepResult> sweep = sweepFlow().sweepOnce(nowEpochMs);
      if (itemContinuationHandler == NOOP_ITEM_CONTINUATION_HANDLER) {
        return sweep;
      }
      return sweep
          .onItem().transformToUni(result -> awaitContinuations().sweepDueItemContinuations(
                  nowEpochMs,
                  limit,
                  itemContinuationHandler,
                  saturatedDelay().toMillis())
              .replaceWith(result));
    });
  }

  Uni<AwaitCompletionResult> completeAwait(AwaitCompletionCommand command) {
    return completeAwait(command, NOOP_ITEM_CONTINUATION_HANDLER);
  }

  Uni<AwaitCompletionResult> completeAwait(
      AwaitCompletionCommand command,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return Uni.createFrom().deferred(() -> {
      if (!ensureQueueModeReady()) {
        return Uni.createFrom().failure(queueModeDisabledException());
      }
      return awaitBoundaryAdmission().complete(command, itemContinuationHandler);
    });
  }

  Uni<List<AwaitInteractionRecord>> queryPendingAwaitInteractions(
      String tenantId,
      String assignee,
      String group,
      String stepId,
      int limit) {
    return Uni.createFrom().deferred(() -> {
      if (!ensureQueueModeReady()) {
        return Uni.createFrom().failure(queueModeDisabledException());
      }
      String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
      RuntimeException admissionFailure = admissionFailure(admissionRequest(
          resolvedTenant,
          ControlPlaneAdmissionOperation.QUERY_PENDING_AWAIT,
          null,
          "api",
          explicitTenant(tenantId)));
      if (admissionFailure != null) {
        return Uni.createFrom().failure(admissionFailure);
      }
      return awaitCoordinator.queryPending(resolvedTenant, assignee, group, stepId, limit <= 0 ? 100 : limit);
    });
  }

  Uni<Optional<AwaitSemanticCheckpoint>> getAwaitSemanticCheckpoint(
      String tenantId,
      String interactionId) {
    return Uni.createFrom().deferred(() -> {
      if (!ensureQueueModeReady()) {
        return Uni.createFrom().failure(queueModeDisabledException());
      }
      String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
      RuntimeException admissionFailure = admissionFailure(admissionRequest(
          resolvedTenant,
          ControlPlaneAdmissionOperation.READ_AWAIT_SEMANTIC_CHECKPOINT,
          null,
          "api",
          explicitTenant(tenantId)));
      if (admissionFailure != null) {
        return Uni.createFrom().failure(admissionFailure);
      }
      return awaitCoordinator.getInteraction(resolvedTenant, interactionId)
          .onItem().transformToUni(interaction -> interaction
              .map(record -> executionStateStore.getExecution(resolvedTenant, record.executionId())
                  .onItem().transform(execution -> execution.map(parent -> new AwaitSemanticCheckpoint(
                      record.tenantId(),
                      record.executionId(),
                      record.interactionId(),
                      record.correlationId(),
                      record.unitId(),
                      record.stepId(),
                      record.status(),
                      parent.pipelineId(),
                      parent.contractVersion(),
                      parent.releaseVersion()))))
              .orElseGet(() -> Uni.createFrom().item(Optional.empty())));
    });
  }

  Uni<List<AwaitSemanticCheckpoint>> getAwaitSemanticCheckpoints(
      String tenantId,
      String executionId,
      int limit) {
    return Uni.createFrom().deferred(() -> {
      if (!ensureQueueModeReady()) {
        return Uni.createFrom().failure(queueModeDisabledException());
      }
      if (executionId == null || executionId.isBlank()) {
        return Uni.createFrom().failure(new IllegalArgumentException("executionId must not be blank"));
      }
      String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
      RuntimeException admissionFailure = admissionFailure(admissionRequest(
          resolvedTenant,
          ControlPlaneAdmissionOperation.READ_AWAIT_SEMANTIC_CHECKPOINT,
          executionId,
          "api",
          explicitTenant(tenantId)));
      if (admissionFailure != null) {
        return Uni.createFrom().failure(admissionFailure);
      }
      return executionStateStore.getExecution(resolvedTenant, executionId)
          .onItem().transformToUni(execution -> execution
              .map(parent -> awaitCoordinator.findByExecution(resolvedTenant, executionId, limit <= 0 ? 100 : limit)
                  .onItem().transform(records -> records.stream()
                      .map(record -> new AwaitSemanticCheckpoint(
                          record.tenantId(), record.executionId(), record.interactionId(), record.correlationId(),
                          record.unitId(), record.stepId(), record.status(), parent.pipelineId(),
                          parent.contractVersion(), parent.releaseVersion()))
                      .toList()))
              .orElseGet(() -> Uni.createFrom().item(List.of())));
    });
  }

  Uni<AwaitItemContinuationResult> processAwaitItemContinuation(
      AwaitItemContinuationCommand command,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return Uni.createFrom().deferred(() -> {
      if (!ensureQueueModeReady()) {
        return Uni.createFrom().failure(queueModeDisabledException());
      }
      if (command == null) {
        return Uni.createFrom().failure(new IllegalArgumentException(
            "AwaitItemContinuationCommand must not be null"));
      }
      return awaitContinuations().processItemContinuation(
          command,
          itemContinuationHandler,
          saturatedDelay().toMillis());
    });
  }

  private Duration saturatedDelay() {
    PipelineOrchestratorConfig.WorkerConfig workerConfig = orchestratorConfig.worker();
    if (workerConfig == null || workerConfig.saturatedDelay() == null) {
      return Duration.ofSeconds(1);
    }
    return workerConfig.saturatedDelay();
  }

  private TransitionPayloadCodec payloadCodec() {
    if (transitionPayloadCodec != null) {
      return transitionPayloadCodec;
    }
    TransitionPayloadCodec fallback = fallbackTransitionPayloadCodec;
    if (fallback == null) {
      synchronized (this) {
        fallback = fallbackTransitionPayloadCodec;
        if (fallback == null) {
          fallback = new JsonTransitionPayloadCodec();
          fallbackTransitionPayloadCodec = fallback;
        }
      }
    }
    return fallback;
  }

  private String pipelineId() {
    return releaseIdentityResolver().pipelineId(orchestratorConfig);
  }

  private String contractVersion() {
    return releaseIdentityResolver().contractVersion();
  }

  private String releaseVersion() {
    return releaseIdentityResolver().releaseVersion(orchestratorConfig);
  }

  private PipelineReleaseIdentityResolver releaseIdentityResolver() {
    if (releaseIdentityResolver != null) {
      return releaseIdentityResolver;
    }
    PipelineReleaseIdentityResolver fallback = fallbackReleaseIdentityResolver;
    if (fallback == null) {
      synchronized (this) {
        fallback = fallbackReleaseIdentityResolver;
        if (fallback == null) {
          fallback = new PipelineReleaseIdentityResolver();
          fallbackReleaseIdentityResolver = fallback;
        }
      }
    }
    return fallback;
  }

  private ControlPlaneAdmissionPolicy admissionPolicy() {
    if (controlPlaneAdmissionPolicy != null) {
      return controlPlaneAdmissionPolicy;
    }
    ControlPlaneAdmissionPolicy fallback = fallbackAdmissionPolicy;
    if (fallback == null) {
      synchronized (this) {
        fallback = fallbackAdmissionPolicy;
        if (fallback == null) {
          fallback = new org.pipelineframework.orchestrator.LocalControlPlaneAdmissionPolicy(orchestratorConfig);
          fallbackAdmissionPolicy = fallback;
        }
      }
    }
    return fallback;
  }

  private RuntimeException admissionFailure(ControlPlaneAdmissionRequest request) {
    ControlPlaneAdmissionDecision decision = admissionPolicy().admit(request);
    return decision.allowed() ? null : new ControlPlaneAdmissionException(decision);
  }

  private ControlPlaneAdmissionRequest admissionRequest(
      String tenantId,
      ControlPlaneAdmissionOperation operation,
      String executionId,
      String source,
      boolean explicitTenant) {
    return new ControlPlaneAdmissionRequest(
        tenantId,
        operation,
        pipelineId(),
        releaseVersion(),
        executionId,
        source,
        explicitTenant);
  }

  private ControlPlaneAdmissionRequest admissionRequest(
      String tenantId,
      ControlPlaneAdmissionOperation operation,
      String pipelineId,
      String releaseVersion,
      String executionId,
      String source,
      boolean explicitTenant) {
    return new ControlPlaneAdmissionRequest(
        tenantId,
        operation,
        pipelineId,
        releaseVersion,
        executionId,
        source,
        explicitTenant);
  }

  private static boolean explicitTenant(String tenantId) {
    return tenantId != null && !tenantId.isBlank();
  }

  private boolean ensureQueueModeReady() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return false;
    }
    if (!queueProvidersInitialized) {
      initializeQueueProviders();
    }
    return true;
  }

  private synchronized void initializeQueueProviders() {
    if (queueProvidersInitialized) {
      return;
    }
    if (missingQueueProviders() || configuredQueueProviderNames()) {
      executionStateStore = selectExecutionStateStore(orchestratorConfig.stateProvider());
      workDispatcher = selectWorkDispatcher(orchestratorConfig.dispatcherProvider());
      deadLetterPublisher = selectDeadLetterPublisher(orchestratorConfig.dlqProvider());
    }
    executionReadModel = null;
    submissionFlow = null;
    redriveFlow = null;
    sweepFlow = null;

    List<String> providerReadinessErrors = new ArrayList<>();
    if (!executionStateStore.supportsLeaseRenewal()) {
      providerReadinessErrors.add(
          "ExecutionStateStore(" + executionStateStore.providerName() + "): live lease renewal is not supported");
    }
    if (localPagingPlan().isPresent()
        && !executionStateStore.supportsPagedProgress()) {
      providerReadinessErrors.add(
          "ExecutionStateStore(" + executionStateStore.providerName()
              + "): fenced paged progress is not supported");
    }
    executionStateStore.startupValidationError()
        .ifPresent(error -> providerReadinessErrors
            .add("ExecutionStateStore(" + executionStateStore.providerName() + "): " + error));
    workDispatcher.startupValidationError()
        .ifPresent(error -> providerReadinessErrors
            .add("WorkDispatcher(" + workDispatcher.providerName() + "): " + error));
    deadLetterPublisher.startupValidationError()
        .ifPresent(error -> providerReadinessErrors
            .add("DeadLetterPublisher(" + deadLetterPublisher.providerName() + "): " + error));
    if (!providerReadinessErrors.isEmpty()) {
      String readinessMessage = "Queue async provider startup validation failed: " + String.join("; ",
          providerReadinessErrors);
      if (orchestratorConfig.strictStartup()) {
        throw new IllegalStateException(readinessMessage);
      }
      LOG.warn(readinessMessage);
    }

    if (orchestratorConfig.strictStartup()
        && orchestratorConfig.idempotencyPolicy() == OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY) {
      throw new IllegalStateException(
          "pipeline.orchestrator.idempotency-policy must be explicitly configured for queue mode when strict startup is enabled.");
    }
    queueProvidersInitialized = true;
  }

  private boolean missingQueueProviders() {
    return executionStateStore == null || workDispatcher == null || deadLetterPublisher == null;
  }

  private boolean configuredQueueProviderNames() {
    return hasText(orchestratorConfig.stateProvider())
        || hasText(orchestratorConfig.dispatcherProvider())
        || hasText(orchestratorConfig.dlqProvider());
  }

  private static boolean hasText(String value) {
    return value != null && !value.isBlank();
  }

  private static IllegalStateException queueModeDisabledException() {
    return new IllegalStateException(
        "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC.");
  }

  Uni<Void> recordAwaitItemContinuation(
      AwaitInteractionRecord interaction,
      org.pipelineframework.awaitable.AwaitUnitRecord unit,
      int aggregateStepIndex,
      ExecutionInputSnapshot continuationInput,
      List<?> segmentOutputs,
      long nowEpochMs) {
    return awaitContinuations().captureItemContinuationOutput(
        interaction,
        unit,
        aggregateStepIndex,
        continuationInput,
        segmentOutputs,
        nowEpochMs);
  }

  Uni<Void> releaseItemizedAwaitParentIfReady(
      ExecutionRecord<Object, Object> parent,
      org.pipelineframework.awaitable.AwaitUnitRecord unit,
      int aggregateStepIndex,
      long nowEpochMs) {
    return awaitContinuations().releaseParentIfReady(parent, unit, aggregateStepIndex, nowEpochMs);
  }

  private AwaitBoundaryAdmission awaitBoundaryAdmission() {
    return new AwaitBoundaryAdmission(
        orchestratorConfig,
        executionInputPolicy,
        admissionPolicy(),
        awaitCoordinator,
        awaitLiveCompletionRegistry,
        this::pipelineId,
        this::releaseVersion,
        this::segmentBoundaryLedger,
        awaitContinuations());
  }

  private ExecutionReadModel executionReadModel() {
    if (!ensureQueueModeReady()) {
      throw queueModeDisabledException();
    }
    ExecutionReadModel current = executionReadModel;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      current = executionReadModel;
      if (current == null) {
        current = new ExecutionReadModel(
            orchestratorConfig,
            executionInputPolicy,
            executionStateStore,
            admissionPolicy(),
            this::pipelineId,
            this::releaseVersion,
            this::payloadCodec);
        executionReadModel = current;
      }
      return current;
    }
  }

  private QueueAsyncSubmissionFlow submissionFlow() {
    if (!ensureQueueModeReady()) {
      throw queueModeDisabledException();
    }
    QueueAsyncSubmissionFlow current = submissionFlow;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      current = submissionFlow;
      if (current == null) {
        current = new QueueAsyncSubmissionFlow(
            orchestratorConfig,
            executionInputPolicy,
            executionResultShapeResolver,
            executionStateStore,
            workDispatcher,
            admissionPolicy(),
            this::pipelineId,
            this::contractVersion,
            this::releaseVersion,
            this::segmentBoundaryLedger,
            this::activateLocalReleaseForSubmission,
            this::pagingPlanFor);
        submissionFlow = current;
      }
      return current;
    }
  }

  private Optional<PipelinePagingPlan> localPagingPlan() {
    return PipelinePagingPlan.from(releaseIdentityResolver().contract());
  }

  private Uni<Optional<PipelinePagingPlan>> pagingPlanFor(PipelineRunSubmission submission) {
    if (targetsLocalContract(submission)) {
      return validatePagingStore(localPagingPlan());
    }
    if (releaseRegistry == null) {
      return Uni.createFrom().failure(new IllegalStateException(
          "submitted release contract is unavailable for paging validation"));
    }
    return releaseRegistry.get(
            submission.tenantId(), submission.pipelineId(), submission.releaseVersion())
        .onItem().transformToUni(release -> {
          if (release.isEmpty() || release.orElseThrow().contract() == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "submitted release contract is unavailable for paging validation: "
                    + submission.pipelineId() + ":" + submission.releaseVersion()));
          }
          var pinned = release.orElseThrow();
          if (!submission.contractVersion().equals(pinned.contractVersion())) {
            return Uni.createFrom().failure(new IllegalStateException(
                "submitted contract version does not match the pinned release contract"));
          }
          return validatePagingStore(PipelinePagingPlan.from(pinned.contract()));
        });
  }

  private Uni<Optional<PipelinePagingPlan>> validatePagingStore(
      Optional<PipelinePagingPlan> plan) {
    if (plan.isPresent() && !executionStateStore.supportsPagedProgress()) {
      return Uni.createFrom().failure(new IllegalStateException(
          "ExecutionStateStore(" + executionStateStore.providerName()
              + ") does not support fenced paged progress"));
    }
    return Uni.createFrom().item(plan);
  }

  private boolean targetsLocalContract(PipelineRunSubmission submission) {
    return pipelineId().equals(submission.pipelineId())
        && contractVersion().equals(submission.contractVersion())
        && releaseVersion().equals(submission.releaseVersion());
  }

  private Uni<Void> activateLocalReleaseForSubmission(PipelineRunSubmission submission) {
    if (localReleaseActivation == null) {
      return Uni.createFrom().voidItem();
    }
    return localReleaseActivation.activateForCurrentRelease(
        submission.tenantId(),
        submission.pipelineId(),
        submission.contractVersion(),
        submission.releaseVersion());
  }

  private QueueAsyncRedriveFlow redriveFlow() {
    if (!ensureQueueModeReady()) {
      throw queueModeDisabledException();
    }
    QueueAsyncRedriveFlow current = redriveFlow;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      current = redriveFlow;
      if (current == null) {
        current = new QueueAsyncRedriveFlow(
            orchestratorConfig,
            executionInputPolicy,
            executionStateStore,
            workDispatcher,
            admissionPolicy(),
            this::pipelineId,
            this::releaseVersion);
        redriveFlow = current;
      }
      return current;
    }
  }

  private QueueAsyncSweepFlow sweepFlow() {
    QueueAsyncSweepFlow current = sweepFlow;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      current = sweepFlow;
      if (current == null) {
        current = new QueueAsyncSweepFlow(
            orchestratorConfig,
            executionStateStore,
            workDispatcher,
            new AwaitTimeoutFlow(awaitCoordinator, executionStateStore, this::segmentBoundaryLedger));
        sweepFlow = current;
      }
      return current;
    }
  }

  private AwaitContinuations awaitContinuations() {
    AwaitContinuations current = awaitContinuations;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      current = awaitContinuations;
      if (current == null) {
        current = new AwaitContinuations(
            executionStateStore,
            workDispatcher,
            awaitCoordinator,
            transitionWorkerExecutor,
            this::segmentBoundaryLedger,
            this::recordAwaitLifecycle,
            this::payloadCodec);
        awaitContinuations = current;
      }
      return current;
    }
  }

  private QueueAsyncSegmentPipeline segmentPipeline() {
    return new QueueAsyncSegmentPipeline(
        orchestratorConfig,
        executionStateStore,
        workDispatcher,
        awaitCoordinator,
        transitionWorkerExecutor,
        admissionPolicy(),
            this::payloadCodec,
            this::segmentBoundaryLedger,
            this::saturatedDelay,
            pipelineStepCount,
            new SegmentCommitEffects(
            executionStateStore,
            workDispatcher,
            deadLetterPublisher,
            awaitCoordinator,
            executionFailureHandler,
            this::segmentBoundaryLedger,
            awaitContinuations(),
            new TerminalPublicationBoundary(
                checkpointPublicationService,
                objectPublishCompletionService,
                this::payloadCodec,
                this::segmentBoundaryLedger),
            this::recordAwaitLifecycle),
        queueWorkerId);
  }

  private int loadPipelineStepCount() {
    return PipelineOrderResourceLoader.loadOrder()
        .filter(order -> !order.isEmpty())
        .map(List::size)
        .orElseThrow(() -> new IllegalStateException(
            "Pipeline order metadata is required to resolve a terminal queue segment"));
  }

  /**
   * Caches generated pipeline metadata for this coordinator's active pipeline deployment.
   * A queue coordinator is tied to one generated application artifact; a new deployment
   * constructs a new coordinator and therefore a new cache.
   */
  static final class CachedIntSupplier implements IntSupplier {

    private final IntSupplier resolver;
    private volatile OptionalInt cached = OptionalInt.empty();

    CachedIntSupplier(IntSupplier resolver) {
      this.resolver = Objects.requireNonNull(resolver, "resolver must not be null");
    }

    @Override
    public int getAsInt() {
      OptionalInt current = cached;
      if (current.isPresent()) {
        return current.getAsInt();
      }
      synchronized (this) {
        current = cached;
        if (current.isPresent()) {
          return current.getAsInt();
        }
        int resolved = resolver.getAsInt();
        if (resolved <= 0) {
          throw new IllegalStateException("Generated pipeline step count must be positive");
        }
        cached = OptionalInt.of(resolved);
        return resolved;
      }
    }
  }

  private SegmentBoundaryLedger segmentBoundaryLedger() {
    return segmentBoundaryLedger == null ? new SegmentBoundaryLedger() : segmentBoundaryLedger;
  }

  private void recordAwaitLifecycle(AwaitReplayLifecycleEvent lifecycleEvent) {
    if (telemetry != null) {
      try {
        telemetry.recordAwaitLifecycle(lifecycleEvent);
      } catch (Exception e) {
        LOG.warnf(e, "Failed to record await lifecycle event: %s",
            lifecycleEvent == null ? "<null>" : lifecycleEvent.eventName());
      }
    }
  }

  private ExecutionStateStore selectExecutionStateStore(String providerName) {
    return executionStateStores.stream()
        .filter(store -> providerMatches(store.providerName(), providerName))
        .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "No ExecutionStateStore provider found for '" + providerName + "'"));
  }

  private WorkDispatcher selectWorkDispatcher(String providerName) {
    return workDispatchers.stream()
        .filter(dispatcher -> providerMatches(dispatcher.providerName(), providerName))
        .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "No WorkDispatcher provider found for '" + providerName + "'"));
  }

  private DeadLetterPublisher selectDeadLetterPublisher(String providerName) {
    return deadLetterPublishers.stream()
        .filter(publisher -> providerMatches(publisher.providerName(), providerName))
        .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "No DeadLetterPublisher provider found for '" + providerName + "'"));
  }

  private static boolean providerMatches(String availableName, String configuredName) {
    if (configuredName == null || configuredName.isBlank()) {
      return true;
    }
    return configuredName.equalsIgnoreCase(availableName);
  }

}

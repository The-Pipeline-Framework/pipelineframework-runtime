package org.pipelineframework;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import io.smallrye.mutiny.Multi;
import org.pipelineframework.awaitable.AwaitCompletionAdmission;
import org.pipelineframework.awaitable.AwaitUnitOccurrence;
import org.pipelineframework.awaitable.AwaitCompletionCursor;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.awaitable.AwaitReleaseDisposition;
import org.pipelineframework.awaitable.AwaitInteractionNotFoundException;
import org.pipelineframework.awaitable.AwaitUnitReleaseAcknowledgement;
import org.pipelineframework.orchestrator.AwaitCompletionObservation;
import org.pipelineframework.orchestrator.AwaitCompletionObservationPage;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.NotFoundException;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionCursor;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.orchestrator.AwaitSemanticCheckpoint;
import org.pipelineframework.orchestrator.AwaitSemanticSelection;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;

class ExecutionReadModel {

  private final PipelineOrchestratorConfig orchestratorConfig;
  private final ExecutionInputPolicy executionInputPolicy;
  private final ExecutionStateStore executionStateStore;
  private final AwaitCoordinator awaitCoordinator;
  private final ControlPlaneAdmissionPolicy admissionPolicy;
  private final Supplier<String> pipelineId;
  private final Supplier<String> releaseVersion;
  private final Supplier<TransitionPayloadCodec> payloadCodec;

  ExecutionReadModel(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionInputPolicy executionInputPolicy,
      ExecutionStateStore executionStateStore,
      AwaitCoordinator awaitCoordinator,
      ControlPlaneAdmissionPolicy admissionPolicy,
      Supplier<String> pipelineId,
      Supplier<String> releaseVersion,
      Supplier<TransitionPayloadCodec> payloadCodec) {
    this.orchestratorConfig = Objects.requireNonNull(orchestratorConfig, "orchestratorConfig must not be null");
    this.executionInputPolicy = Objects.requireNonNull(executionInputPolicy, "executionInputPolicy must not be null");
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.awaitCoordinator = Objects.requireNonNull(awaitCoordinator, "awaitCoordinator must not be null");
    this.admissionPolicy = Objects.requireNonNull(admissionPolicy, "admissionPolicy must not be null");
    this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId must not be null");
    this.releaseVersion = Objects.requireNonNull(releaseVersion, "releaseVersion must not be null");
    this.payloadCodec = Objects.requireNonNull(payloadCodec, "payloadCodec must not be null");
  }

  Uni<AwaitCompletionObservation> getAwaitCompletionObservation(String tenantId, String interactionId) {
    return Uni.createFrom().deferred(() -> {
      String tenant = completionReadAdmission(tenantId);
      return awaitCoordinator.getCompletionAdmission(tenant, interactionId)
          .onItem().transformToUni(fact -> fact.map(value -> observeCompletion(value, tenantId))
              .orElseGet(() -> Uni.createFrom().item(new AwaitCompletionObservation(tenant, interactionId,
                  Optional.empty(), Optional.empty(), AwaitCompletionObservation.State.UNKNOWN, OptionalLong.empty()))));
    });
  }

  Uni<AwaitCompletionObservationPage> queryAwaitCompletionObservations(
      String tenantId, int limit, Optional<AwaitCompletionCursor> cursor) {
    return Uni.createFrom().deferred(() -> {
      String tenant = completionReadAdmission(tenantId);
      return awaitCoordinator.findOpenCompletions(tenant, limit, cursor)
          .onItem().transformToUni(page -> Multi.createFrom().iterable(page.admissions())
              .onItem().transformToUniAndConcatenate(fact -> observeCompletion(fact, tenantId))
              .collect().asList().onItem().transform(observations -> new AwaitCompletionObservationPage(
                  observations, page.nextCursor(), page.evaluatedEntryCount())));
    });
  }

  private String completionReadAdmission(String tenantId) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      throw new IllegalStateException("Async queue mode is disabled");
    }
    String tenant = executionInputPolicy.normalizeTenant(tenantId);
    awaitReadAdmission(tenant, ControlPlaneAdmissionOperation.READ_AWAIT_SEMANTIC_CHECKPOINT,
        Optional.empty(), tenantId).ifPresent(failure -> { throw failure; });
    return tenant;
  }

  private Uni<AwaitCompletionObservation> observeCompletion(AwaitCompletionAdmission fact, String requestedTenant) {
    String tenant = executionInputPolicy.normalizeTenant(requestedTenant);
    if (!tenant.equals(fact.tenantId())) return Uni.createFrom().failure(new IllegalStateException("Foreign completion admission"));
    awaitReadAdmission(tenant, ControlPlaneAdmissionOperation.READ_AWAIT_SEMANTIC_CHECKPOINT,
        Optional.of(fact.executionId()), requestedTenant).ifPresent(failure -> { throw failure; });
    if (fact.unitId().isEmpty()) return Uni.createFrom().item(completionObservation(fact, Optional.empty(),
        AwaitCompletionObservation.State.UNKNOWN));
    if (AwaitUnitOccurrence.fromUnitId(fact.unitId().orElseThrow()).isEmpty()) {
      return Uni.createFrom().item(completionObservation(fact, Optional.empty(), AwaitCompletionObservation.State.UNKNOWN));
    }
    return executionStateStore.getExecution(tenant, fact.executionId()).onItem().transformToUni(parent -> {
      if (parent.isEmpty()) return Uni.createFrom().item(completionObservation(fact, Optional.empty(),
          AwaitCompletionObservation.State.UNKNOWN));
      ExecutionRecord<Object, Object> execution = parent.orElseThrow();
      if (!tenant.equals(execution.tenantId()) || !fact.executionId().equals(execution.executionId())) {
        return Uni.createFrom().failure(new IllegalStateException("Foreign completion parent"));
      }
      return awaitCoordinator.getUnit(tenant, fact.unitId().orElseThrow()).map(Optional::of)
          .onFailure(AwaitInteractionNotFoundException.class).recoverWithItem(Optional.empty())
          .onItem().transformToUni(unit -> executionStateStore.getAwaitUnitReleaseAcknowledgement(
              tenant, fact.executionId(), fact.unitId().orElseThrow()).onItem().transformToUni(ack -> {
                if (unit.isEmpty()) return Uni.createFrom().item(completionObservation(fact, ack,
                    AwaitCompletionObservation.State.UNKNOWN));
                AwaitUnitRecord currentUnit = unit.orElseThrow();
                if (!tenant.equals(currentUnit.tenantId()) || !fact.executionId().equals(currentUnit.executionId())
                    || !fact.unitId().orElseThrow().equals(currentUnit.unitId())
                    || !fact.stepId().equals(currentUnit.stepId()) || fact.stepIndex() != currentUnit.stepIndex()) {
                  return Uni.createFrom().failure(new IllegalStateException("Foreign completion unit"));
                }
                ack.ifPresent(value -> {
                  if (!tenant.equals(value.tenantId()) || !fact.executionId().equals(value.executionId())
                      || !fact.unitId().orElseThrow().equals(value.unitId()) || fact.stepIndex() != value.stepIndex()
                      || !execution.pipelineId().equals(value.pipelineId())
                      || !execution.contractVersion().equals(value.contractVersion())
                      || !execution.releaseVersion().equals(value.releaseVersion())) {
                    throw new IllegalStateException("Completion release identity differs from its semantic owners");
                  }
                });
                if (fact.itemIndex().isEmpty() && !fact.interactionId().equals(currentUnit.primaryInteractionId())) {
                  return Uni.createFrom().item(completionObservation(fact, ack, AwaitCompletionObservation.State.UNKNOWN));
                }
                boolean awaitingAgain = execution.status() == org.pipelineframework.orchestrator.ExecutionStatus.WAITING_EXTERNAL
                    && fact.unitId().orElseThrow().equals(execution.awaitUnitId());
                boolean converged = currentUnit.status().terminal() && !awaitingAgain;
                if (currentUnit.status() == AwaitUnitStatus.COMPLETED && fact.itemIndex().isPresent()) {
                  converged &= currentUnit.dispatchComplete() && currentUnit.expectedItemCount() != null
                      && currentUnit.completedItemCount() == currentUnit.expectedItemCount()
                      && currentUnit.completedContinuationItemCount() == currentUnit.expectedItemCount()
                      && currentUnit.hasContinuationCompletionFact(fact.itemIndex().getAsInt())
                      && currentUnit.completedItemKeys().contains("interaction:" + fact.interactionId());
                }
                if (ack.isEmpty() || !converged) return Uni.createFrom().item(completionObservation(fact, ack,
                    AwaitCompletionObservation.State.OUTSTANDING));
                if (ack.orElseThrow().parentVersion() > execution.version()) {
                  return Uni.createFrom().item(completionObservation(fact, ack, AwaitCompletionObservation.State.UNKNOWN));
                }
                AwaitCompletionObservation.State state = ack.orElseThrow().disposition() == AwaitReleaseDisposition.CONTINUATION_QUEUED
                    ? AwaitCompletionObservation.State.RELEASED : AwaitCompletionObservation.State.TERMINAL_FAILED;
                AwaitCompletionObservation observation = completionObservation(fact, ack, state);
                return awaitCoordinator.retireCompletionAdmission(fact).replaceWith(observation);
              }));
    });
  }

  private AwaitCompletionObservation completionObservation(AwaitCompletionAdmission admission,
      Optional<AwaitUnitReleaseAcknowledgement> ack, AwaitCompletionObservation.State state) {
    OptionalLong latency = OptionalLong.empty();
    if (state == AwaitCompletionObservation.State.RELEASED && ack.isPresent()
        && ack.orElseThrow().acknowledgedAtEpochMs() >= admission.firstAdmittedAtEpochMs()) {
      latency = OptionalLong.of(ack.orElseThrow().acknowledgedAtEpochMs() - admission.firstAdmittedAtEpochMs());
    }
    return new AwaitCompletionObservation(admission.tenantId(), admission.interactionId(), Optional.of(admission), ack, state, latency);
  }

  Uni<ExecutionStatusDto> getExecutionStatus(String tenantId, String executionId) {
    return readExecution(tenantId, executionId, ControlPlaneAdmissionOperation.GET_EXECUTION_STATUS)
        .onItem().transform(record -> new ExecutionResultView(record).statusDto());
  }

  @SuppressWarnings("unchecked")
  <T> Uni<T> getExecutionResult(String tenantId, String executionId, Class<?> outputType, boolean outputStreaming) {
    return readExecution(tenantId, executionId, ControlPlaneAdmissionOperation.GET_EXECUTION_RESULT)
        .onItem().transform(record ->
            (T) new ExecutionResultView(record).typedResult(outputType, outputStreaming, payloadCodec));
  }

  Uni<org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint> getExecutionSemanticCheckpoint(
      String tenantId, String executionId) {
    String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
    return readExecution(tenantId, executionId, ControlPlaneAdmissionOperation.GET_EXECUTION_STATUS)
        .onItem().transform(record -> {
          if (!resolvedTenant.equals(record.tenantId()) || !executionId.equals(record.executionId())) {
            throw new IllegalStateException("Execution point read returned a foreign tenant or execution");
          }
          return new org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint(record.tenantId(), record.executionId(),
              record.pipelineId(), record.contractVersion(), record.releaseVersion(), record.status());
        });
  }

  Uni<Object> getExecutionResultPayload(String tenantId, String executionId) {
    return readExecution(tenantId, executionId, ControlPlaneAdmissionOperation.GET_EXECUTION_RESULT)
        .onItem().transform(record -> new ExecutionResultView(record).rawPayload());
  }

  /** An absent admission remains absent; this lookup never creates or redrives execution state. */
  Uni<Optional<org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint>> getExecutionSemanticCheckpointByClientKey(
      String tenantId, String clientKey) {
    return Uni.createFrom().deferred(() -> {
      if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
        return Uni.createFrom().failure(new IllegalStateException("Async queue mode is disabled"));
      }
      String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
      String resolvedKey = executionInputPolicy.requireRecoverableClientKey(clientKey);
      // There is no execution identifier before the lookup. Policies may reject this tenant-scoped read.
      Optional<RuntimeException> denied = admissionFailure(new ControlPlaneAdmissionRequest(resolvedTenant,
          ControlPlaneAdmissionOperation.GET_EXECUTION_STATUS, pipelineId.get(), releaseVersion.get(),
          "", "api", tenantId != null && !tenantId.isBlank()));
      if (denied.isPresent()) return Uni.createFrom().failure(denied.get());
      return executionStateStore.getExecutionByKey(resolvedTenant, resolvedKey).onItem().transform(record ->
          record.map(value -> {
            if (!resolvedTenant.equals(value.tenantId()) || !resolvedKey.equals(value.executionKey())) {
              throw new IllegalStateException("Execution key lookup returned a foreign tenant or client key");
            }
            Optional<RuntimeException> executionDenied = admissionFailure(new ControlPlaneAdmissionRequest(resolvedTenant,
                ControlPlaneAdmissionOperation.GET_EXECUTION_STATUS, pipelineId.get(), releaseVersion.get(),
                value.executionId(), "api", tenantId != null && !tenantId.isBlank()));
            if (executionDenied.isPresent()) throw executionDenied.get();
            return new org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint(value.tenantId(), value.executionId(),
                value.pipelineId(), value.contractVersion(), value.releaseVersion(), value.status());
          }));
    });
  }

  Uni<Optional<AwaitSemanticCheckpoint>> getAwaitSemanticCheckpoint(
      String tenantId,
      String interactionId) {
    return Uni.createFrom().deferred(() -> {
      if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
        return Uni.createFrom().failure(new IllegalStateException("Async queue mode is disabled"));
      }
      String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
      Optional<RuntimeException> denied = awaitReadAdmission(
          resolvedTenant,
          ControlPlaneAdmissionOperation.READ_AWAIT_SEMANTIC_CHECKPOINT,
          Optional.empty(),
          tenantId);
      if (denied.isPresent()) {
        return Uni.createFrom().failure(denied.orElseThrow());
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

  Uni<Optional<AwaitSemanticCheckpoint>> getCurrentAwaitSemanticCheckpoint(
      String tenantId, String executionId) {
    return getCurrentAwaitSemanticCheckpoint(tenantId, executionId, Optional.empty())
        .onItem().transform(AwaitSemanticSelection::checkpoint);
  }

  Uni<AwaitSemanticSelection> getCurrentAwaitSemanticCheckpoint(
      String tenantId, String executionId,
      Optional<AwaitInteractionCursor> cursor) {
    return Uni.createFrom().deferred(() -> {
      if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
        return Uni.createFrom().failure(new IllegalStateException("Async queue mode is disabled"));
      }
      if (executionId == null || executionId.isBlank()) {
        return Uni.createFrom().failure(new IllegalArgumentException("executionId must not be blank"));
      }
      java.util.Objects.requireNonNull(cursor, "cursor");
      String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
      Optional<RuntimeException> denied = awaitReadAdmission(resolvedTenant,
          ControlPlaneAdmissionOperation.READ_AWAIT_SEMANTIC_CHECKPOINT, Optional.of(executionId), tenantId);
      if (denied.isPresent()) {
        return Uni.createFrom().failure(denied.orElseThrow());
      }
      if (cursor.filter(value -> !resolvedTenant.equals(value.tenantId())).isPresent()) {
        return Uni.createFrom().failure(new IllegalArgumentException("Await cursor belongs to another tenant"));
      }
      return executionStateStore.getExecution(resolvedTenant, executionId)
          .onItem().transformToUni(parent -> parent
              .filter(record -> record.status() == org.pipelineframework.orchestrator.ExecutionStatus.WAITING_EXTERNAL)
              .filter(record -> record.awaitUnitId() != null && !record.awaitUnitId().isBlank())
              .map(record -> awaitCoordinator.getUnit(resolvedTenant, record.awaitUnitId())
                  .onItem().transformToUni(unit -> {
                    if (!record.executionId().equals(unit.executionId()) || !resolvedTenant.equals(unit.tenantId())) {
                      return Uni.createFrom().failure(new IllegalStateException("Await unit does not belong to execution"));
                    }
                    if (cursor.isPresent()) {
                      var previous = cursor.orElseThrow();
                      if (!unit.unitId().equals(previous.unitId())) {
                        // Only the semantic owner can distinguish prior progress from an unrelated unit cursor.
                        return awaitCoordinator.getUnit(resolvedTenant, previous.unitId()).onItem().transform(oldUnit -> {
                          if (!resolvedTenant.equals(oldUnit.tenantId()) || !record.executionId().equals(oldUnit.executionId())) {
                            throw new IllegalArgumentException("Await cursor unit does not belong to execution");
                          }
                          return new AwaitSemanticSelection(Optional.empty(), Optional.empty(), true);
                        });
                      }
                    }
                    var primary = Optional.ofNullable(unit.primaryInteractionId()).filter(id -> !id.isBlank());
                    if (primary.isPresent()) {
                      return getAwaitSemanticCheckpoint(resolvedTenant, primary.orElseThrow())
                          .onItem().transform(checkpoint -> new AwaitSemanticSelection(
                              checkpoint.map(value -> {
                                if (!record.executionId().equals(value.executionId()) || !unit.unitId().equals(value.unitId())) {
                                  throw new IllegalStateException("primary Await interaction does not belong to unit");
                                }
                                return value;
                              }), Optional.empty()));
                    }
                    return awaitCoordinator.findByUnitPage(resolvedTenant, unit.unitId(), 100, cursor)
                        .onItem().transformToUni(page -> selectUnresolvedAwait(record, resolvedTenant, unit.unitId(),
                            page.interactions().stream().sorted(java.util.Comparator
                                .comparingInt((AwaitInteractionRecord entry) -> Optional.ofNullable(entry.itemIndex()).orElse(Integer.MAX_VALUE))
                                .thenComparing(entry -> Optional.ofNullable(entry.causationId()).orElse(""))
                                .thenComparing(AwaitInteractionRecord::interactionId)).toList(), 0).onItem().transform(checkpoint ->
                                new AwaitSemanticSelection(checkpoint, page.nextCursor())));
                  }))
              .orElseGet(() -> Uni.createFrom().item(new AwaitSemanticSelection(
                  Optional.empty(), Optional.empty()))));
    });
  }

  private Uni<Optional<AwaitSemanticCheckpoint>> selectUnresolvedAwait(
      org.pipelineframework.orchestrator.ExecutionRecord<?, ?> parent, String tenantId, String unitId,
      List<AwaitInteractionRecord> candidates, int index) {
    if (index >= candidates.size()) {
      return Uni.createFrom().item(Optional.empty());
    }
    AwaitInteractionRecord candidate = candidates.get(index);
    if (!tenantId.equals(candidate.tenantId()) || !unitId.equals(candidate.unitId())
        || !parent.executionId().equals(candidate.executionId())) {
      return Uni.createFrom().failure(new IllegalStateException("Await page interaction does not belong to execution unit"));
    }
    if (candidate.status().terminal()) {
      return selectUnresolvedAwait(parent, tenantId, unitId, candidates, index + 1);
    }
    // A GSI page is only a locator. Current status comes from the store's authoritative point read.
    return awaitCoordinator.getInteraction(tenantId, candidate.interactionId()).onItem().transformToUni(current -> {
      if (current.isEmpty()) {
        return selectUnresolvedAwait(parent, tenantId, unitId, candidates, index + 1);
      }
      var record = current.orElseThrow();
      if (!tenantId.equals(record.tenantId()) || !unitId.equals(record.unitId())
          || !parent.executionId().equals(record.executionId())) {
        return Uni.createFrom().failure(new IllegalStateException("Await interaction does not belong to execution unit"));
      }
      if (record.status().terminal()) {
        return selectUnresolvedAwait(parent, tenantId, unitId, candidates, index + 1);
      }
      return Uni.createFrom().item(Optional.of(new AwaitSemanticCheckpoint(record.tenantId(), record.executionId(),
          record.interactionId(), record.correlationId(), record.unitId(), record.stepId(), record.status(),
          parent.pipelineId(), parent.contractVersion(), parent.releaseVersion())));
    });
  }

  Uni<List<AwaitSemanticCheckpoint>> getAwaitSemanticCheckpoints(
      String tenantId,
      String executionId,
      int limit) {
    return Uni.createFrom().deferred(() -> {
      if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
        return Uni.createFrom().failure(new IllegalStateException("Async queue mode is disabled"));
      }
      if (executionId == null || executionId.isBlank()) {
        return Uni.createFrom().failure(new IllegalArgumentException("executionId must not be blank"));
      }
      String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
      Optional<RuntimeException> denied = awaitReadAdmission(
          resolvedTenant,
          ControlPlaneAdmissionOperation.READ_AWAIT_SEMANTIC_CHECKPOINT,
          Optional.of(executionId),
          tenantId);
      if (denied.isPresent()) {
        return Uni.createFrom().failure(denied.orElseThrow());
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

  private Optional<RuntimeException> awaitReadAdmission(
      String resolvedTenant, ControlPlaneAdmissionOperation operation,
      Optional<String> executionId, String suppliedTenant) {
    return admissionFailure(new ControlPlaneAdmissionRequest(resolvedTenant, operation,
        pipelineId.get(), releaseVersion.get(), executionId.orElse(""), "api",
        suppliedTenant != null && !suppliedTenant.isBlank()));
  }

  private Uni<ExecutionRecord<Object, Object>> readExecution(
      String tenantId,
      String executionId,
      ControlPlaneAdmissionOperation operation) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
    Optional<RuntimeException> admissionFailure = admissionFailure(new ControlPlaneAdmissionRequest(
        resolvedTenant,
        operation,
        pipelineId.get(),
        releaseVersion.get(),
        executionId,
        "api",
        tenantId != null && !tenantId.isBlank()));
    if (admissionFailure.isPresent()) {
      return Uni.createFrom().failure(admissionFailure.get());
    }
    return executionStateStore.getExecution(resolvedTenant, executionId)
        .onItem().transform(optional -> requireExecution(optional, executionId));
  }

  private Optional<RuntimeException> admissionFailure(ControlPlaneAdmissionRequest request) {
    ControlPlaneAdmissionDecision decision = admissionPolicy.admit(request);
    return decision.allowed()
        ? Optional.empty()
        : Optional.of(new ControlPlaneAdmissionException(decision));
  }

  private ExecutionRecord<Object, Object> requireExecution(
      Optional<ExecutionRecord<Object, Object>> record,
      String executionId) {
    return record.orElseThrow(() -> new NotFoundException("Execution not found: " + executionId));
  }
}

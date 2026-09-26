package org.pipelineframework;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionRedriveIntent;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionWorkerCommand;

/**
 * Immutable view of one queue-async segment claimed by a worker.
 */
record ClaimedSegment(
    ExecutionRecord<Object, Object> record,
    String transitionKey) {

  ClaimedSegment {
    Objects.requireNonNull(record, "record must not be null");
    if (transitionKey == null || transitionKey.isBlank()) {
      throw new IllegalArgumentException("transitionKey must not be blank");
    }
  }

  static ClaimedSegment from(ExecutionRecord<Object, Object> record) {
    Objects.requireNonNull(record, "record must not be null");
    return new ClaimedSegment(
        record,
        transitionKey(record));
  }

  boolean resumesFromAwait() {
    return record.currentStepIndex() > 0
        && record.awaitUnitId() != null
        && !record.awaitUnitId().isBlank();
  }

  TransitionCommandEnvelope transitionCommand(Object payload, TransitionPayloadCodec payloadCodec) {
    Objects.requireNonNull(payloadCodec, "payloadCodec must not be null");
    // A resumed segment after the failed root must not receive that root's already-consumed retry authority.
    boolean retryCompleted = record.redriveIntent() == ExecutionRedriveIntent.RETRY_FAILED_COMMAND
        && record.failedStepIndex() < record.currentStepIndex();
    TransitionWorkerCommand command = new TransitionWorkerCommand(
        record.tenantId(),
        record.executionId(),
        record.currentStepIndex(),
        -1,
        record.attempt(),
        record.resultShape(),
        record.version(),
        transitionKey,
        payload,
        retryCompleted ? ExecutionRedriveIntent.REPLAY : record.redriveIntent(),
        retryCompleted ? -1 : record.failedStepIndex(),
        retryCompleted ? Optional.empty() : record.redriveIntent() == ExecutionRedriveIntent.REISSUE_COMMAND
            ? record.redriveTargetCommandId()
            : record.failedCommandId(),
        retryCompleted ? Optional.empty() : record.redriveReason(),
        record.pagingState().map(org.pipelineframework.orchestrator.PagedExecutionState::toTransitionContext));
    SerializedTransitionPayload encodedPayload = payloadCodec.encode(payload);
    return TransitionCommandEnvelope.from(
        command,
        record.pipelineId(),
        record.contractVersion(),
        record.releaseVersion(),
        transitionKey,
        encodedPayload);
  }

  private static String transitionKey(ExecutionRecord<Object, Object> record) {
    String page = record.pagingState()
        .map(state -> ":page:" + state.pageIndex())
        .orElse("");
    return record.executionId() + page + ":" + record.currentStepIndex() + ":" + record.attempt();
  }
}

package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.AwaitItemContinuationHandler;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/**
 * Internal facade for queue-async orchestration ownership.
 */
public interface PipelineControlPlane {

    /**
     * Initializes the local queue-async control plane if queue mode is enabled.
     */
    void initializeQueueMode();

    Uni<RunAsyncAcceptedDto> executePipelineAsync(
        Object input,
        String tenantId,
        String idempotencyKey,
        boolean outputStreaming);

    Uni<RunAsyncAcceptedDto> executePipelineAsync(
        Object input,
        String tenantId,
        String idempotencyKey,
        boolean outputStreaming,
        String pipelineId,
        String contractVersion,
        String releaseVersion);

    Uni<ExecutionStatusDto> getExecutionStatus(String tenantId, String executionId);

    <T> Uni<T> getExecutionResult(String tenantId, String executionId, Class<?> outputType, boolean outputStreaming);

    Uni<Object> getExecutionResultPayload(String tenantId, String executionId);

    /**
     * Executes one bounded timeout and due-execution sweep using the configured sweep limit.
     *
     * @param nowEpochMs caller-supplied current time in epoch milliseconds
     * @return the successful timeout and dispatch counts for this sweep
     */
    Uni<CoordinatorSweepResult> sweepOnce(long nowEpochMs);

    Uni<ExecutionRedriveResult> redriveExecution(
        String tenantId,
        String executionId,
        Long expectedVersion,
        boolean allowFailed,
        String reason);

    default Uni<ExecutionRedriveResult> redriveExecution(
        String tenantId,
        String executionId,
        Long expectedVersion,
        boolean allowFailed,
        ExecutionRedriveIntent intent,
        String reason) {
        if (intent == null || intent == ExecutionRedriveIntent.REPLAY) {
            return redriveExecution(tenantId, executionId, expectedVersion, allowFailed, reason);
        }
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Pipeline control plane does not support deliberate Command retry redrive"));
    }

    default Uni<ExecutionRedriveResult> redriveExecution(
        String tenantId,
        String executionId,
        Long expectedVersion,
        boolean allowFailed,
        ExecutionRedriveIntent intent,
        String targetCommandId,
        String reason) {
        if (targetCommandId == null || targetCommandId.isBlank()) {
            return redriveExecution(
                tenantId, executionId, expectedVersion, allowFailed, intent, reason);
        }
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Pipeline control plane does not support targeted Command reissue"));
    }

    Uni<AwaitCompletionResult> completeAwait(AwaitCompletionCommand command);

    Uni<AwaitCompletionResult> completeAwait(
        AwaitCompletionCommand command,
        AwaitItemContinuationHandler itemContinuationHandler);

    Uni<List<AwaitInteractionRecord>> queryPendingAwaitInteractions(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit);

    /** Reads one provider-neutral Await checkpoint without exposing the backing store schema. */
    Uni<Optional<AwaitSemanticCheckpoint>> getAwaitSemanticCheckpoint(
        String tenantId,
        String interactionId);

    /** Reads bounded provider-neutral Await checkpoints for one TPF execution. */
    Uni<List<AwaitSemanticCheckpoint>> getAwaitSemanticCheckpoints(
        String tenantId,
        String executionId,
        int limit);

    /**
     * Processes one bounded itemized-Await continuation attempt.
     *
     * <p>The caller owns retry timing and repeated invocation.</p>
     */
    Uni<AwaitItemContinuationResult> processAwaitItemContinuation(
        AwaitItemContinuationCommand command);

    Uni<Void> processExecutionWorkItem(ExecutionWorkItem workItem, PipelineTransitionWorker worker);

    Uni<Void> processExecutionWorkItem(
        ExecutionWorkItem workItem,
        PipelineTransitionWorker worker,
        AwaitItemContinuationHandler itemContinuationHandler);
}

package org.pipelineframework.awaitable.store;

import java.util.List;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.AwaitItemContinuationCommand;

/**
 * Mechanical durable projection used to rediscover admitted itemized-Await continuations.
 * Await interactions and units remain the semantic authority.
 */
public interface AwaitItemContinuationWorkStore {
    Uni<List<AwaitItemContinuationCommand>> findDueItemContinuations(long nowEpochMs, int limit);

    Uni<Void> rescheduleItemContinuation(
        AwaitItemContinuationCommand command,
        int nextAttempt,
        long dueEpochMs);

    Uni<Void> completeItemContinuation(AwaitItemContinuationCommand command);
}

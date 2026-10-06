package org.pipelineframework.awaitable.store;

import java.util.List;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitInteractionRecord;

/** Provider-neutral lookup used to reconstruct Await mechanics from a TPF execution identity. */
public interface AwaitInteractionExecutionLookup {
    Uni<List<AwaitInteractionRecord>> findByExecution(String tenantId, String executionId, int limit);
}

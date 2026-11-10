package org.pipelineframework.orchestrator;

import io.smallrye.mutiny.Uni;

/** Host callback for one persisted due item continuation; the selected runtime owns its processing. */
@FunctionalInterface
public interface AwaitItemContinuationDispatcher {
    Uni<AwaitItemContinuationResult> dispatch(AwaitItemContinuationCommand command);
}

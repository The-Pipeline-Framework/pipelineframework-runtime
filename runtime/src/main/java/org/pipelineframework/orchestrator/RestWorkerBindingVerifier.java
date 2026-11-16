package org.pipelineframework.orchestrator;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.release.PipelineReleaseEntryBinding;

/** Verifies deployment-owned routing evidence for an already matched REST binding. */
public interface RestWorkerBindingVerifier {
    Uni<Void> verify(PipelineReleaseEntryBinding binding);
}

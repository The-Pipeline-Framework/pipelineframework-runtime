package org.pipelineframework.orchestrator;

import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig.RegisteredRestTargetConfig;

/** Optional source of immutable, server-owned snapshots for independently published REST workers. */
public interface RegisteredRestWorkerTargetSource {
    boolean enabled();

    Uni<Map<String, RegisteredRestTargetConfig>> snapshot();

    /** Prepare the pinned credential before handing off to the existing synchronous secret resolver. */
    Uni<Void> prepareCredential(String reference);

    /** Resolve a prepared reference owned by this source; other schemes remain with the local resolver. */
    default Optional<String> preparedCredential(String reference) {
        return Optional.empty();
    }
}

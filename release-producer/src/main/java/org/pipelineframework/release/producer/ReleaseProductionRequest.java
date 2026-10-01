package org.pipelineframework.release.producer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** All build-tool-neutral inputs required to produce one immutable Release Descriptor. */
public record ReleaseProductionRequest(
    PipelineContractDescriptor contract,
    String releaseVersion,
    String compiledTruthArtifactId,
    Path compiledTruthDirectory,
    List<ReleaseArtifactInput> artifacts,
    boolean allowLocalUris
) {
    public ReleaseProductionRequest {
        artifacts = artifacts == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(artifacts));
    }
}

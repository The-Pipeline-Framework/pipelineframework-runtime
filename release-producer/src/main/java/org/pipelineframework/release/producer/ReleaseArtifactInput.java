package org.pipelineframework.release.producer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public record ReleaseArtifactInput(
    String artifactId,
    String kind,
    Path file,
    String uri,
    List<String> stepIds,
    List<String> capabilities
) {
    public ReleaseArtifactInput {
        stepIds = stepIds == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(stepIds));
        capabilities = capabilities == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(capabilities));
    }
}

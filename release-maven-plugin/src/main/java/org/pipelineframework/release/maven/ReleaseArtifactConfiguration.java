package org.pipelineframework.release.maven;

import java.io.File;
import java.util.List;
import org.apache.maven.plugins.annotations.Parameter;

/** Maven configuration for one build-produced release artifact. */
public final class ReleaseArtifactConfiguration {
    @Parameter(required = true)
    private String artifactId;

    @Parameter(required = true)
    private String kind;

    @Parameter(required = true)
    private File file;

    @Parameter(required = true)
    private String uri;

    @Parameter
    private List<String> stepIds;

    @Parameter
    private List<String> capabilities;

    String artifactId() {
        return artifactId;
    }

    String kind() {
        return kind;
    }

    File file() {
        return file;
    }

    String uri() {
        return uri;
    }

    List<String> stepIds() {
        return stepIds;
    }

    List<String> capabilities() {
        return capabilities;
    }
}

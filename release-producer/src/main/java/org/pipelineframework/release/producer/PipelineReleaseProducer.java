package org.pipelineframework.release.producer;

import java.nio.file.Path;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;

/** Build-tool-neutral production of an immutable Pipeline Release Descriptor. */
public interface PipelineReleaseProducer {
    PipelineReleaseDescriptor produce(ReleaseProductionRequest request);

    void write(Path destination, PipelineReleaseDescriptor descriptor);
}

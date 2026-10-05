package org.pipelineframework.aws.durable;

import java.util.Optional;

import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;

/** Reads TPF semantic state through a bounded action, never through provider table knowledge. */
@FunctionalInterface
public interface AwsDurableAwaitCheckpointReader {
    Optional<AwsDurableAwaitCheckpoint> read(String tenantId, String interactionId);
}

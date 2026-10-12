package org.pipelineframework.aws.durable;

/** A successor has permanently fenced this provider generation. */
public final class AwsDurableStaleGenerationException extends IllegalStateException {
    public AwsDurableStaleGenerationException() {
        super("provider generation is no longer current");
    }
}

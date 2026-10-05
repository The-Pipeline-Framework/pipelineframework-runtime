package org.pipelineframework.aws.durable;

import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;

/** Starts a replacement provider generation from the TPF semantic checkpoint. */
@FunctionalInterface
public interface AwsDurableReplacementStarter {
    boolean startReplacement(AwsDurableCallbackBinding obsoleteBinding);
}

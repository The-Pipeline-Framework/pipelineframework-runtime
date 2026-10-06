package org.pipelineframework.aws.durable;

import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;

@FunctionalInterface
public interface AwsDurableActionInvoker {
    AwsDurableActionResponse invoke(AwsDurableActionRequest request);
}

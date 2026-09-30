package org.pipelineframework.awsproof;

import org.pipelineframework.awsproof.model.ProofActionRequest;
import org.pipelineframework.awsproof.model.ProofActionResponse;

@FunctionalInterface
interface ProofActionInvoker {
    ProofActionResponse invoke(ProofActionRequest request);
}

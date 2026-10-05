package org.pipelineframework.awsproof;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.awaitable.AwaitCompletionDescriptor;

/** Reconstructs the generated-shape descriptor needed by independently invoked proof Lambdas. */
@ApplicationScoped
public final class ProofAwaitDescriptorFactory {
    private static final Duration TIMEOUT = Duration.ofMinutes(5);

    public AwaitCompletionDescriptor create() {
        Map<String, Object> transport = Map.of(
            "request", Map.of("queueUrl", requiredEnvironment("TPF_PROOF_AWAIT_REQUEST_QUEUE_URL")),
            "response", Map.of("queueUrl", requiredEnvironment("TPF_PROOF_AWAIT_RESPONSE_QUEUE_URL")));
        return new AwaitCompletionDescriptor(
            "ProofAwaitApproval",
            ProofPipelineInput.class.getName(),
            String.class.getName(),
            "ONE_TO_ONE",
            TIMEOUT,
            "interactionId",
            "sqs",
            transport,
            List.of(),
            ProofPipelineInput.class.getName(),
            String.class.getName());
    }

    private String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required");
        }
        return value;
    }
}

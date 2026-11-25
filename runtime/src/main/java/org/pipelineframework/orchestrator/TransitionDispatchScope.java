package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Immutable coordinator-attempt scope active while executing a transition on the standard path.
 * Carries the exact attempt and version coordinates needed to fence pre-send foreign dispatch
 * intents without widening business-function inputs.
 */
public record TransitionDispatchScope(
    String tenantId,
    String executionId,
    long executionVersion,
    int attempt,
    String transitionKey
) {
    public TransitionDispatchScope {
        tenantId = requireNonBlank(tenantId, "tenantId");
        executionId = requireNonBlank(executionId, "executionId");
        if (executionVersion < 0L) {
            throw new IllegalArgumentException("executionVersion must be non-negative");
        }
        if (attempt < 0) {
            throw new IllegalArgumentException("attempt must be non-negative");
        }
        transitionKey = requireNonBlank(transitionKey, "transitionKey");
    }

    private static String requireNonBlank(String value, String name) {
        if (Objects.requireNonNull(value, name + " must not be null").isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

package org.pipelineframework.orchestrator;

/**
 * Identifies one bounded attempt to continue a completed itemized Await interaction.
 */
public record AwaitItemContinuationCommand(
    String tenantId,
    String executionId,
    String unitId,
    String interactionId,
    int itemIndex,
    int attempt,
    long nowEpochMs) {

    public AwaitItemContinuationCommand {
        tenantId = requireText(tenantId, "tenantId");
        executionId = requireText(executionId, "executionId");
        unitId = requireText(unitId, "unitId");
        interactionId = requireText(interactionId, "interactionId");
        if (itemIndex < 0) {
            throw new IllegalArgumentException("itemIndex must be non-negative");
        }
        if (attempt <= 0) {
            throw new IllegalArgumentException("attempt must be positive");
        }
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("nowEpochMs must be non-negative");
        }
    }

    public AwaitItemContinuationCommand nextAttempt(long nextNowEpochMs) {
        return new AwaitItemContinuationCommand(
            tenantId,
            executionId,
            unitId,
            interactionId,
            itemIndex,
            attempt + 1,
            nextNowEpochMs);
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

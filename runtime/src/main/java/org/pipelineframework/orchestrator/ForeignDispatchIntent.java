package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Immutable pre-send dispatch intent for a single foreign function invocation within a
 * coordinator attempt. Persisted atomically before any bytes are sent over the wire so a crash
 * or expired-lease redelivery of the same attempt fails closed with
 * {@link RemoteTransitionOutcomeUnknownException} instead of silently repeating the call.
 */
public record ForeignDispatchIntent(
    String tenantId,
    String executionId,
    long executionVersion,
    int attempt,
    String transitionKey,
    int stepIndex,
    String stepId,
    String targetUri,
    String requestDigest,
    long createdAtEpochMs
) {
    public ForeignDispatchIntent {
        tenantId = requireNonBlank(tenantId, "tenantId");
        executionId = requireNonBlank(executionId, "executionId");
        if (executionVersion < 0L) {
            throw new IllegalArgumentException("executionVersion must be non-negative");
        }
        if (attempt < 0) {
            throw new IllegalArgumentException("attempt must be non-negative");
        }
        transitionKey = requireNonBlank(transitionKey, "transitionKey");
        if (stepIndex < 0) {
            throw new IllegalArgumentException("stepIndex must be non-negative");
        }
        stepId = requireNonBlank(stepId, "stepId");
        targetUri = requireNonBlank(targetUri, "targetUri");
        requestDigest = requireNonBlank(requestDigest, "requestDigest");
    }

    /**
     * Deterministic partition key scoped to one coordinator attempt and step. Intentionally
     * omits {@code executionVersion} so a reclaimed expired lease for the same unresolved
     * {@code transitionKey} collides with the prior reservation and fails closed.
     */
    public String storageKey() {
        String raw = tenantId + "\n" + executionId + "\n" + transitionKey + "\n" + stepIndex + "\n" + stepId;
        return "foreign-dispatch:v1:" + sha256Hex(raw.getBytes(StandardCharsets.UTF_8));
    }

    public static String sha256Hex(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String requireNonBlank(String value, String name) {
        if (Objects.requireNonNull(value, name + " must not be null").isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

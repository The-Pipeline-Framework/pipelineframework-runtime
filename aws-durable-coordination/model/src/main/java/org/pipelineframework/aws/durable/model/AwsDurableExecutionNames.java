package org.pipelineframework.aws.durable.model;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class AwsDurableExecutionNames {
    private AwsDurableExecutionNames() {
    }

    public static String durableExecutionName(String tenantId, String idempotencyKey, long generation) {
        String material = AwsDurableValidation.required(tenantId, "tenantId") + '\u0000'
            + AwsDurableValidation.required(idempotencyKey, "idempotencyKey") + '\u0000' + generation;
        return hashName(material);
    }

    /** Hosted starts use the same Release scope as semantic admission. Legacy starts keep their names. */
    public static String durableExecutionName(AwsDurableExecutionInput input) {
        if (input.submissionAttemptId().isEmpty()) {
            return durableExecutionName(input.tenantId(), input.idempotencyKey(), input.generation());
        }
        String material = "hosted" + component(input.tenantId()) + component(input.pipelineId())
            + component(input.releaseVersion()) + component(input.idempotencyKey()) + input.generation();
        return hashName(material);
    }

    private static String component(String value) {
        return value.length() + ":" + value;
    }

    private static String hashName(String material) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(material.getBytes(StandardCharsets.UTF_8));
            return "tpf-" + HexFormat.of().formatHex(digest, 0, 24);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", exception);
        }
    }
}

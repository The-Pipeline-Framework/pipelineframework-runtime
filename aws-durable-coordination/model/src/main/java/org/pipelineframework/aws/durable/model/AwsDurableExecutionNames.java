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
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(material.getBytes(StandardCharsets.UTF_8));
            return "tpf-" + HexFormat.of().formatHex(digest, 0, 24);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", exception);
        }
    }
}

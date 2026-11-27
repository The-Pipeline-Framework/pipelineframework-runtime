/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.runtime.spring.polyglot;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Immutable pre-send dispatch intent persisted before invoking a foreign function host
 * from a Spring-hosted runtime.
 */
public record SpringForeignDispatchIntent(
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
    public SpringForeignDispatchIntent {
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
     * omits {@code executionVersion} so a reclaimed expired lease collides and fails closed.
     */
    public String storageKey() {
        String raw = lengthPrefixed(tenantId)
            + lengthPrefixed(executionId)
            + lengthPrefixed(transitionKey)
            + lengthPrefixed(Integer.toString(stepIndex))
            + lengthPrefixed(stepId);
        return "foreign-dispatch:v1:" + sha256Hex(raw.getBytes(StandardCharsets.UTF_8));
    }

    private static String lengthPrefixed(String value) {
        return value.length() + ":" + value + ";";
    }

    public static String sha256Hex(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Class-free canonical durable payload carried across Spring-hosted polyglot step,
 * Await, and checkpoint boundaries.
 */
public record SpringCanonicalPayload(
    String canonicalTypeId,
    String typeExpressionFingerprint,
    String catalogFingerprint,
    String encoding,
    int encodingVersion,
    byte[] payload
) {
    public static final String CANONICAL_JSON_ENCODING = "application/tpf-canonical-json";
    public static final int CANONICAL_JSON_VERSION = 1;

    public SpringCanonicalPayload {
        canonicalTypeId = requireNonBlank(canonicalTypeId, "canonicalTypeId");
        typeExpressionFingerprint = requireNonBlank(typeExpressionFingerprint, "typeExpressionFingerprint");
        catalogFingerprint = requireNonBlank(catalogFingerprint, "catalogFingerprint");
        encoding = requireNonBlank(encoding, "encoding");
        if (encodingVersion <= 0) {
            throw new IllegalArgumentException("encodingVersion must be positive");
        }
        payload = Objects.requireNonNull(payload, "payload").clone();
    }

    public static SpringCanonicalPayload ofJson(
        String canonicalTypeId,
        String typeExpressionFingerprint,
        String catalogFingerprint,
        String canonicalJson
    ) {
        Objects.requireNonNull(canonicalJson, "canonicalJson");
        return new SpringCanonicalPayload(
            canonicalTypeId,
            typeExpressionFingerprint,
            catalogFingerprint,
            CANONICAL_JSON_ENCODING,
            CANONICAL_JSON_VERSION,
            canonicalJson.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    public String payloadJson() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    public Map<String, Object> toDurableMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("canonicalTypeId", canonicalTypeId);
        map.put("typeExpressionFingerprint", typeExpressionFingerprint);
        map.put("catalogFingerprint", catalogFingerprint);
        map.put("encoding", encoding);
        map.put("encodingVersion", encodingVersion);
        map.put("payload", Base64.getEncoder().encodeToString(payload));
        return Map.copyOf(map);
    }

    public static Optional<SpringCanonicalPayload> fromDurableMap(Object value) {
        if (value instanceof SpringCanonicalPayload existing) {
            return Optional.of(existing);
        }
        if (!(value instanceof Map<?, ?> map)) {
            return Optional.empty();
        }
        Object versionObj = map.get("encodingVersion");
        if (!(versionObj instanceof Number number)) {
            return Optional.empty();
        }
        try {
            int version = new BigDecimal(number.toString()).intValueExact();
            return Optional.of(new SpringCanonicalPayload(
                stringField(map, "canonicalTypeId"),
                stringField(map, "typeExpressionFingerprint"),
                stringField(map, "catalogFingerprint"),
                stringField(map, "encoding"),
                version,
                Base64.getDecoder().decode(stringField(map, "payload"))));
        } catch (IllegalArgumentException | ArithmeticException invalid) {
            return Optional.empty();
        }
    }

    private static String stringField(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (!(value instanceof String str) || str.isBlank()) {
            throw new IllegalArgumentException("Field '" + key + "' must be a non-blank string");
        }
        return str;
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

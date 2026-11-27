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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.core.type.TypeReference;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionPayloadEncoding;
import org.pipelineframework.runtime.core.RuntimeAdapters;
import org.pipelineframework.runtime.core.TpfExecutionContext;

/**
 * Interoperable checkpoint handoff codec for cross-pipeline ownership transfer between
 * Spring-hosted (and Quarkus-hosted) pipelines using {@link SerializedTransitionPayload}.
 */
public final class SpringCheckpointHandoffCodec {

    public static final String PAYLOAD_TYPE_PREFIX = "tpf-checkpoint-handoff:v1:";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    /** Ownership metadata attached to a cross-pipeline checkpoint publication. */
    public record CheckpointMetadata(
        String tenantId,
        String sourcePipelineId,
        String targetPipelineId,
        String checkpointId,
        String contractVersion,
        String releaseVersion,
        Optional<String> correlationId
    ) {
        public CheckpointMetadata {
            tenantId = requireNonBlank(tenantId, "tenantId");
            sourcePipelineId = requireNonBlank(sourcePipelineId, "sourcePipelineId");
            targetPipelineId = requireNonBlank(targetPipelineId, "targetPipelineId");
            checkpointId = requireNonBlank(checkpointId, "checkpointId");
            contractVersion = requireNonBlank(contractVersion, "contractVersion");
            releaseVersion = requireNonBlank(releaseVersion, "releaseVersion");
            correlationId = Objects.requireNonNull(correlationId, "correlationId").filter(v -> !v.isBlank());
        }
    }

    /** Decoded and fingerprint-verified checkpoint handoff envelope. */
    public record DecodedCheckpointHandoff(
        CheckpointMetadata metadata,
        SpringCanonicalPayload payload,
        TpfExecutionContext.Snapshot propagatedContext
    ) {
        public DecodedCheckpointHandoff {
            Objects.requireNonNull(metadata, "metadata");
            Objects.requireNonNull(payload, "payload");
            Objects.requireNonNull(propagatedContext, "propagatedContext");
        }
    }

    /** Encodes a canonical payload and checkpoint ownership metadata into {@link SerializedTransitionPayload}. */
    public SerializedTransitionPayload encode(CheckpointMetadata metadata, SpringCanonicalPayload payload) {
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(payload, "payload");
        TpfExecutionContext.Snapshot ctx = TpfExecutionContext.capture();
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("schemaVersion", 1);
        envelope.put("tenantId", metadata.tenantId());
        envelope.put("sourcePipelineId", metadata.sourcePipelineId());
        envelope.put("targetPipelineId", metadata.targetPipelineId());
        envelope.put("checkpointId", metadata.checkpointId());
        envelope.put("contractVersion", metadata.contractVersion());
        envelope.put("releaseVersion", metadata.releaseVersion());
        metadata.correlationId().ifPresent(id -> envelope.put("correlationId", id));

        Map<String, String> contextMap = new LinkedHashMap<>();
        ctx.versionTag().ifPresent(v -> contextMap.put("versionTag", v));
        ctx.replayMode().ifPresent(v -> contextMap.put("replayMode", v));
        ctx.cachePolicy().ifPresent(v -> contextMap.put("cachePolicy", v));
        envelope.put("context", contextMap);
        envelope.put("canonicalPayload", payload.toDurableMap());

        try {
            String json = SpringPolyglotJson.exactMapper().writeValueAsString(envelope);
            return new SerializedTransitionPayload(
                PAYLOAD_TYPE_PREFIX + payload.canonicalTypeId(),
                TransitionPayloadEncoding.JSON,
                json);
        } catch (IOException failure) {
            throw new IllegalStateException("Failed encoding checkpoint handoff payload", failure);
        }
    }

    /**
     * Decodes a {@link SerializedTransitionPayload} checkpoint envelope and verifies canonical
     * type, expression fingerprint, and catalog fingerprint compatibility fail-closed.
     */
    public DecodedCheckpointHandoff decodeAndVerify(
        SerializedTransitionPayload serialized,
        String expectedCanonicalTypeId,
        String expectedTypeFingerprint,
        String expectedCatalogFingerprint
    ) {
        Objects.requireNonNull(serialized, "serialized");
        requireNonBlank(expectedCanonicalTypeId, "expectedCanonicalTypeId");
        requireNonBlank(expectedTypeFingerprint, "expectedTypeFingerprint");
        requireNonBlank(expectedCatalogFingerprint, "expectedCatalogFingerprint");

        if (!TransitionPayloadEncoding.JSON.equals(serialized.payloadEncoding())) {
            throw new IllegalArgumentException(
                "Unsupported checkpoint payload encoding: " + serialized.payloadEncoding());
        }
        String expectedTypePrefix = PAYLOAD_TYPE_PREFIX + expectedCanonicalTypeId;
        if (!expectedTypePrefix.equals(serialized.payloadTypeId())) {
            throw new IllegalArgumentException(
                "Checkpoint payloadTypeId mismatch: expected " + expectedTypePrefix
                    + " but got " + serialized.payloadTypeId());
        }
        Map<String, Object> root;
        try {
            root = SpringPolyglotJson.exactMapper().readValue(serialized.payload(), MAP_TYPE);
        } catch (IOException malformed) {
            throw new IllegalArgumentException("Malformed checkpoint handoff JSON", malformed);
        }

        Object schemaVersionObj = root.get("schemaVersion");
        if (!(schemaVersionObj instanceof Number schemaNumber)) {
            throw new IllegalArgumentException("Missing or non-numeric checkpoint schemaVersion");
        }
        int schemaVersion;
        try {
            schemaVersion = new BigDecimal(schemaNumber.toString()).intValueExact();
        } catch (ArithmeticException | NumberFormatException invalid) {
            throw new IllegalArgumentException("Invalid checkpoint schemaVersion: " + schemaVersionObj, invalid);
        }
        if (schemaVersion != 1) {
            throw new IllegalArgumentException("Unsupported checkpoint schemaVersion: " + schemaVersion);
        }

        SpringCanonicalPayload payload = SpringCanonicalPayload.fromDurableMap(root.get("canonicalPayload"))
            .orElseThrow(() -> new IllegalArgumentException("Missing or malformed canonicalPayload in checkpoint envelope"));

        if (!expectedCanonicalTypeId.equals(payload.canonicalTypeId())) {
            throw new IllegalArgumentException(
                "Checkpoint canonicalTypeId mismatch: expected " + expectedCanonicalTypeId
                    + " but got " + payload.canonicalTypeId());
        }
        if (!expectedTypeFingerprint.equals(payload.typeExpressionFingerprint())) {
            throw new IllegalArgumentException(
                "Checkpoint typeExpressionFingerprint mismatch for " + expectedCanonicalTypeId);
        }
        if (!expectedCatalogFingerprint.equals(payload.catalogFingerprint())) {
            throw new IllegalArgumentException(
                "Checkpoint catalogFingerprint mismatch for " + expectedCanonicalTypeId);
        }

        CheckpointMetadata metadata = new CheckpointMetadata(
            requiredString(root, "tenantId"),
            requiredString(root, "sourcePipelineId"),
            requiredString(root, "targetPipelineId"),
            requiredString(root, "checkpointId"),
            requiredString(root, "contractVersion"),
            requiredString(root, "releaseVersion"),
            optionalString(root, "correlationId"));

        Optional<String> versionTag = Optional.empty();
        Optional<String> replayMode = Optional.empty();
        Optional<String> cachePolicy = Optional.empty();
        if (root.get("context") instanceof Map<?, ?> contextMap) {
            versionTag = optionalString(contextMap, "versionTag");
            replayMode = optionalString(contextMap, "replayMode");
            cachePolicy = optionalString(contextMap, "cachePolicy");
        }
        TpfExecutionContext.Snapshot snapshot = new TpfExecutionContext.Snapshot(versionTag, replayMode, cachePolicy);
        return new DecodedCheckpointHandoff(metadata, payload, snapshot);
    }

    /** Encodes and dispatches a checkpoint handoff via the Spring {@link RuntimeAdapters#workDispatcher()}. */
    public CompletionStage<SerializedTransitionPayload> publishHandoff(
        CheckpointMetadata metadata,
        SpringCanonicalPayload payload
    ) {
        SerializedTransitionPayload encoded = encode(metadata, payload);
        return RuntimeAdapters.workDispatcher().enqueue(encoded).thenApply(ignored -> encoded);
    }

    private static String requiredString(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (!(value instanceof String str) || str.isBlank()) {
            throw new IllegalArgumentException("Checkpoint field '" + key + "' must be a non-blank string");
        }
        return str;
    }

    private static Optional<String> optionalString(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (value instanceof String str && !str.isBlank()) {
            return Optional.of(str);
        }
        return Optional.empty();
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

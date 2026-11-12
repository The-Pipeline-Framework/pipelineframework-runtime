package org.pipelineframework.awaitable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** Compiler-produced Await facts readable without application classes or worker availability. */
public record PinnedAwaitDescriptor(Map<String, Object> metadata) {
    public PinnedAwaitDescriptor {
        metadata = Map.copyOf(metadata);
        Object version = metadata.get("schemaVersion");
        if (!versionOne(version)) {
            throw new IllegalArgumentException("Unsupported pinned Await descriptor schema");
        }
        var normalized = new java.util.LinkedHashMap<String,Object>(metadata);
        normalized.put("schemaVersion", 1); metadata = Map.copyOf(normalized);
        for (String field : List.of("stepId", "inputTypeId", "outputTypeId", "transportInputTypeId",
            "transportOutputTypeId", "canonicalInputTypeId", "canonicalOutputTypeId", "completionPayloadTypeId")) {
            required(metadata, field);
        }
        if (!(metadata.get("requestAwareCompletion") instanceof Boolean)) {
            throw new IllegalArgumentException("Pinned Await requestAwareCompletion is required");
        }
    }

    private static boolean versionOne(Object value) {
        if (!(value instanceof Number number) || number instanceof Double || number instanceof Float) return false;
        try { return new java.math.BigDecimal(number.toString()).compareTo(java.math.BigDecimal.ONE) == 0; }
        catch (NumberFormatException invalid) { return false; }
    }

    public static PinnedAwaitDescriptor resolve(PipelineContractDescriptor contract, String stepId) {
        List<PinnedAwaitDescriptor> matches = contract.steps().stream()
            .filter(step -> step.deferredCompletion().get("runtimeDescriptor") instanceof Map<?, ?>)
            .map(step -> {
                var facts = new java.util.LinkedHashMap<String, Object>(step.deferredCompletion());
                facts.remove("runtimeDescriptor");
                facts.put("cardinality", step.cardinality());
                facts.putAll(cast(step.deferredCompletion().get("runtimeDescriptor")));
                return facts;
            })
            .filter(value -> stepId.equals(value.get("stepId")))
            .map(PinnedAwaitDescriptor::new).distinct().toList();
        if (matches.size() != 1) {
            throw new IllegalStateException("Pinned Release requires one exact Await descriptor for " + stepId);
        }
        return matches.getFirst();
    }

    public String stepId() { return required(metadata, "stepId"); }
    public String inputTypeId() { return required(metadata, "inputTypeId"); }
    public String outputTypeId() { return required(metadata, "outputTypeId"); }
    public String transportInputTypeId() { return required(metadata, "transportInputTypeId"); }
    public String transportOutputTypeId() { return required(metadata, "transportOutputTypeId"); }
    public String canonicalInputTypeId() { return required(metadata, "canonicalInputTypeId"); }
    public String canonicalOutputTypeId() { return required(metadata, "canonicalOutputTypeId"); }
    public String completionPayloadTypeId() { return required(metadata, "completionPayloadTypeId"); }
    public boolean requestAwareCompletion() { return Boolean.TRUE.equals(metadata.get("requestAwareCompletion")); }
    public Optional<String> completionProjectorId() { return optional("completionProjectorId"); }
    public Optional<String> commandAcknowledgementTypeId() { return optional("commandAcknowledgementTypeId"); }
    public Optional<String> canonicalCommandAcknowledgementTypeId() { return optional("canonicalCommandAcknowledgementTypeId"); }
    public Optional<Map<String, Object>> callbackContract() {
        Object value = metadata.get("callbackContract");
        return value == null ? Optional.empty() : Optional.of(Map.copyOf(cast(value)));
    }

    public java.time.Duration timeout() { return java.time.Duration.parse(required(metadata, "timeout")); }
    public String correlationStrategy() { return required(metadata, "correlationStrategy"); }
    public String cardinality() { return required(metadata, "cardinality"); }
    public String transportType() {
        return callbackContract().isPresent() ? AwaitCompletionDescriptor.CONNECTOR_CALLBACK_TRANSPORT
            : required(metadata, "transportType");
    }
    public List<String> idempotencyKeyFields() {
        if (!(metadata.get("idempotencyKeyFields") instanceof List<?> fields)
            || fields.stream().anyMatch(value -> !(value instanceof String))) {
            throw new IllegalStateException("Pinned Await idempotency fields are unavailable");
        }
        return fields.stream().map(String.class::cast).toList();
    }
    public Map<String, Object> transportConfig() {
        Object value = metadata.get("transportConfig");
        if (value == null) throw new IllegalStateException("Pinned Await transport configuration is unavailable");
        return Map.copyOf(cast(value));
    }

    /** Metadata view used solely by existing adapter admission-endpoint policy. */
    AwaitCompletionDescriptor admissionPolicyDescriptor() {
        return new AwaitCompletionDescriptor(stepId(), inputTypeId(), outputTypeId(), cardinality(), timeout(),
            correlationStrategy(), transportType(), transportConfig(), idempotencyKeyFields());
    }

    private Optional<String> optional(String name) {
        return metadata.get(name) == null ? Optional.empty() : Optional.of(required(metadata, name));
    }

    private static String required(Map<String, Object> value, String name) {
        if (!(value.get(name) instanceof String text) || text.isBlank()) {
            throw new IllegalArgumentException("Pinned Await descriptor is missing " + name);
        }
        return text;
    }

    private static Map<String, Object> cast(Object value) {
        if (!(value instanceof Map<?, ?> map) || map.keySet().stream().anyMatch(key -> !(key instanceof String))) {
            throw new IllegalArgumentException("Pinned Await descriptor must be a string-keyed map");
        }
        @SuppressWarnings("unchecked") Map<String, Object> typed = (Map<String, Object>) map;
        return typed;
    }
}

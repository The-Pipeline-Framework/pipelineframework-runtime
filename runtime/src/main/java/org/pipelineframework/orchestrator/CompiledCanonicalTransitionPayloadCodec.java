package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.type.TypeFactory;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** Materializes canonical transition values using only the worker's compiled boundary contract. */
public final class CompiledCanonicalTransitionPayloadCodec implements TransitionPayloadCodec {
    private final TransitionPayloadCodec framing;
    private final JsonDurablePayloadCodec canonical = new JsonDurablePayloadCodec();
    private final Boundary input;
    private final Boundary output;
    private final org.pipelineframework.type.CanonicalTypeCatalogue catalogue;

    public CompiledCanonicalTransitionPayloadCodec(TransitionPayloadCodec framing,
        PipelineContractDescriptor compiledContract, String inputCanonicalTypeId, String outputCanonicalTypeId) {
        this.framing = Objects.requireNonNull(framing, "framing");
        Objects.requireNonNull(compiledContract, "compiledContract");
        input = boundary(compiledContract, inputCanonicalTypeId);
        output = boundary(compiledContract, outputCanonicalTypeId);
        catalogue = org.pipelineframework.type.CanonicalTypeCatalogue.fromCanonicalTypes(compiledContract.canonicalTypes());
    }

    @Override
    public String encoding() {
        return framing.encoding();
    }

    @Override
    public SerializedTransitionPayload encode(Object value) {
        return framing.encode(encodeValue(value));
    }

    @Override
    public Object decode(SerializedTransitionPayload payload) {
        return decodeValue(framing.decode(payload));
    }

    /** Detects canonical framing without resolving a release registry or loading payload-selected classes. */
    public boolean containsCanonicalPayload(SerializedTransitionPayload payload) {
        return containsCanonicalPayload(framing, payload);
    }

    /** Selects canonical handling before constructing any compiled boundary plans. */
    public static boolean containsCanonicalPayload(TransitionPayloadCodec framing, SerializedTransitionPayload payload) {
        return containsCanonicalValue(Objects.requireNonNull(framing, "framing").decode(payload));
    }

    private static boolean containsCanonicalValue(Object value) {
        if (value instanceof ExecutionInputSnapshot snapshot) return containsCanonicalValue(snapshot.payload());
        if (value instanceof List<?> list) return list.stream().anyMatch(CompiledCanonicalTransitionPayloadCodec::containsCanonicalValue);
        return TypedDurablePayload.fromDurableValue(value).isPresent()
            || value instanceof Map<?, ?> map && map.containsKey("canonicalTypeId") && map.containsKey("encodingVersion");
    }

    private Object encodeValue(Object value) {
        if (value instanceof ExecutionInputSnapshot snapshot) {
            Object encoded;
            if (snapshot.shape() == ExecutionInputShape.MULTI) {
                if (!(snapshot.payload() instanceof List<?> items)) {
                    throw new IllegalArgumentException("MULTI snapshot payload must be a List");
                }
                encoded = items.stream().map(this::encodeValue).toList();
            } else {
                encoded = encodeValue(snapshot.payload());
            }
            return new ExecutionInputSnapshot(snapshot.shape(), encoded, snapshot.pipelineContext(), snapshot.invocationPath());
        }
        if (value instanceof List<?> items) {
            for (Object item : items) requireElement(item, output);
            TypedDurablePayload payload = canonical.encode(items, output.collection());
            validateSchema(payload, output);
            return payload;
        }
        requireElement(value, output);
        TypedDurablePayload payload = canonical.encode(value, output.scalar());
        validateSchema(payload, output);
        return payload;
    }

    private Object decodeValue(Object value) {
        if (value instanceof ExecutionInputSnapshot snapshot) {
            Object decoded = decodeValue(snapshot.payload());
            if (snapshot.shape() == ExecutionInputShape.MULTI && !(decoded instanceof List<?>)) {
                throw new IllegalArgumentException("MULTI snapshot payload must decode to a List");
            }
            return new ExecutionInputSnapshot(snapshot.shape(), decoded,
                snapshot.pipelineContext(), snapshot.invocationPath());
        }
        if (value instanceof List<?> items) return items.stream().map(this::decodeValue).toList();
        TypedDurablePayload payload = TypedDurablePayload.fromDurableValue(value)
            .orElseThrow(() -> new IllegalArgumentException("Canonical transition boundary requires a typed durable payload"));
        boolean collection = payload.canonicalTypeId().equals(input.collection().binding().canonicalTypeId());
        validateSchema(payload, input);
        Object decoded = canonical.decode(payload, collection ? input.collection() : input.scalar());
        if (collection) {
            if (!(decoded instanceof List<?> items)) throw new IllegalArgumentException("Canonical collection must decode to a List");
            for (Object item : items) requireElement(item, input);
        } else {
            requireElement(decoded, input);
        }
        return decoded;
    }

    private void validateSchema(TypedDurablePayload payload, Boundary boundary) {
        try {
            var body = org.pipelineframework.config.pipeline.PipelineJson.mapper().reader()
                .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .with(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .readTree(payload.payload());
            String identity = boundary.scalar().binding().canonicalTypeId();
            if (payload.canonicalTypeId().equals(boundary.collection().binding().canonicalTypeId())) {
                if (!body.isArray()) throw new IllegalArgumentException("Canonical collection requires a JSON array");
                for (var item : (com.fasterxml.jackson.databind.JsonNode) body) catalogue.validateAndCanonicalize(identity, item.toString());
            } else {
                catalogue.validateAndCanonicalize(identity, body.toString());
            }
        } catch (java.io.IOException failure) { throw new IllegalArgumentException("Invalid canonical JSON", failure); }
    }

    private static void requireElement(Object value, Boundary boundary) {
        if (value == null || !boundary.scalar().binding().runtimeClass().isInstance(value)) {
            throw new IllegalArgumentException("Value does not match compiled canonical boundary "
                + boundary.scalar().binding().canonicalTypeId());
        }
    }

    private static Boundary boundary(PipelineContractDescriptor contract, String requested) {
        Objects.requireNonNull(requested, "canonical boundary identity");
        String identity = requested;
        Map<String, Object> definition = contract.canonicalTypes().get(identity);
        if (definition == null) {
            // Historical boundary metadata may name a generated Java class. It must identify one definition.
            var matches = contract.canonicalTypes().entrySet().stream()
                .filter(entry -> requested.equals(entry.getValue().get("runtimeClass"))).toList();
            if (matches.size() != 1) {
                throw new IllegalArgumentException("Missing or ambiguous compiled canonical boundary: " + requested);
            }
            identity = matches.getFirst().getKey();
            definition = matches.getFirst().getValue();
        }
        String fingerprint = required(definition, "definitionFingerprint");
        String catalog = contract.canonicalCatalogFingerprint();
        if (catalog == null || catalog.isBlank()) throw new IllegalArgumentException("Compiled canonical catalog fingerprint is required");
        try {
            Class<?> type = CanonicalPayloadRuntimeClassLoader.load(required(definition, "runtimeClass"),
                Thread.currentThread().getContextClassLoader());
            var scalar = new CanonicalPayloadBinding(identity, fingerprint, catalog, type);
            var collection = new CanonicalPayloadBinding("List<" + identity + ">", fingerprint("List<" + fingerprint + ">"),
                catalog, List.class, TypeFactory.defaultInstance().constructCollectionType(List.class, type));
            return new Boundary(CompiledDurablePayloadPlan.compile(scalar), CompiledDurablePayloadPlan.compile(collection));
        } catch (ClassNotFoundException error) {
            throw new IllegalArgumentException("Compiled canonical boundary class is unavailable: " + identity, error);
        }
    }

    private static String required(Map<String, Object> definition, String field) {
        if (!(definition.get(field) instanceof String value) || value.isBlank()) {
            throw new IllegalArgumentException("Compiled canonical definition requires " + field);
        }
        return value;
    }

    private static String fingerprint(String expression) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(expression.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
    }

    private record Boundary(CompiledDurablePayloadPlan scalar, CompiledDurablePayloadPlan collection) { }
}

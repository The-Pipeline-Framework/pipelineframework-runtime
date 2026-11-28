package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/** Materializes canonical transition values using only the worker's compiled boundary contract. */
public final class CompiledCanonicalTransitionPayloadCodec implements TransitionPayloadCodec {
    private static final ObjectMapper EXACT_JSON = CanonicalTypeCatalogue.exactJsonMapper();
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
            if (output.isClassFree()) {
                return encodeClassFreeCollection(items, output);
            }
            for (Object item : items) requireElement(item, output);
            TypedDurablePayload payload = canonical.encode(items, output.typedPlans().orElseThrow().collection());
            validateSchema(payload, output);
            return payload;
        }
        if (output.isClassFree()) {
            return validateAndNormalizeClassFree(value, output, false, true);
        }
        requireElement(value, output);
        TypedDurablePayload payload = canonical.encode(value, output.typedPlans().orElseThrow().scalar());
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
        boolean collection = payload.canonicalTypeId().equals(input.collectionTypeId());
        if (input.isClassFree()) {
            return validateAndNormalizeClassFree(payload, input, collection, true);
        }
        validateSchema(payload, input);
        var plans = input.typedPlans().orElseThrow();
        Object decoded = canonical.decode(payload, collection ? plans.collection() : plans.scalar());
        if (collection) {
            if (!(decoded instanceof List<?> items)) throw new IllegalArgumentException("Canonical collection must decode to a List");
            for (Object item : items) requireElement(item, input);
        } else {
            requireElement(decoded, input);
        }
        return decoded;
    }

    private TypedDurablePayload encodeClassFreeCollection(List<?> items, Boundary boundary) {
        var array = EXACT_JSON.createArrayNode();
        for (Object item : items) {
            TypedDurablePayload normalized = validateAndNormalizeClassFree(item, boundary, false, false);
            try {
                array.add(EXACT_JSON.readTree(normalized.payload()));
            } catch (java.io.IOException failure) {
                throw new IllegalArgumentException("Invalid canonical JSON element", failure);
            }
        }
        return new TypedDurablePayload(
            boundary.collectionTypeId(),
            boundary.collectionFingerprint(),
            boundary.catalogFingerprint(),
            JsonDurablePayloadCodec.ENCODING,
            JsonDurablePayloadCodec.ENCODING_VERSION,
            array.toString().getBytes(StandardCharsets.UTF_8));
    }

    private TypedDurablePayload validateAndNormalizeClassFree(
        Object value, Boundary boundary, boolean collection, boolean allowCollectionEnvelope
    ) {
        TypedDurablePayload payload = TypedDurablePayload.fromDurableValue(value)
            .orElseThrow(() -> new IllegalArgumentException(
                "Class-free canonical boundary requires TypedDurablePayload for " + boundary.canonicalTypeId()));
        boolean isCollection = collection
            || (allowCollectionEnvelope && payload.canonicalTypeId().equals(boundary.collectionTypeId()));
        String expectedTypeId = isCollection ? boundary.collectionTypeId() : boundary.canonicalTypeId();
        String expectedFingerprint = isCollection ? boundary.collectionFingerprint() : boundary.definitionFingerprint();
        if (!expectedTypeId.equals(payload.canonicalTypeId())) {
            throw new IllegalArgumentException("Canonical type mismatch: expected " + expectedTypeId
                + " but found " + payload.canonicalTypeId());
        }
        if (!expectedFingerprint.equals(payload.typeExpressionFingerprint())) {
            throw new IllegalArgumentException("Canonical definition fingerprint mismatch for " + expectedTypeId);
        }
        if (!boundary.catalogFingerprint().equals(payload.catalogFingerprint())) {
            throw new IllegalArgumentException("Canonical catalog fingerprint mismatch for " + expectedTypeId);
        }
        if (!JsonDurablePayloadCodec.ENCODING.equals(payload.encoding())
            || payload.encodingVersion() != JsonDurablePayloadCodec.ENCODING_VERSION) {
            throw new IllegalArgumentException("Unsupported canonical durable payload encoding for " + expectedTypeId);
        }
        String canonicalJson = validateSchema(payload, boundary);
        return new TypedDurablePayload(
            expectedTypeId,
            expectedFingerprint,
            boundary.catalogFingerprint(),
            JsonDurablePayloadCodec.ENCODING,
            JsonDurablePayloadCodec.ENCODING_VERSION,
            canonicalJson.getBytes(StandardCharsets.UTF_8));
    }

    private String validateSchema(TypedDurablePayload payload, Boundary boundary) {
        try {
            var body = EXACT_JSON.reader()
                .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .with(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .readTree(payload.payload());
            String identity = boundary.canonicalTypeId();
            if (payload.canonicalTypeId().equals(boundary.collectionTypeId())) {
                if (!body.isArray()) throw new IllegalArgumentException("Canonical collection requires a JSON array");
                var normalized = EXACT_JSON.createArrayNode();
                for (var item : (com.fasterxml.jackson.databind.JsonNode) body) {
                    normalized.add(EXACT_JSON.readTree(catalogue.validateAndCanonicalize(identity, item.toString())));
                }
                return normalized.toString();
            } else {
                return catalogue.validateAndCanonicalize(identity, body.toString());
            }
        } catch (java.io.IOException failure) { throw new IllegalArgumentException("Invalid canonical JSON", failure); }
    }

    private static void requireElement(Object value, Boundary boundary) {
        var plans = boundary.typedPlans().orElseThrow(() -> new IllegalStateException(
            "Class-free canonical boundary has no Java runtimeClass: " + boundary.canonicalTypeId()));
        if (value == null || !plans.scalar().binding().runtimeClass().isInstance(value)) {
            throw new IllegalArgumentException("Value does not match compiled canonical boundary "
                + boundary.canonicalTypeId());
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
        String collectionTypeId = "List<" + identity + ">";
        String collectionFingerprint = fingerprint("List<" + fingerprint + ">");
        Object rawRuntimeClass = definition.get("runtimeClass");
        if (!(rawRuntimeClass instanceof String runtimeClassName) || runtimeClassName.isBlank()) {
            return new Boundary(identity, fingerprint, collectionTypeId, collectionFingerprint, catalog,
                java.util.Optional.empty());
        }
        try {
            Class<?> type = CanonicalPayloadRuntimeClassLoader.load(runtimeClassName,
                org.pipelineframework.config.pipeline.PipelineResources.resolveClassLoader());
            var scalar = new CanonicalPayloadBinding(identity, fingerprint, catalog, type);
            var collection = new CanonicalPayloadBinding(collectionTypeId, collectionFingerprint,
                catalog, List.class, TypeFactory.defaultInstance().constructCollectionType(List.class, type));
            return new Boundary(identity, fingerprint, collectionTypeId, collectionFingerprint, catalog,
                java.util.Optional.of(new TypedPlans(
                    CompiledDurablePayloadPlan.compile(scalar, contract.canonicalTypes()),
                    CompiledDurablePayloadPlan.compile(collection, contract.canonicalTypes()))));
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

    private record TypedPlans(CompiledDurablePayloadPlan scalar, CompiledDurablePayloadPlan collection) { }

    private record Boundary(
        String canonicalTypeId,
        String definitionFingerprint,
        String collectionTypeId,
        String collectionFingerprint,
        String catalogFingerprint,
        java.util.Optional<TypedPlans> typedPlans
    ) {
        boolean isClassFree() {
            return typedPlans.isEmpty();
        }
    }
}

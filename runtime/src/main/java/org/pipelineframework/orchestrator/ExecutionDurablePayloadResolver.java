package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;

/** Resolves cached canonical payload plans from the release pinned by an execution record. */
@ApplicationScoped
public class ExecutionDurablePayloadResolver {
    public enum Slot { INPUT, CONTINUATION_INPUT, RESULT }

    @Inject PipelineReleaseRegistry releaseRegistry;
    @Inject JsonDurablePayloadCodec codec;
    @Inject PipelineOrchestratorConfig orchestratorConfig;

    private final DurablePayloadPlanRegistry plans = new DurablePayloadPlanRegistry();
    private static final int RELEASE_CACHE_MAXIMUM_SIZE = 256;
    private static final Duration RELEASE_CACHE_EXPIRY = Duration.ofMinutes(15);
    private final BoundedExpiringCache<ReleaseCacheKey, PipelineReleaseRecord> releases =
        new BoundedExpiringCache<>(RELEASE_CACHE_MAXIMUM_SIZE, RELEASE_CACHE_EXPIRY);
    private static final Duration RELEASE_LOOKUP_TIMEOUT = Duration.ofSeconds(10);

    public String encode(ExecutionRecord<?, ?> execution, Slot slot, Object value) {
        if (serializedMode()) {
            return encodeEnvelope(execution, slot, value);
        }
        CompiledDurablePayloadPlan plan = resolve(execution, slot, value instanceof List<?>);
        return encode(execution, slot, value, plan);
    }

    /** Returns whether the execution's pinned release carries the v3 canonical binding catalog. */
    public boolean supportsTypedPayloads(ExecutionRecord<?, ?> execution) {
        return pinnedRelease(execution).contract().schemaVersion() >= 2;
    }

    /** Encodes an explicitly declared boundary type through the execution's pinned release. */
    public String encode(ExecutionRecord<?, ?> execution, String canonicalTypeId, Object value) {
        if (serializedMode()) {
            return encodeEnvelope(execution, Slot.CONTINUATION_INPUT, canonicalTypeId, value);
        }
        CompiledDurablePayloadPlan plan = resolve(
            pinnedRelease(execution), execution, canonicalTypeId, value instanceof List<?>);
        return encode(execution, Slot.CONTINUATION_INPUT, value, plan);
    }

    private String encode(
        ExecutionRecord<?, ?> execution,
        Slot slot,
        Object value,
        CompiledDurablePayloadPlan plan
    ) {
        try {
            return PipelineJson.mapper().writeValueAsString(codec.encode(value, plan));
        } catch (Exception error) {
            throw failure(execution, slot, plan.binding().canonicalTypeId(), "encode", error);
        }
    }

    public Object decode(ExecutionRecord<?, ?> execution, Slot slot, String stored) {
        try {
            TypedDurablePayload payload = TypedDurablePayload.fromSerializedBytes(stored.getBytes(StandardCharsets.UTF_8))
                .orElseThrow(() -> new IllegalArgumentException("not a typed durable payload"));
            if (serializedMode()) {
                validateEnvelope(execution, slot, payload);
                return payload;
            }
            CompiledDurablePayloadPlan plan = resolveStoredPayload(execution, slot, payload);
            return codec.decode(payload, plan);
        } catch (Exception error) {
            throw failure(execution, slot, "<resolved-from-release>", "decode", error);
        }
    }

    /** Restores the result-item container expected by the execution read model. */
    Object decodeResultItems(ExecutionRecord<?, ?> execution, String stored) {
        Object decoded = decode(execution, Slot.RESULT, stored);
        if (!(decoded instanceof TypedDurablePayload payload) || !serializedMode()) {
            return decoded;
        }
        var expression = StoredExpression.parse(payload.canonicalTypeId());
        if (!expression.collection()) {
            if (execution.resultShape() != ExecutionResultShape.SINGLE) {
                throw new IllegalStateException("A multi result requires a canonical collection");
            }
            return List.of(payload);
        }
        try {
            var json = PipelineJson.mapper();
            var items = json.readTree(payload.payload());
            if (execution.resultShape() == ExecutionResultShape.SINGLE && items.size() > 1) {
                throw new IllegalStateException("A single result cannot contain multiple canonical items");
            }
            var contract = pinnedRelease(execution).contract();
            var result = new java.util.ArrayList<TypedDurablePayload>(items.size());
            for (var item : items) {
                result.add(canonicalEnvelope(contract, expression.elementTypeId(), false, json.writeValueAsBytes(item)));
            }
            return List.copyOf(result);
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("Cannot read canonical result items", failure);
        }
    }

    private boolean serializedMode() {
        return orchestratorConfig != null && orchestratorConfig.controlPlane() != null
            && orchestratorConfig.controlPlane().payloadMaterialization()
                == PipelineOrchestratorConfig.ControlPlaneConfig.PayloadMaterialization.SERIALIZED;
    }

    private String encodeEnvelope(ExecutionRecord<?, ?> execution, Slot slot, Object value) {
        String boundary = slot == Slot.CONTINUATION_INPUT && value instanceof TypedDurablePayload payload
            ? StoredExpression.parse(payload.canonicalTypeId()).elementTypeId()
            : canonicalTypeId(pinnedRelease(execution).contract(), execution, slot);
        return encodeEnvelope(execution, slot, boundary, value);
    }

    private String encodeEnvelope(ExecutionRecord<?, ?> execution, Slot slot, String boundary, Object value) {
        TypedDurablePayload payload;
        if (value instanceof TypedDurablePayload envelope) {
            payload = envelope;
        } else if (value instanceof List<?> values && (!values.isEmpty() || slot != Slot.INPUT
            || execution.inputPayload() instanceof ExecutionInputSnapshot snapshot && snapshot.shape() == ExecutionInputShape.MULTI)
            && (values.isEmpty() || values.stream().allMatch(item -> item instanceof TypedDurablePayload))) {
            String expected = serializedDefinition(pinnedRelease(execution).contract().canonicalTypes(), boundary)
                .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::canonicalTypeId).orElse(boundary);
            var bytes = new java.io.ByteArrayOutputStream();
            bytes.write('[');
            for (int index = 0; index < values.size(); index++) {
                var item = (TypedDurablePayload) values.get(index);
                validateEnvelope(execution, slot, item);
                if (!expected.equals(item.canonicalTypeId())) throw new IllegalArgumentException("Canonical items differ from the retained boundary");
                if (index > 0) bytes.write(',');
                bytes.writeBytes(item.payload());
            }
            bytes.write(']');
            payload = canonicalEnvelope(pinnedRelease(execution).contract(), boundary, true, bytes.toByteArray());
        } else if (slot == Slot.INPUT) {
            try {
                boolean multi = execution.inputPayload() instanceof ExecutionInputSnapshot snapshot
                    && snapshot.shape() == ExecutionInputShape.MULTI;
                payload = canonicalEnvelope(pinnedRelease(execution).contract(), boundary, multi,
                    PipelineJson.mapper().writeValueAsBytes(value));
            } catch (java.io.IOException failure) {
                throw new IllegalArgumentException("Execution input is not canonical JSON", failure);
            }
        } else {
            throw new IllegalArgumentException("Serialized continuation and result values require canonical durable envelopes");
        }
        validateEnvelope(execution, slot, payload);
        String expected = serializedDefinition(pinnedRelease(execution).contract().canonicalTypes(), boundary)
                .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::canonicalTypeId).orElse(boundary);
        if (!expected.equals(StoredExpression.parse(payload.canonicalTypeId()).elementTypeId())) {
            throw new IllegalArgumentException("Durable payload differs from the pinned write boundary");
        }
        try {
            return PipelineJson.mapper().writeValueAsString(payload);
        } catch (Exception error) {
            throw failure(execution, slot, payload.canonicalTypeId(), "encode", error);
        }
    }

    /** Validates pinned canonical identity without resolving the application's Java class. */
    private void validateEnvelope(ExecutionRecord<?, ?> execution, Slot slot, TypedDurablePayload payload) {
        PipelineReleaseRecord release = pinnedRelease(execution);
        StoredExpression stored = StoredExpression.parse(payload.canonicalTypeId());
        validatePermitted(release.contract(), slot, stored.elementTypeId(), true);
        TypedDurablePayload expected = canonicalEnvelope(release.contract(), stored.elementTypeId(),
            stored.collection(), payload.payload());
        if (!expected.canonicalTypeId().equals(payload.canonicalTypeId())
            || !expected.typeExpressionFingerprint().equals(payload.typeExpressionFingerprint())
            || !expected.catalogFingerprint().equals(payload.catalogFingerprint())
            || !expected.encoding().equals(payload.encoding())
            || payload.encodingVersion() != expected.encodingVersion()) {
            throw new IllegalArgumentException("Durable payload identity differs from pinned release");
        }
        // Validate canonical schema using compiler-owned definitions without Java materialization.
        try (var parser = PipelineJson.mapper().createParser(payload.payload())) {
            parser.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
            com.fasterxml.jackson.databind.JsonNode body = PipelineJson.mapper().readTree(parser);
            if (body == null || parser.nextToken() != null
                || (stored.collection() && !body.isArray())) {
                throw new IllegalArgumentException("Invalid canonical JSON payload framing");
            }
            if (release.contract().schemaVersion() >= 2) {
                var catalogue = org.pipelineframework.type.CanonicalTypeCatalogue.fromCanonicalTypes(
                    release.contract().canonicalTypes());
                if (stored.collection()) {
                    for (var item : body) catalogue.validateAndCanonicalize(stored.elementTypeId(), item.toString());
                } else {
                    catalogue.validateAndCanonicalize(stored.elementTypeId(), body.toString());
                }
            }
        } catch (java.io.IOException error) {
            throw new IllegalArgumentException("Invalid canonical JSON payload framing", error);
        }
    }

    /** Constructs canonical framing from metadata; deliberately never resolves a Java type. */
    static TypedDurablePayload canonicalEnvelope(PipelineContractDescriptor contract, String typeId,
        boolean collection, byte[] bytes) {
        var definition = serializedDefinition(contract.canonicalTypes(), typeId);
        String canonical = definition.map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::canonicalTypeId)
            .orElse(typeId);
        String expression;
        String catalog;
        if (definition.isPresent()) {
            expression = required(definition.orElseThrow().definition(), "definitionFingerprint", canonical);
            catalog = contract.canonicalCatalogFingerprint();
            if (catalog.isBlank()) throw new IllegalStateException("Pinned canonical catalog is unavailable");
        } else if (contract.schemaVersion() == 1 && canonical.contains(".")) {
            expression = fingerprint(canonical);
            catalog = contract.contractHash();
        } else {
            throw new IllegalStateException("Pinned release has no canonical binding for " + canonical);
        }
        if (collection) expression = fingerprint("List<" + expression + ">");
        return new TypedDurablePayload(collection ? "List<" + canonical + ">" : canonical,
            expression, catalog, JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION, bytes);
    }

    /** Decode-only compatibility path; legacy class hints cannot select a Java target. */
    public Object decodeLegacy(ExecutionRecord<?, ?> execution, Slot slot, String stored) {
        if (serializedMode()) {
            try {
                Object decoded = PipelineJson.mapper().readerFor(Object.class)
                    .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .readValue(stored);
                Object body = decoded instanceof Map<?, ?> map && map.containsKey("_tpf_payload")
                    ? map.get("_tpf_payload") : decoded;
                PipelineContractDescriptor contract = pinnedRelease(execution).contract();
                TypedDurablePayload payload = canonicalEnvelope(contract,
                    canonicalTypeId(contract, execution, slot),
                    execution.resultShape() == ExecutionResultShape.MATERIALIZED_MULTI && slot == Slot.RESULT,
                    PipelineJson.mapper().writeValueAsBytes(body));
                validateEnvelope(execution, slot, payload);
                return payload;
            } catch (Exception error) {
                throw failure(execution, slot, "<resolved-from-release>", "legacy decode", error);
            }
        }
        CompiledDurablePayloadPlan plan = resolve(execution, slot, execution.resultShape() == ExecutionResultShape.MATERIALIZED_MULTI && slot == Slot.RESULT);
        try {
            Object decoded = PipelineJson.mapper().readValue(stored, Object.class);
            Object body = decoded instanceof Map<?, ?> map && map.containsKey("_tpf_payload")
                ? map.get("_tpf_payload")
                : decoded;
            byte[] bytes = PipelineJson.mapper().writeValueAsBytes(body);
            return codec.decode(new TypedDurablePayload(plan.binding().canonicalTypeId(),
                plan.binding().typeExpressionFingerprint(), plan.binding().catalogFingerprint(),
                JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION, bytes), plan);
        } catch (Exception error) {
            throw failure(execution, slot, plan.binding().canonicalTypeId(), "legacy decode", error);
        }
    }

    private CompiledDurablePayloadPlan resolve(ExecutionRecord<?, ?> execution, Slot slot, boolean collection) {
        PipelineReleaseRecord release = pinnedRelease(execution);
        List<PipelineBundleStepDescriptor> steps = release.contract().steps();
        if (steps.isEmpty()) {
            throw new IllegalStateException("pinned release has no pipeline steps");
        }
        return resolve(release, execution, canonicalTypeId(release.contract(), execution, slot), collection);
    }

    /**
     * Typed envelopes are self-identifying.  Cursor/slot semantics choose an expression only
     * when creating a value (or decoding legacy records); they must never reinterpret a typed
     * value after the execution has moved to a later step.
     */
    private CompiledDurablePayloadPlan resolveStoredPayload(
        ExecutionRecord<?, ?> execution,
        Slot slot,
        TypedDurablePayload payload) {
        PipelineReleaseRecord release = pinnedRelease(execution);
        StoredExpression stored = StoredExpression.parse(payload.canonicalTypeId());
        validatePermitted(release.contract(), slot, stored.elementTypeId());
        return resolve(release, execution, stored.elementTypeId(), stored.collection());
    }

    private CompiledDurablePayloadPlan resolve(
        PipelineReleaseRecord release,
        ExecutionRecord<?, ?> execution,
        String requestedTypeId,
        boolean collection) {
        var resolvedDefinition = CanonicalPayloadBindingLookup.resolve(
            release.contract().canonicalTypes(), requestedTypeId);
        String canonicalTypeId = resolvedDefinition
            .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::canonicalTypeId)
            .orElse(requestedTypeId);
        Map<String, Object> definition = resolvedDefinition
            .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::definition)
            .orElse(Map.of());
        String className;
        String definitionFingerprint;
        String catalog;
        if (resolvedDefinition.isPresent()) {
            className = required(definition, "runtimeClass", canonicalTypeId);
            definitionFingerprint = required(definition, "definitionFingerprint", canonicalTypeId);
            catalog = release.contract().canonicalCatalogFingerprint();
            if (catalog.isBlank()) {
                throw new IllegalStateException("pinned release canonical catalog fingerprint is unavailable");
            }
        } else if (release.contract().schemaVersion() == 1 && canonicalTypeId.contains(".")) {
            // Historical v1/v2 contracts stored fully-qualified Java identities rather than a
            // canonical catalog. The pinned release still chooses the target class.
            className = canonicalTypeId;
            definitionFingerprint = fingerprint(canonicalTypeId);
            catalog = release.contract().contractHash();
        } else {
            throw new IllegalStateException("pinned release has no canonical binding for " + canonicalTypeId);
        }
        String expression = collection ? fingerprint("List<" + definitionFingerprint + ">") : definitionFingerprint;
        DurablePayloadReleaseCoordinate coordinate = new DurablePayloadReleaseCoordinate(
            execution.pipelineId(), execution.contractVersion(), execution.releaseVersion());
        var cached = plans.find(coordinate, expression);
        if (cached.isPresent()) {
            return cached.get();
        }
        Class<?> runtimeClass;
        try {
            runtimeClass = CanonicalPayloadRuntimeClassLoader.load(className, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException error) {
            throw new IllegalStateException("pinned release binding class is unavailable: " + className, error);
        }
        JavaType runtimeType = collection
            ? TypeFactory.defaultInstance().constructCollectionType(List.class, runtimeClass)
            : TypeFactory.defaultInstance().constructType(runtimeClass);
        String identity = collection ? "List<" + canonicalTypeId + ">" : canonicalTypeId;
        CanonicalPayloadBinding binding = new CanonicalPayloadBinding(identity, expression, catalog, collection ? List.class : runtimeClass, runtimeType);
        plans.activate(coordinate, Map.of(expression, binding));
        return plans.plan(coordinate, expression);
    }

    private PipelineReleaseRecord pinnedRelease(ExecutionRecord<?, ?> execution) {
        DurablePayloadReleaseCoordinate coordinate = new DurablePayloadReleaseCoordinate(
            execution.pipelineId(), execution.contractVersion(), execution.releaseVersion());
        return releases.getOrLoad(new ReleaseCacheKey(execution.tenantId(), coordinate), ignored -> {
            PipelineReleaseRecord release = releaseRegistry.get(
                    execution.tenantId(), execution.pipelineId(), execution.releaseVersion())
                .await().atMost(RELEASE_LOOKUP_TIMEOUT)
                .orElseThrow(() -> new IllegalStateException("pinned release is unavailable"));
            if (!execution.tenantId().equals(release.tenantId())
                || !execution.pipelineId().equals(release.pipelineId())
                || !execution.contractVersion().equals(release.contractVersion())
                || !execution.releaseVersion().equals(release.releaseVersion())) {
                throw new IllegalStateException("Resolved release differs from execution's pinned identity");
            }
            return release;
        });
    }

    private static void validatePermitted(
        PipelineContractDescriptor contract,
        Slot slot,
        String canonicalTypeId) {
        validatePermitted(contract, slot, canonicalTypeId, false);
    }

    private static void validatePermitted(PipelineContractDescriptor contract, Slot slot,
        String canonicalTypeId, boolean includePhysicalAwaitBoundaries) {
        List<PipelineBundleStepDescriptor> steps = contract.steps();
        Map<String, Map<String, Object>> definitions = contract.canonicalTypes();
        if (steps.isEmpty()) {
            throw new IllegalStateException("pinned release has no pipeline steps");
        }
        // Composition names are the boundary authority. The flat leaf catalog can use Java
        // identities that are shared by several aliases; those must not authorize another name.
        Set<String> permitted = contract.composition().present()
            ? contract.composition().definitions().stream().flatMap(definition -> Stream.concat(
                Stream.of(definition.inputContractId(), definition.outputContractId()),
                definition.nodes().stream().flatMap(node -> Stream.of(node.inputContractId(), node.outputContractId()))))
                .collect(Collectors.toUnmodifiableSet())
            : switch (slot) {
            case INPUT, CONTINUATION_INPUT -> java.util.stream.Stream.concat(
                java.util.stream.Stream.of(steps.getFirst().inputTypeId()),
                steps.stream().map(PipelineBundleStepDescriptor::outputTypeId))
                .map(typeId -> canonicalIdentity(definitions, typeId))
                .collect(Collectors.toUnmodifiableSet());
            case RESULT -> steps.stream()
                .flatMap(step -> java.util.stream.Stream.of(step.inputTypeId(), step.outputTypeId()))
                .map(typeId -> canonicalIdentity(definitions, typeId))
                .collect(Collectors.toUnmodifiableSet());
        };
        if (includePhysicalAwaitBoundaries && !contract.rootBoundary().isEmpty()) {
            var roots = new java.util.HashSet<>(permitted);
            roots.add((String) contract.rootBoundary().get("inputCanonicalTypeId"));
            roots.add((String) contract.rootBoundary().get("outputCanonicalTypeId"));
            permitted = Set.copyOf(roots);
        }
        if (includePhysicalAwaitBoundaries && slot == Slot.CONTINUATION_INPUT) {
            var expanded = new java.util.HashSet<>(permitted);
            for (var step : steps) {
                expanded.add(canonicalIdentity(definitions, step.inputTypeId()));
                Object descriptor = step.deferredCompletion().get("runtimeDescriptor");
                if (descriptor instanceof Map<?,?> metadata && metadata.get("stepId") instanceof String stepId) {
                    var pinned = org.pipelineframework.awaitable.PinnedAwaitDescriptor.resolve(contract, stepId);
                    expanded.add(pinned.canonicalInputTypeId()); expanded.add(pinned.canonicalOutputTypeId());
                }
            }
            permitted = Set.copyOf(expanded);
        }
        if (!permitted.contains(canonicalTypeId)) {
            throw new IllegalStateException("Typed execution durable payload canonical type '" + canonicalTypeId
                + "' is not permitted for " + slot + "; permitted=" + permitted.stream().sorted().toList());
        }
    }

    private static Optional<CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition> serializedDefinition(
        Map<String, Map<String,Object>> definitions, String requested) {
        if (definitions.containsKey(requested)) return Optional.of(
            new CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition(requested, definitions.get(requested)));
        var matches = definitions.entrySet().stream().filter(entry -> requested.equals(entry.getValue().get("runtimeClass"))).toList();
        if (matches.size() > 1) throw new IllegalStateException("Ambiguous historical canonical boundary " + requested);
        return matches.stream().findFirst().map(entry -> new CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition(entry.getKey(), entry.getValue()));
    }

    private static String canonicalIdentity(Map<String, Map<String, Object>> definitions, String typeId) {
        return CanonicalPayloadBindingLookup.resolve(definitions, typeId)
            .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::canonicalTypeId)
            .orElse(typeId);
    }

    private record StoredExpression(String elementTypeId, boolean collection) {
        private static StoredExpression parse(String canonicalTypeId) {
            if (canonicalTypeId.startsWith("List<") && canonicalTypeId.endsWith(">")) {
                String element = canonicalTypeId.substring("List<".length(), canonicalTypeId.length() - 1);
                if (element.isBlank()) {
                    throw new IllegalArgumentException("Typed durable payload list expression has no element type");
                }
                return new StoredExpression(element, true);
            }
            if (canonicalTypeId.isBlank()) {
                throw new IllegalArgumentException("Typed durable payload has no canonical type identity");
            }
            return new StoredExpression(canonicalTypeId, false);
        }
    }

    private record ReleaseCacheKey(String tenantId, DurablePayloadReleaseCoordinate coordinate) {
    }

    private static String canonicalTypeId(PipelineContractDescriptor contract, ExecutionRecord<?, ?> execution, Slot slot) {
        if (slot != Slot.CONTINUATION_INPUT && !contract.rootBoundary().isEmpty()) {
            return (String) contract.rootBoundary().get(slot == Slot.INPUT ? "inputCanonicalTypeId" : "outputCanonicalTypeId");
        }
        if (contract.composition().present() && slot != Slot.CONTINUATION_INPUT) {
            var root = contract.composition().definition(contract.composition().rootDefinitionId());
            return slot == Slot.INPUT ? root.inputContractId() : root.outputContractId();
        }
        List<PipelineBundleStepDescriptor> steps = contract.steps();
        if (slot == Slot.INPUT) {
            // Execution input is immutable across retries, awaits, and later pipeline steps. Its
            // identity is the pipeline entry contract, never the currently resumed step.
            return steps.getFirst().inputTypeId();
        }
        if (slot == Slot.CONTINUATION_INPUT) {
            int current = execution.currentStepIndex();
            // A continuation is admitted to the step at the current cursor. In a branch
            // topology the preceding ordered step may belong to a different branch, so its
            // output is not the continuation contract. The receiving step's input is.
            return current >= 0 && current < steps.size()
                ? steps.get(current).inputTypeId()
                // Generated side-effect steps can extend the runtime cursor beyond the
                // semantic contract. At that boundary, itemized results re-enter through
                // the contract's terminal canonical output rather than the entry input.
                : steps.getLast().outputTypeId();
        }
        // RESULT stores completed execution/segment output, even while markSucceeded still
        // holds the pre-terminal cursor. Shape controls collection materialization, not the
        // canonical element identity; only the pinned terminal output selects that binding.
        return steps.getLast().outputTypeId();
    }

    private static String required(Map<String, Object> definition, String field, String id) {
        Object value = definition.get(field);
        if (!(value instanceof String string) || string.isBlank()) {
            throw new IllegalStateException("pinned release canonical binding " + id + " does not declare " + field);
        }
        return string;
    }

    private static String fingerprint(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception error) {
            throw new IllegalStateException("Unable to fingerprint canonical type expression", error);
        }
    }

    private static IllegalStateException failure(ExecutionRecord<?, ?> execution, Slot slot, String canonicalTypeId, String action, Exception cause) {
        return new IllegalStateException("Execution durable payload " + action + " failed: executionId=" + execution.executionId()
            + ", release=" + execution.releaseVersion() + ", slot=" + slot + ", canonicalTypeId=" + canonicalTypeId, cause);
    }
}

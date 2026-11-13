package org.pipelineframework.awaitable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.pipelineframework.orchestrator.CanonicalPayloadBinding;
import org.pipelineframework.orchestrator.BoundedExpiringCache;
import org.pipelineframework.orchestrator.CompiledDurablePayloadPlan;
import org.pipelineframework.orchestrator.DurablePayloadPlanRegistry;
import org.pipelineframework.orchestrator.DurablePayloadReleaseCoordinate;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.CanonicalPayloadBindingLookup;
import org.pipelineframework.orchestrator.CanonicalPayloadRuntimeClassLoader;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Resolves pinned-release plans for the canonical request/response fields of await interactions. */
@ApplicationScoped
public class AwaitDurablePayloadResolver {
    public enum Slot { REQUEST, RESPONSE, COMMAND_ACKNOWLEDGEMENT, STAGED_COMPLETION }

    /** Direct override for focused tests; production selection is provider-name based. */
    ExecutionStateStore executionStateStore;
    @Inject Instance<ExecutionStateStore> executionStateStores;
    @Inject PipelineOrchestratorConfig orchestratorConfig;
    @Inject PipelineReleaseRegistry releaseRegistry;
    @Inject AwaitCompletionDescriptorRegistry descriptors;
    @Inject JsonDurablePayloadCodec codec;

    private final DurablePayloadPlanRegistry plans = new DurablePayloadPlanRegistry();
    private static final int RELEASE_CACHE_MAXIMUM_SIZE = 256;
    private static final Duration RELEASE_CACHE_EXPIRY = Duration.ofMinutes(15);
    private static final int EXECUTION_CACHE_MAXIMUM_SIZE = 4_096;
    private final BoundedExpiringCache<ReleaseCacheKey, PipelineReleaseRecord> releases =
        new BoundedExpiringCache<>(RELEASE_CACHE_MAXIMUM_SIZE, RELEASE_CACHE_EXPIRY);
    private final BoundedExpiringCache<ExecutionCacheKey, PinnedExecution> executions =
        new BoundedExpiringCache<>(EXECUTION_CACHE_MAXIMUM_SIZE, RELEASE_CACHE_EXPIRY);
    private static final Duration RELEASE_LOOKUP_TIMEOUT = Duration.ofSeconds(10);

    public String encode(AwaitInteractionRecord interaction, Slot slot, Object value) {
        if (serializedMode()) {
            if (!(value instanceof TypedDurablePayload payload)) {
                throw new IllegalArgumentException("Serialized Await coordination requires a canonical envelope");
            }
            validateSerialized(interaction, slot, payload);
            try {
                return PipelineJson.mapper().writeValueAsString(payload);
            } catch (Exception error) {
                throw failure(interaction, slot, payload.canonicalTypeId(), "encode", error);
            }
        }
        CompiledDurablePayloadPlan plan = resolve(interaction, slot);
        try {
            return PipelineJson.mapper().writeValueAsString(codec.encode(value, plan));
        } catch (Exception e) {
            throw failure(interaction, slot, plan.binding().canonicalTypeId(), "encode", e);
        }
    }

    /** Returns whether the interaction's pinned release has the v3 canonical binding catalog. */
    public boolean supportsTypedPayloads(AwaitInteractionRecord interaction) {
        return findOwningExecution(interaction)
            .map(execution -> pinnedRelease(interaction.tenantId(), execution).contract().schemaVersion() >= 2)
            // Remote transition workers do not own coordinator execution state. They return a
            // transport snapshot to the coordinator, which owns durable payload persistence.
            .orElse(false);
    }

    /**
     * Resolves and caches the immutable release metadata before a concurrent itemized await dispatch
     * starts. This keeps the release-registry lookup out of the per-item Dynamo worker pool.
     */
    public Uni<Void> preload(String tenantId, String executionId) {
        ExecutionCacheKey key = new ExecutionCacheKey(tenantId, executionId);
        return executionStateStore().getExecution(tenantId, executionId)
            .onItem().transformToUni(optional -> {
                if (optional.isEmpty()) {
                    return Uni.createFrom().voidItem();
                }
                return preloadRelease(key, PinnedExecution.from(optional.get()), tenantId).replaceWithVoid();
            })
            .onFailure(OwningExecutionUnavailableException.class).recoverWithUni(Uni.createFrom().voidItem());
    }

    public Object decode(AwaitInteractionRecord interaction, Slot slot, String stored) {
        try {
            TypedDurablePayload payload = TypedDurablePayload.fromSerializedBytes(stored.getBytes(StandardCharsets.UTF_8))
                .orElseThrow(() -> new IllegalArgumentException("not a typed durable payload"));
            return decodeEnvelope(interaction, slot, payload);
        } catch (Exception e) {
            throw failure(interaction, slot, slot.name().toLowerCase(), "decode", e);
        }
    }

    /** Restores a typed envelope that crossed a transition command as JSON object metadata. */
    public Object decodeEnvelope(AwaitInteractionRecord interaction, Slot slot, TypedDurablePayload payload) {
        try {
            if (serializedMode()) {
                validateSerialized(interaction, slot, payload);
                return payload;
            }
            CompiledDurablePayloadPlan plan = resolve(interaction, slot);
            return codec.decode(payload, plan);
        } catch (Exception e) {
            throw failure(interaction, slot, slot.name().toLowerCase(), "decode", e);
        }
    }

    boolean serializedMode() {
        return orchestratorConfig != null && orchestratorConfig.controlPlane() != null
            && orchestratorConfig.controlPlane().payloadMaterialization()
                == PipelineOrchestratorConfig.ControlPlaneConfig.PayloadMaterialization.SERIALIZED;
    }

    DurablePayloadReleaseCoordinate coordinateFor(AwaitInteractionRecord interaction) {
        return owningExecution(interaction).coordinate();
    }

    public PinnedAwaitDescriptor pinnedDescriptor(AwaitInteractionRecord interaction) {
        PipelineReleaseRecord release = pinnedRelease(interaction.tenantId(), owningExecution(interaction));
        PinnedAwaitDescriptor descriptor = PinnedAwaitDescriptor.resolve(release.contract(), interaction.stepId());
        if (!descriptor.outputTypeId().equals(interaction.outputType())
            || !descriptor.transportOutputTypeId().equals(interaction.transportOutputType())
            || !descriptor.completionProjectorId().equals(java.util.Optional.ofNullable(
                interaction.transportMetadata().get("tpf.await.completion.projector")).map(Object::toString))
            || !descriptor.callbackContract().equals(java.util.Optional.ofNullable(
                interaction.transportMetadata().get("callbackSelection")))) {
            throw new IllegalStateException("Await interaction differs from its pinned descriptor");
        }
        return descriptor;
    }

    private TypedDurablePayload serializedEnvelope(AwaitInteractionRecord interaction, Slot slot, byte[] bytes) {
        PipelineReleaseRecord release = pinnedRelease(interaction.tenantId(), owningExecution(interaction));
        PinnedAwaitDescriptor descriptor = pinnedDescriptor(interaction);
        String canonicalTypeId = switch (slot) {
            case REQUEST -> descriptor.canonicalInputTypeId();
            case RESPONSE -> descriptor.canonicalOutputTypeId();
            case COMMAND_ACKNOWLEDGEMENT -> descriptor.canonicalCommandAcknowledgementTypeId()
                .orElseThrow(() -> new IllegalStateException("Pinned Await has no Command acknowledgement"));
            case STAGED_COMPLETION -> descriptor.completionPayloadTypeId();
        };
        var definition = release.contract().canonicalTypes().get(canonicalTypeId);
        if (definition == null) {
            String requestedType = canonicalTypeId;
            var matches = release.contract().canonicalTypes().entrySet().stream()
                .filter(entry -> requestedType.equals(entry.getValue().get("runtimeClass"))).toList();
            if (matches.size() != 1) throw new IllegalStateException("Pinned Await canonical binding is unavailable or ambiguous");
            canonicalTypeId = matches.getFirst().getKey();
            definition = matches.getFirst().getValue();
        }
        return new TypedDurablePayload(canonicalTypeId,
            requiredString(definition, "definitionFingerprint", canonicalTypeId),
            release.contract().canonicalCatalogFingerprint(), JsonDurablePayloadCodec.ENCODING,
            JsonDurablePayloadCodec.ENCODING_VERSION, bytes);
    }

    private void validateSerialized(AwaitInteractionRecord interaction, Slot slot, TypedDurablePayload payload) {
        TypedDurablePayload expected = serializedEnvelope(interaction, slot, payload.payload());
        if (!expected.canonicalTypeId().equals(payload.canonicalTypeId())
            || !expected.typeExpressionFingerprint().equals(payload.typeExpressionFingerprint())
            || !expected.catalogFingerprint().equals(payload.catalogFingerprint())
            || !expected.encoding().equals(payload.encoding()) || expected.encodingVersion() != payload.encodingVersion()) {
            throw new IllegalArgumentException("Await canonical envelope differs from pinned slot");
        }
        try (var parser = PipelineJson.mapper().createParser(payload.payload())) {
            parser.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
            com.fasterxml.jackson.databind.JsonNode body = PipelineJson.mapper().readTree(parser);
            if (body == null || parser.nextToken() != null) {
                throw new IllegalArgumentException("Invalid Await canonical JSON framing");
            }
            var release = pinnedRelease(interaction.tenantId(), owningExecution(interaction));
            org.pipelineframework.type.CanonicalTypeCatalogue.fromCanonicalTypes(release.contract().canonicalTypes())
                .validateAndCanonicalize(payload.canonicalTypeId(), body.toString());
        } catch (java.io.IOException error) {
            throw new IllegalArgumentException("Invalid Await canonical JSON framing", error);
        }
    }

    /**
     * Decode-only compatibility for the former class-wrapper JSON shape. The stored class hint is
     * deliberately ignored: the pinned release and expected slot choose the only permissible target.
     */
    public Object decodeLegacy(AwaitInteractionRecord interaction, Slot slot, String stored) {
        if (serializedMode()) {
            try {
                Object decoded = PipelineJson.mapper().readerFor(Object.class)
                    .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readValue(stored);
                Object body = decoded instanceof Map<?, ?> map && map.containsKey("_tpf_payload")
                    ? map.get("_tpf_payload") : decoded;
                TypedDurablePayload payload = serializedEnvelope(interaction, slot, PipelineJson.mapper().writeValueAsBytes(body));
                validateSerialized(interaction, slot, payload);
                return payload;
            } catch (Exception error) {
                throw failure(interaction, slot, slot.name(), "legacy decode", error);
            }
        }
        CompiledDurablePayloadPlan plan = resolve(interaction, slot);
        try {
            Object decoded = PipelineJson.mapper().readValue(stored, Object.class);
            Object body = decoded instanceof Map<?, ?> map && map.containsKey("_tpf_payload")
                ? map.get("_tpf_payload")
                : decoded;
            byte[] canonicalBytes = PipelineJson.mapper().writeValueAsBytes(body);
            return codec.decode(new TypedDurablePayload(
                plan.binding().canonicalTypeId(),
                plan.binding().typeExpressionFingerprint(),
                plan.binding().catalogFingerprint(),
                JsonDurablePayloadCodec.ENCODING,
                JsonDurablePayloadCodec.ENCODING_VERSION,
                canonicalBytes), plan);
        } catch (Exception e) {
            throw failure(interaction, slot, plan.binding().canonicalTypeId(), "legacy decode", e);
        }
    }

    private CompiledDurablePayloadPlan resolve(AwaitInteractionRecord interaction, Slot slot) {
        PinnedExecution execution = owningExecution(interaction);
        PipelineReleaseRecord release = pinnedRelease(interaction.tenantId(), execution);
        AwaitCompletionDescriptor descriptor = descriptors.descriptorByStepIdNow(interaction.stepId());
        String requestedTypeId = switch (slot) {
            case REQUEST -> descriptor.inputType();
            case RESPONSE -> interaction.outputType();
            case COMMAND_ACKNOWLEDGEMENT -> {
                if (!descriptor.acknowledgementAwareCommandCompletion()) {
                    throw new IllegalStateException("Command acknowledgement is unavailable in the pinned descriptor");
                }
                yield descriptor.commandAcknowledgementType().orElseThrow();
            }
            case STAGED_COMPLETION -> {
                if (!descriptor.acknowledgementAwareCommandCompletion()) {
                    throw new IllegalStateException("Staged callback is unavailable in the pinned descriptor");
                }
                // Retain the callback contract here. outputType is the final projected value,
                // which can have a different shape and does not exist until acknowledgement joins.
                yield descriptor.transportOutputType();
            }
        };
        var resolvedDefinition = CanonicalPayloadBindingLookup.resolve(
            release.contract().canonicalTypes(), requestedTypeId);
        String canonicalTypeId = resolvedDefinition
            .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::canonicalTypeId)
            .orElse(requestedTypeId);
        Map<String, Object> canonicalDefinition = resolvedDefinition
            .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::definition)
            .orElse(Map.of());
        String runtimeType;
        String expression;
        String catalog;
        if (resolvedDefinition.isPresent()) {
            runtimeType = requiredString(canonicalDefinition, "runtimeClass", canonicalTypeId);
            expression = requiredString(canonicalDefinition, "definitionFingerprint", canonicalTypeId);
            catalog = release.contract().canonicalCatalogFingerprint();
            if (catalog.isBlank()) {
                throw new IllegalStateException("pinned release canonical catalog fingerprint is unavailable");
            }
        } else if (release.contract().schemaVersion() == 1) {
            runtimeType = requestedTypeId;
            expression = fingerprint(canonicalTypeId);
            catalog = release.contract().contractHash();
        } else {
            throw new IllegalStateException("pinned release has no canonical binding for " + canonicalTypeId);
        }
        if (slot == Slot.RESPONSE) {
            String descriptorOutputType = CanonicalPayloadBindingLookup.resolve(
                release.contract().canonicalTypes(), descriptor.outputType())
                .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::canonicalTypeId)
                .orElse(descriptor.outputType());
            if (!descriptorOutputType.equals(canonicalTypeId)) {
                throw new IllegalStateException("await durable-contract compatibility failed for stepId="
                    + interaction.stepId() + ": durable canonical outputType=" + canonicalTypeId
                    + " differs from rebuilt descriptor outputType=" + descriptorOutputType);
            }
        }
        DurablePayloadReleaseCoordinate coordinate = execution.coordinate();
        var cached = plans.find(coordinate, expression);
        if (cached.isPresent()) {
            return cached.get();
        }
        Class<?> runtimeClass;
        try {
            runtimeClass = CanonicalPayloadRuntimeClassLoader.load(runtimeType, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("pinned release binding class is unavailable: " + runtimeType, e);
        }
        CanonicalPayloadBinding binding = new CanonicalPayloadBinding(canonicalTypeId, expression,
            catalog, runtimeClass);
        plans.activate(coordinate, Map.of(expression, binding));
        return plans.plan(coordinate, expression);
    }

    private PipelineReleaseRecord pinnedRelease(String tenantId, PinnedExecution execution) {
        DurablePayloadReleaseCoordinate coordinate = execution.coordinate();
        return releases.getOrLoad(new ReleaseCacheKey(tenantId, coordinate), ignored -> validateRelease(
            tenantId, execution, releaseRegistry.get(tenantId, execution.pipelineId(), execution.releaseVersion())
                .await().atMost(RELEASE_LOOKUP_TIMEOUT)
                .orElseThrow(() -> new IllegalStateException("pinned release is unavailable"))));
    }

    private static PipelineReleaseRecord validateRelease(String tenantId, PinnedExecution execution,
        PipelineReleaseRecord release) {
        if (!tenantId.equals(release.tenantId()) || !execution.pipelineId().equals(release.pipelineId())
            || !execution.contractVersion().equals(release.contractVersion())
            || !execution.releaseVersion().equals(release.releaseVersion())) {
            throw new IllegalStateException("Await resolved Release differs from owning execution pin");
        }
        return release;
    }

    private Uni<PipelineReleaseRecord> preloadRelease(
        ExecutionCacheKey executionKey,
        PinnedExecution execution,
        String tenantId
    ) {
        executions.getOrLoad(executionKey, ignored -> execution);
        DurablePayloadReleaseCoordinate coordinate = execution.coordinate();
        return releaseRegistry.get(tenantId, execution.pipelineId(), execution.releaseVersion())
            .onItem().transform(optional -> optional
                .orElseThrow(() -> new OwningExecutionUnavailableException()))
            .onItem().invoke(release -> releases.getOrLoad(
                new ReleaseCacheKey(tenantId, coordinate), ignored -> validateRelease(tenantId, execution, release)));
    }

    private PinnedExecution owningExecution(AwaitInteractionRecord interaction) {
        return findOwningExecution(interaction)
            .orElseThrow(() -> new IllegalStateException("owning execution is unavailable"));
    }

    private java.util.Optional<PinnedExecution> findOwningExecution(AwaitInteractionRecord interaction) {
        ExecutionCacheKey key = new ExecutionCacheKey(interaction.tenantId(), interaction.executionId());
        try {
            return java.util.Optional.of(executions.getOrLoad(key, ignored -> executionStateStore()
                .getExecution(interaction.tenantId(), interaction.executionId())
                .await().atMost(RELEASE_LOOKUP_TIMEOUT)
                .map(PinnedExecution::from)
                .orElseThrow(() -> new OwningExecutionUnavailableException())));
        } catch (OwningExecutionUnavailableException ignored) {
            return java.util.Optional.empty();
        }
    }

    private record ReleaseCacheKey(String tenantId, DurablePayloadReleaseCoordinate coordinate) {
    }

    private record ExecutionCacheKey(String tenantId, String executionId) {
    }

    private record PinnedExecution(String pipelineId, String contractVersion, String releaseVersion) {
        private static PinnedExecution from(ExecutionRecord<?, ?> execution) {
            return new PinnedExecution(execution.pipelineId(), execution.contractVersion(), execution.releaseVersion());
        }

        private DurablePayloadReleaseCoordinate coordinate() {
            return new DurablePayloadReleaseCoordinate(pipelineId, contractVersion, releaseVersion);
        }
    }

    private static final class OwningExecutionUnavailableException extends RuntimeException {
    }

    private static IllegalStateException failure(AwaitInteractionRecord interaction, Slot slot, String type, String action, Exception cause) {
        return new IllegalStateException("Await durable payload " + action + " failed: interactionId="
            + interaction.interactionId() + ", executionId=" + interaction.executionId() + ", slot=" + slot
            + ", canonicalTypeId=" + type, cause);
    }

    private ExecutionStateStore executionStateStore() {
        if (executionStateStore != null) {
            return executionStateStore;
        }
        if (executionStateStores == null || orchestratorConfig == null) {
            throw new IllegalStateException("Await durable payload resolver has no execution-state provider");
        }
        String requested = orchestratorConfig.stateProvider();
        return executionStateStores.stream()
            .filter(store -> requested == null || requested.isBlank() || requested.equalsIgnoreCase(store.providerName()))
            .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No ExecutionStateStore provider found for '" + requested + "'"));
    }

    private static String fingerprint(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to fingerprint canonical type expression", e);
        }
    }

    private static String requiredString(Map<String, Object> definition, String field, String canonicalTypeId) {
        Object value = definition.get(field);
        if (!(value instanceof String string) || string.isBlank()) {
            throw new IllegalStateException("pinned release canonical binding " + canonicalTypeId
                + " does not declare " + field);
        }
        return string;
    }
}

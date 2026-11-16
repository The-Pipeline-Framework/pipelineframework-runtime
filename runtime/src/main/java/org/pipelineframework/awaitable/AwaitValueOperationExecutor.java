package org.pipelineframework.awaitable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.CompiledCanonicalTransitionPayloadCodec;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** Runs pure typed Await value operations; never owns interaction state or completion admission. */
@ApplicationScoped
public class AwaitValueOperationExecutor {
    @Inject PipelineReleaseIdentityResolver identities;
    @Inject PipelineOrchestratorConfig config;
    @Inject AwaitCompletionDescriptorRegistry descriptors;
    @Inject TransitionPayloadCodec framing;

    /** Advertises support only when this package carries usable immutable Await metadata. */
    public boolean supported() {
        try {
            Optional<PipelineContractDescriptor> compiled = identities.compiledContract();
            if (compiled.isEmpty()) return false;
            boolean found = false;
            for (var step : compiled.orElseThrow().steps()) {
                if (step.deferredCompletion().isEmpty()) continue;
                Object value = step.deferredCompletion().get("runtimeDescriptor");
                if (!(value instanceof Map<?, ?> metadata) || !(metadata.get("stepId") instanceof String stepId)) {
                    return false;
                }
                PinnedAwaitDescriptor pinned = PinnedAwaitDescriptor.resolve(compiled.orElseThrow(), stepId);
                if (!matchesDescriptor(pinned, descriptors.descriptorByStepIdNow(stepId))) return false;
                found = true;
            }
            return found;
        } catch (IllegalArgumentException | IllegalStateException unavailable) {
            return false;
        }
    }

    public Uni<AwaitValueOperationResult> execute(AwaitValueOperationRequest request) {
        return Uni.createFrom().item(() -> executeNow(request));
    }

    private AwaitValueOperationResult executeNow(AwaitValueOperationRequest request) {
        if (!identities.pipelineId(config).equals(request.pipelineId())
            || !identities.contractVersion().equals(request.contractVersion())
            || !identities.releaseVersion(config).equals(request.releaseVersion())) {
            throw new IllegalArgumentException("Await value operation differs from the compiled Release");
        }
        PipelineContractDescriptor contract = identities.compiledContract().orElseThrow(() ->
            new IllegalStateException("Await value operations require compiled Release metadata"));
        PinnedAwaitDescriptor pinned = PinnedAwaitDescriptor.resolve(contract, request.stepId());
        AwaitCompletionDescriptor descriptor = descriptors.descriptorByStepIdNow(request.stepId());
        validateDescriptor(request, pinned, descriptor);
        Object value;
        String resultType;
        switch (request.operation()) {
            case NORMALIZE_ACKNOWLEDGEMENT -> {
                resultType = pinned.canonicalCommandAcknowledgementTypeId().orElseThrow();
                value = normalize(request.acknowledgementPayload().orElseThrow(), contract, resultType,
                    descriptor.commandAcknowledgementType().orElseThrow(), java.util.function.Function.identity());
            }
            case NORMALIZE_COMPLETION -> {
                resultType = pinned.completionPayloadTypeId();
                value = normalize(request.completionPayload().orElseThrow(), contract, resultType,
                    descriptor.transportOutputType(), descriptor.outputFromTransport());
            }
            case PROJECT_COMPLETION, NORMALIZE_AND_PROJECT_COMPLETION -> {
                Object input = codec(contract, pinned.canonicalInputTypeId())
                    .decode(request.requestPayload().orElseThrow());
                Object completion = request.operation() == AwaitValueOperation.NORMALIZE_AND_PROJECT_COMPLETION
                    ? normalize(request.completionPayload().orElseThrow(), contract, pinned.completionPayloadTypeId(),
                        descriptor.transportOutputType(), descriptor.outputFromTransport())
                    : codec(contract, pinned.completionPayloadTypeId()).decode(request.completionPayload().orElseThrow());
                AwaitCompletionMetadata metadata = new AwaitCompletionMetadata(request.interactionId(), request.actor(),
                    Instant.ofEpochMilli(request.admittedAtEpochMs()));
                if (descriptor.acknowledgementAwareCommandCompletion()) {
                    Object acknowledgement = codec(contract, pinned.canonicalCommandAcknowledgementTypeId().orElseThrow())
                        .decode(request.acknowledgementPayload().orElseThrow());
                    value = descriptor.commandCompletionProjector().orElseThrow()
                        .project(input, acknowledgement, completion, metadata);
                } else {
                    value = descriptor.requestAwareCompletion()
                        ? descriptor.completionProjector().project(input, completion, metadata) : completion;
                }
                resultType = pinned.canonicalOutputTypeId();
            }
            default -> throw new IllegalArgumentException("Unsupported Await value operation");
        }
        return new AwaitValueOperationResult(request.operation(), request.operationId(), request.requestDigest(),
            codec(contract, resultType).encode(value));
    }

    private Object normalize(SerializedTransitionPayload payload, PipelineContractDescriptor contract,
        String canonicalType, String transportType, java.util.function.Function<Object, Object> conversion) {
        if (CompiledCanonicalTransitionPayloadCodec.containsCanonicalPayload(framing, payload)) {
            return codec(contract, canonicalType).decode(payload);
        }
        try {
            Class<?> expected = AwaitPayloadSupport.resolvePayloadClass(transportType,
                Thread.currentThread().getContextClassLoader());
            return conversion.apply(AwaitPayloadSupport.coercePayload(framing.decode(payload), expected));
        } catch (ClassNotFoundException failure) {
            throw new IllegalStateException("Compiled Await representation is unavailable", failure);
        }
    }

    private CompiledCanonicalTransitionPayloadCodec codec(PipelineContractDescriptor contract, String type) {
        return new CompiledCanonicalTransitionPayloadCodec(framing, contract, type, type);
    }

    private static void validateDescriptor(AwaitValueOperationRequest request, PinnedAwaitDescriptor pinned,
        AwaitCompletionDescriptor descriptor) {
        if (!matchesDescriptor(pinned, descriptor)
            || !pinned.completionProjectorId().equals(request.completionProjectorId())
            || !pinned.callbackContract().map(AwaitValueOperationExecutor::callbackFingerprint)
                .equals(request.callbackContractFingerprint())) {
            throw new IllegalArgumentException("Await value operation descriptor differs from the pinned compiled contract");
        }
    }

    private static boolean matchesDescriptor(PinnedAwaitDescriptor pinned, AwaitCompletionDescriptor descriptor) {
        return pinned.inputTypeId().equals(descriptor.inputType())
            && pinned.outputTypeId().equals(descriptor.outputType())
            && pinned.transportInputTypeId().equals(descriptor.transportInputType())
            && pinned.transportOutputTypeId().equals(descriptor.transportOutputType())
            && pinned.requestAwareCompletion() == descriptor.requestAwareCompletion()
            && pinned.completionProjectorId().equals(Optional.ofNullable(descriptor.completionProjectorId()))
            && pinned.commandAcknowledgementTypeId().equals(descriptor.commandAcknowledgementType())
            && pinned.callbackContract().equals(descriptor.callback().map(ConnectorCallbackSelection::contractMetadata));
    }

    /** Callback identity digest shared by the caller and selected compiled worker. */
    public static String callbackFingerprint(Map<String, Object> contract) {
        try {
            byte[] bytes = PipelineJson.mapper().writeValueAsString(new TreeMap<>(contract)).getBytes(StandardCharsets.UTF_8);
            return "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception failure) {
            throw new IllegalStateException("Unable to fingerprint callback contract", failure);
        }
    }
}

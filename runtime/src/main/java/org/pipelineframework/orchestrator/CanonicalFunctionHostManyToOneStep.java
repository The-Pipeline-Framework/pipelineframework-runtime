package org.pipelineframework.orchestrator;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepManyToOne;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Incremental streaming {@link StepManyToOne} adapter over chunked NDJSON HTTP requests
 * ({@code POST /tpf/functions/{stepId}/reduce}) using JDK {@link HttpRequest.BodyPublishers#fromPublisher}.
 *
 * <p>Upstream {@link Multi} items are validated and streamed incrementally with end-to-end
 * Reactive Streams backpressure straight from the HTTP socket — zero {@code List<T>}
 * materialization and zero blocking pipe threads.</p>
 */
public final class CanonicalFunctionHostManyToOneStep extends ConfigurableStep
    implements StepManyToOne<TypedDurablePayload, TypedDurablePayload> {

    public static final String REDUCE_SUFFIX = "/reduce";
    public static final String STREAMING_BODY_DIGEST_SENTINEL = "STREAMING-NDJSON-V1";

    private static final String PROTOCOL = "function-host-reduce";
    private static final byte[] COMPLETE_FRAME = "{\"event\":\"complete\"}\n".getBytes(StandardCharsets.UTF_8);

    /** Immutable binding for one streaming MANY_TO_ONE foreign reducer function. */
    public record Spec(
        String stepId,
        int stepIndex,
        String inputCanonicalTypeId,
        String inputDefinitionFingerprint,
        String outputCanonicalTypeId,
        String outputDefinitionFingerprint,
        String catalogFingerprint,
        CanonicalTypeCatalogue catalogue,
        URI baseUri,
        String sharedSecret,
        Duration requestTimeout,
        Optional<String> commandId,
        ForeignDispatchIntentStore dispatchIntentStore,
        int maxResponseBytes
    ) {
        public Spec {
            CanonicalFunctionHostSupport.requireNonBlank(stepId, "stepId");
            if (stepIndex < 0) {
                throw new IllegalArgumentException("stepIndex must be non-negative");
            }
            CanonicalFunctionHostSupport.requireNonBlank(inputCanonicalTypeId, "inputCanonicalTypeId");
            CanonicalFunctionHostSupport.requireNonBlank(inputDefinitionFingerprint, "inputDefinitionFingerprint");
            CanonicalFunctionHostSupport.requireNonBlank(outputCanonicalTypeId, "outputCanonicalTypeId");
            CanonicalFunctionHostSupport.requireNonBlank(outputDefinitionFingerprint, "outputDefinitionFingerprint");
            CanonicalFunctionHostSupport.requireNonBlank(catalogFingerprint, "catalogFingerprint");
            Objects.requireNonNull(catalogue, "catalogue");
            Objects.requireNonNull(baseUri, "baseUri");
            CanonicalFunctionHostSupport.requireNonBlank(sharedSecret, "sharedSecret");
            CanonicalFunctionHostSupport.requirePositive(requestTimeout, "requestTimeout");
            commandId = Objects.requireNonNull(commandId, "commandId").filter(v -> !v.isBlank());
            Objects.requireNonNull(dispatchIntentStore, "dispatchIntentStore");
            if (maxResponseBytes <= 0) {
                throw new IllegalArgumentException("maxResponseBytes must be positive");
            }
        }

        public Spec(
            String stepId,
            int stepIndex,
            String inputCanonicalTypeId,
            String inputDefinitionFingerprint,
            String outputCanonicalTypeId,
            String outputDefinitionFingerprint,
            String catalogFingerprint,
            CanonicalTypeCatalogue catalogue,
            URI baseUri,
            String sharedSecret,
            Duration requestTimeout,
            Optional<String> commandId
        ) {
            this(stepId, stepIndex, inputCanonicalTypeId, inputDefinitionFingerprint,
                outputCanonicalTypeId, outputDefinitionFingerprint, catalogFingerprint,
                catalogue, baseUri, sharedSecret, requestTimeout, commandId,
                ForeignDispatchIntentStore.noop(), CanonicalFunctionHostSupport.DEFAULT_MAX_RESPONSE_BYTES);
        }

        public Spec withDispatchIntentStore(ForeignDispatchIntentStore store) {
            return new Spec(stepId, stepIndex, inputCanonicalTypeId, inputDefinitionFingerprint,
                outputCanonicalTypeId, outputDefinitionFingerprint, catalogFingerprint,
                catalogue, baseUri, sharedSecret, requestTimeout, commandId,
                store, maxResponseBytes);
        }
    }

    private final Spec spec;
    private final HttpClient httpClient;

    public CanonicalFunctionHostManyToOneStep(Spec spec, boolean allowLoopbackHttp) {
        this(spec, allowLoopbackHttp, CanonicalFunctionHostSupport.newHttpClient(
            Objects.requireNonNull(spec, "spec").requestTimeout()));
    }

    CanonicalFunctionHostManyToOneStep(Spec spec, boolean allowLoopbackHttp, HttpClient httpClient) {
        this.spec = Objects.requireNonNull(spec, "spec");
        RegisteredRestWorkerTargets.normalizeOrigin(spec.baseUri().toString(), allowLoopbackHttp);
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
    }

    public Spec spec() {
        return spec;
    }

    @Override
    public boolean shouldRetry(Throwable failure) {
        if (failure == null || CanonicalFunctionHostSupport.isNonRetryableOrUnknown(failure)) {
            return false;
        }
        return super.shouldRetry(failure);
    }

    @Override
    public Uni<TypedDurablePayload> applyReduce(Multi<TypedDurablePayload> upstream) {
        CanonicalFunctionHostSupport.CapturedContext captured = CanonicalFunctionHostSupport.CapturedContext.capture();
        return Uni.createFrom().deferred(() -> {
            long startNanos = System.nanoTime();
            long deadlineMs = spec.requestTimeout().toMillis();
            String path = CanonicalFunctionHostStep.FUNCTION_PATH_PREFIX + spec.stepId() + REDUCE_SUFFIX;
            URI target = spec.baseUri().resolve(path);
            byte[] sentinelBytes = STREAMING_BODY_DIGEST_SENTINEL.getBytes(StandardCharsets.UTF_8);

            Optional<ForeignDispatchIntent> reservedIntent = CanonicalFunctionHostSupport.reserveDispatchIntent(
                spec.dispatchIntentStore(), captured.dispatchScope(), spec.stepIndex(), spec.stepId(),
                target, sentinelBytes, PROTOCOL, startNanos, deadlineMs);

            AtomicReference<Throwable> upstreamFailure = new AtomicReference<>();
            Multi<ByteBuffer> ndjsonBody = Multi.createBy().concatenating().streams(
                    upstream.onItem().transform(this::encodeValidatedItemFrame),
                    Multi.createFrom().item(() -> ByteBuffer.wrap(COMPLETE_FRAME)))
                .onFailure().invoke(err -> upstreamFailure.compareAndSet(null, err));

            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(target)
                .timeout(spec.requestTimeout())
                .header("Content-Type", "application/x-ndjson")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.fromPublisher(ndjsonBody));
            CanonicalFunctionHostSupport.signHeaders(
                requestBuilder, spec.sharedSecret(), path, sentinelBytes, spec.stepId(), spec.stepIndex());

            return Uni.createFrom().completionStage(() -> httpClient.sendAsync(
                    requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray()))
                .onFailure().transform(error -> {
                    Throwable upErr = upstreamFailure.get();
                    Throwable resolved = upErr != null
                        ? upErr
                        : CanonicalFunctionHostSupport.unknownOutcome(
                            PROTOCOL, target, startNanos, deadlineMs,
                            "Uncertain transport outcome invoking foreign reducer " + spec.stepId(),
                            Optional.of(error));
                    CanonicalFunctionHostSupport.settleDispatchIntent(
                        spec.dispatchIntentStore(), reservedIntent,
                        CanonicalFunctionHostSupport.settlementForFailure(resolved));
                    return resolved;
                })
                .onItem().transform(response -> {
                    CanonicalFunctionHostSupport.CapturedContext previous = captured.install();
                    try {
                        Throwable upErr = upstreamFailure.get();
                        if (upErr != null) {
                            CanonicalFunctionHostSupport.settleDispatchIntent(
                                spec.dispatchIntentStore(), reservedIntent,
                                CanonicalFunctionHostSupport.settlementForFailure(upErr));
                            if (upErr instanceof RuntimeException runtime) {
                                throw runtime;
                            }
                            throw new IllegalStateException("Upstream failure during reduce", upErr);
                        }
                        TypedDurablePayload result = decodeResponse(response, target, startNanos, deadlineMs);
                        CanonicalFunctionHostSupport.settleDispatchIntent(
                            spec.dispatchIntentStore(), reservedIntent,
                            ForeignDispatchIntentStore.SettlementStatus.VALIDATED_SUCCEEDED);
                        return result;
                    } catch (RuntimeException failure) {
                        CanonicalFunctionHostSupport.settleDispatchIntent(
                            spec.dispatchIntentStore(), reservedIntent,
                            CanonicalFunctionHostSupport.settlementForFailure(failure));
                        throw failure;
                    } finally {
                        previous.restore();
                    }
                });
        });
    }

    private ByteBuffer encodeValidatedItemFrame(TypedDurablePayload item) {
        String canonicalJson = CanonicalFunctionHostSupport.validateCanonicalInput(
            item, spec.stepId(), spec.stepIndex(), spec.commandId(),
            spec.inputCanonicalTypeId(), spec.inputDefinitionFingerprint(),
            spec.catalogFingerprint(), spec.catalogue());
        try {
            Map<String, Object> frame = new LinkedHashMap<>();
            frame.put("event", "item");
            frame.put("inputTypeId", spec.inputCanonicalTypeId());
            frame.put("payload", CanonicalFunctionHostSupport.json().readTree(canonicalJson));
            return ByteBuffer.wrap(
                (CanonicalFunctionHostSupport.json().writeValueAsString(frame) + "\n").getBytes(StandardCharsets.UTF_8));
        } catch (IOException io) {
            throw new IllegalStateException("Failed serializing canonical reduce frame for " + spec.stepId(), io);
        }
    }

    private TypedDurablePayload decodeResponse(
        HttpResponse<byte[]> response, URI target, long startNanos, long deadlineMs
    ) {
        int status = response.statusCode();
        if (status == 401 || status == 403) {
            throw CanonicalFunctionHostSupport.nonRetryable(
                "Foreign function host rejected authentication for reducer step " + spec.stepId()
                    + " (HTTP " + status + ")",
                spec.stepIndex(), spec.commandId());
        }
        if (status != 200) {
            throw CanonicalFunctionHostSupport.unknownOutcome(PROTOCOL, target, startNanos, deadlineMs,
                "Foreign reducer returned HTTP " + status + " for step " + spec.stepId(), Optional.empty());
        }
        JsonNode root = CanonicalFunctionHostSupport.readStrictObject(
            response.body(), spec.maxResponseBytes(), PROTOCOL, target, startNanos, deadlineMs, spec.stepId());
        String outcomeStatus = root.path("status").asText("");
        return switch (outcomeStatus) {
            case "OK" -> CanonicalFunctionHostSupport.validateCanonicalOutput(
                root, spec.stepId(), spec.outputCanonicalTypeId(), spec.outputDefinitionFingerprint(),
                spec.catalogFingerprint(), spec.catalogue(), PROTOCOL, target, startNanos, deadlineMs);
            case "ERROR" -> throw CanonicalFunctionHostSupport.decodePortableError(
                root, spec.stepId(), spec.stepIndex(), spec.commandId(),
                PROTOCOL, target, startNanos, deadlineMs);
            default -> throw CanonicalFunctionHostSupport.unknownOutcome(PROTOCOL, target, startNanos, deadlineMs,
                "Unknown reducer status '" + outcomeStatus + "' for step " + spec.stepId(), Optional.empty());
        };
    }
}

package org.pipelineframework.orchestrator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Demand-driven incremental {@link StepOneToMany} adapter over NDJSON HTTP streaming
 * ({@code POST /tpf/functions/{stepId}/stream}).
 *
 * <p>Guarantees:</p>
 * <ul>
 *   <li>No whole-stream {@code List<T>} materialization: frames are read one-by-one only as
 *       downstream Reactive Streams demand arrives.</li>
 *   <li>Cancellation propagation: downstream {@code Subscription.cancel()} closes the HTTP stream
 *       so the foreign generator aborts immediately.</li>
 *   <li>Explicit completion sentinel: EOF without {@code {"event":"complete"}} fails closed with
 *       {@link RemoteTransitionOutcomeUnknownException}.</li>
 *   <li>Per-item canonical schema validation, bounded frame size, per-frame idle deadline, and
 *       pre-send dispatch-intent durability.</li>
 * </ul>
 */
public final class CanonicalFunctionHostOneToManyStep extends ConfigurableStep
    implements StepOneToMany<TypedDurablePayload, TypedDurablePayload> {

    public static final String STREAM_SUFFIX = "/stream";
    private static final String PROTOCOL = "function-host-stream";

    /** Immutable binding for one streaming ONE_TO_MANY foreign business function. */
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
        Duration connectTimeout,
        Duration itemIdleTimeout,
        Optional<String> commandId,
        ForeignDispatchIntentStore dispatchIntentStore,
        int maxFrameBytes
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
            CanonicalFunctionHostSupport.requirePositive(connectTimeout, "connectTimeout");
            CanonicalFunctionHostSupport.requirePositive(itemIdleTimeout, "itemIdleTimeout");
            commandId = Objects.requireNonNull(commandId, "commandId").filter(v -> !v.isBlank());
            Objects.requireNonNull(dispatchIntentStore, "dispatchIntentStore");
            if (maxFrameBytes <= 0) {
                throw new IllegalArgumentException("maxFrameBytes must be positive");
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
            Duration connectTimeout,
            Duration itemIdleTimeout,
            Optional<String> commandId
        ) {
            this(stepId, stepIndex, inputCanonicalTypeId, inputDefinitionFingerprint,
                outputCanonicalTypeId, outputDefinitionFingerprint, catalogFingerprint,
                catalogue, baseUri, sharedSecret, connectTimeout, itemIdleTimeout, commandId,
                ForeignDispatchIntentStore.noop(), CanonicalFunctionHostSupport.DEFAULT_MAX_RESPONSE_BYTES);
        }

        public Spec withDispatchIntentStore(ForeignDispatchIntentStore store) {
            return new Spec(stepId, stepIndex, inputCanonicalTypeId, inputDefinitionFingerprint,
                outputCanonicalTypeId, outputDefinitionFingerprint, catalogFingerprint,
                catalogue, baseUri, sharedSecret, connectTimeout, itemIdleTimeout, commandId,
                store, maxFrameBytes);
        }
    }

    private final Spec spec;
    private final HttpClient httpClient;

    public CanonicalFunctionHostOneToManyStep(Spec spec, boolean allowLoopbackHttp) {
        this(spec, allowLoopbackHttp, CanonicalFunctionHostSupport.newHttpClient(
            Objects.requireNonNull(spec, "spec").connectTimeout()));
    }

    CanonicalFunctionHostOneToManyStep(Spec spec, boolean allowLoopbackHttp, HttpClient httpClient) {
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
    public Multi<TypedDurablePayload> applyOneToMany(TypedDurablePayload input) {
        CanonicalFunctionHostSupport.CapturedContext captured = CanonicalFunctionHostSupport.CapturedContext.capture();
        return Multi.createFrom().publisher(subscriber -> {
            DemandDrivenStreamSubscription subscription = new DemandDrivenStreamSubscription(
                input, subscriber, captured);
            subscriber.onSubscribe(subscription);
        });
    }

    private final class DemandDrivenStreamSubscription implements Flow.Subscription {
        private final TypedDurablePayload input;
        private final Flow.Subscriber<? super TypedDurablePayload> subscriber;
        private final CanonicalFunctionHostSupport.CapturedContext captured;
        private final AtomicLong requested = new AtomicLong(0L);
        private final AtomicBoolean draining = new AtomicBoolean(false);
        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private final AtomicReference<InputStream> activeStream = new AtomicReference<>();
        private final AtomicReference<BufferedReader> activeReader = new AtomicReference<>();
        private final AtomicReference<CompletableFuture<HttpResponse<InputStream>>> activeRequest = new AtomicReference<>();
        private final AtomicReference<Optional<ForeignDispatchIntent>> reservedIntent =
            new AtomicReference<>(Optional.empty());
        private final long startNanos = System.nanoTime();
        private volatile URI targetUri;

        private DemandDrivenStreamSubscription(
            TypedDurablePayload input,
            Flow.Subscriber<? super TypedDurablePayload> subscriber,
            CanonicalFunctionHostSupport.CapturedContext captured
        ) {
            this.input = input;
            this.subscriber = subscriber;
            this.captured = captured;
        }

        @Override
        public void request(long n) {
            if (n <= 0L) {
                fail(new IllegalArgumentException("Demand must be positive: " + n));
                return;
            }
            if (cancelled.get() || terminated.get()) {
                return;
            }
            requested.updateAndGet(cur -> Long.MAX_VALUE - cur <= n ? Long.MAX_VALUE : cur + n);
            scheduleDrain();
        }

        @Override
        public void cancel() {
            if (cancelled.compareAndSet(false, true)) {
                closeResources();
                CanonicalFunctionHostSupport.settleDispatchIntent(
                    spec.dispatchIntentStore(), reservedIntent.get(),
                    ForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
            }
        }

        private void scheduleDrain() {
            if (draining.compareAndSet(false, true)) {
                Infrastructure.getDefaultWorkerPool().execute(this::drainLoop);
            }
        }

        private void drainLoop() {
            CanonicalFunctionHostSupport.CapturedContext previous = captured.install();
            try {
                if (cancelled.get() || terminated.get()) {
                    return;
                }
                if (activeReader.get() == null && !openStream()) {
                    return;
                }
                BufferedReader reader = activeReader.get();
                while (!cancelled.get() && !terminated.get() && requested.get() > 0L) {
                    Optional<String> lineOpt = readLineWithDeadline(reader);
                    if (cancelled.get() || terminated.get()) {
                        return;
                    }
                    if (lineOpt.isEmpty()) {
                        fail(unknownOutcome("Foreign stream ended without explicit completion sentinel", Optional.empty()));
                        return;
                    }
                    String line = lineOpt.orElseThrow().trim();
                    if (line.isEmpty()) {
                        continue;
                    }
                    if (!handleFrame(line)) {
                        return;
                    }
                }
            } catch (Throwable failure) {
                if (!cancelled.get()) {
                    fail(failure);
                }
            } finally {
                previous.restore();
                draining.set(false);
                if (!cancelled.get() && !terminated.get() && requested.get() > 0L) {
                    scheduleDrain();
                }
            }
        }

        private boolean openStream() {
            String normalizedInputJson;
            try {
                normalizedInputJson = CanonicalFunctionHostSupport.validateCanonicalInput(
                    input, spec.stepId(), spec.stepIndex(), spec.commandId(),
                    spec.inputCanonicalTypeId(), spec.inputDefinitionFingerprint(),
                    spec.catalogFingerprint(), spec.catalogue());
            } catch (RuntimeException validationFailure) {
                fail(validationFailure);
                return false;
            }
            byte[] requestBytes = serializeRequestEnvelope(normalizedInputJson);
            String path = CanonicalFunctionHostStep.FUNCTION_PATH_PREFIX + spec.stepId() + STREAM_SUFFIX;
            targetUri = spec.baseUri().resolve(path);
            try {
                reservedIntent.set(CanonicalFunctionHostSupport.reserveDispatchIntent(
                    spec.dispatchIntentStore(), captured.dispatchScope(), spec.stepIndex(), spec.stepId(),
                    targetUri, requestBytes, PROTOCOL, startNanos, spec.itemIdleTimeout().toMillis()));
            } catch (RuntimeException reservationRejected) {
                fail(reservationRejected);
                return false;
            }

            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(targetUri)
                .timeout(spec.connectTimeout())
                .header("Content-Type", "application/json")
                .header("Accept", "application/x-ndjson")
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestBytes));
            CanonicalFunctionHostSupport.signHeaders(
                requestBuilder, spec.sharedSecret(), path, requestBytes, spec.stepId(), spec.stepIndex());

            CompletableFuture<HttpResponse<InputStream>> future = httpClient.sendAsync(
                requestBuilder.build(), HttpResponse.BodyHandlers.ofInputStream());
            activeRequest.set(future);
            HttpResponse<InputStream> response;
            try {
                response = future.get(spec.connectTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException timeout) {
                future.cancel(true);
                fail(unknownOutcome("Timed out opening foreign stream", Optional.of(timeout)));
                return false;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                future.cancel(true);
                fail(unknownOutcome("Interrupted opening foreign stream", Optional.of(interrupted)));
                return false;
            } catch (ExecutionException exec) {
                Throwable cause = exec.getCause() == null ? exec : exec.getCause();
                fail(unknownOutcome("Transport failure opening foreign stream", Optional.of(cause)));
                return false;
            }
            InputStream stream = response.body();
            activeStream.set(stream);
            if (cancelled.get()) {
                closeResources();
                return false;
            }
            int status = response.statusCode();
            if (status == 401 || status == 403) {
                closeResources();
                fail(CanonicalFunctionHostSupport.nonRetryable(
                    "Foreign function host rejected authentication for stream step " + spec.stepId()
                        + " (HTTP " + status + ")",
                    spec.stepIndex(), spec.commandId()));
                return false;
            }
            if (status != 200) {
                closeResources();
                fail(unknownOutcome("Foreign stream returned HTTP " + status, Optional.empty()));
                return false;
            }
            activeReader.set(new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)));
            return true;
        }

        private Optional<String> readLineWithDeadline(BufferedReader reader) {
            CompletableFuture<Optional<String>> lineFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    return Optional.ofNullable(reader.readLine());
                } catch (IOException io) {
                    throw new java.io.UncheckedIOException(io);
                }
            }, Infrastructure.getDefaultWorkerPool());
            try {
                return lineFuture.get(spec.itemIdleTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException timeout) {
                closeResources();
                lineFuture.cancel(true);
                throw unknownOutcome(
                    "Foreign stream exceeded item idle deadline of " + spec.itemIdleTimeout().toMillis() + "ms",
                    Optional.of(timeout));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                closeResources();
                throw unknownOutcome("Interrupted waiting for foreign stream frame", Optional.of(interrupted));
            } catch (ExecutionException exec) {
                Throwable cause = exec.getCause() == null ? exec : exec.getCause();
                closeResources();
                if (cancelled.get()) {
                    return Optional.empty();
                }
                throw unknownOutcome("Stream read interrupted mid-flight: " + cause.getMessage(), Optional.of(cause));
            }
        }

        private boolean handleFrame(String line) {
            URI uri = targetUri == null ? spec.baseUri() : targetUri;
            JsonNode root = CanonicalFunctionHostSupport.readStrictObject(
                line.getBytes(StandardCharsets.UTF_8), spec.maxFrameBytes(), PROTOCOL, uri,
                startNanos, spec.itemIdleTimeout().toMillis(), spec.stepId());
            String event = root.path("event").asText("");
            return switch (event) {
                case "item" -> {
                    TypedDurablePayload validated = CanonicalFunctionHostSupport.validateCanonicalOutput(
                        root, spec.stepId(), spec.outputCanonicalTypeId(), spec.outputDefinitionFingerprint(),
                        spec.catalogFingerprint(), spec.catalogue(), PROTOCOL, uri,
                        startNanos, spec.itemIdleTimeout().toMillis());
                    requested.updateAndGet(cur -> cur == Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0L, cur - 1L));
                    subscriber.onNext(validated);
                    yield true;
                }
                case "complete" -> {
                    complete();
                    yield false;
                }
                case "error" -> {
                    fail(CanonicalFunctionHostSupport.decodePortableError(
                        root, spec.stepId(), spec.stepIndex(), spec.commandId(),
                        PROTOCOL, uri, startNanos, spec.itemIdleTimeout().toMillis()));
                    yield false;
                }
                default -> {
                    fail(unknownOutcome("Unknown foreign stream event '" + event + "'", Optional.empty()));
                    yield false;
                }
            };
        }

        private void complete() {
            if (terminated.compareAndSet(false, true)) {
                closeResources();
                CanonicalFunctionHostSupport.settleDispatchIntent(
                    spec.dispatchIntentStore(), reservedIntent.get(),
                    ForeignDispatchIntentStore.SettlementStatus.VALIDATED_SUCCEEDED);
                subscriber.onComplete();
            }
        }

        private void fail(Throwable failure) {
            if (terminated.compareAndSet(false, true)) {
                closeResources();
                CanonicalFunctionHostSupport.settleDispatchIntent(
                    spec.dispatchIntentStore(), reservedIntent.get(),
                    CanonicalFunctionHostSupport.settlementForFailure(failure));
                subscriber.onError(failure);
            }
        }

        private void closeResources() {
            CompletableFuture<HttpResponse<InputStream>> req = activeRequest.getAndSet(null);
            if (req != null && !req.isDone()) {
                req.cancel(true);
            }
            BufferedReader reader = activeReader.getAndSet(null);
            if (reader != null) {
                try { reader.close(); } catch (IOException ignored) { }
            }
            InputStream stream = activeStream.getAndSet(null);
            if (stream != null) {
                try { stream.close(); } catch (IOException ignored) { }
            }
        }

        private RemoteTransitionOutcomeUnknownException unknownOutcome(String reason, Optional<Throwable> cause) {
            URI uri = targetUri == null ? spec.baseUri() : targetUri;
            return CanonicalFunctionHostSupport.unknownOutcome(
                PROTOCOL, uri, startNanos, spec.itemIdleTimeout().toMillis(), reason, cause);
        }
    }

    private byte[] serializeRequestEnvelope(String normalizedInputJson) {
        try {
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("stepId", spec.stepId());
            envelope.put("stepIndex", spec.stepIndex());
            envelope.put("inputTypeId", spec.inputCanonicalTypeId());
            envelope.put("outputTypeId", spec.outputCanonicalTypeId());
            envelope.put("catalogFingerprint", spec.catalogFingerprint());
            envelope.put("payload", CanonicalFunctionHostSupport.json().readTree(normalizedInputJson));
            return CanonicalFunctionHostSupport.json().writeValueAsBytes(envelope);
        } catch (IOException failure) {
            throw new IllegalStateException("Failed serializing stream request envelope for " + spec.stepId(), failure);
        }
    }
}

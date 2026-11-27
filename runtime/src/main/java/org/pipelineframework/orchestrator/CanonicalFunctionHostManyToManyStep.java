package org.pipelineframework.orchestrator;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Full-duplex incremental {@link StepManyToMany} adapter over chunked HTTP/1.1 bidirectional
 * NDJSON streaming ({@code POST /tpf/functions/{stepId}/duplex}).
 *
 * <p>Guarantees:</p>
 * <ul>
 *   <li>True full-duplex streaming: response frames are consumed on downstream Reactive Streams
 *       demand concurrently while upstream items are validated and chunk-written to the foreign
 *       host — zero whole-stream buffering in either direction.</li>
 *   <li>Transport security: remote targets require HTTPS with strict certificate and hostname
 *       verification ({@code HTTPS} endpoint identification + SNI); plaintext HTTP is permitted
 *       strictly on loopback addresses when explicitly enabled.</li>
 *   <li>Demand-driven backpressure and cancellation propagation: downstream
 *       {@code Subscription.cancel()} cancels upstream and closes the duplex socket immediately.</li>
 *   <li>Fail-closed framing: missing {@code {"event":"complete"}} sentinel, oversized frames,
 *       or per-frame idle timeouts fail closed with {@link RemoteTransitionOutcomeUnknownException}.</li>
 *   <li>Pre-send {@link ForeignDispatchIntentStore} reservation and settlement parity.</li>
 * </ul>
 */
public final class CanonicalFunctionHostManyToManyStep extends ConfigurableStep
    implements StepManyToMany<TypedDurablePayload, TypedDurablePayload> {

    public static final String DUPLEX_SUFFIX = "/duplex";
    public static final String STREAMING_BODY_DIGEST_SENTINEL =
        CanonicalFunctionHostManyToOneStep.STREAMING_BODY_DIGEST_SENTINEL;

    private static final String PROTOCOL = "function-host-duplex";
    private static final byte[] COMPLETE_FRAME = "{\"event\":\"complete\"}\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ZERO_CHUNK = "0\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_HEADER_BYTES = 32 * 1024;

    /** Immutable binding for one full-duplex MANY_TO_MANY foreign streaming step. */
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
    private final SSLSocketFactory sslSocketFactory;

    public CanonicalFunctionHostManyToManyStep(Spec spec, boolean allowLoopbackHttp) {
        this(spec, allowLoopbackHttp, (SSLSocketFactory) SSLSocketFactory.getDefault());
    }

    CanonicalFunctionHostManyToManyStep(Spec spec, boolean allowLoopbackHttp, SSLSocketFactory sslSocketFactory) {
        this.spec = Objects.requireNonNull(spec, "spec");
        RegisteredRestWorkerTargets.normalizeOrigin(spec.baseUri().toString(), allowLoopbackHttp);
        this.sslSocketFactory = Objects.requireNonNull(sslSocketFactory, "sslSocketFactory");
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
    public Multi<TypedDurablePayload> applyTransform(Multi<TypedDurablePayload> upstream) {
        Objects.requireNonNull(upstream, "upstream must not be null");
        CanonicalFunctionHostSupport.CapturedContext captured = CanonicalFunctionHostSupport.CapturedContext.capture();
        return Multi.createFrom().publisher(subscriber -> {
            DuplexStreamSubscription subscription = new DuplexStreamSubscription(upstream, subscriber, captured);
            subscriber.onSubscribe(subscription);
        });
    }

    private final class DuplexStreamSubscription implements Flow.Subscription {
        private final Multi<TypedDurablePayload> upstream;
        private final Flow.Subscriber<? super TypedDurablePayload> subscriber;
        private final CanonicalFunctionHostSupport.CapturedContext captured;
        private final AtomicLong requested = new AtomicLong(0L);
        private final AtomicBoolean draining = new AtomicBoolean(false);
        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private final AtomicBoolean upstreamEnded = new AtomicBoolean(false);
        private final AtomicBoolean writeDraining = new AtomicBoolean(false);
        private final java.util.concurrent.ConcurrentLinkedQueue<Runnable> writeQueue =
            new java.util.concurrent.ConcurrentLinkedQueue<>();
        private final AtomicReference<Socket> activeSocket = new AtomicReference<>();
        private final AtomicReference<DuplexResponseReader> activeReader = new AtomicReference<>();
        private final AtomicReference<Flow.Subscription> upstreamSubscription = new AtomicReference<>();
        private final AtomicReference<Throwable> upstreamFailure = new AtomicReference<>();
        private final AtomicReference<Optional<ForeignDispatchIntent>> reservedIntent =
            new AtomicReference<>(Optional.empty());
        private final AtomicBoolean intentSettled = new AtomicBoolean(false);
        private final Object writeLock = new Object();
        private final Object signalLock = new Object();
        private final long startNanos = System.nanoTime();
        private volatile URI targetUri;
        private volatile TypedDurablePayload pendingItem;

        private DuplexStreamSubscription(
            Multi<TypedDurablePayload> upstream,
            Flow.Subscriber<? super TypedDurablePayload> subscriber,
            CanonicalFunctionHostSupport.CapturedContext captured
        ) {
            this.upstream = upstream;
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
                settleIntentOnce(ForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
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
                if (activeReader.get() == null && !openDuplexStream()) {
                    return;
                }
                DuplexResponseReader reader = activeReader.get();
                while (!cancelled.get() && !terminated.get()) {
                    TypedDurablePayload buffered = pendingItem;
                    if (buffered != null) {
                        if (requested.get() <= 0L) {
                            break;
                        }
                        pendingItem = null;
                        if (!deliverItem(buffered)) {
                            return;
                        }
                        continue;
                    }
                    // Bounded 1-frame lookahead when item demand reaches 0 so terminal frames
                    // (complete / error / EOF) are delivered immediately per Reactive Streams spec.
                    Optional<String> lineOpt = readNextLine(reader);
                    if (cancelled.get() || terminated.get()) {
                        return;
                    }
                    if (lineOpt.isEmpty()) {
                        Throwable upErr = upstreamFailure.get();
                        if (upErr != null) {
                            fail(upErr);
                        } else {
                            fail(unknownOutcome(
                                "Foreign duplex stream ended without explicit completion sentinel",
                                Optional.empty()));
                        }
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
                if (!cancelled.get() && !terminated.get()) {
                    Throwable upErr = upstreamFailure.get();
                    fail(upErr != null ? upErr : failure);
                }
            } finally {
                previous.restore();
                draining.set(false);
                if (!cancelled.get() && !terminated.get()
                    && (requested.get() > 0L || pendingItem == null)) {
                    scheduleDrain();
                }
            }
        }

        private boolean openDuplexStream() {
            String path = CanonicalFunctionHostStep.FUNCTION_PATH_PREFIX + spec.stepId() + DUPLEX_SUFFIX;
            targetUri = spec.baseUri().resolve(path);
            byte[] sentinelBytes = STREAMING_BODY_DIGEST_SENTINEL.getBytes(StandardCharsets.UTF_8);

            try {
                Optional<ForeignDispatchIntent> intent = CanonicalFunctionHostSupport.reserveDispatchIntent(
                    spec.dispatchIntentStore(), captured.dispatchScope(), spec.stepIndex(), spec.stepId(),
                    targetUri, sentinelBytes, PROTOCOL, startNanos, spec.itemIdleTimeout().toMillis());
                reservedIntent.set(intent);
                if (cancelled.get() || terminated.get()) {
                    settleIntentOnce(ForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
                    return false;
                }
            } catch (RuntimeException reservationRejected) {
                fail(reservationRejected);
                return false;
            }

            Socket socket;
            OutputStream output;
            BufferedInputStream input;
            try {
                socket = openSocket(targetUri, activeSocket, cancelled);
                if (cancelled.get() || terminated.get()) {
                    closeResources();
                    settleIntentOnce(ForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
                    return false;
                }
                output = socket.getOutputStream();
                input = new BufferedInputStream(socket.getInputStream());
                writeRequestHeaders(output, targetUri, path, sentinelBytes);
            } catch (IOException transportFailure) {
                if (cancelled.get() || terminated.get()) {
                    closeResources();
                    settleIntentOnce(ForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
                    return false;
                }
                fail(unknownOutcome("Transport failure opening foreign duplex stream: " + transportFailure.getMessage(),
                    Optional.of(transportFailure)));
                return false;
            }

            // Subscribe to upstream immediately so request items stream concurrently while response headers/frames arrive.
            subscribeUpstream(output);

            // Read HTTP response status & headers (non-blocking w.r.t. upstream completion).
            ResponseHeaderInfo headerInfo;
            try {
                headerInfo = readResponseHeaders(input);
            } catch (SocketTimeoutException timeout) {
                fail(unknownOutcome(
                    "Timed out awaiting response headers from foreign duplex stream",
                    Optional.of(timeout)));
                return false;
            } catch (IOException io) {
                if (cancelled.get() || terminated.get()) {
                    return false;
                }
                Throwable upErr = upstreamFailure.get();
                fail(upErr != null
                    ? upErr
                    : unknownOutcome("Transport failure reading duplex response headers: " + io.getMessage(), Optional.of(io)));
                return false;
            }

            int status = headerInfo.statusCode();
            if (status == 401 || status == 403) {
                fail(CanonicalFunctionHostSupport.nonRetryable(
                    "Foreign function host rejected authentication for duplex step " + spec.stepId()
                        + " (HTTP " + status + ")",
                    spec.stepIndex(), spec.commandId()));
                return false;
            }
            if (status != 200) {
                fail(unknownOutcome("Foreign duplex stream returned HTTP " + status, Optional.empty()));
                return false;
            }
            activeReader.set(new DuplexResponseReader(input, headerInfo.chunked(), spec.maxFrameBytes()));
            return true;
        }

        private void enqueueWrite(Runnable task) {
            writeQueue.add(task);
            scheduleWriteDrain();
        }

        private void scheduleWriteDrain() {
            if (writeDraining.compareAndSet(false, true)) {
                Infrastructure.getDefaultWorkerPool().execute(this::drainWriteLoop);
            }
        }

        private void drainWriteLoop() {
            CanonicalFunctionHostSupport.CapturedContext previous = captured.install();
            try {
                Runnable task;
                while ((task = writeQueue.poll()) != null) {
                    task.run();
                }
            } finally {
                previous.restore();
                writeDraining.set(false);
                if (!writeQueue.isEmpty()) {
                    scheduleWriteDrain();
                }
            }
        }

        private void subscribeUpstream(OutputStream output) {
            upstream.subscribe().withSubscriber(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription sub) {
                    if (cancelled.get() || terminated.get()) {
                        sub.cancel();
                        return;
                    }
                    if (upstreamSubscription.compareAndSet(null, sub)) {
                        if (cancelled.get() || terminated.get()) {
                            cancelUpstream();
                            return;
                        }
                        sub.request(1L);
                    } else {
                        sub.cancel();
                    }
                }

                @Override
                public void onNext(TypedDurablePayload item) {
                    if (cancelled.get() || terminated.get()) {
                        cancelUpstream();
                        return;
                    }
                    enqueueWrite(() -> {
                        try {
                            if (cancelled.get() || terminated.get()) {
                                cancelUpstream();
                                return;
                            }
                            byte[] frame = encodeValidatedItemFrame(item);
                            synchronized (writeLock) {
                                if (!cancelled.get() && !terminated.get() && !upstreamEnded.get()) {
                                    writeChunk(output, frame);
                                }
                            }
                            Flow.Subscription sub = upstreamSubscription.get();
                            if (sub != null && !cancelled.get() && !terminated.get()) {
                                sub.request(1L);
                            }
                        } catch (Throwable failure) {
                            if (!cancelled.get() && !terminated.get()) {
                                upstreamFailure.compareAndSet(null, failure);
                                fail(failure);
                            }
                        }
                    });
                }

                @Override
                public void onError(Throwable throwable) {
                    if (!cancelled.get() && !terminated.get()) {
                        upstreamFailure.compareAndSet(null, throwable);
                        // Deliver upstream failure immediately (closing the socket notifies the host
                        // without risking a blocking write on a full TCP send buffer).
                        fail(throwable);
                    }
                }

                @Override
                public void onComplete() {
                    if (cancelled.get() || terminated.get()) {
                        return;
                    }
                    enqueueWrite(() -> {
                        synchronized (writeLock) {
                            if (!cancelled.get() && !terminated.get() && upstreamEnded.compareAndSet(false, true)) {
                                try {
                                    writeChunk(output, COMPLETE_FRAME);
                                    output.write(ZERO_CHUNK);
                                    output.flush();
                                } catch (IOException ignored) {
                                    // Reader loop will surface any transport termination cleanly.
                                }
                            }
                        }
                    });
                }
            });
        }

        private byte[] encodeValidatedItemFrame(TypedDurablePayload item) {
            String canonicalJson = CanonicalFunctionHostSupport.validateCanonicalInput(
                item, spec.stepId(), spec.stepIndex(), spec.commandId(),
                spec.inputCanonicalTypeId(), spec.inputDefinitionFingerprint(),
                spec.catalogFingerprint(), spec.catalogue());
            try {
                Map<String, Object> frame = new LinkedHashMap<>();
                frame.put("event", "item");
                frame.put("inputTypeId", spec.inputCanonicalTypeId());
                frame.put("payload", CanonicalFunctionHostSupport.json().readTree(canonicalJson));
                return (CanonicalFunctionHostSupport.json().writeValueAsString(frame) + "\n")
                    .getBytes(StandardCharsets.UTF_8);
            } catch (IOException io) {
                throw new IllegalStateException("Failed serializing canonical duplex frame for " + spec.stepId(), io);
            }
        }

        private Optional<String> readNextLine(DuplexResponseReader reader) {
            try {
                return reader.readLine();
            } catch (SocketTimeoutException timeout) {
                // Claim the timeout as the terminal unknown outcome BEFORE closing the socket so
                // any SocketException triggered in a concurrent writer cannot replace it.
                fail(unknownOutcome(
                    "Foreign duplex stream exceeded item idle deadline of " + spec.itemIdleTimeout().toMillis() + "ms",
                    Optional.of(timeout)));
                return Optional.empty();
            } catch (IOException io) {
                if (cancelled.get() || terminated.get()) {
                    return Optional.empty();
                }
                Throwable upErr = upstreamFailure.get();
                if (upErr != null) {
                    fail(upErr);
                    return Optional.empty();
                }
                fail(unknownOutcome("Duplex stream read interrupted mid-flight: " + io.getMessage(), Optional.of(io)));
                return Optional.empty();
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
                    if (requested.get() > 0L) {
                        yield deliverItem(validated);
                    }
                    pendingItem = validated;
                    yield false;
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
                    fail(unknownOutcome("Unknown foreign duplex stream event '" + event + "'", Optional.empty()));
                    yield false;
                }
            };
        }

        private boolean deliverItem(TypedDurablePayload validated) {
            synchronized (signalLock) {
                if (cancelled.get() || terminated.get()) {
                    return false;
                }
                requested.updateAndGet(cur -> cur == Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0L, cur - 1L));
                subscriber.onNext(validated);
                return true;
            }
        }

        private void complete() {
            if (terminated.compareAndSet(false, true)) {
                closeResources();
                settleIntentOnce(ForeignDispatchIntentStore.SettlementStatus.VALIDATED_SUCCEEDED);
                synchronized (signalLock) {
                    subscriber.onComplete();
                }
            }
        }

        private void fail(Throwable failure) {
            if (terminated.compareAndSet(false, true)) {
                closeResources();
                settleIntentOnce(CanonicalFunctionHostSupport.settlementForFailure(failure));
                synchronized (signalLock) {
                    subscriber.onError(failure);
                }
            }
        }

        private void settleIntentOnce(ForeignDispatchIntentStore.SettlementStatus status) {
            Optional<ForeignDispatchIntent> intent = reservedIntent.get();
            if (intent.isPresent() && intentSettled.compareAndSet(false, true)) {
                CanonicalFunctionHostSupport.settleDispatchIntent(spec.dispatchIntentStore(), intent, status);
            }
        }

        private void cancelUpstream() {
            Flow.Subscription sub = upstreamSubscription.getAndSet(null);
            if (sub != null) {
                sub.cancel();
            }
        }

        private void closeResources() {
            cancelUpstream();
            Socket socket = activeSocket.getAndSet(null);
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // Best-effort socket cleanup.
                }
            }
        }

        private RemoteTransitionOutcomeUnknownException unknownOutcome(String reason, Optional<Throwable> cause) {
            URI uri = targetUri == null ? spec.baseUri() : targetUri;
            return CanonicalFunctionHostSupport.unknownOutcome(
                PROTOCOL, uri, startNanos, spec.itemIdleTimeout().toMillis(), reason, cause);
        }
    }

    private Socket openSocket(
        URI targetUri,
        AtomicReference<Socket> activeSocketRef,
        AtomicBoolean cancelledFlag
    ) throws IOException {
        boolean https = "https".equalsIgnoreCase(targetUri.getScheme());
        String host = targetUri.getHost();
        int port = targetUri.getPort() > 0 ? targetUri.getPort() : (https ? 443 : 80);
        int connectMs = (int) Math.min(Integer.MAX_VALUE, Math.max(1L, spec.connectTimeout().toMillis()));
        int idleMs = (int) Math.min(Integer.MAX_VALUE, Math.max(1L, spec.itemIdleTimeout().toMillis()));

        Socket socket = https ? sslSocketFactory.createSocket() : new Socket();
        activeSocketRef.set(socket);
        boolean configured = false;
        try {
            if (cancelledFlag.get()) {
                throw new IOException("Duplex stream cancelled before connecting");
            }
            socket.connect(new InetSocketAddress(host, port), connectMs);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(idleMs);
            if (socket instanceof SSLSocket sslSocket) {
                SSLParameters params = sslSocket.getSSLParameters();
                params.setEndpointIdentificationAlgorithm("HTTPS");
                if (!isIpLiteral(host)) {
                    params.setServerNames(List.of(new SNIHostName(host)));
                }
                sslSocket.setSSLParameters(params);
                sslSocket.startHandshake();
            }
            if (cancelledFlag.get()) {
                throw new IOException("Duplex stream cancelled during handshake");
            }
            configured = true;
            return socket;
        } finally {
            if (!configured) {
                activeSocketRef.compareAndSet(socket, null);
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    private static boolean isIpLiteral(String host) {
        if (host == null || host.isBlank()) {
            return true;
        }
        String stripped = host.startsWith("[") && host.endsWith("]")
            ? host.substring(1, host.length() - 1)
            : host;
        if (stripped.indexOf(':') >= 0) {
            return true;
        }
        boolean digitsAndDots = true;
        for (int i = 0; i < stripped.length(); i++) {
            char c = stripped.charAt(i);
            if ((c < '0' || c > '9') && c != '.') {
                digitsAndDots = false;
                break;
            }
        }
        if (digitsAndDots) {
            try {
                InetAddress.getByName(stripped);
                return true;
            } catch (IOException ignored) {
                return false;
            }
        }
        return false;
    }

    private void writeRequestHeaders(OutputStream out, URI targetUri, String path, byte[] sentinelBytes) throws IOException {
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            spec.sharedSecret(), "POST", path, timestamp, nonce, sentinelBytes);
        String hostHeader = targetUri.getPort() > 0
            ? targetUri.getHost() + ":" + targetUri.getPort()
            : targetUri.getHost();
        StringBuilder sb = new StringBuilder(256);
        sb.append("POST ").append(path).append(" HTTP/1.1\r\n");
        sb.append("Host: ").append(hostHeader).append("\r\n");
        sb.append("Content-Type: application/x-ndjson\r\n");
        sb.append("Accept: application/x-ndjson\r\n");
        sb.append("Transfer-Encoding: chunked\r\n");
        sb.append("Connection: close\r\n");
        sb.append(TransitionWorkerSignature.TIMESTAMP_HEADER).append(": ").append(timestamp).append("\r\n");
        sb.append(TransitionWorkerSignature.NONCE_HEADER).append(": ").append(nonce).append("\r\n");
        sb.append(TransitionWorkerSignature.SIGNATURE_HEADER).append(": ").append(signature).append("\r\n");
        sb.append(CanonicalFunctionHostStep.STEP_ID_HEADER).append(": ").append(spec.stepId()).append("\r\n");
        sb.append(CanonicalFunctionHostStep.STEP_INDEX_HEADER).append(": ").append(spec.stepIndex()).append("\r\n");
        sb.append("\r\n");
        out.write(sb.toString().getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private static void writeChunk(OutputStream out, byte[] payload) throws IOException {
        if (payload.length == 0) {
            return;
        }
        out.write((Integer.toHexString(payload.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(payload);
        out.write(CRLF);
        out.flush();
    }

    private record ResponseHeaderInfo(int statusCode, boolean chunked) {
    }

    private static ResponseHeaderInfo readResponseHeaders(InputStream in) throws IOException {
        ByteArrayOutputStream raw = new ByteArrayOutputStream(256);
        int state = 0;
        int b;
        while ((b = in.read()) != -1) {
            raw.write(b);
            if (raw.size() > MAX_HEADER_BYTES) {
                throw new IOException("Foreign duplex response headers exceeded " + MAX_HEADER_BYTES + " bytes");
            }
            if (state == 0 && b == '\r') state = 1;
            else if (state == 1 && b == '\n') state = 2;
            else if (state == 2 && b == '\r') state = 3;
            else if (state == 3 && b == '\n') break;
            else state = (b == '\r') ? 1 : 0;
        }
        if (state != 3) {
            throw new IOException("Truncated HTTP response headers from foreign duplex stream");
        }
        String headerText = raw.toString(StandardCharsets.ISO_8859_1);
        String[] lines = headerText.split("\r\n");
        if (lines.length == 0) {
            throw new IOException("Empty HTTP response status line");
        }
        String[] statusParts = lines[0].split(" ", 3);
        if (statusParts.length < 2 || !statusParts[0].startsWith("HTTP/1.")) {
            throw new IOException("Malformed HTTP status line: " + lines[0]);
        }
        int statusCode;
        try {
            statusCode = Integer.parseInt(statusParts[1].trim());
        } catch (NumberFormatException nfe) {
            throw new IOException("Invalid HTTP status code: " + statusParts[1], nfe);
        }
        boolean chunked = false;
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            if (colon > 0) {
                String name = lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT);
                String val = lines[i].substring(colon + 1).trim().toLowerCase(Locale.ROOT);
                if ("transfer-encoding".equals(name) && val.contains("chunked")) {
                    chunked = true;
                }
            }
        }
        return new ResponseHeaderInfo(statusCode, chunked);
    }

    /**
     * Streaming NDJSON line reader over either HTTP/1.1 chunked transfer-encoding or raw stream,
     * bounding each frame to {@code maxFrameBytes} to prevent memory exhaustion.
     */
    private static final class DuplexResponseReader {
        private final InputStream stream;
        private final int maxFrameBytes;
        private final ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream(256);

        private DuplexResponseReader(InputStream raw, boolean chunked, int maxFrameBytes) {
            this.stream = chunked ? new ChunkedInputStream(raw) : raw;
            this.maxFrameBytes = maxFrameBytes;
        }

        private Optional<String> readLine() throws IOException {
            lineBuffer.reset();
            int b;
            while ((b = stream.read()) != -1) {
                if (b == '\n') {
                    return Optional.of(stripTrailingCr(lineBuffer.toString(StandardCharsets.UTF_8)));
                }
                if (lineBuffer.size() >= maxFrameBytes) {
                    throw new IOException("Foreign duplex NDJSON frame exceeded " + maxFrameBytes + " bytes");
                }
                lineBuffer.write(b);
            }
            if (lineBuffer.size() == 0) {
                return Optional.empty();
            }
            throw new IOException("Foreign duplex NDJSON stream ended with unterminated frame (missing newline)");
        }

        private static String stripTrailingCr(String s) {
            return s.endsWith("\r") ? s.substring(0, s.length() - 1) : s;
        }
    }

    /** Minimal RFC 9112 HTTP/1.1 chunked transfer-coding decoder. */
    private static final class ChunkedInputStream extends InputStream {
        private final InputStream in;
        private int remainingInChunk;
        private boolean eof;

        private ChunkedInputStream(InputStream in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            if (eof) {
                return -1;
            }
            if (remainingInChunk == 0 && !advanceChunk()) {
                return -1;
            }
            int b = in.read();
            if (b == -1) {
                throw new IOException("Unexpected EOF inside HTTP chunk");
            }
            remainingInChunk--;
            if (remainingInChunk == 0) {
                consumeCrlf();
            }
            return b;
        }

        private boolean advanceChunk() throws IOException {
            StringBuilder sizeLine = new StringBuilder(16);
            int b;
            while ((b = in.read()) != -1) {
                if (b == '\n') {
                    break;
                }
                if (sizeLine.length() > 1024) {
                    throw new IOException("HTTP chunk size line exceeded limit");
                }
                sizeLine.append((char) b);
            }
            if (b == -1 && sizeLine.isEmpty()) {
                throw new IOException("Unexpected EOF before terminating HTTP chunk");
            }
            String trimmed = sizeLine.toString().trim();
            int semi = trimmed.indexOf(';');
            String hex = semi >= 0 ? trimmed.substring(0, semi).trim() : trimmed;
            int size;
            try {
                size = Integer.parseInt(hex, 16);
            } catch (NumberFormatException nfe) {
                throw new IOException("Malformed HTTP chunk size: " + trimmed, nfe);
            }
            if (size < 0) {
                throw new IOException("Negative HTTP chunk size: " + size);
            }
            if (size == 0) {
                // Consume optional trailer headers until blank CRLF line.
                consumeTrailers();
                eof = true;
                return false;
            }
            remainingInChunk = size;
            return true;
        }

        private void consumeCrlf() throws IOException {
            int cr = in.read();
            int lf = in.read();
            if (cr != '\r' || lf != '\n') {
                throw new IOException("Missing CRLF after HTTP chunk payload");
            }
        }

        private void consumeTrailers() throws IOException {
            int lineLen = 0;
            int prev = -1;
            int b;
            while ((b = in.read()) != -1) {
                if (b == '\n') {
                    if (lineLen == 0 || (lineLen == 1 && prev == '\r')) {
                        return;
                    }
                    lineLen = 0;
                } else {
                    lineLen++;
                    if (lineLen > MAX_HEADER_BYTES) {
                        throw new IOException("HTTP chunk trailer exceeded limit");
                    }
                }
                prev = b;
            }
        }
    }
}

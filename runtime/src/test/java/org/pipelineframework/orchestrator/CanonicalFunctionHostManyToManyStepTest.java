package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Verifies {@link CanonicalFunctionHostManyToManyStep} true full-duplex interleaved streaming,
 * exact 29-digit {@code BigDecimal} preservation, cancellation propagation, fail-closed framing,
 * per-frame idle deadlines, portable failure classification, auth rejection, and pre-send
 * {@link ForeignDispatchIntentStore} parity.
 */
class CanonicalFunctionHostManyToManyStepTest {

    private static final String SECRET = "duplex-test-hmac-secret";
    private static final String CATALOG_FP = "fp-duplex-catalog";
    private static final ObjectMapper EXACT_JSON = CanonicalTypeCatalogue.exactJsonMapper();
    private static final CanonicalTypeCatalogue CATALOGUE = buildCatalogue();

    private HttpServer server;
    private ExecutorService serverExecutor;

    @AfterEach
    void tearDown() {
        TransitionDispatchScopeHolder.clear();
        if (server != null) {
            server.stop(0);
            server = null;
        }
        if (serverExecutor != null) {
            serverExecutor.shutdownNow();
            serverExecutor = null;
        }
    }

    @Test
    void streamsInterleavedFullDuplexWithExact29DigitDecimalAndDispatchIntentSettlement() throws Exception {
        AtomicBoolean sawValidSignature = new AtomicBoolean(false);
        AtomicInteger itemsReceivedByHost = new AtomicInteger(0);
        startServer("/tpf/functions/stream-transform/duplex", exchange -> {
            verifyStreamingSignature(exchange, "/tpf/functions/stream-transform/duplex", sawValidSignature);
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, 0);
            try (var reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));
                 var out = exchange.getResponseBody()) {
                String line;
                int emitted = 0;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) continue;
                    JsonNode frame = EXACT_JSON.readTree(line);
                    if ("complete".equals(frame.path("event").asText())) break;
                    if ("item".equals(frame.path("event").asText())) {
                        itemsReceivedByHost.incrementAndGet();
                        JsonNode p = frame.path("payload");
                        String id = p.path("id").asText();
                        String amount = p.path("amount").decimalValue().toPlainString();
                        String resp = "{\"event\":\"item\",\"outputTypeId\":\"OutFrame\",\"payload\":{\"id\":\""
                            + id + "-out\",\"amount\":" + amount + "}}\n";
                        out.write(resp.getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        emitted++;
                    }
                }
                out.write(("{\"event\":\"complete\",\"itemCount\":" + emitted + "}\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        });

        var recordedSettlements = new java.util.concurrent.ConcurrentHashMap<ForeignDispatchIntent, ForeignDispatchIntentStore.SettlementStatus>();
        InMemoryForeignDispatchIntentStore delegateIntentStore = new InMemoryForeignDispatchIntentStore();
        ForeignDispatchIntentStore intentStore = new ForeignDispatchIntentStore() {
            @Override
            public ReservationOutcome reserve(ForeignDispatchIntent intent) {
                return delegateIntentStore.reserve(intent);
            }

            @Override
            public void recordSettlement(ForeignDispatchIntent intent, SettlementStatus status, long settledAtEpochMs) {
                recordedSettlements.put(intent, status);
                delegateIntentStore.recordSettlement(intent, status, settledAtEpochMs);
            }
        };
        TransitionDispatchScopeHolder.set(new TransitionDispatchScope("t1", "exec-duplex-1", 0L, 0, "tk-duplex-1"));
        CanonicalFunctionHostManyToManyStep step = new CanonicalFunctionHostManyToManyStep(
            baseSpec("stream-transform", intentStore, Duration.ofSeconds(5)), true);

        // True full-duplex proof: submit item #1 ONLY after upstream is subscribed,
        // and await output #1 BEFORE submitting item #2!
        CountDownLatch upstreamSubscribed = new CountDownLatch(1);
        AtomicReference<io.smallrye.mutiny.subscription.MultiEmitter<? super TypedDurablePayload>> emitterRef =
            new AtomicReference<>();
        Multi<TypedDurablePayload> upstream = Multi.createFrom().emitter(emitter -> {
            emitterRef.set(emitter);
            upstreamSubscribed.countDown();
        });
        String exact29 = "10000000000000000000.123456789";
        List<TypedDurablePayload> received = new ArrayList<>();
        CountDownLatch firstReceived = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        step.applyTransform(upstream)
            .subscribe().withSubscriber(new Flow.Subscriber<>() {
                private Flow.Subscription sub;

                @Override
                public void onSubscribe(Flow.Subscription s) {
                    this.sub = s;
                    s.request(1);
                }

                @Override
                public void onNext(TypedDurablePayload item) {
                    received.add(item);
                    firstReceived.countDown();
                    sub.request(1);
                }

                @Override
                public void onError(Throwable t) {
                    failure.set(t);
                    firstReceived.countDown();
                    completed.countDown();
                }

                @Override
                public void onComplete() {
                    completed.countDown();
                }
            });

        assertTrue(upstreamSubscribed.await(5, TimeUnit.SECONDS), "Duplex step must subscribe to upstream");
        emitterRef.get().emit(inFrame("f1", exact29));
        assertTrue(firstReceived.await(5, TimeUnit.SECONDS),
            "Output #1 must arrive BEFORE upstream emits item #2 (true full-duplex)");
        assertEquals(1, received.size());
        JsonNode out1 = EXACT_JSON.readTree(received.get(0).payload());
        assertEquals("f1-out", out1.path("id").asText());
        assertEquals(exact29, out1.path("amount").decimalValue().toPlainString());

        // Now send item #2 and complete upstream
        emitterRef.get().emit(inFrame("f2", "42.50"));
        emitterRef.get().complete();
        assertTrue(completed.await(5, TimeUnit.SECONDS), "Duplex stream must complete cleanly");
        if (failure.get() != null) {
            throw new AssertionError("Unexpected duplex failure", failure.get());
        }
        assertEquals(2, received.size());
        assertEquals("f2-out", EXACT_JSON.readTree(received.get(1).payload()).path("id").asText());
        assertEquals("42.50", EXACT_JSON.readTree(received.get(1).payload()).path("amount").decimalValue().toPlainString());
        assertTrue(sawValidSignature.get(), "HMAC signature over STREAMING-NDJSON-V1 must validate");
        assertEquals(2, itemsReceivedByHost.get());
        assertEquals(1, delegateIntentStore.reservationCount());
        assertEquals(1, recordedSettlements.size(), "Original reservation must be settled once");
        assertEquals(ForeignDispatchIntentStore.SettlementStatus.VALIDATED_SUCCEEDED,
            recordedSettlements.values().iterator().next(),
            "Successful duplex stream must settle reservation as VALIDATED_SUCCEEDED");

        // Duplicate dispatch with the same TransitionDispatchScope must be suppressed fail-closed!
        Throwable duplicateErr = assertThrows(Throwable.class, () ->
            step.applyTransform(Multi.createFrom().item(inFrame("f3", "1.00")))
                .collect().asList().await().atMost(Duration.ofSeconds(5)));
        assertInstanceOf(RemoteTransitionOutcomeUnknownException.class, duplicateErr);
        assertEquals(2, itemsReceivedByHost.get(), "Duplicate resend must be suppressed before opening socket");
    }

    @Test
    void deliversTerminalCompleteWhenDemandReachesZeroAndRejectsUnterminatedNdjsonFrame() throws Exception {
        // 1. Subscriber requests exactly 1 item; host emits 1 item + complete -> onComplete must fire with 0 remaining demand!
        startServer("/tpf/functions/exact-demand/duplex", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, 0);
            try (var reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));
                 var out = exchange.getResponseBody()) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) continue;
                    JsonNode f = EXACT_JSON.readTree(line);
                    if ("item".equals(f.path("event").asText())) {
                        out.write("{\"event\":\"item\",\"outputTypeId\":\"OutFrame\",\"payload\":{\"id\":\"only-1\",\"amount\":9.99}}\n"
                            .getBytes(StandardCharsets.UTF_8));
                        out.flush();
                    } else if ("complete".equals(f.path("event").asText())) {
                        out.write("{\"event\":\"complete\",\"itemCount\":1}\n".getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        break;
                    }
                }
            }
        });
        CanonicalFunctionHostManyToManyStep exactStep = new CanonicalFunctionHostManyToManyStep(
            baseSpec("exact-demand", ForeignDispatchIntentStore.noop(), Duration.ofSeconds(5)), true);
        List<TypedDurablePayload> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();
        exactStep.applyTransform(Multi.createFrom().item(inFrame("only", "9.99")))
            .subscribe().withSubscriber(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription s) { s.request(1L); }
                @Override public void onNext(TypedDurablePayload item) { received.add(item); }
                @Override public void onError(Throwable t) { err.set(t); done.countDown(); }
                @Override public void onComplete() { done.countDown(); }
            });
        assertTrue(done.await(5, TimeUnit.SECONDS), "onComplete must arrive even after demand hits 0");
        assertNull(err.get(), () -> "Unexpected error: " + err.get());
        assertEquals(1, received.size());

        // 2. Unterminated NDJSON frame (missing trailing newline before EOF) must fail closed!
        server.createContext("/tpf/functions/unterminated/duplex", exchange -> {
            byte[] body = "{\"event\":\"complete\",\"itemCount\":0}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        CanonicalFunctionHostManyToManyStep unterminatedStep = new CanonicalFunctionHostManyToManyStep(
            baseSpec("unterminated", ForeignDispatchIntentStore.noop(), Duration.ofSeconds(5)), true);
        Throwable unterminatedErr = assertThrows(Throwable.class, () ->
            unterminatedStep.applyTransform(Multi.createFrom().item(inFrame("u1", "1.00")))
                .collect().asList().await().atMost(Duration.ofSeconds(5)));
        assertInstanceOf(RemoteTransitionOutcomeUnknownException.class, unterminatedErr);
    }

    @Test
    void propagatesDownstreamCancellationAndFailsClosedOnTruncatedIdleErrorAndAuth() throws Exception {
        // 1. Downstream cancellation (take 1 of infinite-like stream) cancels upstream & closes socket
        CountDownLatch upstreamCancelled = new CountDownLatch(1);
        startServer("/tpf/functions/cancel-duplex/duplex", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, 0);
            try (var reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));
                 var out = exchange.getResponseBody()) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) continue;
                    JsonNode f = EXACT_JSON.readTree(line);
                    if ("item".equals(f.path("event").asText())) {
                        out.write("{\"event\":\"item\",\"outputTypeId\":\"OutFrame\",\"payload\":{\"id\":\"c1\",\"amount\":1.00}}\n"
                            .getBytes(StandardCharsets.UTF_8));
                        out.flush();
                    }
                }
            }
        });
        CanonicalFunctionHostManyToManyStep cancelStep = new CanonicalFunctionHostManyToManyStep(
            baseSpec("cancel-duplex", ForeignDispatchIntentStore.noop(), Duration.ofSeconds(5)), true);
        Multi<TypedDurablePayload> openStream = Multi.createFrom().emitter(em -> {
            em.onTermination(upstreamCancelled::countDown);
            em.emit(inFrame("a", "1.00"));
        });
        List<TypedDurablePayload> firstOnly = cancelStep.applyTransform(openStream).select().first(1)
            .collect().asList().await().atMost(Duration.ofSeconds(5));
        assertEquals(1, firstOnly.size());
        assertTrue(upstreamCancelled.await(2, TimeUnit.SECONDS), "Downstream cancel must cancel upstream Multi");

        // 2. Truncated stream (EOF without complete sentinel) -> RemoteTransitionOutcomeUnknownException
        server.createContext("/tpf/functions/trunc-duplex/duplex", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write("{\"event\":\"item\",\"outputTypeId\":\"OutFrame\",\"payload\":{\"id\":\"t1\",\"amount\":1.00}}\n"
                    .getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        });
        CanonicalFunctionHostManyToManyStep truncStep = new CanonicalFunctionHostManyToManyStep(
            baseSpec("trunc-duplex", ForeignDispatchIntentStore.noop(), Duration.ofSeconds(5)), true);
        Throwable truncErr = assertThrows(Throwable.class, () ->
            truncStep.applyTransform(Multi.createFrom().item(inFrame("a", "1.00")))
                .collect().asList().await().atMost(Duration.ofSeconds(5)));
        assertInstanceOf(RemoteTransitionOutcomeUnknownException.class, truncErr);
        assertFalse(truncStep.shouldRetry(truncErr));

        // 3. Per-frame idle timeout -> RemoteTransitionOutcomeUnknownException
        server.createContext("/tpf/functions/slow-duplex/duplex", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream out = exchange.getResponseBody()) {
                out.flush();
                Thread.sleep(600);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        CanonicalFunctionHostManyToManyStep slowStep = new CanonicalFunctionHostManyToManyStep(
            baseSpec("slow-duplex", ForeignDispatchIntentStore.noop(), Duration.ofMillis(120)), true);
        Throwable slowErr = assertThrows(Throwable.class, () ->
            slowStep.applyTransform(Multi.createFrom().item(inFrame("a", "1.00")))
                .collect().asList().await().atMost(Duration.ofSeconds(5)));
        assertInstanceOf(RemoteTransitionOutcomeUnknownException.class, slowErr);

        // 4. BUSINESS_REJECTION portable error frame -> non-retryable TransitionWorkerFailureException
        server.createContext("/tpf/functions/rej-duplex/duplex", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write("{\"event\":\"error\",\"disposition\":\"BUSINESS_REJECTION\",\"code\":\"BAD_ITEM\",\"message\":\"nope\"}\n"
                    .getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        });
        CanonicalFunctionHostManyToManyStep rejStep = new CanonicalFunctionHostManyToManyStep(
            baseSpec("rej-duplex", ForeignDispatchIntentStore.noop(), Duration.ofSeconds(5)), true);
        Throwable rejErr = assertThrows(Throwable.class, () ->
            rejStep.applyTransform(Multi.createFrom().item(inFrame("a", "1.00")))
                .collect().asList().await().atMost(Duration.ofSeconds(5)));
        assertInstanceOf(TransitionWorkerFailureException.class, rejErr);
        assertInstanceOf(NonRetryableException.class, rejErr.getCause());
        assertFalse(rejStep.shouldRetry(rejErr));

        // 5. HTTP 401 -> non-retryable auth failure
        server.createContext("/tpf/functions/unauth-duplex/duplex", exchange -> {
            byte[] resp = "{\"status\":\"ERROR\",\"code\":\"UNAUTHORIZED\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(401, resp.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(resp);
            }
        });
        CanonicalFunctionHostManyToManyStep unauthStep = new CanonicalFunctionHostManyToManyStep(
            baseSpec("unauth-duplex", ForeignDispatchIntentStore.noop(), Duration.ofSeconds(5)), true);
        Throwable unauthErr = assertThrows(Throwable.class, () ->
            unauthStep.applyTransform(Multi.createFrom().item(inFrame("a", "1.00")))
                .collect().asList().await().atMost(Duration.ofSeconds(5)));
        assertInstanceOf(TransitionWorkerFailureException.class, unauthErr);
        assertFalse(unauthStep.shouldRetry(unauthErr));
    }

    private CanonicalFunctionHostManyToManyStep.Spec baseSpec(
        String stepId, ForeignDispatchIntentStore intentStore, Duration idleTimeout
    ) {
        return new CanonicalFunctionHostManyToManyStep.Spec(
            stepId, 0, "InFrame", "fp-InFrame", "OutFrame", "fp-OutFrame", CATALOG_FP, CATALOGUE,
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()), SECRET,
            Duration.ofSeconds(2), idleTimeout, Optional.of("cmd-" + stepId),
            intentStore, CanonicalFunctionHostSupport.DEFAULT_MAX_RESPONSE_BYTES);
    }

    private void startServer(String path, com.sun.net.httpserver.HttpHandler handler) throws IOException {
        serverExecutor = Executors.newCachedThreadPool();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(serverExecutor);
        server.createContext(path, handler);
        server.start();
    }

    private static void verifyStreamingSignature(HttpExchange ex, String path, AtomicBoolean valid) {
        String ts = ex.getRequestHeaders().getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER);
        String nonce = ex.getRequestHeaders().getFirst(TransitionWorkerSignature.NONCE_HEADER);
        String sig = ex.getRequestHeaders().getFirst(TransitionWorkerSignature.SIGNATURE_HEADER);
        String expected = TransitionWorkerSignature.sign(
            SECRET, "POST", path, ts, nonce,
            CanonicalFunctionHostManyToManyStep.STREAMING_BODY_DIGEST_SENTINEL.getBytes(StandardCharsets.UTF_8));
        valid.set(expected.equals(sig));
    }

    private static TypedDurablePayload inFrame(String id, String amount) {
        String json = "{\"id\":\"" + id + "\",\"amount\":" + amount + "}";
        return new TypedDurablePayload("InFrame", "fp-InFrame", CATALOG_FP,
            JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
            json.getBytes(StandardCharsets.UTF_8));
    }

    private static CanonicalTypeCatalogue buildCatalogue() {
        Map<String, Map<String, Object>> types = new LinkedHashMap<>();
        for (String name : List.of("InFrame", "OutFrame")) {
            Map<String, Object> idField = Map.of("name", "id", "presence", "REQUIRED", "nullability", "NON_NULL",
                "type", Map.of("kind", "scalar", "id", "string"));
            Map<String, Object> amtField = Map.of("name", "amount", "presence", "REQUIRED", "nullability", "NON_NULL",
                "type", Map.of("kind", "scalar", "id", "decimal"));
            types.put(name, Map.of(
                "definitionFingerprint", "fp-" + name,
                "definition", Map.of("kind", "record", "id", name, "fields", List.of(idField, amtField))));
        }
        return CanonicalTypeCatalogue.fromCanonicalTypes(types);
    }
}

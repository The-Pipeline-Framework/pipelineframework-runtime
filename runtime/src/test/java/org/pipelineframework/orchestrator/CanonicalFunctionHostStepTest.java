package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
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
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.type.CanonicalTypeCatalogue;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

/**
 * Unit tests for {@link CanonicalFunctionHostStep} (1:1), {@link CanonicalFunctionHostOneToManyStep} (1:N),
 * {@link CanonicalFunctionHostManyToOneStep} (N:1), and pre-send {@link ForeignDispatchIntentStore}
 * durability (in-memory and conditional DynamoDB PutItem).
 */
class CanonicalFunctionHostStepTest {

    private static final String SECRET = "unit-test-shared-secret";
    private static final String CATALOG_FP = "fp-catalog";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private HttpServer server;
    private URI baseUri;
    private CanonicalTypeCatalogue catalogue;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        baseUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        catalogue = buildCatalogue();
    }

    @AfterEach
    void tearDown() {
        TransitionDispatchScopeHolder.clear();
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void oneToOneSignsValidatesAndTransformsCanonicalPayload() {
        AtomicReference<String> verifiedSig = new AtomicReference<>();
        server.createContext("/tpf/functions/approveInvoice", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String ts = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER);
            String nonce = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.NONCE_HEADER);
            String sig = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.SIGNATURE_HEADER);
            String expected = TransitionWorkerSignature.sign(SECRET, "POST", "/tpf/functions/approveInvoice", ts, nonce, body);
            verifiedSig.set(sig.equals(expected) ? "MATCH" : "MISMATCH");
            respondJson(exchange, 200, """
                {"status":"OK","outputTypeId":"ApprovedInvoice","payload":{"invoiceId":"inv-1","approvedAmountCents":1500}}
                """);
        });

        var step = unaryStep("approveInvoice", 0, Optional.empty(), ForeignDispatchIntentStore.noop(), 4 * 1024 * 1024);
        TypedDurablePayload out = step.applyOneToOne(invoicePayload("inv-1", 1500)).await().atMost(TIMEOUT);

        assertEquals("MATCH", verifiedSig.get());
        assertEquals("ApprovedInvoice", out.canonicalTypeId());
        assertEquals("{\"approvedAmountCents\":1500,\"invoiceId\":\"inv-1\"}",
            new String(out.payload(), StandardCharsets.UTF_8));
    }

    @Test
    void oneToOneRejectsRedirectsAuthFailuresAndOversizedResponsesFailClosed() {
        AtomicInteger calls = new AtomicInteger();
        server.createContext("/tpf/functions/redirStep", exchange -> {
            calls.incrementAndGet();
            exchange.getResponseHeaders().set("Location", baseUri + "/tpf/functions/other");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.createContext("/tpf/functions/authStep", exchange -> respondJson(exchange, 403, "{\"error\":\"denied\"}"));
        server.createContext("/tpf/functions/bigStep", exchange -> respondJson(exchange, 200,
            "{\"status\":\"OK\",\"outputTypeId\":\"ApprovedInvoice\",\"payload\":{\"invoiceId\":\""
                + "x".repeat(512) + "\",\"approvedAmountCents\":10}}"));

        var redirStep = unaryStep("redirStep", 0, Optional.empty(), ForeignDispatchIntentStore.noop(), 4096);
        assertThrows(RemoteTransitionOutcomeUnknownException.class,
            () -> redirStep.applyOneToOne(invoicePayload("inv-1", 10)).await().atMost(TIMEOUT));
        assertEquals(1, calls.get(), "HttpClient must never follow redirects");

        var authStep = unaryStep("authStep", 2, Optional.of("cmd-auth"), ForeignDispatchIntentStore.noop(), 4096);
        TransitionWorkerFailureException authErr = assertThrows(TransitionWorkerFailureException.class,
            () -> authStep.applyOneToOne(invoicePayload("inv-1", 10)).await().atMost(TIMEOUT));
        assertInstanceOf(NonRetryableException.class, authErr.getCause());
        assertEquals(2, authErr.failedStepIndex());
        assertEquals(Optional.of("cmd-auth"), authErr.failedCommandId());

        var bigStep = unaryStep("bigStep", 0, Optional.empty(), ForeignDispatchIntentStore.noop(), 128);
        assertThrows(RemoteTransitionOutcomeUnknownException.class,
            () -> bigStep.applyOneToOne(invoicePayload("inv-1", 10)).await().atMost(TIMEOUT));
    }

    @Test
    void oneToOnePreservesFailureIdentityAndSuppressesDuplicateSendOnUnresolvedIntent() {
        AtomicInteger sends = new AtomicInteger();
        server.createContext("/tpf/functions/chargeStep", exchange -> {
            sends.incrementAndGet();
            respondJson(exchange, 200, """
                {"status":"ERROR","disposition":"BUSINESS_REJECTION","code":"LIMIT_EXCEEDED","message":"Over cap"}
                """);
        });

        var intentStore = new InMemoryForeignDispatchIntentStore();
        var step = unaryStep("chargeStep", 3, Optional.of("cmd-charge"), intentStore, 4096);

        // 1. Pre-seed an unresolved reservation simulating a crash/SIGKILL right before receiving response
        TransitionDispatchScope scope = new TransitionDispatchScope("tenant-1", "exec-crash", 1L, 0, "exec-crash:3:0");
        TransitionDispatchScopeHolder.set(scope);
        ForeignDispatchIntent crashedIntent = new ForeignDispatchIntent(
            "tenant-1", "exec-crash", 1L, 0, "exec-crash:3:0", 3, "chargeStep",
            baseUri + "/tpf/functions/chargeStep", "digest", System.currentTimeMillis());
        assertEquals(ForeignDispatchIntentStore.ReservationOutcome.RESERVED, intentStore.reserve(crashedIntent));

        assertThrows(RemoteTransitionOutcomeUnknownException.class,
            () -> step.applyOneToOne(invoicePayload("inv-crash", 500)).await().atMost(TIMEOUT));
        assertEquals(0, sends.get(), "Unresolved pre-send intent must block any duplicate HTTP send");

        // 2. Fresh attempt reserves, executes, records VALIDATED_TERMINAL, and preserves stepIndex/commandId
        TransitionDispatchScopeHolder.set(new TransitionDispatchScope("tenant-1", "exec-fresh", 1L, 0, "exec-fresh:3:0"));
        TransitionWorkerFailureException rejected = assertThrows(TransitionWorkerFailureException.class,
            () -> step.applyOneToOne(invoicePayload("inv-fresh", 500)).await().atMost(TIMEOUT));
        assertInstanceOf(NonRetryableException.class, rejected.getCause());
        assertEquals(3, rejected.failedStepIndex());
        assertEquals(Optional.of("cmd-charge"), rejected.failedCommandId());
        assertEquals(1, sends.get());
        assertEquals(2, intentStore.reservationCount());
    }

    @Test
    void oneToManyEnforcesDemandCancellationTruncationAndDispatchIntent() throws Exception {
        AtomicInteger streamOpens = new AtomicInteger();
        CountDownLatch streamCancelled = new CountDownLatch(1);
        server.createContext("/tpf/functions/splitItems/stream", exchange -> {
            streamOpens.incrementAndGet();
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream out = exchange.getResponseBody()) {
                for (int i = 0; i < 5; i++) {
                    String frame = "{\"event\":\"item\",\"outputTypeId\":\"ApprovedInvoice\","
                        + "\"payload\":{\"invoiceId\":\"inv-" + i + "\",\"approvedAmountCents\":" + (100 + i) + "}}\n";
                    out.write(frame.getBytes(StandardCharsets.UTF_8));
                    out.flush();
                }
                out.write("{\"event\":\"complete\"}\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        });
        server.createContext("/tpf/functions/truncated/stream", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write("{\"event\":\"item\",\"outputTypeId\":\"ApprovedInvoice\",\"payload\":{\"invoiceId\":\"inv-0\",\"approvedAmountCents\":100}}\n"
                    .getBytes(StandardCharsets.UTF_8));
                out.flush();
                // Intentionally close without {"event":"complete"} sentinel!
            }
        });
        server.createContext("/tpf/functions/slowCancel/stream", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write("{\"event\":\"item\",\"outputTypeId\":\"ApprovedInvoice\",\"payload\":{\"invoiceId\":\"inv-0\",\"approvedAmountCents\":100}}\n"
                    .getBytes(StandardCharsets.UTF_8));
                out.flush();
                // Keep writing until peer cancels/closes socket
                for (int i = 1; i < 200; i++) {
                    Thread.sleep(25);
                    out.write(("{\"event\":\"item\",\"outputTypeId\":\"ApprovedInvoice\",\"payload\":{\"invoiceId\":\"inv-"
                        + i + "\",\"approvedAmountCents\":100}}\n").getBytes(StandardCharsets.UTF_8));
                    out.flush();
                }
            } catch (IOException | InterruptedException closed) {
                streamCancelled.countDown();
            }
        });

        var intentStore = new InMemoryForeignDispatchIntentStore();
        var splitStep = oneToManyStep("splitItems", intentStore, Duration.ofSeconds(3));
        splitStep.initialiseWithConfig(new StepConfig().retryLimit(0));

        // 1. Happy path streaming
        TransitionDispatchScopeHolder.set(new TransitionDispatchScope("t1", "exec-stream", 1L, 0, "exec-stream:0:0"));
        List<TypedDurablePayload> items = splitStep.apply(io.smallrye.mutiny.Uni.createFrom().item(invoicePayload("inv-batch", 500)))
            .collect().asList().await().atMost(TIMEOUT);
        assertEquals(5, items.size());
        assertEquals(1, streamOpens.get());

        // 2. Duplicate attempt with same transitionKey blocked by pre-send dispatch intent
        assertThrows(RemoteTransitionOutcomeUnknownException.class,
            () -> splitStep.applyOneToMany(invoicePayload("inv-batch", 500)).collect().asList().await().atMost(TIMEOUT));
        assertEquals(1, streamOpens.get());

        // 3. Truncated stream without completion sentinel fails closed
        TransitionDispatchScopeHolder.clear();
        var truncStep = oneToManyStep("truncated", ForeignDispatchIntentStore.noop(), Duration.ofSeconds(3));
        assertThrows(RemoteTransitionOutcomeUnknownException.class,
            () -> truncStep.applyOneToMany(invoicePayload("inv-t", 100)).collect().asList().await().atMost(TIMEOUT));

        // 4. Demand-driven request(1) + cancel() aborts stream promptly
        var cancelStep = oneToManyStep("slowCancel", ForeignDispatchIntentStore.noop(), Duration.ofSeconds(3));
        List<TypedDurablePayload> received = new CopyOnWriteArrayList<>();
        CountDownLatch gotOne = new CountDownLatch(1);
        cancelStep.applyOneToMany(invoicePayload("inv-c", 100)).subscribe().withSubscriber(new Flow.Subscriber<>() {
            private Flow.Subscription sub;
            @Override public void onSubscribe(Flow.Subscription s) {
                this.sub = s;
                s.request(1);
            }
            @Override public void onNext(TypedDurablePayload item) {
                received.add(item);
                sub.cancel();
                gotOne.countDown();
            }
            @Override public void onError(Throwable t) { }
            @Override public void onComplete() { }
        });
        assertTrue(gotOne.await(5, TimeUnit.SECONDS));
        assertEquals(1, received.size());
        assertTrue(streamCancelled.await(5, TimeUnit.SECONDS), "Cancelling subscription must close HTTP stream");
    }

    @Test
    void manyToOneStreamsNdjsonAndEnforcesAuthAndDispatchIntent() {
        List<String> receivedLines = new CopyOnWriteArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        server.createContext("/tpf/functions/sumBatch/reduce", exchange -> {
            calls.incrementAndGet();
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            for (String line : body.split("\n")) {
                if (!line.isBlank()) {
                    receivedLines.add(line);
                }
            }
            respondJson(exchange, 200, """
                {"status":"OK","outputTypeId":"ApprovedInvoice","payload":{"invoiceId":"batch-sum","approvedAmountCents":300}}
                """);
        });
        server.createContext("/tpf/functions/sumAuthFail/reduce", exchange -> respondJson(exchange, 401, "{}"));

        var intentStore = new InMemoryForeignDispatchIntentStore();
        var reduceStep = manyToOneStep("sumBatch", intentStore);
        reduceStep.initialiseWithConfig(new StepConfig().retryLimit(0));

        TransitionDispatchScopeHolder.set(new TransitionDispatchScope("t1", "exec-reduce", 1L, 0, "exec-reduce:1:0"));
        TypedDurablePayload reduced = reduceStep.apply(
                Multi.createFrom().items(invoicePayload("a", 100), invoicePayload("b", 200)))
            .await().atMost(TIMEOUT);
        assertEquals("ApprovedInvoice", reduced.canonicalTypeId());
        assertEquals(3, receivedLines.size());
        assertTrue(receivedLines.get(2).contains("\"complete\""));
        assertEquals(1, calls.get());

        // Duplicate reduce on same transitionKey fails closed without sending
        assertThrows(RemoteTransitionOutcomeUnknownException.class,
            () -> reduceStep.applyReduce(Multi.createFrom().item(invoicePayload("a", 100))).await().atMost(TIMEOUT));
        assertEquals(1, calls.get());

        // 401 auth rejection is terminal NonRetryableException
        TransitionDispatchScopeHolder.clear();
        var authFailStep = manyToOneStep("sumAuthFail", ForeignDispatchIntentStore.noop());
        TransitionWorkerFailureException authErr = assertThrows(TransitionWorkerFailureException.class,
            () -> authFailStep.applyReduce(Multi.createFrom().item(invoicePayload("a", 100))).await().atMost(TIMEOUT));
        assertInstanceOf(NonRetryableException.class, authErr.getCause());
    }

    @Test
    void dynamoDispatchIntentStoreUsesConditionalPutItemOnly() {
        DynamoDbClient mockDynamo = mock(DynamoDbClient.class);
        when(mockDynamo.putItem(any(PutItemRequest.class)))
            .thenReturn(PutItemResponse.builder().build())
            .thenThrow(ConditionalCheckFailedException.builder().message("exists").build())
            .thenReturn(PutItemResponse.builder().build());

        var store = new DynamoForeignDispatchIntentStore(mockDynamo, "tpf-dispatch-intents");
        var intent = new ForeignDispatchIntent(
            "tenant-1", "exec-1", 1L, 0, "exec-1:0:0", 0, "approveInvoice",
            "https://worker.internal/tpf/functions/approveInvoice", "abc123", 1000L);

        assertEquals(ForeignDispatchIntentStore.ReservationOutcome.RESERVED, store.reserve(intent));
        assertEquals(ForeignDispatchIntentStore.ReservationOutcome.ALREADY_DISPATCHED_UNCERTAIN, store.reserve(intent));
        store.recordSettlement(intent, ForeignDispatchIntentStore.SettlementStatus.VALIDATED_SUCCEEDED, 2000L);

        ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(mockDynamo, times(3)).putItem(captor.capture());
        List<PutItemRequest> requests = captor.getAllValues();
        for (PutItemRequest req : requests) {
            assertEquals("tpf-dispatch-intents", req.tableName());
            assertEquals("attribute_not_exists(#pk) AND attribute_not_exists(#sk)", req.conditionExpression());
        }
        assertEquals("RESERVE", requests.get(0).item().get("sk").s());
        assertEquals("SETTLE", requests.get(2).item().get("sk").s());
        assertEquals("VALIDATED_SUCCEEDED", requests.get(2).item().get("settlementStatus").s());
        assertFalse(requests.get(0).item().containsKey("sharedSecret"));
    }

    private CanonicalFunctionHostStep unaryStep(
        String stepId, int stepIndex, Optional<String> commandId,
        ForeignDispatchIntentStore intentStore, int maxResponseBytes
    ) {
        return new CanonicalFunctionHostStep(
            new CanonicalFunctionHostStep.Spec(
                stepId, stepIndex, "Invoice", "fp-Invoice", "ApprovedInvoice", "fp-ApprovedInvoice",
                CATALOG_FP, catalogue, baseUri, SECRET, Duration.ofSeconds(3), commandId,
                intentStore, maxResponseBytes),
            true);
    }

    private CanonicalFunctionHostOneToManyStep oneToManyStep(
        String stepId, ForeignDispatchIntentStore intentStore, Duration idleTimeout
    ) {
        return new CanonicalFunctionHostOneToManyStep(
            new CanonicalFunctionHostOneToManyStep.Spec(
                stepId, 0, "Invoice", "fp-Invoice", "ApprovedInvoice", "fp-ApprovedInvoice",
                CATALOG_FP, catalogue, baseUri, SECRET, Duration.ofSeconds(3), idleTimeout,
                Optional.empty(), intentStore, 4 * 1024 * 1024),
            true);
    }

    private CanonicalFunctionHostManyToOneStep manyToOneStep(
        String stepId, ForeignDispatchIntentStore intentStore
    ) {
        return new CanonicalFunctionHostManyToOneStep(
            new CanonicalFunctionHostManyToOneStep.Spec(
                stepId, 1, "Invoice", "fp-Invoice", "ApprovedInvoice", "fp-ApprovedInvoice",
                CATALOG_FP, catalogue, baseUri, SECRET, Duration.ofSeconds(3), Optional.empty(),
                intentStore, 4 * 1024 * 1024),
            true);
    }

    private TypedDurablePayload invoicePayload(String id, int amountCents) {
        String json = "{\"id\":\"" + id + "\",\"amountCents\":" + amountCents + "}";
        return new TypedDurablePayload(
            "Invoice", "fp-Invoice", CATALOG_FP,
            JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
            json.getBytes(StandardCharsets.UTF_8));
    }

    private static void respondJson(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static CanonicalTypeCatalogue buildCatalogue() {
        Map<String, Map<String, Object>> defs = new LinkedHashMap<>();
        defs.put("Invoice", recordDef("Invoice", List.of(
            field("id", "string"),
            field("amountCents", "int32"))));
        defs.put("ApprovedInvoice", recordDef("ApprovedInvoice", List.of(
            field("invoiceId", "string"),
            field("approvedAmountCents", "int32"))));
        return CanonicalTypeCatalogue.fromCanonicalTypes(defs);
    }

    private static Map<String, Object> recordDef(String id, List<Map<String, Object>> fields) {
        Map<String, Object> def = new LinkedHashMap<>();
        def.put("kind", "record");
        def.put("id", id);
        def.put("fields", new ArrayList<>(fields));
        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("definitionFingerprint", "fp-" + id);
        wrapper.put("definition", def);
        return wrapper;
    }

    private static Map<String, Object> field(String name, String scalarId) {
        Map<String, Object> type = new LinkedHashMap<>();
        type.put("kind", "scalar");
        type.put("id", scalarId);
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("name", name);
        f.put("presence", "REQUIRED");
        f.put("nullability", "NON_NULL");
        f.put("type", type);
        return f;
    }
}

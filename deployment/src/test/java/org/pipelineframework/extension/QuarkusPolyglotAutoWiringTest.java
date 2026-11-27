package org.pipelineframework.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.quarkus.test.QuarkusExtensionTest;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.PolyglotStepAutoWirer;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ForeignDispatchIntentStore;
import org.pipelineframework.orchestrator.InMemoryExecutionStateStore;
import org.pipelineframework.orchestrator.InMemoryForeignDispatchIntentStore;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/**
 * Boots a real Quarkus application with ZERO user Java domain classes and ZERO user Java step classes,
 * proving automatic CDI wiring of {@code CanonicalFunctionHost*} (1:1, 1:N, N:1) and Tier-2
 * {@code PolyglotConnectorProvider} (Query + Command) directly from {@code META-INF/pipeline/pipeline-contract.json}
 * and {@code pipeline.yaml}.
 */
@Isolated
class QuarkusPolyglotAutoWiringTest {

    private static final ObjectMapper EXACT_JSON = PipelineJson.mapper().copy()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String SHARED_SECRET = "quarkus-polyglot-hmac-secret";
    private static final String FX_SECRET = "host-fx-secret-42";
    private static final String LEDGER_SECRET = "host-ledger-secret-99";
    private static final String CATALOG_FP = "fp-quarkus-polyglot-catalog";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private static final String PIPELINE_YAML = """
        version: 3
        appName: QuarkusPolyglotAutoWiring
        basePackage: com.example.polyglot.quarkus
        transport: LOCAL
        platform: COMPUTE
        connectors:
          fx-api:
            provider: polyglot.connector
            version: 1
            config:
              connection: polyglot-connector-host
          ledger-host:
            provider: polyglot.connector
            version: 1
            config:
              connection: polyglot-connector-host
              reconciliationSupported: true
              receiptReferenceKind: ledger.entry.id
        types:
          OrderInput:
            fields:
              - name: orderId
                type: string
              - name: mode
                type: string
              - name: amount
                type: decimal
          FxRequest:
            fields:
              - name: orderId
                type: string
              - name: amount
                type: decimal
          FxQuote:
            fields:
              - name: orderId
                type: string
              - name: convertedAmount
                type: decimal
          LedgerReceipt:
            fields:
              - name: orderId
                type: string
              - name: entryId
                type: string
              - name: postedAmount
                type: decimal
          PricedSlice:
            fields:
              - name: orderId
                type: string
              - name: entryId
                type: string
              - name: sliceAmount
                type: decimal
          SettlementSummary:
            fields:
              - name: orderId
                type: string
              - name: entryId
                type: string
              - name: sliceCount
                type: int32
              - name: totalAmount
                type: decimal
        steps:
          - name: Prepare Order
            cardinality: ONE_TO_ONE
            input: OrderInput
            output: FxRequest
          - name: Lookup Fx Quote
            kind: query
            using: fx-api
            operation: fx.quote
            operationVersion: 1
            cardinality: ONE_TO_ONE
            input: FxRequest
            output: FxQuote
          - name: Post Ledger
            kind: command
            using: ledger-host
            operation: ledger.post
            operationVersion: 1
            cardinality: ONE_TO_ONE
            input: FxQuote
            output: LedgerReceipt
          - name: Split Slices
            cardinality: ONE_TO_MANY
            input: LedgerReceipt
            output: PricedSlice
          - name: Summarize Settlement
            cardinality: MANY_TO_ONE
            input: PricedSlice
            output: SettlementSummary
        """;

    @RegisterExtension
    static final QuarkusExtensionTest APPLICATION = buildApplication();

    private static QuarkusExtensionTest buildApplication() {
        HttpServer foreignHost = startForeignHost();
        String hostUri = "http://127.0.0.1:" + foreignHost.getAddress().getPort();
        return new QuarkusExtensionTest()
            .withApplicationRoot(archive -> archive
                .addClass(TestLambdaHandler.class)
                .addAsResource(new StringAsset(contractJson()), PipelineContractDescriptor.RESOURCE_PATH)
                .addAsResource(new StringAsset(PIPELINE_YAML), "pipeline.yaml"))
            .overrideConfigKey("quarkus.devservices.enabled", "false")
            .overrideConfigKey("quarkus.http.test-port", "0")
            .overrideConfigKey("pipeline.orchestrator.mode", "QUEUE_ASYNC")
            .overrideConfigKey("pipeline.orchestrator.idempotency-policy", "CLIENT_KEY_REQUIRED")
            .overrideConfigKey("pipeline.orchestrator.process-loops-disabled", "false")
            .overrideConfigKey("pipeline.orchestrator.max-retries", "2")
            .overrideConfigKey("pipeline.orchestrator.retry-delay", "PT0.05S")
            .overrideConfigKey("pipeline.orchestrator.retry-multiplier", "1.0")
            .overrideConfigKey("pipeline.orchestrator.sweep-interval", "PT0.05S")
            .overrideConfigKey("pipeline.defaults.retry-limit", "0")
            .overrideConfigKey("pipeline.polyglot.enabled", "true")
            .overrideConfigKey("pipeline.polyglot.function-host-uri", hostUri)
            .overrideConfigKey("pipeline.polyglot.shared-secret", SHARED_SECRET)
            .overrideConfigKey("pipeline.polyglot.allow-loopback-http", "true")
            .overrideConfigKey("pipeline.polyglot.dispatch-intent-store", "memory")
            .overrideConfigKey("pipeline.polyglot.connector.host-uri", hostUri)
            .overrideConfigKey("pipeline.polyglot.connector.secrets.fx-api-key", FX_SECRET)
            .overrideConfigKey("pipeline.polyglot.connector.secrets.ledger-api-key", LEDGER_SECRET)
            .setAfterAllCustomizer(() -> foreignHost.stop(0));
    }

    @Inject PolyglotStepAutoWirer autoWirer;
    @Inject PipelineExecutionService executionService;
    @Inject InMemoryExecutionStateStore stateStore;
    @Inject ConnectorBindingRegistry bindingRegistry;
    @Inject ForeignDispatchIntentStore intentStore;
    @Inject org.pipelineframework.orchestrator.PolyglotRuntimeConfig polyglotConfig;

    @Test
    void zeroUserJavaQuarkusAppExecutesEndToEndPolyglotStreamingAndTier2Connectors() throws Exception {
        assertTrue(autoWirer.isActive(), "PolyglotStepAutoWirer must activate for class-free v3 contract");
        assertEquals(5, autoWirer.loadOrderedSteps().size());
        assertTrue(bindingRegistry.providers().containsKey(ConnectorBindingName.of("fx-api")));
        assertTrue(bindingRegistry.providers().containsKey(ConnectorBindingName.of("ledger-host")));
        assertTrue(io.quarkus.arc.ClientProxy.unwrap(intentStore) instanceof InMemoryForeignDispatchIntentStore);

        // 1. Happy path with 29-digit decimal across 1:1 -> Query -> Command -> 1:N -> N:1
        String exact29 = "10000000000000000000.123456789";
        TypedDurablePayload okInput = orderPayload("ord-1", "OK", exact29);
        var okAccepted = executionService.executePipelineAsync(okInput, "tenant-q", "key-ok-1", false)
            .await().atMost(TIMEOUT);
        ExecutionRecord<Object, Object> okRecord = awaitTerminal("tenant-q", okAccepted.executionId());
        assertEquals(ExecutionStatus.SUCCEEDED, okRecord.status(),
            "Expected SUCCEEDED, got: " + okRecord.errorCode() + " / " + okRecord.errorMessage());

        TypedDurablePayload typedOut = executionService
            .<TypedDurablePayload>getExecutionResult("tenant-q", okAccepted.executionId(), TypedDurablePayload.class, false)
            .await().atMost(TIMEOUT);
        JsonNode summary = EXACT_JSON.readTree(typedOut.payload());
        assertEquals("ord-1", summary.path("orderId").asText());
        assertEquals("LEDGER-ord-1", summary.path("entryId").asText());
        assertEquals(2, summary.path("sliceCount").asInt());
        assertEquals("20000000000000000000.246913578", summary.path("totalAmount").decimalValue().toPlainString(),
            "29-digit decimal must survive 1:1 -> Query -> Command -> 1:N -> N:1 inside Quarkus");

        JsonNode stats = fetchHostStats(polyglotConfig.functionHostUri().orElseThrow());
        assertTrue(stats.path("requestCount").asInt() >= 5, "Expected at least 5 signed host requests, got: " + stats);
        assertTrue(stats.path("allSignaturesValid").asBoolean(),
            "All foreign host requests must carry valid HMAC-SHA256 signatures: " + stats);
        assertTrue(stats.path("fxQuerySawSecret").asBoolean(),
            "fx.quote connector must receive fx-api-key: " + stats);
        assertTrue(stats.path("ledgerCommandSawSecret").asBoolean(),
            "ledger.post connector must receive ledger-api-key: " + stats);
        assertFalse(stats.path("functionSawConnectorSecret").asBoolean(),
            "Pure business functions must never see connector secrets: " + stats);

        // 2. BUSINESS_REJECTION fails terminally with 0 retries, preserving failedStepIndex and failedCommandId
        var rejAccepted = executionService.executePipelineAsync(
            orderPayload("ord-rej", "REJECT", "1.00"), "tenant-q", "key-rej-1", false)
            .await().atMost(TIMEOUT);
        ExecutionRecord<Object, Object> rejRecord = awaitTerminal("tenant-q", rejAccepted.executionId());
        assertEquals(ExecutionStatus.FAILED, rejRecord.status());
        assertEquals(0, rejRecord.attempt());
        assertEquals(0, rejRecord.failedStepIndex());
        assertEquals("cmd-prepare-order", rejRecord.failedCommandId().orElse(""));
        assertNotNull(rejRecord.errorMessage());
        assertTrue(rejRecord.errorMessage().contains("INVALID_ORDER"));

        // 3. TRANSIENT failure pauses in WAIT_RETRY and completes automatically via Quarkus sweeper
        var retryAccepted = executionService.executePipelineAsync(
            orderPayload("ord-ret", "TRANSIENT_ONCE", "5.50"), "tenant-q", "key-ret-1", false)
            .await().atMost(TIMEOUT);
        ExecutionRecord<Object, Object> retryDone = awaitTerminal("tenant-q", retryAccepted.executionId());
        assertEquals(ExecutionStatus.SUCCEEDED, retryDone.status(), retryDone.errorMessage());
        assertEquals(1, retryDone.attempt(), "TRANSIENT failure must succeed on retry attempt 1");
    }

    private ExecutionRecord<Object, Object> awaitTerminal(String tenant, String executionId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
        ExecutionRecord<Object, Object> last = null;
        while (System.currentTimeMillis() < deadline) {
            last = stateStore.getExecution(tenant, executionId).await().atMost(TIMEOUT).orElseThrow();
            if (last.status().terminal()) {
                return last;
            }
            Thread.sleep(20);
        }
        return last;
    }

    private static TypedDurablePayload orderPayload(String orderId, String mode, String amount) {
        String json = "{\"orderId\":\"" + orderId + "\",\"mode\":\"" + mode + "\",\"amount\":" + amount + "}";
        return new TypedDurablePayload(
            "OrderInput", "fp-OrderInput", CATALOG_FP,
            JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
            json.getBytes(StandardCharsets.UTF_8));
    }

    private static String contractJson() {
        Map<String, Map<String, Object>> types = new LinkedHashMap<>();
        types.put("OrderInput", recordDef("OrderInput", List.of(
            field("orderId", "string"), field("mode", "string"), field("amount", "decimal"))));
        types.put("FxRequest", recordDef("FxRequest", List.of(
            field("orderId", "string"), field("amount", "decimal"))));
        types.put("FxQuote", recordDef("FxQuote", List.of(
            field("orderId", "string"), field("convertedAmount", "decimal"))));
        types.put("LedgerReceipt", recordDef("LedgerReceipt", List.of(
            field("orderId", "string"), field("entryId", "string"), field("postedAmount", "decimal"))));
        types.put("PricedSlice", recordDef("PricedSlice", List.of(
            field("orderId", "string"), field("entryId", "string"), field("sliceAmount", "decimal"))));
        types.put("SettlementSummary", recordDef("SettlementSummary", List.of(
            field("orderId", "string"), field("entryId", "string"),
            field("sliceCount", "int32"), field("totalAmount", "decimal"))));

        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            3, "QuarkusPolyglotAutoWiring", "v1", "hash-quarkus-polyglot", "COMPUTE", "LOCAL",
            null, false, "MONOLITH",
            List.of(
                new PipelineBundleStepDescriptor(0, "Prepare Order", "internal", "ONE_TO_ONE", "OrderInput", "FxRequest", null, null, Map.of()),
                new PipelineBundleStepDescriptor(1, "Lookup Fx Quote", "query", "ONE_TO_ONE", "FxRequest", "FxQuote", null, null, Map.of()),
                new PipelineBundleStepDescriptor(2, "Post Ledger", "command", "ONE_TO_ONE", "FxQuote", "LedgerReceipt", null, null, Map.of()),
                new PipelineBundleStepDescriptor(3, "Split Slices", "internal", "ONE_TO_MANY", "LedgerReceipt", "PricedSlice", null, null, Map.of()),
                new PipelineBundleStepDescriptor(4, "Summarize Settlement", "internal", "MANY_TO_ONE", "PricedSlice", "SettlementSummary", null, null, Map.of())),
            PipelineBundleCapabilities.defaults(),
            types,
            CATALOG_FP,
            null,
            List.of(),
            List.of(),
            List.of(),
            Map.of("schemaVersion", 1, "inputCanonicalTypeId", "OrderInput", "outputCanonicalTypeId", "SettlementSummary"));
        try {
            return EXACT_JSON.writeValueAsString(contract);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Map<String, Object> recordDef(String id, List<Map<String, Object>> fields) {
        Map<String, Object> def = new LinkedHashMap<>();
        def.put("kind", "record");
        def.put("id", id);
        def.put("fields", fields);
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

    private static JsonNode fetchHostStats(String hostUri) throws Exception {
        var client = java.net.http.HttpClient.newHttpClient();
        var req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(hostUri + "/_test/stats")).GET().build();
        var resp = client.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        return EXACT_JSON.readTree(resp.body());
    }

    private static HttpServer startForeignHost() {
        AtomicBoolean allSignaturesValid = new AtomicBoolean(true);
        AtomicBoolean functionSawConnectorSecret = new AtomicBoolean(false);
        AtomicBoolean fxQuerySawSecret = new AtomicBoolean(false);
        AtomicBoolean ledgerCommandSawSecret = new AtomicBoolean(false);
        AtomicInteger transientFailures = new AtomicInteger(0);
        AtomicInteger requestCount = new AtomicInteger(0);
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(Executors.newCachedThreadPool());

            server.createContext("/_test/stats", exchange -> {
                String json = "{\"requestCount\":" + requestCount.get()
                    + ",\"allSignaturesValid\":" + allSignaturesValid.get()
                    + ",\"fxQuerySawSecret\":" + fxQuerySawSecret.get()
                    + ",\"ledgerCommandSawSecret\":" + ledgerCommandSawSecret.get()
                    + ",\"functionSawConnectorSecret\":" + functionSawConnectorSecret.get() + "}";
                respond(exchange, 200, "application/json", json);
            });

            server.createContext("/tpf/functions/prepare-order", exchange -> {
                byte[] body = verifySignedRequest(exchange, "/tpf/functions/prepare-order", true,
                    requestCount, allSignaturesValid, functionSawConnectorSecret);
                JsonNode payload = EXACT_JSON.readTree(body).path("payload");
                String mode = payload.path("mode").asText();
                if ("REJECT".equals(mode)) {
                    respond(exchange, 200, "application/json",
                        "{\"status\":\"ERROR\",\"disposition\":\"BUSINESS_REJECTION\",\"code\":\"INVALID_ORDER\",\"message\":\"rejected\"}");
                    return;
                }
                if ("TRANSIENT_ONCE".equals(mode) && transientFailures.getAndIncrement() == 0) {
                    respond(exchange, 200, "application/json",
                        "{\"status\":\"ERROR\",\"disposition\":\"TRANSIENT\",\"code\":\"BUSY\",\"message\":\"try again\"}");
                    return;
                }
                String resp = "{\"status\":\"OK\",\"outputTypeId\":\"FxRequest\",\"payload\":"
                    + "{\"orderId\":\"" + payload.path("orderId").asText() + "\",\"amount\":"
                    + payload.path("amount").decimalValue().toPlainString() + "}}";
                respond(exchange, 200, "application/json", resp);
            });

            server.createContext("/tpf/connector/query/fx.quote", exchange -> {
                byte[] body = verifySignedRequest(exchange, "/tpf/connector/query/fx.quote", false,
                    requestCount, allSignaturesValid, functionSawConnectorSecret);
                JsonNode root = EXACT_JSON.readTree(body);
                if (FX_SECRET.equals(root.path("secrets").path("fx-api-key").asText())) {
                    fxQuerySawSecret.set(true);
                }
                JsonNode inp = root.path("input");
                String resp = "{\"outcome\":\"FOUND\",\"payload\":"
                    + "{\"orderId\":\"" + inp.path("orderId").asText() + "\",\"convertedAmount\":"
                    + inp.path("amount").decimalValue().toPlainString() + "}}";
                respond(exchange, 200, "application/json", resp);
            });

            server.createContext("/tpf/connector/command/ledger.post/dispatch", exchange -> {
                byte[] body = verifySignedRequest(exchange, "/tpf/connector/command/ledger.post/dispatch", false,
                    requestCount, allSignaturesValid, functionSawConnectorSecret);
                JsonNode root = EXACT_JSON.readTree(body);
                if (LEDGER_SECRET.equals(root.path("secrets").path("ledger-api-key").asText())) {
                    ledgerCommandSawSecret.set(true);
                }
                JsonNode inp = root.path("input");
                String orderId = inp.path("orderId").asText();
                String entryId = "LEDGER-" + orderId;
                String resp = "{\"outcome\":\"SUCCEEDED\",\"receiptReference\":\"" + entryId + "\",\"payload\":"
                    + "{\"orderId\":\"" + orderId + "\",\"entryId\":\"" + entryId + "\",\"postedAmount\":"
                    + inp.path("convertedAmount").decimalValue().toPlainString() + "}}";
                respond(exchange, 200, "application/json", resp);
            });

            server.createContext("/tpf/functions/split-slices/stream", exchange -> {
                byte[] body = verifySignedRequest(exchange, "/tpf/functions/split-slices/stream", true,
                    requestCount, allSignaturesValid, functionSawConnectorSecret);
                JsonNode inp = EXACT_JSON.readTree(body).path("payload");
                String orderId = inp.path("orderId").asText();
                String entryId = inp.path("entryId").asText();
                String amt = inp.path("postedAmount").decimalValue().toPlainString();
                String item = "{\"event\":\"item\",\"outputTypeId\":\"PricedSlice\",\"payload\":"
                    + "{\"orderId\":\"" + orderId + "\",\"entryId\":\"" + entryId + "\",\"sliceAmount\":" + amt + "}}";
                String ndjson = item + "\n" + item + "\n{\"event\":\"complete\",\"itemCount\":2}\n";
                respond(exchange, 200, "application/x-ndjson", ndjson);
            });

            server.createContext("/tpf/functions/summarize-settlement/reduce", exchange -> {
                byte[] body = verifySignedRequest(exchange, "/tpf/functions/summarize-settlement/reduce", true,
                    requestCount, allSignaturesValid, functionSawConnectorSecret);
                String[] lines = new String(body, StandardCharsets.UTF_8).split("\n");
                String orderId = "";
                String entryId = "";
                int count = 0;
                BigDecimal total = BigDecimal.ZERO;
                for (String line : lines) {
                    if (line.isBlank()) continue;
                    JsonNode frame = EXACT_JSON.readTree(line);
                    if ("item".equals(frame.path("event").asText())) {
                        JsonNode p = frame.path("payload");
                        orderId = p.path("orderId").asText();
                        entryId = p.path("entryId").asText();
                        total = total.add(p.path("sliceAmount").decimalValue());
                        count++;
                    }
                }
                String resp = "{\"status\":\"OK\",\"outputTypeId\":\"SettlementSummary\",\"payload\":"
                    + "{\"orderId\":\"" + orderId + "\",\"entryId\":\"" + entryId
                    + "\",\"sliceCount\":" + count + ",\"totalAmount\":" + total.toPlainString() + "}}";
                respond(exchange, 200, "application/json", resp);
            });

            server.start();
            return server;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] verifySignedRequest(
        HttpExchange exchange,
        String path,
        boolean isPureFunction,
        AtomicInteger requestCount,
        AtomicBoolean allSignaturesValid,
        AtomicBoolean functionSawConnectorSecret
    ) throws IOException {
        requestCount.incrementAndGet();
        byte[] body = exchange.getRequestBody().readAllBytes();
        String ts = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER);
        String nonce = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.NONCE_HEADER);
        String sig = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.SIGNATURE_HEADER);
        boolean isStreamUpload = path.endsWith("/reduce");
        byte[] signedBytes = isStreamUpload
            ? org.pipelineframework.orchestrator.CanonicalFunctionHostManyToOneStep.STREAMING_BODY_DIGEST_SENTINEL
                .getBytes(StandardCharsets.UTF_8)
            : body;
        String expected = TransitionWorkerSignature.sign(SHARED_SECRET, "POST", path, ts, nonce, signedBytes);
        if (!expected.equals(sig)) {
            allSignaturesValid.set(false);
        }
        if (isPureFunction) {
            String text = new String(body, StandardCharsets.UTF_8);
            if (text.contains(FX_SECRET) || text.contains(LEDGER_SECRET) || text.contains("\"secrets\"")) {
                functionSawConnectorSecret.set(true);
            }
        }
        return body;
    }

    private static void respond(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    /** Satisfies quarkus-amazon-lambda on deployment's test classpath; unused by the polyglot pipeline. */
    public static final class TestLambdaHandler
        implements com.amazonaws.services.lambda.runtime.RequestHandler<Object, Object> {
        @Override
        public Object handleRequest(Object input, com.amazonaws.services.lambda.runtime.Context context) {
            return input;
        }
    }
}

package org.pipelineframework.connector.polyglot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.command.CommandEffectRecord;
import org.pipelineframework.command.CommandEffectStatus;
import org.pipelineframework.command.CommandOutcomeException;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.command.NativeCommandSelector;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderLease;
import org.pipelineframework.connector.ConnectorRegistry;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.orchestrator.CoordinationHost;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;
import org.pipelineframework.query.InMemoryQueryCaptureStore;
import org.pipelineframework.query.NativeQuerySelector;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * End-to-end tests for Tier-2 {@link PolyglotConnectorProvider} wired through TPF's real
 * {@link ConnectorBindingRegistry}, {@link QueryStepSupport} + {@link InMemoryQueryCaptureStore},
 * and {@link CommandStepSupport} + {@link InMemoryCommandEffectStore}.
 */
class PolyglotConnectorProviderTest {

    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final String SHARED_SECRET = "polyglot-connector-hmac-secret";
    private static final String LEDGER_API_KEY = "host-secret-ledger-key-777";
    private static final ConnectorBindingName BINDING = ConnectorBindingName.of("node-custom");

    private HttpServer server;
    private URI hostUri;
    private InMemoryCommandEffectStore effectStore;
    private InMemoryQueryCaptureStore captureStore;
    private CommandStepSupport commandStepSupport;
    private QueryStepSupport queryStepSupport;

    private final AtomicInteger queryCalls = new AtomicInteger();
    private final AtomicInteger dispatchCalls = new AtomicInteger();
    private final AtomicInteger reconcileCalls = new AtomicInteger();
    private final AtomicBoolean verifiedSignature = new AtomicBoolean();
    private final AtomicBoolean verifiedHostSecret = new AtomicBoolean();
    private final AtomicReference<String> queryMode = new AtomicReference<>("NORMAL");
    private final AtomicReference<String> commandMode = new AtomicReference<>("NORMAL");
    private final Map<String, ObjectNode> ledgerDb = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/tpf/connector/query/custom.inventory.lookup", this::handleQuery);
        server.createContext("/tpf/connector/command/custom.ledger.post/dispatch", this::handleDispatch);
        server.createContext("/tpf/connector/command/custom.ledger.post/reconcile", this::handleReconcile);
        server.start();
        hostUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());

        effectStore = new InMemoryCommandEffectStore();
        captureStore = new InMemoryQueryCaptureStore();

        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <T extends ResolvedConnection> java.util.concurrent.CompletionStage<T> resolve(
                ConnectionResolutionRequest<T> request
            ) {
                if (request.connectionType().equals(PolyglotConnectorConnection.class)
                    && "node-custom-host".equals(request.reference().value())) {
                    PolyglotConnectorConnection conn = new PolyglotConnectorConnection(
                        hostUri, SHARED_SECRET, Map.of("ledger-api-key", LEDGER_API_KEY), true);
                    return CompletableFuture.completedStage(request.connectionType().cast(conn));
                }
                return CompletableFuture.failedStage(
                    new ConnectionResolutionException("Unknown connection: " + request.reference().value()));
            }
        };

        ConnectorRuntimeContext runtimeContext = ConnectorRuntimeContext.of(
            "polyglot-test", Executors.newCachedThreadPool(), Clock.systemUTC(), Optional.of(resolver));

        PolyglotConnectorSpec spec = new PolyglotConnectorSpec(
            buildCatalogue(),
            List.of(new PolyglotConnectorSpec.QueryOperationSpec(
                "custom.inventory.lookup", 1, "InventoryQuery", "InventoryStock")),
            List.of(new PolyglotConnectorSpec.CommandOperationSpec(
                "custom.ledger.post", 1, "LedgerPostRequest", "LedgerReceipt",
                true, Optional.of("ledger.entry.id"))));

        PolyglotConnectorProvider provider = new PolyglotConnectorProvider(spec);
        ConnectorBindingDefinition bindingDef = new ConnectorBindingDefinition(
            BINDING,
            PolyglotConnectorProvider.PROVIDER_ID,
            1,
            new ConnectorConfigurationDocument(Map.of(
                "connection", "node-custom-host",
                "recoveryTargetIdentity", "polyglot-ledger-primary",
                "timeoutMs", 4000)));

        ConnectorBindingRegistry bindingRegistry = ConnectorBindingRegistry.fromProviders(
            List.of(bindingDef),
            List.of(provider),
            p -> ConnectorProviderLease.of(p, () -> { }));

        // Uses our new 5-arg CommandStepSupport constructor directly (no upfront manual start required!)
        commandStepSupport = new CommandStepSupport(
            new ConnectorRegistry(List.of()),
            bindingRegistry,
            runtimeContext,
            List.of(effectStore),
            minimalConfig());
        queryStepSupport = new QueryStepSupport(
            List.of(),
            List.of(captureStore),
            bindingRegistry,
            runtimeContext);
    }

    @AfterEach
    void tearDown() {
        PipelineExecutionContextHolder.clear();
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void enforcesHttpsUnlessLoopbackIsExplicitlyAllowed() {
        assertThrows(IllegalArgumentException.class, () -> new PolyglotConnectorConnection(
            URI.create("http://127.0.0.1:8080"), SHARED_SECRET, Map.of(), false));
        assertThrows(IllegalArgumentException.class, () -> new PolyglotConnectorConnection(
            URI.create("http://remote.example.test"), SHARED_SECRET, Map.of(), true));
        var httpsConn = new PolyglotConnectorConnection(
            URI.create("https://connector.internal:8443"), SHARED_SECRET, Map.of("k", "v"), false);
        assertEquals("https", httpsConn.hostUri().getScheme());
    }

    @Test
    void queryCapturesReplaysWithoutDuplicateCallsAndFailsClosedOnSchemaViolation() {
        PipelineExecutionContextHolder.set(execCtx("exec-q1", 1));
        QueryStepDescriptor desc = queryDescriptor();
        ObjectNode req = JSON.createObjectNode().put("sku", "SKU-42");

        JsonNode live = queryStepSupport.queryOneToOne(desc, req, JsonNode.class).await().atMost(TIMEOUT);
        assertEquals("eu-west-1", live.path("warehouse").asText());
        assertEquals(42, live.path("availableUnits").asInt());
        assertTrue(verifiedSignature.get());
        assertTrue(verifiedHostSecret.get());
        assertEquals(1, queryCalls.get());

        // Replay in same execution hits QueryCaptureStore with 0 upstream calls
        JsonNode replayed = queryStepSupport.queryOneToOne(desc, req, JsonNode.class).await().atMost(TIMEOUT);
        assertEquals(live, replayed);
        assertEquals(1, queryCalls.get());

        // Malformed Query payload fails closed (schema violation)
        queryMode.set("BAD_SCHEMA");
        PipelineExecutionContextHolder.set(execCtx("exec-q-bad", 1));
        assertThrows(RuntimeException.class,
            () -> queryStepSupport.queryOneToOne(desc, req, JsonNode.class).await().atMost(TIMEOUT));
    }

    @Test
    void commandReplaysRecordedResultAndAutomaticallyReconcilesAmbiguousDrop() {
        CommandDescriptor desc = commandDescriptor();
        ObjectNode req = JSON.createObjectNode()
            .put("orderId", "ord-1")
            .put("amountCents", 9500);

        // 1. Happy path + RETURN_RECORDED duplicate replay (0 duplicate dispatches)
        PipelineExecutionContextHolder.set(execCtx("exec-c1", 2));
        JsonNode first = commandStepSupport.<JsonNode, JsonNode>execute(desc, (d, i) -> "cmd-ord-1", req)
            .await().atMost(TIMEOUT);
        assertEquals("ldg-ord-1", first.path("entryId").asText());
        assertEquals(1, dispatchCalls.get());

        JsonNode replay = commandStepSupport.<JsonNode, JsonNode>execute(desc, (d, i) -> "cmd-ord-1", req)
            .await().atMost(TIMEOUT);
        assertEquals(first, replay);
        assertEquals(1, dispatchCalls.get(), "Duplicate replay must return recorded result without re-dispatching");

        // 2. Mid-dispatch socket destruction -> AMBIGUOUS -> re-invocation triggers automatic /reconcile -> SUCCEEDED!
        commandMode.set("DROP_AFTER_APPLY");
        ObjectNode recReq = JSON.createObjectNode().put("orderId", "ord-rec").put("amountCents", 4200);
        PipelineExecutionContextHolder.set(execCtx("exec-c-rec", 2));
        CommandOutcomeException ambErr = assertThrows(CommandOutcomeException.class,
            () -> commandStepSupport.<JsonNode, JsonNode>execute(desc, (d, i) -> "cmd-ord-rec", recReq)
                .await().atMost(TIMEOUT));
        assertEquals(CommandEffectStatus.AMBIGUOUS, ambErr.status());
        assertEquals(2, dispatchCalls.get());
        assertEquals(0, reconcileCalls.get());

        CommandEffectRecord ambRecord = effectStore.find("tenant-1", "cmd-ord-rec").await().atMost(TIMEOUT).orElseThrow();
        assertTrue(ambRecord.currentAttempt().recoveryBinding().isPresent());

        // Re-invoke -> CommandStepSupport automatically invokes /reconcile (0 re-dispatches!)
        commandMode.set("NORMAL");
        JsonNode reconciled = commandStepSupport.<JsonNode, JsonNode>execute(desc, (d, i) -> "cmd-ord-rec", recReq)
            .await().atMost(TIMEOUT);
        assertEquals("ldg-ord-rec", reconciled.path("entryId").asText());
        assertEquals(4200, reconciled.path("amountCents").asInt());
        assertEquals(2, dispatchCalls.get(), "Reconciliation must NOT re-dispatch the ambiguous command");
        assertEquals(1, reconcileCalls.get());

        CommandEffectRecord doneRecord = effectStore.find("tenant-1", "cmd-ord-rec").await().atMost(TIMEOUT).orElseThrow();
        assertEquals(CommandEffectStatus.SUCCEEDED, doneRecord.status());
        assertTrue(doneRecord.currentAttempt().reconciliationReceipt().isPresent());

        // 3. Schema-violating Command response fails closed as AMBIGUOUS (never SUCCEEDED!)
        commandMode.set("BAD_SCHEMA");
        ObjectNode badReq = JSON.createObjectNode().put("orderId", "ord-bad").put("amountCents", 100);
        PipelineExecutionContextHolder.set(execCtx("exec-c-bad", 2));
        CommandOutcomeException badErr = assertThrows(CommandOutcomeException.class,
            () -> commandStepSupport.<JsonNode, JsonNode>execute(desc, (d, i) -> "cmd-ord-bad", badReq)
                .await().atMost(TIMEOUT));
        assertEquals(CommandEffectStatus.AMBIGUOUS, badErr.status());
    }

    private void handleQuery(HttpExchange ex) throws IOException {
        byte[] raw = ex.getRequestBody().readAllBytes();
        if (!verifyHmac(ex, "/tpf/connector/query/custom.inventory.lookup", raw)) {
            respond(ex, 401, "{\"error\":\"bad-sig\"}");
            return;
        }
        verifiedSignature.set(true);
        JsonNode body = JSON.readTree(raw);
        if (LEDGER_API_KEY.equals(body.path("secrets").path("ledger-api-key").asText())) {
            verifiedHostSecret.set(true);
        }
        queryCalls.incrementAndGet();
        if ("BAD_SCHEMA".equals(queryMode.get())) {
            respond(ex, 200, "{\"outcome\":\"FOUND\",\"payload\":{\"sku\":\"SKU-42\",\"availableUnits\":\"bad\",\"warehouse\":\"eu\"}}");
            return;
        }
        respond(ex, 200, "{\"outcome\":\"FOUND\",\"payload\":{\"sku\":\""
            + body.path("input").path("sku").asText() + "\",\"availableUnits\":42,\"warehouse\":\"eu-west-1\"}}");
    }

    private void handleDispatch(HttpExchange ex) throws IOException {
        byte[] raw = ex.getRequestBody().readAllBytes();
        if (!verifyHmac(ex, "/tpf/connector/command/custom.ledger.post/dispatch", raw)) {
            respond(ex, 401, "{\"error\":\"bad-sig\"}");
            return;
        }
        dispatchCalls.incrementAndGet();
        JsonNode body = JSON.readTree(raw);
        String key = body.path("dispatchIdentity").path("providerIdempotencyKey").asText();
        String orderId = body.path("input").path("orderId").asText();
        int amount = body.path("input").path("amountCents").asInt();

        if ("BAD_SCHEMA".equals(commandMode.get())) {
            respond(ex, 200, "{\"outcome\":\"SUCCEEDED\",\"payload\":{\"entryId\":123,\"orderId\":\"bad\",\"amountCents\":\"no\",\"posted\":\"nope\"},\"receiptReference\":\"bad\"}");
            return;
        }
        ObjectNode entry = JSON.createObjectNode()
            .put("entryId", "ldg-" + orderId)
            .put("orderId", orderId)
            .put("amountCents", amount)
            .put("posted", true);
        ledgerDb.put(key, entry);

        if ("DROP_AFTER_APPLY".equals(commandMode.get())) {
            // Abruptly drop response after applying side effect!
            ex.close();
            return;
        }
        ObjectNode resp = JSON.createObjectNode();
        resp.put("outcome", "SUCCEEDED");
        resp.set("payload", entry);
        resp.put("receiptReference", "ldg-" + orderId);
        respond(ex, 200, JSON.writeValueAsString(resp));
    }

    private void handleReconcile(HttpExchange ex) throws IOException {
        byte[] raw = ex.getRequestBody().readAllBytes();
        if (!verifyHmac(ex, "/tpf/connector/command/custom.ledger.post/reconcile", raw)) {
            respond(ex, 401, "{\"error\":\"bad-sig\"}");
            return;
        }
        reconcileCalls.incrementAndGet();
        JsonNode body = JSON.readTree(raw);
        String key = body.path("dispatchIdentity").path("providerIdempotencyKey").asText();
        ObjectNode found = ledgerDb.get(key);
        if (found == null) {
            respond(ex, 200, "{\"outcome\":\"UNRESOLVED\",\"code\":\"not-found\"}");
            return;
        }
        ObjectNode resp = JSON.createObjectNode();
        resp.put("outcome", "CONFIRMED_SUCCEEDED");
        resp.set("payload", found);
        resp.put("receiptReference", found.path("entryId").asText());
        respond(ex, 200, JSON.writeValueAsString(resp));
    }

    private boolean verifyHmac(HttpExchange ex, String path, byte[] body) {
        String ts = ex.getRequestHeaders().getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER);
        String nonce = ex.getRequestHeaders().getFirst(TransitionWorkerSignature.NONCE_HEADER);
        String sig = ex.getRequestHeaders().getFirst(TransitionWorkerSignature.SIGNATURE_HEADER);
        if (ts == null || nonce == null || sig == null) {
            return false;
        }
        String expected = TransitionWorkerSignature.sign(SHARED_SECRET, "POST", path, ts, nonce, body);
        return expected.equals(sig);
    }

    private static void respond(HttpExchange ex, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static PipelineExecutionContext execCtx(String execId, int stepIndex) {
        return new PipelineExecutionContext(
            "tenant-1", execId, "polyglot-connector-test", "1.0.0", "26.11.3",
            stepIndex, Optional.of(execId), Optional.of(execId));
    }

    private static QueryStepDescriptor queryDescriptor() {
        return QueryStepDescriptor.nativeQuery(
            "step-1-inv",
            JsonNode.class.getName(),
            JsonNode.class.getName(),
            "ONE_TO_ONE",
            new NativeQuerySelector(
                BINDING,
                new ConnectorOperationIdentity(
                    PolyglotConnectorProvider.PROVIDER_ID, "custom.inventory.lookup", ConnectorOperationKind.QUERY, 1),
                1),
            Map.of("scope", "warehouse"));
    }

    private static CommandDescriptor commandDescriptor() {
        return CommandDescriptor.nativeCommand(
            "step-2-ledger",
            new NativeCommandSelector(
                Optional.of(BINDING),
                new ConnectorOperationIdentity(
                    PolyglotConnectorProvider.PROVIDER_ID, "custom.ledger.post", ConnectorOperationKind.COMMAND, 1),
                1,
                new CommandPolicy(true, true, true, Optional.empty(),
                    Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED), false)),
            JsonNode.class.getName(),
            JsonNode.class.getName(),
            "ledger-id-gen",
            CommandDuplicatePolicy.RETURN_RECORDED,
            Map.of("scope", "general-ledger"));
    }

    private static CanonicalTypeCatalogue buildCatalogue() {
        Map<String, Map<String, Object>> defs = new LinkedHashMap<>();
        defs.put("InventoryQuery", recordDef("InventoryQuery", List.of(field("sku", "string"))));
        defs.put("InventoryStock", recordDef("InventoryStock", List.of(
            field("sku", "string"), field("availableUnits", "int32"), field("warehouse", "string"))));
        defs.put("LedgerPostRequest", recordDef("LedgerPostRequest", List.of(
            field("orderId", "string"), field("amountCents", "int32"))));
        defs.put("LedgerReceipt", recordDef("LedgerReceipt", List.of(
            field("entryId", "string"), field("orderId", "string"),
            field("amountCents", "int32"), field("posted", "bool"))));
        return CanonicalTypeCatalogue.fromCanonicalTypes(defs);
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

    private static PipelineOrchestratorConfig minimalConfig() {
        return new PipelineOrchestratorConfig() {
            @Override public OrchestratorMode mode() { return OrchestratorMode.QUEUE_ASYNC; }
            @Override public CoordinationHost coordinationHost() { return CoordinationHost.NATIVE; }
            @Override public boolean processLoopsDisabled() { return false; }
            @Override public String defaultTenant() { return "tenant-1"; }
            @Override public String pipelineId() { return "polyglot-connector-test"; }
            @Override public String releaseVersion() { return "26.11.3"; }
            @Override public int executionTtlDays() { return 1; }
            @Override public long leaseMs() { return 30_000L; }
            @Override public int maxRetries() { return 1; }
            @Override public Optional<Duration> maxCircuitDeferral() { return Optional.empty(); }
            @Override public Duration retryDelay() { return Duration.ofMillis(10); }
            @Override public double retryMultiplier() { return 1.0d; }
            @Override public Duration sweepInterval() { return Duration.ofSeconds(30); }
            @Override public int sweepLimit() { return 10; }
            @Override public OrchestratorIdempotencyPolicy idempotencyPolicy() {
                return OrchestratorIdempotencyPolicy.CLIENT_KEY_REQUIRED;
            }
            @Override public String stateProvider() { return ""; }
            @Override public String dispatcherProvider() { return ""; }
            @Override public String dlqProvider() { return ""; }
            @Override public Optional<String> queueUrl() { return Optional.empty(); }
            @Override public Optional<String> dlqUrl() { return Optional.empty(); }
            @Override public Optional<String> resumeTokenSecret() { return Optional.empty(); }
            @Override public DynamoConfig dynamo() { return null; }
            @Override public SqsConfig sqs() { return null; }
            @Override public TenancyConfig tenancy() { return null; }
            @Override public QuotaConfig quotas() { return null; }
            @Override public ControlPlaneConfig controlPlane() { return null; }
            @Override public AdminConfig admin() { return null; }
            @Override public ReleasesConfig releases() { return null; }
            @Override public WorkerConfig worker() { return null; }
            @Override public boolean strictStartup() { return false; }
            @Override public ExperimentalConfig experimental() { return () -> () -> false; }
            @Override public boolean experimentalNativeWorkerEnabled() { return false; }
        };
    }
}

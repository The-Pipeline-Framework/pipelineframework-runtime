package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorRegistry;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.orchestrator.CanonicalFunctionHostStep;
import org.pipelineframework.orchestrator.CoordinationHost;
import org.pipelineframework.orchestrator.DeadLetterEnvelope;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionResultShapeResolver;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.ForeignDispatchIntent;
import org.pipelineframework.orchestrator.ForeignDispatchIntentStore;
import org.pipelineframework.orchestrator.InMemoryExecutionStateStore;
import org.pipelineframework.orchestrator.InMemoryForeignDispatchIntentStore;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.LocalControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutionMode;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.step.ConfigFactory;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * End-to-end standard-path coordinator test verifying {@link CanonicalFunctionHostStep}
 * with {@link QueueAsyncCoordinator}, {@link PipelineExecutionService},
 * {@link InMemoryExecutionStateStore}, and {@link InMemoryForeignDispatchIntentStore}:
 *
 * <ul>
 *   <li>Class-free canonical execution ({@code runtimeClass} absent) with {@code experimentalNativeWorkerEnabled=false}</li>
 *   <li>Coordinator-owned retry on {@code TRANSIENT} incrementing attempt &amp; transitionKey</li>
 *   <li>Immediate terminal {@code FAILED} on {@code BUSINESS_REJECTION} preserving {@code failedStepIndex}/{@code failedCommandId}</li>
 *   <li>Pre-send dispatch-intent suppression on crashed/redelivered attempt -> {@code REMOTE_OUTCOME_UNKNOWN} with 0 duplicate HTTP sends</li>
 * </ul>
 */
class CanonicalFunctionHostCoordinatorTest {

    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final String SECRET = "coordinator-test-secret";
    private static final String CATALOG_FP = "fp-coord-catalog";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @TempDir
    Path tempDir;

    private HttpServer server;
    private URI baseUri;
    private CanonicalTypeCatalogue catalogue;
    private InMemoryExecutionStateStore executionStore;
    private InMemoryForeignDispatchIntentStore intentStore;
    private Queue<ExecutionWorkItem> workQueue;
    private Queue<DeadLetterEnvelope> deadLetters;
    private QueueAsyncCoordinator coordinator;
    private PipelineExecutionService executionService;
    private ClassLoader orderClassLoader;
    private final AtomicInteger stepACalls = new AtomicInteger();

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/tpf/functions/approveInvoice", this::handleApprove);
        server.start();
        baseUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        catalogue = buildCatalogue();
        executionStore = new InMemoryExecutionStateStore();
        intentStore = new InMemoryForeignDispatchIntentStore();
        workQueue = new ConcurrentLinkedQueue<>();
        deadLetters = new ConcurrentLinkedQueue<>();
        stepACalls.set(0);

        PipelineOrchestratorConfig config = orchestratorConfig();
        CanonicalFunctionHostStep stepA = new CanonicalFunctionHostStep(
            new CanonicalFunctionHostStep.Spec(
                "approveInvoice", 0, "Invoice", "fp-Invoice", "ApprovedInvoice", "fp-ApprovedInvoice",
                CATALOG_FP, catalogue, baseUri, SECRET, Duration.ofSeconds(3), Optional.of("cmd-approve"),
                intentStore, 4 * 1024 * 1024),
            true);
        stepA.initialiseWithConfig(new StepConfig().retryLimit(0));
        List<Object> steps = List.of(stepA);

        Path orderFile = tempDir.resolve("order.json");
        Files.write(orderFile, JSON.writeValueAsBytes(Map.of(
            "order", List.of(CanonicalFunctionHostStep.class.getName()),
            "boundarySchemaVersion", 1,
            "stepBoundaries", List.of(Map.of(
                "stepClass", CanonicalFunctionHostStep.class.getName(),
                "inputTypeId", "Invoice",
                "outputTypeId", "ApprovedInvoice")),
            "definitionBoundaries", List.of())));
        var orderUrl = orderFile.toUri().toURL();
        orderClassLoader = new ClassLoader(getClass().getClassLoader()) {
            @Override
            public Enumeration<java.net.URL> getResources(String name) throws IOException {
                return "META-INF/pipeline/order.json".equals(name)
                    ? Collections.enumeration(List.of(orderUrl))
                    : super.getResources(name);
            }
        };

        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            "invoice-polyglot", CATALOG_FP, CATALOG_FP,
            "LOCAL", "COMPUTE", "SINGLE", false, "",
            List.of(), PipelineBundleCapabilities.defaults(),
            canonicalDefs(), CATALOG_FP, null, List.of(), List.of(), List.of(),
            Map.of("schemaVersion", 1, "inputCanonicalTypeId", "Invoice", "outputCanonicalTypeId", "ApprovedInvoice"),
            null);

        JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
        WorkDispatcher dispatcher = new WorkDispatcher() {
            @Override public Uni<Void> enqueueNow(ExecutionWorkItem item) {
                workQueue.add(item);
                return Uni.createFrom().voidItem();
            }
            @Override public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
                workQueue.add(item);
                return Uni.createFrom().voidItem();
            }
            @Override public String providerName() { return "test-queue"; }
            @Override public Optional<String> startupValidationError() { return Optional.empty(); }
        };
        DeadLetterPublisher dlq = new DeadLetterPublisher() {
            @Override public Uni<Void> publish(DeadLetterEnvelope envelope) {
                deadLetters.add(envelope);
                return Uni.createFrom().voidItem();
            }
            @Override public Optional<String> startupValidationError() { return Optional.empty(); }
        };

        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(new InMemoryControlPlaneJournal());
        ExecutionFailureHandler failureHandler = new ExecutionFailureHandler();
        failureHandler.orchestratorConfig = config;
        failureHandler.segmentBoundaryLedger = ledger;

        PipelineReleaseIdentityResolver identityResolver = new PipelineReleaseIdentityResolver() {
            @Override public Optional<PipelineContractDescriptor> compiledContract() {
                return Optional.of(contract);
            }
        };

        AwaitCoordinator awaitCoordinator = new AwaitCoordinator();

        coordinator = new QueueAsyncCoordinator();
        coordinator.orchestratorConfig = config;
        coordinator.executionStateStore = executionStore;
        coordinator.workDispatcher = dispatcher;
        coordinator.deadLetterPublisher = dlq;
        coordinator.awaitCoordinator = awaitCoordinator;
        coordinator.checkpointPublicationService = new CheckpointPublicationService() {
            @Override public boolean enabled() { return false; }
            @Override public Uni<Void> publishIfConfigured(ExecutionRecord<Object, Object> r, Object payload) {
                return Uni.createFrom().voidItem();
            }
        };
        coordinator.objectPublishCompletionService = new ObjectPublishCompletionService() {
            @Override public boolean enabled() { return false; }
        };
        coordinator.executionInputPolicy = new ExecutionInputPolicy();
        coordinator.executionInputPolicy.orchestratorConfig = config;
        coordinator.executionResultShapeResolver = new ExecutionResultShapeResolver() {
            @Override public ExecutionResultShape resolve() { return ExecutionResultShape.SINGLE; }
        };
        coordinator.transitionPayloadCodec = payloadCodec;
        coordinator.releaseIdentityResolver = identityResolver;
        coordinator.controlPlaneAdmissionPolicy = new LocalControlPlaneAdmissionPolicy();
        coordinator.executionFailureHandler = failureHandler;
        coordinator.segmentBoundaryLedger = ledger;
        coordinator.transitionWorkerExecutor = new TransitionWorkerExecutor(config, new PipelineInvocationRuntime());

        PipelineStepConfig stepConfig = stepConfig();
        PipelineConfig pipelineConfig = new PipelineConfig();
        pipelineConfig.defaults().retryLimit(0);
        pipelineConfig.parallelism(ParallelismPolicy.SEQUENTIAL);

        org.pipelineframework.telemetry.PipelineTelemetry telemetry =
            new org.pipelineframework.telemetry.PipelineTelemetry(stepConfig);
        PipelineRunner runner = new PipelineRunner();
        runner.configFactory = new ConfigFactory();
        runner.pipelineConfig = pipelineConfig;
        runner.runTelemetry = telemetry;
        runner.stepTelemetry = telemetry;
        runner.stepOrderer = new PipelineStepOrderer() {
            @Override List<Object> orderSteps(List<Object> inputSteps) { return inputSteps; }
        };
        runner.parallelismPolicyResolver = new PipelineParallelismPolicyResolver() {
            @Override ParallelismPolicy resolveParallelismPolicy(PipelineConfig c) { return ParallelismPolicy.SEQUENTIAL; }
            @Override int resolveMaxConcurrency(PipelineConfig c) { return 1; }
        };
        runner.cacheSupportFactory = new PipelineCacheSupportFactory(null, null, "none") {
            @Override PipelineRunner.CacheReadSupport buildCacheReadSupport() { return null; }
        };
        runner.stepExecutor = new PipelineStepExecutor();

        LocalPipelineControlPlane localControlPlane = new LocalPipelineControlPlane() {
            @Override public void initializeQueueMode() { }
        };
        localControlPlane.queueAsyncCoordinator = coordinator;

        executionService = new PipelineExecutionService();
        executionService.orchestratorConfig = config;
        executionService.pipelineConfig = pipelineConfig;
        executionService.pipelineRunner = runner;
        executionService.healthCheckService = new HealthCheckService() {
            @Override public boolean checkHealthOfDependentServices(List<Object> s) { return true; }
        };
        executionService.controlPlane = localControlPlane;
        executionService.configFactory = new ConfigFactory();
        executionService.pipelineStepResolver = new PipelineStepResolver() {
            @Override List<Object> loadPipelineSteps() { return steps; }
        };
        executionService.pipelineStepConfig = stepConfig;
        executionService.stepOrderer = runner.stepOrderer;
        executionService.awaitCoordinator = awaitCoordinator;
        executionService.executionHooks = new ExecutionHooks();
        executionService.executionInputPolicy = coordinator.executionInputPolicy;
        executionService.transitionPayloadCodec = payloadCodec;
        executionService.releaseIdentityResolver = identityResolver;
        localControlPlane.pipelineExecutionService = executionService;
        executionService.runStartupHealthChecks();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void coordinatorRetriesTransientAndRejectsBusinessRejectionWithFailureIdentity() {
        // 1. TRANSIENT on attempt 0 -> coordinator schedules retry (attempt 1) -> SUCCEEDED!
        ExecutionRecord<Object, Object> transientRecord = runWithOrderClassLoader(() -> {
            var accepted = coordinator.executePipelineAsync(
                    invoice("transient-once", 500), "tenant-1", "key-transient", false)
                .await().atMost(TIMEOUT);
            drainQueue();
            // Wait past the 10ms retryDelay so claimLease sees the due retry
            try {
                Thread.sleep(30L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            for (var due : executionStore.findDueExecutions(System.currentTimeMillis() + 1_000L, 10)
                    .await().atMost(TIMEOUT)) {
                workQueue.add(new ExecutionWorkItem(due.tenantId(), due.executionId()));
            }
            drainQueue();
            return executionStore.getExecution("tenant-1", accepted.executionId())
                .await().atMost(TIMEOUT).orElseThrow();
        });
        assertEquals(ExecutionStatus.SUCCEEDED, transientRecord.status());
        assertEquals(2, stepACalls.get());

        // 2. BUSINESS_REJECTION -> terminal FAILED with failedStepIndex=0 and failedCommandId="cmd-approve"
        stepACalls.set(0);
        ExecutionRecord<Object, Object> rejectedRecord = runWithOrderClassLoader(() -> {
            var accepted = coordinator.executePipelineAsync(
                    invoice("reject-me", 500), "tenant-1", "key-reject", false)
                .await().atMost(TIMEOUT);
            drainQueue();
            return executionStore.getExecution("tenant-1", accepted.executionId())
                .await().atMost(TIMEOUT).orElseThrow();
        });
        assertEquals(ExecutionStatus.FAILED, rejectedRecord.status());
        assertEquals(0, rejectedRecord.failedStepIndex());
        assertEquals(Optional.of("cmd-approve"), rejectedRecord.failedCommandId());
        assertEquals(1, stepACalls.get());
        assertEquals(1, deadLetters.size());
    }

    @Test
    void unresolvedPreSendIntentBlocksDuplicateDispatchAndMarksRemoteOutcomeUnknown() {
        ExecutionRecord<Object, Object> record = runWithOrderClassLoader(() -> {
            var accepted = coordinator.executePipelineAsync(
                    invoice("normal-1", 700), "tenant-1", "key-crashed", false)
                .await().atMost(TIMEOUT);
            String execId = accepted.executionId();

            // Simulate a prior crashed coordinator that persisted pre-send intent for attempt 0 (execId:0:0)
            ForeignDispatchIntent priorCrash = new ForeignDispatchIntent(
                "tenant-1", execId, 1L, 0, execId + ":0:0", 0, "approveInvoice",
                baseUri + "/tpf/functions/approveInvoice", "prior-digest", System.currentTimeMillis());
            assertEquals(ForeignDispatchIntentStore.ReservationOutcome.RESERVED, intentStore.reserve(priorCrash));

            drainQueue();
            return executionStore.getExecution("tenant-1", execId).await().atMost(TIMEOUT).orElseThrow();
        });

        assertEquals(ExecutionStatus.REMOTE_OUTCOME_UNKNOWN, record.status());
        assertEquals(0, stepACalls.get(), "Reclaimed attempt with unresolved pre-send intent must NEVER send HTTP");
    }

    @Test
    void commandStepSupportAcceptsExplicitNonCdiConnectorRuntimeContext() {
        ConnectorRuntimeContext customCtx = ConnectorRuntimeContext.of(
            "non-cdi-test", Executors.newSingleThreadExecutor(), Clock.systemUTC(), Optional.empty());
        CommandStepSupport support = new CommandStepSupport(
            new ConnectorRegistry(List.of()),
            ConnectorBindingRegistry.empty(),
            customCtx,
            List.of(new InMemoryCommandEffectStore()),
            orchestratorConfig());
        assertTrue(support != null);
    }

    private void drainQueue() {
        PipelineTransitionWorker tcclWorker = new PipelineTransitionWorker() {
            @Override
            public Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command) {
                return runWithOrderClassLoader(() -> executionService.executeTransition(command));
            }
            @Override
            public String providerName() {
                return executionService.providerName();
            }
        };
        ExecutionWorkItem item;
        int guard = 0;
        while ((item = workQueue.poll()) != null && guard++ < 20) {
            ExecutionWorkItem current = item;
            runWithOrderClassLoader(() -> {
                coordinator.processExecutionWorkItem(current, tcclWorker).await().atMost(TIMEOUT);
                return null;
            });
        }
    }

    private <T> T runWithOrderClassLoader(java.util.function.Supplier<T> action) {
        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(orderClassLoader);
        try {
            return action.get();
        } finally {
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    private void handleApprove(HttpExchange exchange) throws IOException {
        int call = stepACalls.incrementAndGet();
        JsonNode req = JSON.readTree(exchange.getRequestBody().readAllBytes());
        String id = req.path("payload").path("id").asText();
        int amount = req.path("payload").path("amountCents").asInt();
        String response;
        if ("transient-once".equals(id) && call == 1) {
            response = "{\"status\":\"ERROR\",\"disposition\":\"TRANSIENT\",\"code\":\"BUSY\",\"message\":\"try again\"}";
        } else if ("reject-me".equals(id)) {
            response = "{\"status\":\"ERROR\",\"disposition\":\"BUSINESS_REJECTION\",\"code\":\"DECLINED\",\"message\":\"denied\"}";
        } else {
            response = "{\"status\":\"OK\",\"outputTypeId\":\"ApprovedInvoice\","
                + "\"payload\":{\"invoiceId\":\"" + id + "\",\"approvedAmountCents\":" + amount + "}}";
        }
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private TypedDurablePayload invoice(String id, int amountCents) {
        String json = "{\"id\":\"" + id + "\",\"amountCents\":" + amountCents + "}";
        return new TypedDurablePayload(
            "Invoice", "fp-Invoice", CATALOG_FP,
            JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
            json.getBytes(StandardCharsets.UTF_8));
    }

    private static CanonicalTypeCatalogue buildCatalogue() {
        return CanonicalTypeCatalogue.fromCanonicalTypes(canonicalDefs());
    }

    private static Map<String, Map<String, Object>> canonicalDefs() {
        Map<String, Map<String, Object>> defs = new LinkedHashMap<>();
        defs.put("Invoice", recordDef("Invoice", List.of(field("id", "string"), field("amountCents", "int32"))));
        defs.put("ApprovedInvoice", recordDef("ApprovedInvoice", List.of(
            field("invoiceId", "string"), field("approvedAmountCents", "int32"))));
        return defs;
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

    private static PipelineOrchestratorConfig orchestratorConfig() {
        return new PipelineOrchestratorConfig() {
            @Override public OrchestratorMode mode() { return OrchestratorMode.QUEUE_ASYNC; }
            @Override public CoordinationHost coordinationHost() { return CoordinationHost.NATIVE; }
            @Override public boolean processLoopsDisabled() { return false; }
            @Override public String defaultTenant() { return "tenant-1"; }
            @Override public String pipelineId() { return "invoice-polyglot"; }
            @Override public String releaseVersion() { return "26.11.3"; }
            @Override public int executionTtlDays() { return 1; }
            @Override public long leaseMs() { return Duration.ofSeconds(30).toMillis(); }
            @Override public int maxRetries() { return 2; }
            @Override public Optional<Duration> maxCircuitDeferral() { return Optional.empty(); }
            @Override public Duration retryDelay() { return Duration.ofMillis(10); }
            @Override public double retryMultiplier() { return 1.0d; }
            @Override public Duration sweepInterval() { return Duration.ofSeconds(30); }
            @Override public int sweepLimit() { return 50; }
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
            @Override public WorkerConfig worker() { return WORKER_CONFIG; }
            @Override public boolean strictStartup() { return false; }
            @Override public ExperimentalConfig experimental() { return () -> () -> false; }
            @Override public boolean experimentalNativeWorkerEnabled() { return false; }
        };
    }

    private static final PipelineOrchestratorConfig.WorkerConfig WORKER_CONFIG =
        new PipelineOrchestratorConfig.WorkerConfig() {
            @Override public PipelineOrchestratorConfig.WorkerAwaitAuthorityConfig awaitAuthority() { return null; }
            @Override public String targetingMode() { return "local"; }
            @Override public Map<String, PipelineOrchestratorConfig.RegisteredRestTargetConfig> targets() { return Map.of(); }
            @Override public boolean allowLoopbackHttp() { return true; }
            @Override public TransitionWorkerExecutionMode executionMode() { return TransitionWorkerExecutionMode.SAME_THREAD; }
            @Override public int maxInFlight() { return 16; }
            @Override public Duration saturatedDelay() { return Duration.ofMillis(50); }
            @Override public List<String> allowedPayloadPrefixes() { return List.of(); }
            @Override public Optional<String> artifactId() { return Optional.empty(); }
            @Override public Optional<String> artifactDigest() { return Optional.empty(); }
            @Override public PipelineOrchestratorConfig.RestWorkerConfig rest() { return null; }
            @Override public PipelineOrchestratorConfig.GrpcWorkerConfig grpc() { return null; }
            @Override public PipelineOrchestratorConfig.SqsWorkerConfig sqs() { return null; }
            @Override public PipelineOrchestratorConfig.WorkerLifecycleConfig lifecycle() { return null; }
        };

    private static PipelineStepConfig stepConfig() {
        return new PipelineStepConfig() {
            @Override public StepConfig defaults() { return null; }
            @Override public ParallelismPolicy parallelism() { return ParallelismPolicy.SEQUENTIAL; }
            @Override public Integer maxConcurrency() { return 1; }
            @Override public Integer maxRecursiveDepth() { return 64; }
            @Override public AwaitAdmissionConfig awaitAdmission() { return null; }
            @Override public CallbackConfig callback() { return () -> true; }
            @Override public HealthConfig health() {
                return new HealthConfig() {
                    @Override public Boolean enabled() { return false; }
                    @Override public Duration startupTimeout() { return Duration.ofSeconds(5); }
                    @Override public String restPath() { return "/q/health"; }
                };
            }
            @Override public CacheConfig cache() { return null; }
            @Override public TelemetryConfig telemetry() { return null; }
            @Override public KillSwitchConfig killSwitch() { return null; }
            @Override public Map<String, StepConfig> step() { return Map.of(); }
            @Override public Map<String, ModuleConfig> module() { return Map.of(); }
            @Override public ClientConfig client() { return null; }
        };
    }
}

package org.pipelineframework;

import java.lang.reflect.*;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;
import com.example.echo.*;
import com.sun.net.httpserver.*;
import io.smallrye.config.*;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.context.ThreadContext;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.command.*;
import org.pipelineframework.config.*;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.*;
import org.pipelineframework.execution.*;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.controlplane.*;
import org.pipelineframework.orchestrator.release.*;
import org.pipelineframework.orchestrator.worker.*;
import org.pipelineframework.pipeline.service.pipeline.ProcessEchoCommandClientStep;
import org.pipelineframework.step.*;
import org.pipelineframework.telemetry.PipelineTelemetry;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Executed packaged proof loaded directly from the built function.zip archive.
 * Exercises the compiler-generated Command step and deferred Command/Await settlement
 * across an app-free shared Coordinator and a standalone registered SYNC REST worker.
 */
public final class PackagedRestWorkerCommandProof {
    private static final String TENANT = "tenant-packaged";
    private static final String RELEASE_VERSION = "rest-worker-command-release-1";
    private static final String ENTRY_ID = "worker";
    private static final String ARTIFACT_ID = "worker";
    private static final String WORKER_ID = "packaged-sync-worker-1";
    private static final String DEPLOYMENT_OP_ID = "dep-op-packaged-1";
    private static final String SECRET_REF = "secret://packaged-worker-key";
    private static final String SECRET = "packaged-worker-shared-secret-32-chars";
    private static final String TABLE = "tpf-packaged-command-effects";
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    public static void main(String[] args) {
        try {
            run(Path.of(args[0]));
            System.out.println("PASS packaged REST/FUNCTION SYNC worker Command + Command/Await proof");
            System.exit(0);
        } catch (Throwable failure) {
            failure.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void run(Path releaseFile) throws Exception {
        PipelineReleaseDescriptor release = PipelineJson.mapper().readValue(
            Files.readAllBytes(releaseFile), PipelineReleaseDescriptor.class);
        PipelineContractDescriptor contract = new PipelineContractDescriptorLoader().load()
            .orElseThrow(() -> new AssertionError("Missing META-INF/pipeline/pipeline-contract.json in function.zip"));
        String artifactDigest = release.artifacts().getFirst().digest();

        verifyMemoryStoreRejectedBeforeAdmission(contract, release, artifactDigest);
        verifyOrdinaryPackagedCommandExecutionAndReplay(contract, release, artifactDigest);
        verifyDeferredCommandAwaitSettlementAcrossPackagedWorker(artifactDigest);
    }

    private static void verifyMemoryStoreRejectedBeforeAdmission(
        PipelineContractDescriptor contract,
        PipelineReleaseDescriptor release,
        String artifactDigest
    ) throws Exception {
        EchoProvider.CALLS.set(0);
        try (var gateway = new WorkerGateway();
             var worker = new SyncWorker(contract, artifactDigest, new InMemoryCommandEffectStore(), Optional.empty(), null, false);
             var coordinator = new AppFreeCoordinator(contract, release, artifactDigest, gateway.origin())) {
            gateway.attach(worker);
            var availability = coordinator.targets.check(new PipelineWorkerAvailabilityRequest(
                TENANT, contract.pipelineId(), contract.contractVersion(), RELEASE_VERSION, ARTIFACT_ID, artifactDigest))
                .await().atMost(TIMEOUT);
            if (availability.available()) {
                throw new AssertionError("Default InMemoryCommandEffectStore must be rejected before readiness");
            }
            boolean rejected = false;
            try {
                coordinator.admit(TENANT, "exec-mem-1", canonicalPayload(contract, "EchoInput", new EchoInput("blocked")));
            } catch (RuntimeException expected) {
                rejected = true;
            }
            if (!rejected || worker.executeCalls.get() != 0 || EchoProvider.CALLS.get() != 0) {
                throw new AssertionError("Coordinator must reject unverified Command worker before dispatch");
            }
        }
    }

    private static void verifyOrdinaryPackagedCommandExecutionAndReplay(
        PipelineContractDescriptor contract,
        PipelineReleaseDescriptor release,
        String artifactDigest
    ) throws Exception {
        EchoProvider.CALLS.set(0);
        var dynamoTable = new ConditionalRevisionDynamoTable();
        try (var gateway = new WorkerGateway();
             var worker1 = new SyncWorker(contract, artifactDigest, new DynamoCommandEffectStore(dynamoTable.client(), TABLE), Optional.empty(), null, false);
             var coordinator = new AppFreeCoordinator(contract, release, artifactDigest, gateway.origin())) {
            gateway.attach(worker1);

            // Unscoped SYNC execution on the packaged worker stays rejected.
            boolean unscopedRejected = false;
            try {
                worker1.service.executePipelineUnary(Uni.createFrom().item(new EchoInput("unscoped")))
                    .await().atMost(TIMEOUT);
            } catch (IllegalStateException expected) {
                unscopedRejected = true;
            }
            if (!unscopedRejected || EchoProvider.CALLS.get() != 0) {
                throw new AssertionError("Unscoped SYNC Command execution must be rejected");
            }

            // Production Coordinator submission -> leased REST dispatch -> generated ProcessEchoCommandClientStep -> Dynamo store.
            ExecutionRecord<Object, Object> first = coordinator.admitAndProcess(
                TENANT, "exec-packaged-1", canonicalPayload(contract, "EchoInput", new EchoInput("hello")));
            if (first.status() != ExecutionStatus.SUCCEEDED) {
                throw new AssertionError("Expected SUCCEEDED, got " + first.status());
            }
            if (EchoProvider.CALLS.get() != 1) {
                throw new AssertionError("Expected 1 provider dispatch, got " + EchoProvider.CALLS.get());
            }
            if (!"hello".equals(canonicalField(first.resultPayload(), "value"))) {
                throw new AssertionError("Unexpected ordinary Command output: " + first.resultPayload());
            }
            if (dynamoTable.conditionalPutCalls.get() == 0) {
                throw new AssertionError("DynamoCommandEffectStore must issue conditional writes");
            }

            // Swap to a fresh packaged worker on the same stable gateway (retaining the original dispatch binding & pin).
            try (var worker2 = new SyncWorker(contract, artifactDigest, new DynamoCommandEffectStore(dynamoTable.client(), TABLE), Optional.empty(), null, false)) {
                gateway.attach(worker2);
                ExecutionRecord<Object, Object> duplicate = coordinator.admitAndProcess(
                    TENANT, "exec-packaged-replay", canonicalPayload(contract, "EchoInput", new EchoInput("hello")));
                if (duplicate.status() != ExecutionStatus.SUCCEEDED) {
                    throw new AssertionError("Expected replay SUCCEEDED, got " + duplicate.status());
                }
                if (EchoProvider.CALLS.get() != 1) {
                    throw new AssertionError("Replay across fresh worker re-dispatched Command: " + EchoProvider.CALLS.get());
                }
                if (!"hello".equals(canonicalField(duplicate.resultPayload(), "value"))) {
                    throw new AssertionError("Unexpected replay output: " + duplicate.resultPayload());
                }
            }
        }
    }

    private static void verifyDeferredCommandAwaitSettlementAcrossPackagedWorker(String artifactDigest) throws Exception {
        var dynamoTable = new ConditionalRevisionDynamoTable();
        var deferredProvider = new DeferredEchoProvider();
        PipelineContractDescriptor deferredContract = deferredContract(deferredProvider);
        var entry = new PipelineReleaseDeployment.Entry(ENTRY_ID, ARTIFACT_ID, "hosted-rest-worker-command-app", ".",
            "io.quarkus.amazon.lambda.http.LambdaHttpHandler", "AWS_DURABLE_REST_WORKER", List.of("command-effects"));
        var artifact = new PipelineReleaseArtifactDescriptor(ARTIFACT_ID, "lambda-zip",
            "maven:org.pipelineframework.it:hosted-rest-worker-command-app:zip:1.0.0", artifactDigest,
            List.of("EchoDeferred"), List.of("local", "rest", "grpc", "sqs"));
        var deployment = new PipelineReleaseDeployment("aws-durable-java21-rest-worker-v1", List.of(entry),
            List.of(new PipelineReleaseDeployment.Binding("command-effects", "aws:dynamodb:command-effects")),
            new PipelineReleaseDeployment.Check(ENTRY_ID, Map.of("payload", "{}"), Map.of("payload", "{}"), 5000));
        var deferredRelease = new PipelineReleaseDescriptor(2, deferredContract.pipelineId(),
            deferredContract.contractVersion(), RELEASE_VERSION, ARTIFACT_ID, List.of(artifact), deployment);

        try (var gateway = new WorkerGateway();
             var coordinator = new AppFreeCoordinator(deferredContract, deferredRelease, artifactDigest, gateway.origin());
             var worker = new SyncWorker(deferredContract, artifactDigest,
                 new DynamoCommandEffectStore(dynamoTable.client(), TABLE),
                 Optional.of(coordinator.lifecycleOrigin()), deferredProvider, true)) {
            gateway.attach(worker);

            ExecutionRecord<Object, Object> waiting = coordinator.admitAndProcess(
                TENANT, "exec-packaged-await-1", canonicalPayload(deferredContract, "EchoInput", new EchoInput("hello-cb")));
            if (waiting.status() != ExecutionStatus.WAITING_EXTERNAL) {
                throw new AssertionError("Expected WAITING_EXTERNAL, got " + waiting.status());
            }
            if (DeferredEchoProvider.CALLS.get() != 1 || DeferredEchoProvider.LAST_CALLBACK_URI.get() == null) {
                throw new AssertionError("Expected 1 deferred Command dispatch with Coordinator callback URL");
            }
            var interactions = coordinator.interactions.findByExecution(TENANT, waiting.executionId(), 10).await().atMost(TIMEOUT);
            if (interactions.size() != 1 || interactions.getFirst().status() != AwaitInteractionStatus.DISPATCHED
                || interactions.getFirst().commandAcknowledgement().isEmpty()) {
                throw new AssertionError("Coordinator must retain settled Command acknowledgement");
            }

            String cbUri = DeferredEchoProvider.LAST_CALLBACK_URI.get().toASCIIString();
            String token = cbUri.substring(cbUri.lastIndexOf('/') + 1);
            var completion = coordinator.runAppFree(() -> coordinator.coordinator.completeAwait(new AwaitCompletionCommand(
                TENANT, interactions.getFirst().interactionId(), null, token, "cb-key-1",
                Map.of("value", "confirmed"), "external-provider", System.currentTimeMillis())).await().atMost(TIMEOUT));
            if (completion.duplicate() || completion.record().status() != AwaitInteractionStatus.COMPLETED) {
                throw new AssertionError("Unexpected Await completion result: " + completion);
            }

            ExecutionRecord<Object, Object> resumed = coordinator.process(TENANT, waiting.executionId());
            if (resumed.status() != ExecutionStatus.SUCCEEDED) {
                throw new AssertionError("Expected resumed SUCCEEDED, got " + resumed.status());
            }
            if (DeferredEchoProvider.CALLS.get() != 1) {
                throw new AssertionError("Resume must not re-dispatch settled Command");
            }
            if (!"hello-cb/ack:hello-cb/confirmed".equals(canonicalField(resumed.resultPayload(), "value"))) {
                throw new AssertionError("Unexpected projected Command/Await output: " + resumed.resultPayload());
            }
        }
    }

    private static final class WorkerGateway implements AutoCloseable {
        final HttpServer server;
        final ExecutorService executor;
        final AtomicReference<SyncWorker> active = new AtomicReference<>();

        WorkerGateway() throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            executor = Executors.newFixedThreadPool(4, r -> new Thread(r, "packaged-sync-worker-gateway"));
            server.setExecutor(executor);
            server.createContext("/pipeline/worker", exchange -> {
                SyncWorker worker = active.get();
                if (worker == null) {
                    exchange.sendResponseHeaders(503, -1);
                    exchange.close();
                    return;
                }
                worker.handle(exchange);
            });
            server.start();
        }

        void attach(SyncWorker worker) { active.set(worker); }
        URI origin() { return URI.create("http://127.0.0.1:" + server.getAddress().getPort()); }
        @Override public void close() { server.stop(0); executor.close(); }
    }

    private static final class AppFreeCoordinator implements AutoCloseable {
        final InMemoryExecutionStateStore executions = new InMemoryExecutionStateStore();
        final InMemoryAwaitInteractionStore interactions = new InMemoryAwaitInteractionStore();
        final InMemoryAwaitUnitStore units = new InMemoryAwaitUnitStore();
        final InMemoryPipelineReleaseRegistry releases = new InMemoryPipelineReleaseRegistry();
        final InMemoryPipelineWorkerRegistry workerRegistry = new InMemoryPipelineWorkerRegistry();
        final JsonTransitionPayloadCodec wireCodec = new JsonTransitionPayloadCodec();
        final RegisteredRestWorkerTargets targets;
        final QueueAsyncCoordinator coordinator;
        final PipelineExecutionService coordinatorService;
        final RestPipelineTransitionWorker restClient;
        final HttpServer lifecycleServer;
        final ExecutorService lifecycleExecutor;
        final ExecutorService restHttpExecutor;
        final PipelineContractDescriptor contract;

        AppFreeCoordinator(PipelineContractDescriptor contract, PipelineReleaseDescriptor release,
            String artifactDigest, URI workerOrigin) throws Exception {
            this.contract = contract;
            long now = System.currentTimeMillis();
            var artifact = release.artifacts().getFirst();
            releases.register(new PipelineReleaseRecord(TENANT, contract.pipelineId(), contract.contractVersion(),
                RELEASE_VERSION, PipelineReleaseStatus.ACTIVE, release, ARTIFACT_ID, artifactDigest, artifact.uri(),
                0, "", contract, now, now, now)).await().atMost(TIMEOUT);
            workerRegistry.register(new PipelineWorkerRegistration(TENANT, contract.pipelineId(), contract.contractVersion(),
                RELEASE_VERSION, WORKER_ID, "rest", workerOrigin.toASCIIString(), ARTIFACT_ID, artifactDigest), now)
                .await().atMost(TIMEOUT);
            releases.registerEntryBinding(new PipelineReleaseEntryBinding(TENANT, contract.pipelineId(),
                contract.contractVersion(), RELEASE_VERSION, ENTRY_ID, ARTIFACT_ID, artifactDigest,
                "aws-durable-java21-rest-worker-v1", "arn:aws:lambda:us-east-1:123456789012:function:worker:1",
                "sha256:code", DEPLOYMENT_OP_ID,
                Optional.of(new PipelineReleaseDispatchBinding("rest", workerOrigin.toASCIIString(), SECRET_REF, WORKER_ID))))
                .await().atMost(TIMEOUT);

            var config = new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class).withValidateUnknown(false)
                .withDefaultValue("pipeline.orchestrator.mode", "QUEUE_ASYNC")
                .withDefaultValue("pipeline.orchestrator.strict-startup", "false")
                .withDefaultValue("pipeline.orchestrator.max-retries", "0")
                .withDefaultValue("pipeline.orchestrator.state-provider", "memory")
                .withDefaultValue("pipeline.orchestrator.worker.targeting-mode", "registered-rest")
                .withDefaultValue("pipeline.orchestrator.worker.allow-loopback-http", "true")
                .withDefaultValue("pipeline.orchestrator.control-plane.enabled", "true")
                .withDefaultValue("pipeline.orchestrator.control-plane.require-remote-worker", "true")
                .withDefaultValue("pipeline.orchestrator.control-plane.payload-materialization", "SERIALIZED")
                .build().getConfigMapping(PipelineOrchestratorConfig.class);

            restHttpExecutor = Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "coordinator-rest-http");
                t.setContextClassLoader(appFreeClassLoader());
                return t;
            });
            restClient = construct(RestPipelineTransitionWorker.class,
                new Class<?>[]{HttpClient.class, PipelineInvocationRuntime.class},
                HttpClient.newBuilder().executor(restHttpExecutor).build(), new PipelineInvocationRuntime());
            setField(restClient, "orchestratorConfig", config);
            setField(restClient, "secretResolver", (ControlPlaneSecretResolver) ref -> SECRET);

            targets = new RegisteredRestWorkerTargets();
            setField(targets, "config", config);
            setField(targets, "executionStores", singleInstance((ExecutionStateStore) executions));
            setField(targets, "releases", releases);
            setField(targets, "workers", workerRegistry);
            setField(targets, "rest", restClient);
            var targetConfig = proxyConfig(PipelineOrchestratorConfig.RegisteredRestTargetConfig.class, Map.ofEntries(
                Map.entry("entryId", Optional.of(ENTRY_ID)),
                Map.entry("tenantId", TENANT),
                Map.entry("pipelineId", contract.pipelineId()),
                Map.entry("contractVersion", contract.contractVersion()),
                Map.entry("releaseVersion", RELEASE_VERSION),
                Map.entry("workerId", WORKER_ID),
                Map.entry("protocol", "rest"),
                Map.entry("endpoint", workerOrigin.toASCIIString()),
                Map.entry("sharedSecretRef", SECRET_REF),
                Map.entry("artifactId", ARTIFACT_ID),
                Map.entry("artifactDigest", artifactDigest)));
            setField(targets, "targetSources", singleInstance(new RegisteredRestWorkerTargetSource() {
                @Override public boolean enabled() { return true; }
                @Override public Uni<Map<String, PipelineOrchestratorConfig.RegisteredRestTargetConfig>> snapshot() {
                    return Uni.createFrom().item(Map.of("primary", targetConfig));
                }
                @Override public Uni<Void> prepareCredential(String reference) { return Uni.createFrom().voidItem(); }
            }));

            var resumeTokens = construct(AwaitResumeTokenService.class, new Class<?>[]{String.class},
                "coordinator-only-signing-secret-32");
            var resolver = new AwaitDurablePayloadResolver();
            setField(resolver, "orchestratorConfig", config);
            setField(resolver, "executionStateStore", executions);
            setField(resolver, "releaseRegistry", releases);

            var awaitCoordinator = new AwaitCoordinator();
            setField(awaitCoordinator, "interactionStores", singleInstance((AwaitInteractionStore) interactions));
            setField(awaitCoordinator, "unitStores", singleInstance((AwaitUnitStore) units));
            setField(awaitCoordinator, "orchestratorConfig", config);
            setField(awaitCoordinator, "durablePayloadResolver", resolver);
            setField(awaitCoordinator, "registeredWorkers", targets);
            setField(awaitCoordinator, "transitionPayloadCodec", wireCodec);
            setField(awaitCoordinator, "resumeTokenService", resumeTokens);

            WorkDispatcher workDispatcher = new WorkDispatcher() {
                @Override public String providerName() { return "event"; }
                @Override public Optional<String> startupValidationError() { return Optional.empty(); }
                @Override public Uni<Void> enqueueNow(ExecutionWorkItem item) { return Uni.createFrom().voidItem(); }
                @Override public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) { return Uni.createFrom().voidItem(); }
            };
            DeadLetterPublisher dlq = new DeadLetterPublisher() {
                @Override public String providerName() { return "log"; }
                @Override public Optional<String> startupValidationError() { return Optional.empty(); }
                @Override public Uni<Void> publish(DeadLetterEnvelope envelope) { return Uni.createFrom().voidItem(); }
            };

            coordinator = new QueueAsyncCoordinator();
            coordinator.orchestratorConfig = config;
            coordinator.executionStateStore = executions;
            coordinator.executionStateStores = singleInstance((ExecutionStateStore) executions);
            coordinator.workDispatcher = workDispatcher;
            coordinator.workDispatchers = singleInstance(workDispatcher);
            coordinator.deadLetterPublisher = dlq;
            coordinator.deadLetterPublishers = singleInstance(dlq);
            coordinator.executionInputPolicy = new ExecutionInputPolicy();
            setField(coordinator.executionInputPolicy, "orchestratorConfig", config);
            coordinator.executionResultShapeResolver = new ExecutionResultShapeResolver();
            setField(coordinator.executionResultShapeResolver, "releases", releases);
            setField(coordinator.executionResultShapeResolver, "contractLoader", new PipelineContractDescriptorLoader() {
                @Override public Optional<PipelineContractDescriptor> load() { return Optional.empty(); }
            });
            coordinator.executionFailureHandler = new ExecutionFailureHandler();
            coordinator.executionFailureHandler.orchestratorConfig = config;
            coordinator.awaitCoordinator = awaitCoordinator;
            coordinator.awaitLiveCompletionRegistry = new AwaitLiveCompletionRegistry();
            coordinator.transitionWorkerExecutor = new TransitionWorkerExecutor(config, new PipelineInvocationRuntime());
            coordinator.transitionPayloadCodec = wireCodec;
            coordinator.segmentBoundaryLedger = new SegmentBoundaryLedger(new InMemoryControlPlaneJournal());
            coordinator.releaseRegistry = releases;
            coordinator.registeredTargets = targets;

            var selector = new PipelineTransitionWorkerSelector();
            setField(selector, "orchestratorConfig", config);
            setField(selector, "registeredTargets", targets);

            var controlPlane = new LocalPipelineControlPlane();
            controlPlane.queueAsyncCoordinator = coordinator;
            coordinatorService = new PipelineExecutionService();
            coordinatorService.controlPlane = controlPlane;
            coordinatorService.queueAsyncCoordinator = coordinator;
            coordinatorService.transitionWorkerSelector = selector;
            coordinatorService.transitionWorkerExecutor = coordinator.transitionWorkerExecutor;
            coordinatorService.transitionPayloadCodec = wireCodec;
            coordinatorService.awaitCoordinator = awaitCoordinator;
            coordinatorService.pipelineStepResolver = new PipelineStepResolver() {
                @Override public List<Object> loadPipelineSteps() { return List.of(); }
            };
            coordinatorService.stepOrderer = new PipelineStepOrderer() {
                @Override public List<Object> orderSteps(List<Object> steps) { return steps; }
            };
            controlPlane.pipelineExecutionService = coordinatorService;

            var authority = new AwaitTransitionAuthority();
            setField(authority, "executionStores", singleInstance((ExecutionStateStore) executions));
            setField(authority, "interactionStores", singleInstance((AwaitInteractionStore) interactions));
            setField(authority, "config", config);
            setField(authority, "releases", releases);
            setField(authority, "coordinator", awaitCoordinator);

            var lifecycleResource = new RestAwaitLifecycleResource();
            setField(lifecycleResource, "config", config);
            setField(lifecycleResource, "workers", targets);
            setField(lifecycleResource, "secrets", (ControlPlaneSecretResolver) ref -> SECRET);
            setField(lifecycleResource, "authority", authority);
            setField(lifecycleResource, "requestContext", directThreadContext(Runnable::run));

            lifecycleServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            lifecycleExecutor = Executors.newFixedThreadPool(4, r -> {
                Thread t = new Thread(r, "coordinator-lifecycle-http");
                t.setContextClassLoader(appFreeClassLoader());
                return t;
            });
            lifecycleServer.setExecutor(lifecycleExecutor);
            lifecycleServer.createContext(RestAwaitLifecycleProtocol.ROOT, exchange -> {
                try {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    var headers = exchange.getRequestHeaders();
                    String ts = headers.getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER);
                    String nonce = headers.getFirst(TransitionWorkerSignature.NONCE_HEADER);
                    String sig = headers.getFirst(TransitionWorkerSignature.SIGNATURE_HEADER);
                    Uni<Response> pending = switch (exchange.getRequestURI().getPath()) {
                        case RestAwaitLifecycleProtocol.REGISTRATION_PATH -> lifecycleResource.register(ts, nonce, sig, body);
                        case RestAwaitLifecycleProtocol.DISPATCH_CLAIM_PATH -> lifecycleResource.claim(ts, nonce, sig, body);
                        case RestAwaitLifecycleProtocol.REPORT_PATH -> lifecycleResource.report(ts, nonce, sig, body);
                        case RestAwaitLifecycleProtocol.UNIT_PREPARE_PATH -> lifecycleResource.prepareUnit(ts, nonce, sig, body);
                        case RestAwaitLifecycleProtocol.UNIT_SEAL_PATH -> lifecycleResource.sealUnit(ts, nonce, sig, body);
                        case RestAwaitLifecycleProtocol.UNIT_SUSPENSION_PATH -> lifecycleResource.suspension(ts, nonce, sig, body);
                        case RestAwaitLifecycleProtocol.UNIT_RESUME_PATH -> lifecycleResource.resume(ts, nonce, sig, body);
                        default -> Uni.createFrom().item(Response.status(404).build());
                    };
                    try (Response response = pending.await().atMost(TIMEOUT)) {
                        byte[] payload = response.getEntity() == null
                            ? new byte[0]
                            : PipelineJson.mapper().writeValueAsBytes(response.getEntity());
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.sendResponseHeaders(response.getStatus(), payload.length);
                        if (payload.length > 0) exchange.getResponseBody().write(payload);
                    }
                } catch (Throwable failure) {
                    failure.printStackTrace(System.err);
                    byte[] payload = String.valueOf(failure).getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(500, payload.length);
                    exchange.getResponseBody().write(payload);
                } finally {
                    exchange.close();
                }
            });
            lifecycleServer.start();
        }

        URI lifecycleOrigin() { return URI.create("http://127.0.0.1:" + lifecycleServer.getAddress().getPort()); }

        <T> T runAppFree(java.util.function.Supplier<T> action) {
            Thread current = Thread.currentThread();
            ClassLoader previous = current.getContextClassLoader();
            String previousName = current.getName();
            current.setName("coordinator-thread");
            current.setContextClassLoader(appFreeClassLoader());
            try {
                return action.get();
            } finally {
                current.setContextClassLoader(previous);
                current.setName(previousName);
            }
        }

        ExecutionRecord<Object, Object> admit(String tenant, String key, TypedDurablePayload canonicalInput) {
            return runAppFree(() -> {
                var accepted = coordinator.executePipelineAsync(
                    canonicalInput, tenant, key, false, contract.pipelineId(), contract.contractVersion(), RELEASE_VERSION)
                    .await().atMost(TIMEOUT);
                return executions.getExecution(tenant, accepted.executionId()).await().atMost(TIMEOUT).orElseThrow();
            });
        }

        ExecutionRecord<Object, Object> admitAndProcess(String tenant, String key, TypedDurablePayload canonicalInput) {
            var created = admit(tenant, key, canonicalInput);
            return process(tenant, created.executionId());
        }

        ExecutionRecord<Object, Object> process(String tenant, String executionId) {
            return runAppFree(() -> {
                coordinatorService.processExecutionWorkItem(new ExecutionWorkItem(tenant, executionId))
                    .await().atMost(TIMEOUT);
                return executions.getExecution(tenant, executionId).await().atMost(TIMEOUT).orElseThrow();
            });
        }

        @Override public void close() {
            invokeNoArg(restClient, "close");
            lifecycleServer.stop(0);
            lifecycleExecutor.close();
            restHttpExecutor.close();
        }
    }

    private static final class SyncWorker implements AutoCloseable {
        final PipelineExecutionService service;
        final RestTransitionWorkerResource resource;
        final ClassLoader workerClassLoader;
        final RestAwaitLifecycleClient lifecycleClient;
        final ExecutorService awaitHttpExecutor;
        final AtomicInteger executeCalls = new AtomicInteger();

        SyncWorker(PipelineContractDescriptor contract, String artifactDigest, CommandEffectStore store,
            Optional<URI> coordinatorLifecycleOrigin, DeferredEchoProvider deferredProvider, boolean deferred) throws Exception {
            var builder = new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class).withValidateUnknown(false)
                .withDefaultValue("pipeline.orchestrator.mode", "SYNC")
                .withDefaultValue("pipeline.orchestrator.process-loops-disabled", "true")
                .withDefaultValue("pipeline.orchestrator.pipeline-id", contract.pipelineId())
                .withDefaultValue("pipeline.orchestrator.release-version", RELEASE_VERSION)
                .withDefaultValue("pipeline.orchestrator.worker.artifact-id", ARTIFACT_ID)
                .withDefaultValue("pipeline.orchestrator.worker.artifact-digest", artifactDigest)
                .withDefaultValue("pipeline.orchestrator.worker.allowed-payload-prefixes", "org.pipelineframework.,com.example.echo.")
                .withDefaultValue("pipeline.orchestrator.worker.allow-loopback-http", "true")
                .withDefaultValue("pipeline.orchestrator.worker.rest.server-enabled", "true")
                .withDefaultValue("pipeline.orchestrator.worker.rest.shared-secret-ref", SECRET_REF);
            coordinatorLifecycleOrigin.ifPresent(origin -> builder
                .withDefaultValue("pipeline.orchestrator.worker.await-authority.base-url", origin.toASCIIString())
                .withDefaultValue("pipeline.orchestrator.worker.await-authority.worker-id", WORKER_ID)
                .withDefaultValue("pipeline.orchestrator.worker.await-authority.entry-id", ENTRY_ID));
            var config = builder.build().getConfigMapping(PipelineOrchestratorConfig.class);

            var wireCodec = new JsonTransitionPayloadCodec();
            setField(wireCodec, "orchestratorConfig", config);
            var identityResolver = new PipelineReleaseIdentityResolver() {
                @Override public PipelineContractDescriptor contract() { return contract; }
                @Override public Optional<PipelineContractDescriptor> compiledContract() { return Optional.of(contract); }
                @Override public String pipelineId(PipelineOrchestratorConfig cfg) { return contract.pipelineId(); }
                @Override public String contractVersion() { return contract.contractVersion(); }
                @Override public String releaseVersion(PipelineOrchestratorConfig cfg) { return RELEASE_VERSION; }
                @Override public String artifactId(PipelineOrchestratorConfig cfg) { return ARTIFACT_ID; }
                @Override public String artifactDigest(PipelineOrchestratorConfig cfg) { return artifactDigest; }
                @Override public PipelineBundleCapabilities capabilities() { return PipelineBundleCapabilities.defaults(); }
                @Override public Optional<String> validateCommandIdentity(TransitionCommandEnvelope cmd, PipelineOrchestratorConfig cfg) { return Optional.empty(); }
            };

            awaitHttpExecutor = Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "packaged-worker-await-http");
                t.setContextClassLoader(PackagedRestWorkerCommandProof.class.getClassLoader());
                return t;
            });
            lifecycleClient = new RestAwaitLifecycleClient();
            setField(lifecycleClient, "config", config);
            setField(lifecycleClient, "secrets", (ControlPlaneSecretResolver) ref -> SECRET);
            setField(lifecycleClient, "requestContext", directThreadContext(awaitHttpExecutor));
            setField(lifecycleClient, "http", HttpClient.newBuilder().executor(awaitHttpExecutor).build());

            CommandStepSupport commandSupport;
            RemoteAwaitWorkerLifecycle remoteAwait = null;
            AwaitCoordinator workerAwaitCoordinator = null;
            AwaitValueOperationExecutor valueExecutor = new AwaitValueOperationExecutor() {
                @Override public boolean supported() { return false; }
            };
            List<Object> steps;
            ClassLoader cl;

            if (!deferred) {
                var provider = new EchoProvider();
                var bindings = ConnectorRegistryLifecycle.createBindingRegistry(
                    List.of(new ConnectorBindingDefinition(ConnectorBindingName.of("store"),
                        ConnectorProviderId.of("fixture.echo"), 1, ConnectorConfigurationDocument.empty())),
                    List.of(provider));
                commandSupport = new CommandStepSupport(new ConnectorRegistry(List.of(provider)), bindings, List.of(store), config);
                var generatedStep = new ProcessEchoCommandClientStep();
                setField(generatedStep, "support", commandSupport);
                setField(generatedStep, "commandIdGenerator", new StableId());
                steps = List.of(generatedStep);
                cl = PackagedRestWorkerCommandProof.class.getClassLoader();
            } else {
                var bindings = ConnectorRegistryLifecycle.createBindingRegistry(
                    List.of(new ConnectorBindingDefinition(ConnectorBindingName.of("store"),
                        ConnectorProviderId.of("fixture.echo.deferred"), 1, ConnectorConfigurationDocument.empty())),
                    List.of(deferredProvider));
                commandSupport = new CommandStepSupport(new ConnectorRegistry(List.of(deferredProvider)), bindings, List.of(store), config);
                var opId = new ConnectorOperationIdentity(ConnectorProviderId.of("fixture.echo.deferred"), "echo", ConnectorOperationKind.COMMAND, 1);
                var cmdDescriptor = CommandDescriptor.nativeCommand("EchoDeferred",
                    new NativeCommandSelector(Optional.of(ConnectorBindingName.of("store")), opId, 1, CommandPolicy.none()),
                    EchoInput.class.getName(), EchoOutput.class.getName(), StableId.class.getName(),
                    CommandDuplicatePolicy.RETURN_RECORDED, Map.of());

                remoteAwait = new RemoteAwaitWorkerLifecycle();
                setField(remoteAwait, "config", config);
                setField(remoteAwait, "identity", identityResolver);
                setField(remoteAwait, "framing", wireCodec);
                setField(remoteAwait, "client", lifecycleClient);

                workerAwaitCoordinator = new AwaitCoordinator();
                setField(workerAwaitCoordinator, "orchestratorConfig", config);
                setField(workerAwaitCoordinator, "remoteLifecycle", remoteAwait);

                var deferredSupport = new CommandDeferredCompletionSupport();
                setField(deferredSupport, "coordinator", workerAwaitCoordinator);
                setField(deferredSupport, "config", config);
                setField(deferredSupport, "allowHttp", true);

                var cbSelection = new ConnectorCallbackSelection(ConnectorBindingName.of("store"), opId, 1,
                    deferredProvider.operation.callbacks().getFirst(), "fixture.EndpointResolver", "fixture.Authenticator");
                var awaitDescriptor = new AwaitCompletionDescriptor("EchoDeferred",
                    EchoInput.class.getName(), EchoOutput.class.getName(), "ONE_TO_ONE", Duration.ofMinutes(5),
                    "signedResumeToken", "", Map.of(), List.of(),
                    EchoInput.class.getName(), EchoOutput.class.getName(),
                    Function.identity(), Function.identity(), "fixture.EchoProjector", null, true,
                    Optional.of(cbSelection), EchoOutput.class.getName(),
                    (req, ack, cb, meta) -> new EchoOutput(((EchoInput) req).value() + "/" + ((EchoOutput) ack).value() + "/" + ((EchoOutput) cb).value()));

                var registry = new AwaitCompletionDescriptorRegistry();
                registry.register(awaitDescriptor);
                valueExecutor = new AwaitValueOperationExecutor();
                setField(valueExecutor, "identities", identityResolver);
                setField(valueExecutor, "descriptors", registry);
                setField(valueExecutor, "framing", wireCodec);
                setField(valueExecutor, "config", config);

                var stableId = new StableId();
                StepOneToOne<EchoInput, EchoOutput> deferredStep = new DeferredEchoStep(input ->
                    deferredSupport.execute(awaitDescriptor, input, req -> URI.create("http://127.0.0.1:8080/"),
                        callback -> commandSupport.execute(cmdDescriptor, stableId, input, callback)));
                steps = List.of(deferredStep);

                Path metaDir = Files.createTempDirectory("tpf-packaged-deferred-meta-");
                Path orderFile = metaDir.resolve("META-INF/pipeline/order.json");
                Files.createDirectories(orderFile.getParent());
                PipelineJson.mapper().writeValue(orderFile.toFile(), Map.of(
                    "boundarySchemaVersion", 1,
                    "order", List.of(DeferredEchoStep.class.getName()),
                    "stepBoundaries", List.of(Map.of(
                        "stepClass", DeferredEchoStep.class.getName(),
                        "inputTypeId", "EchoInput",
                        "outputTypeId", "EchoOutput"))));
                URL orderUrl = orderFile.toUri().toURL();
                cl = new ClassLoader(PackagedRestWorkerCommandProof.class.getClassLoader()) {
                    @Override public Enumeration<URL> getResources(String name) throws java.io.IOException {
                        if ("META-INF/pipeline/order.json".equals(name)) return Collections.enumeration(List.of(orderUrl));
                        if ("META-INF/pipeline/definition-boundaries.json".equals(name)) return Collections.emptyEnumeration();
                        return super.getResources(name);
                    }
                    @Override public URL getResource(String name) {
                        if ("META-INF/pipeline/order.json".equals(name)) return orderUrl;
                        if ("META-INF/pipeline/definition-boundaries.json".equals(name)) return null;
                        return super.getResource(name);
                    }
                };
            }
            this.workerClassLoader = cl;

            var stepConfig = new SmallRyeConfigBuilder().withMapping(PipelineStepConfig.class)
                .withDefaultValue("pipeline.defaults.retry-limit", "0").build().getConfigMapping(PipelineStepConfig.class);
            var pipelineConfig = new PipelineConfig();
            pipelineConfig.defaults().retryLimit(0);
            var telemetry = new PipelineTelemetry(stepConfig);
            var stepResolver = new PipelineStepResolver() {
                @Override public List<Object> loadPipelineSteps() { return steps; }
            };
            var stepOrderer = new PipelineStepOrderer() {
                @Override public List<Object> orderSteps(List<Object> input) { return input; }
            };
            var runner = new PipelineRunner();
            runner.pipelineConfig = pipelineConfig;
            runner.configFactory = new ConfigFactory();
            runner.stepOrderer = stepOrderer;
            runner.parallelismPolicyResolver = new PipelineParallelismPolicyResolver();
            runner.cacheSupportFactory = new PipelineCacheSupportFactory(emptyInstance(), emptyInstance(), "bypass-cache");
            runner.stepExecutor = new PipelineStepExecutor();
            runner.runTelemetry = telemetry;
            runner.stepTelemetry = telemetry;

            service = new PipelineExecutionService();
            service.pipelineRunner = runner;
            service.pipelineConfig = pipelineConfig;
            service.pipelineStepConfig = stepConfig;
            service.pipelineStepResolver = stepResolver;
            service.stepOrderer = stepOrderer;
            service.configFactory = new ConfigFactory();
            service.executionHooks = new ExecutionHooks();
            service.executionInputPolicy = new ExecutionInputPolicy();
            service.orchestratorConfig = config;
            service.commandStepSupport = commandSupport;
            service.remoteAwaitLifecycle = remoteAwait;
            service.awaitCoordinator = workerAwaitCoordinator;
            service.transitionPayloadCodec = wireCodec;
            setField(service, "releaseIdentityResolver", identityResolver);
            setField(service, "startupHealthState", new AtomicReference<>(PipelineExecutionService.StartupHealthState.HEALTHY));

            resource = new RestTransitionWorkerResource();
            setField(resource, "orchestratorConfig", config);
            setField(resource, "executionService", service);
            setField(resource, "identityResolver", identityResolver);
            setField(resource, "secretResolver", (ControlPlaneSecretResolver) ref -> SECRET);
            setField(resource, "awaitValueOperations", valueExecutor);
            setField(resource, "requestThreadContext", directThreadContext(Runnable::run));
            invokeNoArg(resource, "validateServerConfig");
        }

        void handle(HttpExchange exchange) throws java.io.IOException {
            Thread current = Thread.currentThread();
            ClassLoader previous = current.getContextClassLoader();
            current.setContextClassLoader(workerClassLoader);
            try {
                byte[] body = exchange.getRequestBody().readAllBytes();
                var headers = exchange.getRequestHeaders();
                String ts = headers.getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER);
                String nonce = headers.getFirst(TransitionWorkerSignature.NONCE_HEADER);
                String sig = headers.getFirst(TransitionWorkerSignature.SIGNATURE_HEADER);
                String path = exchange.getRequestURI().getPath();
                Uni<Response> pending;
                if (RestTransitionWorkerProtocol.CAPABILITIES_PATH.equals(path)) {
                    pending = resource.capabilities(ts, nonce, sig);
                } else if (RestTransitionWorkerProtocol.EXECUTE_PATH.equals(path)) {
                    executeCalls.incrementAndGet();
                    pending = resource.execute(ts, nonce, sig, body);
                } else if (RestTransitionWorkerProtocol.AWAIT_VALUE_OPERATION_PATH.equals(path)) {
                    pending = resource.executeAwaitValueOperation(ts, nonce, sig, body);
                } else {
                    pending = Uni.createFrom().item(Response.status(404).build());
                }
                try (Response response = pending.await().atMost(TIMEOUT)) {
                    byte[] payload = response.getEntity() == null ? new byte[0]
                        : PipelineJson.mapper().writeValueAsBytes(response.getEntity());
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(response.getStatus(), payload.length);
                    if (payload.length > 0) exchange.getResponseBody().write(payload);
                }
            } finally {
                current.setContextClassLoader(previous);
                exchange.close();
            }
        }

        @Override public void close() {
            invokeNoArg(lifecycleClient, "close");
            awaitHttpExecutor.close();
        }
    }

    public static final class DeferredEchoStep extends ConfigurableStep implements StepOneToOne<EchoInput, EchoOutput> {
        private final Function<EchoInput, Uni<EchoOutput>> fn;
        public DeferredEchoStep(Function<EchoInput, Uni<EchoOutput>> fn) { this.fn = fn; }
        @Override public Uni<EchoOutput> applyOneToOne(EchoInput input) { return fn.apply(input); }
    }

    public static final class DeferredEchoProvider implements ConnectorProvider<Void> {
        public record Config() {}
        static final AtomicInteger CALLS = new AtomicInteger();
        static final AtomicReference<URI> LAST_CALLBACK_URI = new AtomicReference<>();
        final CommandOperation<EchoInput, Config, EchoOutput> operation = new CommandOperation<>() {
            @Override public String id() { return "echo"; }
            @Override public Optional<ConnectorConfigSchema<Config>> configurationSchema() {
                return Optional.of(ConnectorConfigSchema.record(Config.class, "fixture.echo.deferred.config", 1));
            }
            @Override public List<ConnectorOperationCallbackDescriptor> callbacks() {
                return List.of(new ConnectorOperationCallbackDescriptor(
                    "completed", new ConnectorOperationTypeContract(EchoOutput.class.getName(), Optional.empty()), true));
            }
            @Override public CompletionStage<CommandOutcome<EchoOutput>> dispatch(CommandInvocation<EchoInput, Config> inv) {
                CALLS.incrementAndGet();
                inv.callbackContext().ifPresent(cb -> LAST_CALLBACK_URI.set(cb.callbackUri()));
                return CompletableFuture.completedFuture(new CommandOutcome.Succeeded<>(
                    new EchoOutput("ack:" + inv.input().value()), CommandConfirmation.none(), List.of()));
            }
        };
        @Override public ConnectorProviderId id() { return ConnectorProviderId.of("fixture.echo.deferred"); }
        @Override public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
        @Override public Collection<? extends ConnectorOperation> operations() { return List.of(operation); }
    }

    private static PipelineContractDescriptor deferredContract(DeferredEchoProvider provider) {
        var opId = new ConnectorOperationIdentity(ConnectorProviderId.of("fixture.echo.deferred"), "echo", ConnectorOperationKind.COMMAND, 1);
        var cbSelection = new ConnectorCallbackSelection(ConnectorBindingName.of("store"), opId, 1,
            provider.operation.callbacks().getFirst(), "fixture.EndpointResolver", "fixture.Authenticator");
        var rd = new LinkedHashMap<String, Object>();
        rd.put("schemaVersion", 1);
        rd.put("stepId", "EchoDeferred");
        rd.put("inputTypeId", EchoInput.class.getName());
        rd.put("outputTypeId", EchoOutput.class.getName());
        rd.put("transportInputTypeId", EchoInput.class.getName());
        rd.put("transportOutputTypeId", EchoOutput.class.getName());
        rd.put("canonicalInputTypeId", "EchoInput");
        rd.put("canonicalOutputTypeId", "EchoOutput");
        rd.put("completionPayloadTypeId", "EchoOutput");
        rd.put("requestAwareCompletion", true);
        rd.put("completionProjectorId", "fixture.EchoProjector");
        rd.put("acknowledgementAwareCommandCompletion", true);
        rd.put("commandAcknowledgementTypeId", EchoOutput.class.getName());
        rd.put("canonicalCommandAcknowledgementTypeId", "EchoOutput");
        rd.put("callbackContract", cbSelection.contractMetadata());
        rd.put("positions", List.of(Map.of("stepIndex", 0)));
        Map<String, Map<String, Object>> types = Map.of(
            "EchoInput", typeDef("EchoInput", EchoInput.class.getName()),
            "EchoOutput", typeDef("EchoOutput", EchoOutput.class.getName()));
        return new PipelineContractDescriptor(3, "hosted-rest-worker-command-deferred", "contract-deferred", "hash-deferred",
            "FUNCTION", "REST", "hosted-rest-worker-command-app", false, "PIPELINE_RUNTIME",
            List.of(new PipelineBundleStepDescriptor(0, "EchoDeferred", "command", "ONE_TO_ONE",
                EchoInput.class.getName(), EchoOutput.class.getName(),
                DeferredEchoStep.class.getName(), DeferredEchoStep.class.getName(),
                Map.of("mode", "CONNECTOR_CALLBACK", "commandCompletionMode", "ACKNOWLEDGEMENT_REQUIRED",
                    "correlationStrategy", "signedResumeToken", "timeout", "PT5M",
                    "idempotencyKeyFields", List.of(), "runtimeDescriptor", Map.copyOf(rd)))),
            PipelineBundleCapabilities.defaults(), types, "catalog-deferred",
            org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor.empty(),
            List.of(), List.of(), List.of(),
            Map.of("schemaVersion", 1, "inputCanonicalTypeId", "EchoInput", "outputCanonicalTypeId", "EchoOutput"));
    }

    private static Map<String, Object> typeDef(String id, String runtimeClass) {
        return Map.of("runtimeClass", runtimeClass, "definitionFingerprint", id + "-fp",
            "definition", Map.of("kind", "record", "id", id,
                "fields", List.of(Map.of("name", "value", "presence", "REQUIRED", "nullability", "NON_NULL",
                    "type", Map.of("kind", "scalar", "id", "string")))));
    }

    private static final class ConditionalRevisionDynamoTable {
        final ConcurrentMap<String, NavigableMap<Long, Map<String, AttributeValue>>> rows = new ConcurrentHashMap<>();
        final AtomicInteger conditionalPutCalls = new AtomicInteger();

        DynamoDbClient client() {
            return (DynamoDbClient) Proxy.newProxyInstance(DynamoDbClient.class.getClassLoader(),
                new Class<?>[]{DynamoDbClient.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "putItem" -> putItem((PutItemRequest) args[0]);
                    case "query" -> query((QueryRequest) args[0]);
                    case "serviceName" -> "dynamodb";
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }

        private PutItemResponse putItem(PutItemRequest request) {
            String pk = request.item().get(DynamoCommandEffectStore.COMMAND_KEY).s();
            long rev = Long.parseLong(request.item().get(DynamoCommandEffectStore.REVISION).n());
            String cond = request.conditionExpression();
            Map<String, String> names = request.expressionAttributeNames();
            boolean conditional = cond != null
                && cond.contains("attribute_not_exists(#commandKey)")
                && DynamoCommandEffectStore.COMMAND_KEY.equals(names.get("#commandKey"))
                && cond.contains("attribute_not_exists(#revision)")
                && DynamoCommandEffectStore.REVISION.equals(names.get("#revision"));
            if (conditional) conditionalPutCalls.incrementAndGet();
            var revs = rows.computeIfAbsent(pk, ignored -> new ConcurrentSkipListMap<>());
            synchronized (revs) {
                if (conditional && revs.containsKey(rev)) {
                    throw ConditionalCheckFailedException.builder().message("ConditionalCheckFailed").build();
                }
                revs.put(rev, Map.copyOf(request.item()));
            }
            return PutItemResponse.builder().build();
        }

        private QueryResponse query(QueryRequest request) {
            String pk = request.expressionAttributeValues().get(":commandKey").s();
            var revs = rows.get(pk);
            if (revs == null || revs.isEmpty()) return QueryResponse.builder().items(List.of()).build();
            List<Map<String, AttributeValue>> items;
            synchronized (revs) {
                items = Boolean.FALSE.equals(request.scanIndexForward())
                    ? List.of(revs.lastEntry().getValue()) : new ArrayList<>(revs.values());
            }
            return QueryResponse.builder().items(items).build();
        }
    }

    private static ClassLoader appFreeClassLoader() {
        return new ClassLoader(PackagedRestWorkerCommandProof.class.getClassLoader()) {
            @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (Thread.currentThread().getName().startsWith("coordinator-")
                    && (name.startsWith("com.example.echo.") || name.startsWith("org.pipelineframework.pipeline."))) {
                    throw new AssertionError("App-free Coordinator attempted to load application class: " + name);
                }
                return super.loadClass(name, resolve);
            }
        };
    }

    private static TypedDurablePayload canonicalPayload(PipelineContractDescriptor contract, String typeId, Object value) throws Exception {
        String fp = (String) contract.canonicalTypes().get(typeId).get("definitionFingerprint");
        return new TypedDurablePayload(typeId, fp, contract.canonicalCatalogFingerprint(),
            JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
            PipelineJson.mapper().writeValueAsBytes(value));
    }

    private static String canonicalField(Object durableResult, String field) throws Exception {
        Object unwrapped = durableResult instanceof List<?> items && !items.isEmpty() ? items.getFirst() : durableResult;
        var serialized = SerializedTransitionPayload.fromDurableValue(unwrapped);
        if (serialized.isPresent()) unwrapped = new JsonTransitionPayloadCodec().decode(serialized.orElseThrow());
        var canonical = TypedDurablePayload.fromDurableValue(unwrapped);
        if (canonical.isPresent()) return PipelineJson.mapper().readTree(canonical.orElseThrow().payload()).path(field).asText();
        return PipelineJson.mapper().valueToTree(unwrapped).path(field).asText();
    }

    @SuppressWarnings("unchecked")
    private static <T> Instance<T> emptyInstance() {
        return (Instance<T>) Proxy.newProxyInstance(Instance.class.getClassLoader(), new Class<?>[]{Instance.class},
            (p, m, a) -> switch (m.getName()) {
                case "isUnsatisfied" -> true;
                case "isAmbiguous", "isResolvable" -> false;
                case "stream" -> java.util.stream.Stream.empty();
                case "iterator" -> Collections.emptyIterator();
                default -> throw new UnsupportedOperationException(m.getName());
            });
    }

    @SuppressWarnings("unchecked")
    private static <T> Instance<T> singleInstance(T item) {
        return (Instance<T>) Proxy.newProxyInstance(Instance.class.getClassLoader(), new Class<?>[]{Instance.class},
            (p, m, a) -> switch (m.getName()) {
                case "isUnsatisfied", "isAmbiguous" -> false;
                case "isResolvable" -> true;
                case "get" -> item;
                case "stream" -> java.util.stream.Stream.of(item);
                case "iterator" -> List.of(item).iterator();
                default -> throw new UnsupportedOperationException(m.getName());
            });
    }

    private static ThreadContext directThreadContext(Executor executor) {
        return (ThreadContext) Proxy.newProxyInstance(ThreadContext.class.getClassLoader(), new Class<?>[]{ThreadContext.class},
            (p, m, a) -> "currentContextExecutor".equals(m.getName()) ? executor : null);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxyConfig(Class<T> iface, Map<String, Object> values) {
        return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[]{iface},
            (p, m, a) -> values.get(m.getName()));
    }

    private static <T> T construct(Class<T> type, Class<?>[] paramTypes, Object... args) throws Exception {
        Constructor<T> ctor = type.getDeclaredConstructor(paramTypes);
        ctor.setAccessible(true);
        return ctor.newInstance(args);
    }

    private static void invokeNoArg(Object target, String name) {
        try {
            Method m = target.getClass().getDeclaredMethod(name);
            m.setAccessible(true);
            m.invoke(target);
        } catch (Exception failure) {
            throw new IllegalStateException("Failed invoking " + name, failure);
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Class<?> type = target.getClass();
        while (type != null) {
            try {
                Field f = type.getDeclaredField(name);
                f.setAccessible(true);
                f.set(target, value);
                return;
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name + " on " + target.getClass().getName());
    }
}

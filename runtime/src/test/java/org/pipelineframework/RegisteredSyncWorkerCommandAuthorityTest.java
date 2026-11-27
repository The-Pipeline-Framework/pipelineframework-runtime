package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import com.sun.net.httpserver.HttpServer;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.command.*;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.*;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.release.*;
import org.pipelineframework.orchestrator.worker.*;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * End-to-end proof of admitted Command execution across an app-free shared Coordinator
 * ({@code mode=QUEUE_ASYNC}, {@code targeting-mode=registered-rest}, {@code SERIALIZED}) and a
 * standalone registered {@code SYNC} REST transition worker over signed HTTP.
 *
 * <p>Covers: capability/admission rejection of the default in-memory store, unscoped {@code SYNC}
 * rejection, ordinary {@code kind: command} execution under verified tenant/release/worker/segment
 * scope and explicit recoverable {@link DynamoCommandEffectStore}, duplicate dispatch replay across
 * fresh worker instances, retry/reissue redrive, uncertain provider outcome reconciliation, and
 * {@code kind: command} + {@code await:} callback settlement.
 */
class RegisteredSyncWorkerCommandAuthorityTest {
    private static final String TENANT = "tenant-alpha";
    private static final String PIPELINE_ID = "org.example.sync.command";
    private static final String CONTRACT_VERSION = "sha256:contract-sync-cmd-1";
    private static final String RELEASE_VERSION = "rel-sync-cmd-1";
    private static final String ARTIFACT_ID = "sync-worker-app";
    private static final String ARTIFACT_DIGEST = "sha256:worker-zip-digest-1";
    private static final String WORKER_ID = "worker-sync-1";
    private static final String ENTRY_ID = "rest-worker-entry";
    private static final String DEPLOYMENT_OP_ID = "deploy-op-1";
    private static final String SECRET = "registered-sync-worker-shared-secret-32";
    private static final String SECRET_REF = "fixture:worker-secret";
    private static final String TABLE = "tpf_command_effect_proof";
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    private static final AtomicReference<ProviderState> ACTIVE_PROVIDER_STATE = new AtomicReference<>();

    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() {
        ACTIVE_PROVIDER_STATE.set(null);
        AwaitExecutionContextHolder.clear();
        PipelineExecutionContextHolder.clear();
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception ignored) {
                // Best-effort cleanup for test HTTP servers and clients.
            }
        }
        closeables.clear();
    }

    @Test
    void rejectsDefaultInMemoryEffectStoreAtCapabilityAndAdmissionBeforeDispatch() throws Exception {
        var provider = new ScriptedCommandProvider();
        var contract = ordinaryCommandContract();
        var gateway = startWorkerGateway();
        var worker = startSyncWorker(contract, provider, new InMemoryCommandEffectStore(), Optional.empty(), false);
        gateway.attach(worker);
        var coordinator = startSharedCoordinator(contract, gateway.origin());

        PipelineWorkerAvailabilityResult availability = coordinator.targets.check(
            new PipelineWorkerAvailabilityRequest(TENANT, PIPELINE_ID, CONTRACT_VERSION, RELEASE_VERSION, ARTIFACT_ID, ARTIFACT_DIGEST))
            .await().atMost(TIMEOUT);
        assertFalse(availability.available(),
            "Worker with default InMemoryCommandEffectStore must be rejected before readiness");

        TypedDurablePayload clientInput = canonicalPayload(contract, "OrderInput", new OrderInput("ord-1"));
        assertThrows(RuntimeException.class, () -> coordinator.admit(TENANT, "exec-key-mem", clientInput));
        assertEquals(0, worker.executeCalls.get(), "Coordinator must never dispatch to an unverified Command worker");
        assertEquals(0, provider.dispatchCalls.get());
    }

    @Test
    void executesOrdinaryCommandOnRegisteredSyncWorkerReplaysAcrossFreshWorkerAndRejectsUnscopedSync() throws Exception {
        var dynamoTable = new ConditionalRevisionDynamoTable();
        var provider = new ScriptedCommandProvider();
        var contract = ordinaryCommandContract();
        var gateway = startWorkerGateway();
        var worker1 = startSyncWorker(contract, provider, new DynamoCommandEffectStore(dynamoTable.client(), TABLE),
            Optional.empty(), false);
        gateway.attach(worker1);
        var coordinator = startSharedCoordinator(contract, gateway.origin());

        // 1. Unscoped SYNC calls on the worker must stay rejected.
        assertThrows(IllegalStateException.class, () -> worker1.service.executePipelineUnary(Uni.createFrom().item(new OrderInput("unscoped")))
            .await().atMost(TIMEOUT));
        PipelineExecutionContextHolder.set(new PipelineExecutionContext(
            TENANT, "manual-exec", PIPELINE_ID, CONTRACT_VERSION, RELEASE_VERSION, 0, Optional.empty(), Optional.empty()));
        try {
            assertThrows(IllegalStateException.class, () -> worker1.commandSupport
                .<OrderInput, OrderOutput>execute(worker1.commandDescriptor, (d, i) -> "cmd-" + i.orderId(), new OrderInput("unscoped"))
                .await().atMost(TIMEOUT));
        } finally {
            PipelineExecutionContextHolder.clear();
        }
        assertEquals(0, provider.dispatchCalls.get());

        // 2. Admitted execution through app-free Coordinator's production submission path completes on registered SYNC worker.
        var availability = coordinator.targets.check(
            new PipelineWorkerAvailabilityRequest(TENANT, PIPELINE_ID, CONTRACT_VERSION, RELEASE_VERSION, ARTIFACT_ID, ARTIFACT_DIGEST))
            .await().atMost(TIMEOUT);
        assertTrue(availability.available());
        assertTrue(availability.capability().workerProtocols().contains(CommandStepSupport.COMMAND_AUTHORITY_CAPABILITY));

        TypedDurablePayload firstInput = canonicalPayload(contract, "OrderInput", new OrderInput("ord-100"));
        ExecutionRecord<Object, Object> first = coordinator.admitAndProcess(TENANT, "exec-ordinary-1", firstInput);
        assertEquals(ExecutionStatus.SUCCEEDED, first.status());
        assertEquals(1, provider.dispatchCalls.get());
        assertEquals("dispatched:ord-100#1", canonicalField(first.resultPayload(), "receipt"));
        assertTrue(dynamoTable.conditionalPutCalls.get() > 0, "DynamoCommandEffectStore must issue conditional writes");
        var capturedAfterFirst = new DynamoCommandEffectStore(dynamoTable.client(), TABLE)
            .find(TENANT, "cmd-ord-100").await().atMost(TIMEOUT).orElseThrow();
        String capturedAttemptId = capturedAfterFirst.currentAttempt().attemptId();
        TransitionCommandEnvelope originalEnvelope = worker1.lastEnvelope.get();
        assertNotNull(originalEnvelope, "Worker must have received a leased TransitionCommandEnvelope");

        // 3. Swap to a fresh worker behind the same stable origin (retaining the original dispatch binding & pin).
        var worker2 = startSyncWorker(contract, provider, new DynamoCommandEffectStore(dynamoTable.client(), TABLE),
            Optional.empty(), false);
        gateway.attach(worker2);
        worker1.close();

        // 3a. Duplicate delivery of the exact same leased transition envelope replays recorded output with zero second dispatch.
        TransitionResultEnvelope sameEnvelopeReplay = coordinator.runAppFree(() -> coordinator.targets
            .executeTransition(originalEnvelope)
            .await().atMost(TIMEOUT));
        assertEquals(TransitionWorkerOutcome.COMPLETED, sameEnvelopeReplay.outcome());
        assertEquals(1, provider.dispatchCalls.get(), "Same leased transition replay must not re-dispatch");
        assertEquals("dispatched:ord-100#1", canonicalField(sameEnvelopeReplay.decodeOutputItems(coordinator.wireCodec), "receipt"));
        var capturedAfterSameReplay = new DynamoCommandEffectStore(dynamoTable.client(), TABLE)
            .find(TENANT, "cmd-ord-100").await().atMost(TIMEOUT).orElseThrow();
        assertEquals("cmd-ord-100", capturedAfterSameReplay.commandId());
        assertEquals(capturedAttemptId, capturedAfterSameReplay.currentAttempt().attemptId());
        assertEquals(1, capturedAfterSameReplay.attempts().size());

        // 3b. Separate execution reusing logical commandId across fresh worker replays recorded output with zero second dispatch.
        TypedDurablePayload duplicateInput = canonicalPayload(contract, "OrderInput", new OrderInput("ord-100"));
        ExecutionRecord<Object, Object> duplicate = coordinator.admitAndProcess(TENANT, "exec-ordinary-duplicate", duplicateInput);
        assertEquals(ExecutionStatus.SUCCEEDED, duplicate.status());
        assertEquals(1, provider.dispatchCalls.get(), "Duplicate logical commandId across fresh worker must not re-dispatch");
        assertEquals("dispatched:ord-100#1", canonicalField(duplicate.resultPayload(), "receipt"));
    }

    @Test
    void preservesRetryRedriveAndReissueOnRegisteredSyncWorker() throws Exception {
        var dynamoTable = new ConditionalRevisionDynamoTable();
        var provider = new ScriptedCommandProvider();
        provider.nextOutcome.set(invocation -> new CommandOutcome.RetryableFailure<>("provider-busy", List.of()));
        var contract = ordinaryCommandContract();
        var gateway = startWorkerGateway();
        var worker = startSyncWorker(contract, provider, new DynamoCommandEffectStore(dynamoTable.client(), TABLE),
            Optional.empty(), false);
        gateway.attach(worker);
        var coordinator = startSharedCoordinator(contract, gateway.origin());

        // 1. Initial attempt hits FAILED_RETRYABLE -> coordinator marks execution FAILED (maxRetries=0).
        TypedDurablePayload retryInput = canonicalPayload(contract, "OrderInput", new OrderInput("ord-retry"));
        ExecutionRecord<Object, Object> failed = coordinator.admitAndProcess(TENANT, "exec-retry-1", retryInput);
        assertEquals(ExecutionStatus.FAILED, failed.status());
        assertEquals(1, provider.dispatchCalls.get());
        var storedAfterFailure = new DynamoCommandEffectStore(dynamoTable.client(), TABLE)
            .find(TENANT, "cmd-ord-retry").await().atMost(TIMEOUT).orElseThrow();
        assertEquals(CommandEffectStatus.FAILED_RETRYABLE, storedAfterFailure.status());

        // 2. Deliberate RETRY_FAILED_COMMAND redrive appends attempt 2 in DynamoCommandEffectStore and succeeds.
        provider.nextOutcome.set(null);
        coordinator.runAppFree(() -> coordinator.coordinator.redriveExecution(TENANT, failed.executionId(), failed.version(), true,
            ExecutionRedriveIntent.RETRY_FAILED_COMMAND, "operator-retry")
            .await().atMost(TIMEOUT));
        ExecutionRecord<Object, Object> retried = coordinator.process(TENANT, failed.executionId());
        assertEquals(ExecutionStatus.SUCCEEDED, retried.status());
        assertEquals(2, provider.dispatchCalls.get());
        assertEquals("dispatched:ord-retry#2", canonicalField(retried.resultPayload(), "receipt"));
        var storedAfterRetry = new DynamoCommandEffectStore(dynamoTable.client(), TABLE)
            .find(TENANT, "cmd-ord-retry").await().atMost(TIMEOUT).orElseThrow();
        assertEquals(2, storedAfterRetry.attempts().size());
        assertEquals(CommandAttemptPurpose.RETRY, storedAfterRetry.currentAttempt().purpose());

        // 3. Deliberate REISSUE_COMMAND redrive appends a new occurrence without weakening replay safety.
        coordinator.runAppFree(() -> coordinator.coordinator.redriveExecution(TENANT, retried.executionId(), retried.version(), false,
            ExecutionRedriveIntent.REISSUE_COMMAND, "cmd-ord-retry", "approved-reissue")
            .await().atMost(TIMEOUT));
        ExecutionRecord<Object, Object> reissued = coordinator.process(TENANT, retried.executionId());
        assertEquals(ExecutionStatus.SUCCEEDED, reissued.status());
        assertEquals(3, provider.dispatchCalls.get());
        assertEquals("dispatched:ord-retry#3", canonicalField(reissued.resultPayload(), "receipt"));
        var storedAfterReissue = new DynamoCommandEffectStore(dynamoTable.client(), TABLE)
            .find(TENANT, "cmd-ord-retry").await().atMost(TIMEOUT).orElseThrow();
        assertEquals(3, storedAfterReissue.attempts().size());
        assertEquals(CommandAttemptPurpose.REISSUE, storedAfterReissue.currentAttempt().purpose());
    }

    @Test
    void reconcilesUncertainAmbiguousProviderOutcomeWithoutRedispatch() throws Exception {
        var dynamoTable = new ConditionalRevisionDynamoTable();
        var provider = new ScriptedCommandProvider();
        provider.nextOutcome.set(invocation -> new CommandOutcome.Ambiguous<>("timeout-after-write", List.of()));
        var contract = ordinaryCommandContract();
        var gateway = startWorkerGateway();
        var worker1 = startSyncWorker(contract, provider, new DynamoCommandEffectStore(dynamoTable.client(), TABLE),
            Optional.empty(), false);
        gateway.attach(worker1);
        var coordinator = startSharedCoordinator(contract, gateway.origin());

        TypedDurablePayload ambigInput = canonicalPayload(contract, "OrderInput", new OrderInput("ord-ambig"));
        ExecutionRecord<Object, Object> ambiguous = coordinator.admitAndProcess(TENANT, "exec-ambiguous-1", ambigInput);
        assertEquals(ExecutionStatus.FAILED, ambiguous.status());
        assertEquals(1, provider.dispatchCalls.get());
        var storedAmbiguous = new DynamoCommandEffectStore(dynamoTable.client(), TABLE)
            .find(TENANT, "cmd-ord-ambig").await().atMost(TIMEOUT).orElseThrow();
        assertEquals(CommandEffectStatus.AMBIGUOUS, storedAmbiguous.status());
        assertTrue(storedAmbiguous.currentAttempt().recoveryBinding().isPresent());

        // Swap to a fresh worker behind the same stable origin (original retained binding & execution pin stay untouched).
        var worker2 = startSyncWorker(contract, provider, new DynamoCommandEffectStore(dynamoTable.client(), TABLE),
            Optional.empty(), false);
        gateway.attach(worker2);
        worker1.close();

        coordinator.runAppFree(() -> coordinator.coordinator.redriveExecution(TENANT, ambiguous.executionId(), ambiguous.version(), true,
            ExecutionRedriveIntent.REPLAY, "reconcile-ambiguous")
            .await().atMost(TIMEOUT));
        ExecutionRecord<Object, Object> reconciled = coordinator.process(TENANT, ambiguous.executionId());
        assertEquals(ExecutionStatus.SUCCEEDED, reconciled.status());
        assertEquals(1, provider.dispatchCalls.get(), "Reconciliation must settle uncertain effect without re-dispatching");
        assertEquals(1, provider.reconcileCalls.get());
        assertEquals("dispatched:ord-ambig#1", canonicalField(reconciled.resultPayload(), "receipt"));
    }

    @Test
    void executesCommandWithDeferredAwaitCallbackAndSettlementAcrossAppFreeCoordinatorAndSyncWorker() throws Exception {
        var dynamoTable = new ConditionalRevisionDynamoTable();
        var provider = new ScriptedCommandProvider();
        var contract = deferredCommandContract(provider);
        var gateway = startWorkerGateway();
        // Start shared Coordinator first so worker knows the Coordinator Await lifecycle origin; binding pins the stable gateway.
        var coordinator = startSharedCoordinator(contract, gateway.origin());
        var worker = startSyncWorker(contract, provider, new DynamoCommandEffectStore(dynamoTable.client(), TABLE),
            Optional.of(coordinator.lifecycleOrigin()), true);
        gateway.attach(worker);

        // 1. Admission & first leased segment: worker registers & claims Await on Coordinator, runs CommandStepSupport
        //    under verified CommandWorkerExecutionScope + DynamoCommandEffectStore, settles acknowledgement, and suspends.
        TypedDurablePayload cbInput = canonicalPayload(contract, "OrderInput", new OrderInput("ord-cb"));
        ExecutionRecord<Object, Object> waiting = coordinator.admitAndProcess(TENANT, "exec-cmd-await-1", cbInput);
        assertEquals(ExecutionStatus.WAITING_EXTERNAL, waiting.status());
        assertEquals(1, provider.dispatchCalls.get());
        assertNotNull(provider.lastCallbackUri.get(), "Provider must receive Coordinator-signed callback URL");
        assertTrue(provider.lastCallbackUri.get().toASCIIString().contains(CommandDeferredCompletionSupport.CALLBACK_PATH));
        var storedEffect = new DynamoCommandEffectStore(dynamoTable.client(), TABLE)
            .find(TENANT, "cmd-ord-cb").await().atMost(TIMEOUT).orElseThrow();
        assertEquals(CommandEffectStatus.SUCCEEDED, storedEffect.status());

        var interactions = coordinator.interactions.findByExecution(TENANT, waiting.executionId(), 10).await().atMost(TIMEOUT);
        assertEquals(1, interactions.size());
        var interaction = interactions.getFirst();
        assertEquals(AwaitInteractionStatus.DISPATCHED, interaction.status());
        assertTrue(interaction.commandAcknowledgement().isPresent(), "Coordinator must retain settled Command acknowledgement");

        // 2. Callback completion arrives on the app-free Coordinator; Coordinator invokes pure AwaitValueOperation on the
        //    worker (without loading app DTOs), projects final output, and queues resume.
        String token = provider.lastCallbackUri.get().toASCIIString()
            .substring(provider.lastCallbackUri.get().toASCIIString().lastIndexOf('/') + 1);
        var completion = coordinator.runAppFree(() -> coordinator.coordinator.completeAwait(new AwaitCompletionCommand(
            TENANT, interaction.interactionId(), null, token, "cb-key-1",
            Map.of("status", "confirmed"), "external-provider", System.currentTimeMillis())).await().atMost(TIMEOUT));
        assertFalse(completion.duplicate());
        assertEquals(AwaitInteractionStatus.COMPLETED, completion.record().status());

        // 3. Coordinator dispatches resumed segment to registered SYNC worker and completes execution.
        ExecutionRecord<Object, Object> resumed = coordinator.process(TENANT, waiting.executionId());
        assertEquals(ExecutionStatus.SUCCEEDED, resumed.status());
        assertEquals(1, provider.dispatchCalls.get(), "Resume must not re-dispatch the settled Command");
        assertEquals("ord-cb/dispatched:ord-cb#1/confirmed", canonicalField(resumed.resultPayload(), "receipt"));
    }

    // -------------------------------------------------------------------------
    // Shared App-Free Coordinator Harness (no application DTOs or compiled contract)
    // -------------------------------------------------------------------------

    private CoordinatorHarness startSharedCoordinator(PipelineContractDescriptor contract, URI initialWorkerOrigin) throws Exception {
        var harness = new CoordinatorHarness(contract, initialWorkerOrigin);
        closeables.add(harness);
        return harness;
    }

    private final class CoordinatorHarness implements AutoCloseable {
        final InMemoryExecutionStateStore executions = new InMemoryExecutionStateStore();
        final InMemoryAwaitInteractionStore interactions = new InMemoryAwaitInteractionStore();
        final InMemoryAwaitUnitStore units = new InMemoryAwaitUnitStore();
        final InMemoryPipelineReleaseRegistry releases = new InMemoryPipelineReleaseRegistry();
        final InMemoryPipelineWorkerRegistry workerRegistry = new InMemoryPipelineWorkerRegistry();
        final AwaitResumeTokenService resumeTokens;
        final JsonTransitionPayloadCodec wireCodec = new JsonTransitionPayloadCodec();
        final AtomicReference<URI> workerOrigin = new AtomicReference<>();
        final RegisteredRestWorkerTargets targets;
        final AwaitCoordinator awaitCoordinator;
        final QueueAsyncCoordinator coordinator;
        final PipelineExecutionService coordinatorService;
        final RestPipelineTransitionWorker restClient;
        final HttpServer lifecycleServer;
        final ExecutorService lifecycleExecutor;
        final PipelineContractDescriptor contract;

        CoordinatorHarness(PipelineContractDescriptor contract, URI initialOrigin) throws Exception {
            this.contract = contract;
            this.workerOrigin.set(initialOrigin);
            this.resumeTokens = construct(AwaitResumeTokenService.class, new Class<?>[]{String.class},
                "coordinator-only-signing-secret-32");
            long now = System.currentTimeMillis();
            var entry = new PipelineReleaseDeployment.Entry(ENTRY_ID, ARTIFACT_ID, "app", ".",
                "io.quarkus.amazon.lambda.http.LambdaHttpHandler", "AWS_DURABLE_REST_WORKER", List.of());
            var artifact = new PipelineReleaseArtifactDescriptor(ARTIFACT_ID, "lambda-zip", "s3://releases/worker.zip",
                ARTIFACT_DIGEST, contract.steps().stream().map(PipelineBundleStepDescriptor::authoredName).toList(),
                List.of("local", "rest", "grpc", "sqs"));
            var deployment = new PipelineReleaseDeployment("aws-durable-java21-rest-worker-v1", List.of(entry), List.of(),
                new PipelineReleaseDeployment.Check(ENTRY_ID, Map.of("payload", "{}"), Map.of("payload", "{}"), 5000));
            var releaseDescriptor = new PipelineReleaseDescriptor(2, PIPELINE_ID, CONTRACT_VERSION, RELEASE_VERSION,
                ARTIFACT_ID, List.of(artifact), deployment);
            releases.register(new PipelineReleaseRecord(TENANT, PIPELINE_ID, CONTRACT_VERSION, RELEASE_VERSION,
                PipelineReleaseStatus.ACTIVE, releaseDescriptor, ARTIFACT_ID, ARTIFACT_DIGEST, artifact.uri(),
                0, "", contract, now, now, now)).await().atMost(TIMEOUT);

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

            var restHttpExecutor = Executors.newCachedThreadPool(runnable -> {
                Thread t = new Thread(runnable, "coordinator-rest-http");
                t.setContextClassLoader(appFreeCoordinatorClassLoader());
                return t;
            });
            closeables.add(restHttpExecutor::close);
            restClient = construct(RestPipelineTransitionWorker.class,
                new Class<?>[]{HttpClient.class, PipelineInvocationRuntime.class},
                HttpClient.newBuilder().executor(restHttpExecutor).build(),
                new PipelineInvocationRuntime());
            setField(restClient, "orchestratorConfig", config);
            setField(restClient, "secretResolver", (ControlPlaneSecretResolver) ref -> SECRET);

            targets = new RegisteredRestWorkerTargets();
            setField(targets, "config", config);
            setField(targets, "executionStores", instanceOf((ExecutionStateStore) executions));
            setField(targets, "releases", releases);
            setField(targets, "workers", workerRegistry);
            setField(targets, "rest", restClient);
            var source = new RegisteredRestWorkerTargetSource() {
                @Override public boolean enabled() { return true; }
                @Override public Uni<Map<String, PipelineOrchestratorConfig.RegisteredRestTargetConfig>> snapshot() {
                    URI current = workerOrigin.get();
                    if (current == null) return Uni.createFrom().item(Map.of());
                    var target = mock(PipelineOrchestratorConfig.RegisteredRestTargetConfig.class);
                    when(target.entryId()).thenReturn(Optional.of(ENTRY_ID));
                    when(target.tenantId()).thenReturn(TENANT);
                    when(target.pipelineId()).thenReturn(PIPELINE_ID);
                    when(target.contractVersion()).thenReturn(CONTRACT_VERSION);
                    when(target.releaseVersion()).thenReturn(RELEASE_VERSION);
                    when(target.workerId()).thenReturn(WORKER_ID);
                    when(target.protocol()).thenReturn("rest");
                    when(target.endpoint()).thenReturn(current.toASCIIString());
                    when(target.sharedSecretRef()).thenReturn(SECRET_REF);
                    when(target.artifactId()).thenReturn(ARTIFACT_ID);
                    when(target.artifactDigest()).thenReturn(ARTIFACT_DIGEST);
                    return Uni.createFrom().item(Map.of("primary", target));
                }
                @Override public Uni<Void> prepareCredential(String reference) { return Uni.createFrom().voidItem(); }
            };
            setField(targets, "targetSources", instanceOf(source));
            if (initialOrigin != null) {
                registerWorkerAndBindingOnce(initialOrigin);
            }

            var resolver = new AwaitDurablePayloadResolver();
            setField(resolver, "orchestratorConfig", config);
            setField(resolver, "executionStateStore", executions);
            setField(resolver, "releaseRegistry", releases);

            awaitCoordinator = new AwaitCoordinator();
            setField(awaitCoordinator, "interactionStores", instanceOf((AwaitInteractionStore) interactions));
            setField(awaitCoordinator, "unitStores", instanceOf((AwaitUnitStore) units));
            setField(awaitCoordinator, "orchestratorConfig", config);
            setField(awaitCoordinator, "durablePayloadResolver", resolver);
            setField(awaitCoordinator, "registeredWorkers", targets);
            setField(awaitCoordinator, "transitionPayloadCodec", wireCodec);
            setField(awaitCoordinator, "resumeTokenService", resumeTokens);

            var workDispatcher = mock(WorkDispatcher.class);
            when(workDispatcher.providerName()).thenReturn("event");
            when(workDispatcher.startupValidationError()).thenReturn(Optional.empty());
            when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
            when(workDispatcher.enqueueDelayed(any(), any())).thenReturn(Uni.createFrom().voidItem());
            var dlq = mock(DeadLetterPublisher.class);
            when(dlq.providerName()).thenReturn("log");
            when(dlq.startupValidationError()).thenReturn(Optional.empty());
            when(dlq.publish(any())).thenReturn(Uni.createFrom().voidItem());

            coordinator = new QueueAsyncCoordinator();
            coordinator.orchestratorConfig = config;
            coordinator.executionStateStore = executions;
            coordinator.executionStateStores = instanceOf((ExecutionStateStore) executions);
            coordinator.workDispatcher = workDispatcher;
            coordinator.workDispatchers = instanceOf(workDispatcher);
            coordinator.deadLetterPublisher = dlq;
            coordinator.deadLetterPublishers = instanceOf(dlq);
            coordinator.executionInputPolicy = new ExecutionInputPolicy();
            setField(coordinator.executionInputPolicy, "orchestratorConfig", config);
            coordinator.executionResultShapeResolver = new ExecutionResultShapeResolver();
            setField(coordinator.executionResultShapeResolver, "releases", releases);
            var emptyContractLoader = mock(PipelineContractDescriptorLoader.class);
            when(emptyContractLoader.load()).thenReturn(Optional.empty());
            setField(coordinator.executionResultShapeResolver, "contractLoader", emptyContractLoader);
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
            // Coordinator has NO application steps loaded.
            var emptyResolver = mock(PipelineStepResolver.class);
            when(emptyResolver.loadPipelineSteps()).thenReturn(List.of());
            var passthroughOrderer = mock(PipelineStepOrderer.class);
            when(passthroughOrderer.orderSteps(any())).thenAnswer(inv -> inv.getArgument(0));
            coordinatorService.pipelineStepResolver = emptyResolver;
            coordinatorService.stepOrderer = passthroughOrderer;
            controlPlane.pipelineExecutionService = coordinatorService;

            // Wire Coordinator-hosted RestAwaitLifecycleResource over loopback HTTP.
            var authority = new AwaitTransitionAuthority();
            setField(authority, "executionStores", instanceOf((ExecutionStateStore) executions));
            setField(authority, "interactionStores", instanceOf((AwaitInteractionStore) interactions));
            setField(authority, "config", config);
            setField(authority, "releases", releases);
            setField(authority, "coordinator", awaitCoordinator);

            var lifecycleResource = new RestAwaitLifecycleResource();
            setField(lifecycleResource, "config", config);
            setField(lifecycleResource, "workers", targets);
            setField(lifecycleResource, "secrets", (ControlPlaneSecretResolver) ref -> SECRET);
            setField(lifecycleResource, "authority", authority);
            var reqCtx = mock(org.eclipse.microprofile.context.ThreadContext.class);
            when(reqCtx.currentContextExecutor()).thenReturn(Runnable::run);
            setField(lifecycleResource, "requestContext", reqCtx);

            lifecycleServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            lifecycleExecutor = Executors.newFixedThreadPool(4, runnable -> {
                Thread t = new Thread(runnable, "coordinator-lifecycle-http");
                t.setContextClassLoader(appFreeCoordinatorClassLoader());
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
                        if (payload.length > 0) {
                            exchange.getResponseBody().write(payload);
                        }
                    }
                } catch (Throwable failure) {
                    failure.printStackTrace(System.err);
                    byte[] payload = String.valueOf(failure).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(500, payload.length);
                    exchange.getResponseBody().write(payload);
                } finally {
                    exchange.close();
                }
            });
            lifecycleServer.start();
        }

        URI lifecycleOrigin() {
            return URI.create("http://127.0.0.1:" + lifecycleServer.getAddress().getPort());
        }

        private void registerWorkerAndBindingOnce(URI origin) {
            workerOrigin.set(origin);
            long now = System.currentTimeMillis();
            workerRegistry.register(new PipelineWorkerRegistration(TENANT, PIPELINE_ID, CONTRACT_VERSION, RELEASE_VERSION,
                WORKER_ID, "rest", origin.toASCIIString(), ARTIFACT_ID, ARTIFACT_DIGEST), now).await().atMost(TIMEOUT);
            releases.registerEntryBinding(new PipelineReleaseEntryBinding(TENANT, PIPELINE_ID, CONTRACT_VERSION,
                RELEASE_VERSION, ENTRY_ID, ARTIFACT_ID, ARTIFACT_DIGEST, "aws-durable-java21-rest-worker-v1",
                "arn:aws:lambda:us-east-1:123456789012:function:worker:1", "sha256:code", DEPLOYMENT_OP_ID,
                Optional.of(new PipelineReleaseDispatchBinding("rest", origin.toASCIIString(), SECRET_REF, WORKER_ID))))
                .await().atMost(TIMEOUT);
        }

        <T> T runAppFree(java.util.function.Supplier<T> action) {
            Thread current = Thread.currentThread();
            ClassLoader previous = current.getContextClassLoader();
            String previousName = current.getName();
            current.setName("coordinator-thread");
            current.setContextClassLoader(appFreeCoordinatorClassLoader());
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
                    canonicalInput, tenant, key, false, PIPELINE_ID, CONTRACT_VERSION, RELEASE_VERSION)
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

        @Override
        public void close() {
            invokeNoArg(restClient, "close");
            lifecycleServer.stop(0);
            lifecycleExecutor.close();
        }
    }

    private static ClassLoader appFreeCoordinatorClassLoader() {
        ClassLoader parent = RegisteredSyncWorkerCommandAuthorityTest.class.getClassLoader();
        Set<String> forbidden = Set.of(
            OrderInput.class.getName(),
            OrderOutput.class.getName(),
            OrderCallback.class.getName(),
            OrderStep.class.getName(),
            ScriptedCommandProvider.class.getName());
        return new ClassLoader(parent) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (Thread.currentThread().getName().startsWith("coordinator-") && forbidden.contains(name)) {
                    throw new AssertionError("App-free Coordinator attempted to load application class: " + name);
                }
                return super.loadClass(name, resolve);
            }
        };
    }

    // -------------------------------------------------------------------------
    // Stable Worker Gateway (preserves retained dispatch binding across worker restarts)
    // -------------------------------------------------------------------------

    private WorkerGateway startWorkerGateway() throws Exception {
        var gateway = new WorkerGateway();
        closeables.add(gateway);
        return gateway;
    }

    private final class WorkerGateway implements AutoCloseable {
        final HttpServer server;
        final ExecutorService executor;
        final AtomicReference<SyncWorkerHarness> active = new AtomicReference<>();

        WorkerGateway() throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            executor = Executors.newFixedThreadPool(4, runnable -> new Thread(runnable, "sync-worker-gateway"));
            server.setExecutor(executor);
            server.createContext("/pipeline/worker", exchange -> {
                SyncWorkerHarness worker = active.get();
                if (worker == null) {
                    exchange.sendResponseHeaders(503, -1);
                    exchange.close();
                    return;
                }
                worker.handle(exchange);
            });
            server.start();
        }

        void attach(SyncWorkerHarness worker) {
            active.set(worker);
        }

        URI origin() {
            return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        }

        @Override
        public void close() {
            server.stop(0);
            executor.close();
        }
    }

    // -------------------------------------------------------------------------
    // Standalone Registered SYNC REST Worker Harness (no Coordinator, no signing keys)
    // -------------------------------------------------------------------------

    private SyncWorkerHarness startSyncWorker(
        PipelineContractDescriptor contract,
        ScriptedCommandProvider provider,
        CommandEffectStore store,
        Optional<URI> coordinatorLifecycleOrigin,
        boolean deferredAwait
    ) throws Exception {
        var harness = new SyncWorkerHarness(contract, provider, store, coordinatorLifecycleOrigin, deferredAwait);
        closeables.add(harness);
        return harness;
    }

    private final class SyncWorkerHarness implements AutoCloseable {
        final PipelineExecutionService service;
        final CommandStepSupport commandSupport;
        final CommandDescriptor commandDescriptor;
        final RestTransitionWorkerResource resource;
        final ClassLoader workerClassLoader;
        final RestAwaitLifecycleClient lifecycleClient;
        final AtomicInteger executeCalls = new AtomicInteger();
        final AtomicReference<TransitionCommandEnvelope> lastEnvelope = new AtomicReference<>();

        SyncWorkerHarness(
            PipelineContractDescriptor contract,
            ScriptedCommandProvider provider,
            CommandEffectStore store,
            Optional<URI> coordinatorLifecycleOrigin,
            boolean deferredAwait
        ) throws Exception {
            var builder = new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class).withValidateUnknown(false)
                .withDefaultValue("pipeline.orchestrator.mode", "SYNC")
                .withDefaultValue("pipeline.orchestrator.process-loops-disabled", "true")
                .withDefaultValue("pipeline.orchestrator.pipeline-id", PIPELINE_ID)
                .withDefaultValue("pipeline.orchestrator.release-version", RELEASE_VERSION)
                .withDefaultValue("pipeline.orchestrator.worker.artifact-id", ARTIFACT_ID)
                .withDefaultValue("pipeline.orchestrator.worker.artifact-digest", ARTIFACT_DIGEST)
                .withDefaultValue("pipeline.orchestrator.worker.allow-loopback-http", "true")
                .withDefaultValue("pipeline.orchestrator.worker.rest.server-enabled", "true")
                .withDefaultValue("pipeline.orchestrator.worker.rest.shared-secret-ref", SECRET_REF);
            coordinatorLifecycleOrigin.ifPresent(origin -> builder
                .withDefaultValue("pipeline.orchestrator.worker.await-authority.base-url", origin.toASCIIString())
                .withDefaultValue("pipeline.orchestrator.worker.await-authority.worker-id", WORKER_ID)
                .withDefaultValue("pipeline.orchestrator.worker.await-authority.entry-id", ENTRY_ID));
            var config = builder.build().getConfigMapping(PipelineOrchestratorConfig.class);

            var bindings = ConnectorBindingRegistry.fromProviders(List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("orders"), ConnectorProviderId.of("fixture.orders"), 1,
                ConnectorConfigurationDocument.empty())), List.of(provider));
            ACTIVE_PROVIDER_STATE.set(provider.state);
            bindings.start(ConnectorRuntimeContext.empty()).toCompletableFuture().get(5, TimeUnit.SECONDS);

            commandSupport = new CommandStepSupport(new ConnectorRegistry(List.of(provider)), bindings, List.of(store), config);
            var operationId = new ConnectorOperationIdentity(
                ConnectorProviderId.of("fixture.orders"), "submit", ConnectorOperationKind.COMMAND, 1);
            commandDescriptor = CommandDescriptor.nativeCommand(
                "SubmitOrder",
                new NativeCommandSelector(Optional.of(ConnectorBindingName.of("orders")), operationId, 1, CommandPolicy.none()),
                OrderInput.class.getName(),
                OrderOutput.class.getName(),
                "fixture-order-id-gen",
                CommandDuplicatePolicy.RETURN_RECORDED,
                Map.of());

            var identityResolver = mock(PipelineReleaseIdentityResolver.class);
            when(identityResolver.contract()).thenReturn(contract);
            when(identityResolver.compiledContract()).thenReturn(Optional.of(contract));
            when(identityResolver.pipelineId(any())).thenReturn(PIPELINE_ID);
            when(identityResolver.contractVersion()).thenReturn(CONTRACT_VERSION);
            when(identityResolver.releaseVersion(any())).thenReturn(RELEASE_VERSION);
            when(identityResolver.artifactId(any())).thenReturn(ARTIFACT_ID);
            when(identityResolver.artifactDigest(any())).thenReturn(ARTIFACT_DIGEST);
            when(identityResolver.capabilities()).thenReturn(PipelineBundleCapabilities.defaults());
            when(identityResolver.validateCommandIdentity(any(), any())).thenReturn(Optional.empty());

            var wireCodec = new JsonTransitionPayloadCodec();
            lifecycleClient = new RestAwaitLifecycleClient();
            setField(lifecycleClient, "config", config);
            setField(lifecycleClient, "secrets", (ControlPlaneSecretResolver) ref -> SECRET);
            var httpExecutor = Executors.newCachedThreadPool(runnable -> {
                Thread t = new Thread(runnable, "sync-worker-await-http");
                t.setContextClassLoader(RegisteredSyncWorkerCommandAuthorityTest.class.getClassLoader());
                return t;
            });
            closeables.add(httpExecutor::close);
            var awaitReqCtx = mock(org.eclipse.microprofile.context.ThreadContext.class);
            when(awaitReqCtx.currentContextExecutor()).thenReturn(httpExecutor);
            setField(lifecycleClient, "requestContext", awaitReqCtx);
            setField(lifecycleClient, "http", HttpClient.newBuilder().executor(httpExecutor).build());

            RemoteAwaitWorkerLifecycle remoteAwait = null;
            AwaitCoordinator workerAwaitCoordinator = null;
            AwaitValueOperationExecutor valueExecutor = mock(AwaitValueOperationExecutor.class);
            when(valueExecutor.supported()).thenReturn(false);

            List<Object> steps;
            if (deferredAwait) {
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

                var callbackSelection = new ConnectorCallbackSelection(
                    ConnectorBindingName.of("orders"), operationId, 1,
                    provider.operation.callbacks().getFirst(), "fixture.EndpointResolver", "fixture.Authenticator");
                var awaitDescriptor = new AwaitCompletionDescriptor(
                    "SubmitOrder",
                    OrderInput.class.getName(),
                    OrderOutput.class.getName(),
                    "ONE_TO_ONE",
                    Duration.ofMinutes(5),
                    "signedResumeToken",
                    "",
                    Map.of(),
                    List.of(),
                    OrderInput.class.getName(),
                    OrderCallback.class.getName(),
                    Function.identity(),
                    Function.identity(),
                    "fixture.OrderProjector",
                    null,
                    true,
                    Optional.of(callbackSelection),
                    OrderOutput.class.getName(),
                    (req, ack, cb, meta) -> new OrderOutput(((OrderInput) req).orderId() + "/" + ((OrderOutput) ack).receipt() + "/" + ((OrderCallback) cb).status()));

                var registry = new AwaitCompletionDescriptorRegistry();
                registry.register(awaitDescriptor);
                valueExecutor = new AwaitValueOperationExecutor();
                setField(valueExecutor, "identities", identityResolver);
                setField(valueExecutor, "descriptors", registry);
                setField(valueExecutor, "framing", wireCodec);
                setField(valueExecutor, "config", config);

                StepOneToOne<OrderInput, OrderOutput> deferredStep = new OrderStep(input ->
                    deferredSupport.execute(awaitDescriptor, input,
                        req -> URI.create("http://127.0.0.1:8080/"),
                        callback -> commandSupport.execute(commandDescriptor, (d, i) -> "cmd-" + i.orderId(), input, callback)));
                steps = List.of(deferredStep);
            } else {
                StepOneToOne<OrderInput, OrderOutput> ordinaryStep = new OrderStep(input ->
                    commandSupport.execute(commandDescriptor, (d, i) -> "cmd-" + i.orderId(), input));
                steps = List.of(ordinaryStep);
            }

            var workerStepResolver = mock(PipelineStepResolver.class);
            when(workerStepResolver.loadPipelineSteps()).thenReturn(steps);
            var workerOrderer = mock(PipelineStepOrderer.class);
            when(workerOrderer.orderSteps(any())).thenAnswer(inv -> inv.getArgument(0));

            var stepConfig = new SmallRyeConfigBuilder().withMapping(PipelineStepConfig.class)
                .withDefaultValue("pipeline.defaults.retry-limit", "0")
                .build()
                .getConfigMapping(PipelineStepConfig.class);
            var pipelineConfig = new org.pipelineframework.config.PipelineConfig();
            pipelineConfig.defaults().retryLimit(0);
            var telemetry = new org.pipelineframework.telemetry.PipelineTelemetry(stepConfig);
            var runner = new PipelineRunner();
            runner.pipelineConfig = pipelineConfig;
            runner.configFactory = new org.pipelineframework.step.ConfigFactory();
            runner.stepOrderer = workerOrderer;
            runner.parallelismPolicyResolver = new PipelineParallelismPolicyResolver();
            runner.cacheSupportFactory = new PipelineCacheSupportFactory(emptyInstance(), emptyInstance(), "bypass-cache");
            runner.stepExecutor = new PipelineStepExecutor();
            runner.runTelemetry = telemetry;
            runner.stepTelemetry = telemetry;
            var hooks = new ExecutionHooks();

            service = new PipelineExecutionService();
            service.pipelineRunner = runner;
            service.pipelineConfig = pipelineConfig;
            service.pipelineStepConfig = stepConfig;
            service.pipelineStepResolver = workerStepResolver;
            service.stepOrderer = workerOrderer;
            service.configFactory = new org.pipelineframework.step.ConfigFactory();
            service.executionHooks = hooks;
            service.executionInputPolicy = new ExecutionInputPolicy();
            service.orchestratorConfig = config;
            service.commandStepSupport = commandSupport;
            service.remoteAwaitLifecycle = remoteAwait;
            service.awaitCoordinator = workerAwaitCoordinator;
            service.transitionPayloadCodec = wireCodec;
            setField(service, "releaseIdentityResolver", identityResolver);
            setField(service, "startupHealthState",
                new AtomicReference<>(PipelineExecutionService.StartupHealthState.HEALTHY));

            resource = new RestTransitionWorkerResource();
            setField(resource, "orchestratorConfig", config);
            setField(resource, "executionService", service);
            setField(resource, "identityResolver", identityResolver);
            setField(resource, "secretResolver", (ControlPlaneSecretResolver) ref -> SECRET);
            setField(resource, "awaitValueOperations", valueExecutor);
            var reqCtx = mock(org.eclipse.microprofile.context.ThreadContext.class);
            when(reqCtx.currentContextExecutor()).thenReturn(Runnable::run);
            setField(resource, "requestThreadContext", reqCtx);
            invokeNoArg(resource, "validateServerConfig");

            java.nio.file.Path workerMetaDir = java.nio.file.Files.createTempDirectory("tpf-sync-worker-meta-");
            java.nio.file.Path orderFile = workerMetaDir.resolve("META-INF/pipeline/order.json");
            java.nio.file.Files.createDirectories(orderFile.getParent());
            PipelineJson.mapper().writeValue(orderFile.toFile(), Map.of(
                "boundarySchemaVersion", 1,
                "order", List.of(OrderStep.class.getName()),
                "stepBoundaries", List.of(Map.of(
                    "stepClass", OrderStep.class.getName(),
                    "inputTypeId", "OrderInput",
                    "outputTypeId", "OrderOutput"))));
            java.net.URL orderUrl = orderFile.toUri().toURL();
            workerClassLoader = new ClassLoader(RegisteredSyncWorkerCommandAuthorityTest.class.getClassLoader()) {
                @Override
                public Enumeration<java.net.URL> getResources(String name) throws java.io.IOException {
                    if ("META-INF/pipeline/order.json".equals(name)) {
                        return Collections.enumeration(List.of(orderUrl));
                    }
                    if ("META-INF/pipeline/definition-boundaries.json".equals(name)) {
                        return Collections.emptyEnumeration();
                    }
                    return super.getResources(name);
                }

                @Override
                public java.net.URL getResource(String name) {
                    if ("META-INF/pipeline/order.json".equals(name)) {
                        return orderUrl;
                    }
                    if ("META-INF/pipeline/definition-boundaries.json".equals(name)) {
                        return null;
                    }
                    return super.getResource(name);
                }
            };
        }

        void handle(com.sun.net.httpserver.HttpExchange exchange) throws java.io.IOException {
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
                    lastEnvelope.set(PipelineJson.mapper().readValue(body, TransitionCommandEnvelope.class));
                    pending = resource.execute(ts, nonce, sig, body);
                } else if (RestTransitionWorkerProtocol.AWAIT_VALUE_OPERATION_PATH.equals(path)) {
                    pending = resource.executeAwaitValueOperation(ts, nonce, sig, body);
                } else {
                    pending = Uni.createFrom().item(Response.status(404).build());
                }
                try (Response response = pending.await().atMost(TIMEOUT)) {
                    byte[] payload = response.getEntity() == null
                        ? new byte[0]
                        : PipelineJson.mapper().writeValueAsBytes(response.getEntity());
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(response.getStatus(), payload.length);
                    if (payload.length > 0) {
                        exchange.getResponseBody().write(payload);
                    }
                }
            } finally {
                current.setContextClassLoader(previous);
                exchange.close();
            }
        }

        @Override
        public void close() {
            invokeNoArg(lifecycleClient, "close");
        }
    }

    // -------------------------------------------------------------------------
    // Typed Application Fixtures & Conditional-Write DynamoDB Test Double
    // -------------------------------------------------------------------------

    public record OrderInput(String orderId) {}
    public record OrderOutput(String receipt) {}
    public record OrderCallback(String status) {}
    public record OrderConfig() {}

    private static final class OrderStep extends ConfigurableStep implements StepOneToOne<OrderInput, OrderOutput>, CommandStep {
        private final Function<OrderInput, Uni<OrderOutput>> delegate;
        OrderStep(Function<OrderInput, Uni<OrderOutput>> delegate) { this.delegate = delegate; }
        @Override public Uni<OrderOutput> applyOneToOne(OrderInput input) { return delegate.apply(input); }
    }

    private static final class ProviderState {
        final AtomicInteger dispatchCalls = new AtomicInteger();
        final AtomicInteger reconcileCalls = new AtomicInteger();
        final AtomicReference<URI> lastCallbackUri = new AtomicReference<>();
        final AtomicReference<Function<CommandInvocation<OrderInput, OrderConfig>, CommandOutcome<OrderOutput>>> nextOutcome = new AtomicReference<>();
        final Map<String, String> providerReceipts = new ConcurrentHashMap<>();
    }

    public static final class ScriptedCommandProvider implements ConnectorProvider<Void> {
        final ProviderState state;
        final AtomicInteger dispatchCalls;
        final AtomicInteger reconcileCalls;
        final AtomicReference<URI> lastCallbackUri;
        final AtomicReference<Function<CommandInvocation<OrderInput, OrderConfig>, CommandOutcome<OrderOutput>>> nextOutcome;
        final Map<String, String> providerReceipts;
        final OrderCommandOperation operation;

        public ScriptedCommandProvider() {
            this(Optional.ofNullable(ACTIVE_PROVIDER_STATE.get()).orElseGet(ProviderState::new));
        }

        private ScriptedCommandProvider(ProviderState state) {
            this.state = state;
            this.dispatchCalls = state.dispatchCalls;
            this.reconcileCalls = state.reconcileCalls;
            this.lastCallbackUri = state.lastCallbackUri;
            this.nextOutcome = state.nextOutcome;
            this.providerReceipts = state.providerReceipts;
            this.operation = new OrderCommandOperation();
        }

        @Override public ConnectorProviderId id() { return ConnectorProviderId.of("fixture.orders"); }
        @Override public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
        @Override public Collection<? extends ConnectorOperation> operations() { return List.of(operation); }

        public final class OrderCommandOperation implements CommandOperation<OrderInput, OrderConfig, OrderOutput> {
            private final ConnectorOperationCallbackDescriptor callbackDescriptor = new ConnectorOperationCallbackDescriptor(
                "completed", new ConnectorOperationTypeContract(OrderCallback.class.getName(), Optional.empty()), true);

            @Override public String id() { return "submit"; }
            @Override public Optional<ConnectorConfigSchema<OrderConfig>> configurationSchema() {
                return Optional.of(ConnectorConfigSchema.record(OrderConfig.class, "fixture.orders.submit", 1));
            }
            @Override public CommandCapabilities capabilities() {
                return new CommandCapabilities(true, true, true, CommandExecutionPosture.AUTOMATED,
                    CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false, Set.of("receipt"));
            }
            @Override public List<ConnectorOperationCallbackDescriptor> callbacks() { return List.of(callbackDescriptor); }
            @Override public Optional<CommandRecoveryTarget> recoveryTarget(CommandInvocation<OrderInput, OrderConfig> invocation) {
                return Optional.of(new CommandRecoveryTarget("fixture-orders-target",
                    ConnectorConfigurationSnapshot.from(configurationSchema().orElseThrow(), ConnectorConfigurationDocument.empty(), false)));
            }
            @Override public CompletionStage<CommandOutcome<OrderOutput>> dispatch(CommandInvocation<OrderInput, OrderConfig> invocation) {
                int call = dispatchCalls.incrementAndGet();
                invocation.callbackContext().ifPresent(cb -> lastCallbackUri.set(cb.callbackUri()));
                String id = invocation.dispatchIdentity().orElseThrow().commandId();
                String receipt = "dispatched:" + invocation.input().orderId() + "#" + call;
                String referenceId = "receipt-" + invocation.input().orderId() + "-" + call;
                providerReceipts.put(id, receipt);
                var override = nextOutcome.getAndSet(null);
                if (override != null) {
                    return CompletableFuture.completedFuture(override.apply(invocation));
                }
                return CompletableFuture.completedFuture(new CommandOutcome.Succeeded<>(
                    new OrderOutput(receipt),
                    new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false),
                    Set.of(),
                    List.of(new CommandReference("receipt", referenceId, CommandReferencePurpose.RECONCILIATION))));
            }
            @Override public CompletionStage<CommandReconciliationResult<OrderOutput>> reconcile(
                CommandReconciliationInvocation<OrderInput, OrderConfig> invocation
            ) {
                reconcileCalls.incrementAndGet();
                var binding = invocation.binding();
                String receipt = providerReceipts.get(binding.commandId());
                if (receipt == null) {
                    return CompletableFuture.completedFuture(new CommandReconciliationResult.Unresolved<>("not-found"));
                }
                var outcome = new CommandOutcome.Succeeded<>(
                    new OrderOutput(receipt),
                    new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false),
                    Set.of(),
                    List.of(new CommandReference("receipt", "reconciled-" + binding.commandId(), CommandReferencePurpose.RECONCILIATION)));
                return CompletableFuture.completedFuture(new CommandReconciliationResult.ConfirmedSucceeded<>(
                    binding, outcome, outcome.references().getFirst()));
            }
        }
    }

    /** Exact conditional-write, append-only DynamoDB table test double for {@link DynamoCommandEffectStore}. */
    private static final class ConditionalRevisionDynamoTable {
        private final ConcurrentMap<String, NavigableMap<Long, Map<String, AttributeValue>>> rows = new ConcurrentHashMap<>();
        final AtomicInteger conditionalPutCalls = new AtomicInteger();

        DynamoDbClient client() {
            return (DynamoDbClient) Proxy.newProxyInstance(
                DynamoDbClient.class.getClassLoader(),
                new Class<?>[]{DynamoDbClient.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "putItem" -> putItem((PutItemRequest) args[0]);
                    case "query" -> query((QueryRequest) args[0]);
                    case "serviceName" -> "dynamodb";
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException("Unexpected DynamoDbClient call: " + method.getName());
                });
        }

        private PutItemResponse putItem(PutItemRequest request) {
            if (!TABLE.equals(request.tableName())) {
                throw new IllegalArgumentException("Unexpected table " + request.tableName());
            }
            String pk = request.item().get(DynamoCommandEffectStore.COMMAND_KEY).s();
            long rev = Long.parseLong(request.item().get(DynamoCommandEffectStore.REVISION).n());
            String condition = request.conditionExpression();
            Map<String, String> names = request.expressionAttributeNames();
            boolean requiresAbsentRevision = condition != null
                && ((condition.contains("attribute_not_exists(#commandKey)")
                        && DynamoCommandEffectStore.COMMAND_KEY.equals(names.get("#commandKey"))
                        && condition.contains("attribute_not_exists(#revision)")
                        && DynamoCommandEffectStore.REVISION.equals(names.get("#revision")))
                    || (condition.contains("attribute_not_exists(" + DynamoCommandEffectStore.COMMAND_KEY + ")")
                        && condition.contains("attribute_not_exists(" + DynamoCommandEffectStore.REVISION + ")")));
            if (requiresAbsentRevision) {
                conditionalPutCalls.incrementAndGet();
            }
            var revisions = rows.computeIfAbsent(pk, ignored -> new ConcurrentSkipListMap<>());
            synchronized (revisions) {
                if (requiresAbsentRevision && revisions.containsKey(rev)) {
                    throw ConditionalCheckFailedException.builder().message("ConditionalCheckFailed").build();
                }
                revisions.put(rev, Map.copyOf(request.item()));
            }
            return PutItemResponse.builder().build();
        }

        private QueryResponse query(QueryRequest request) {
            if (!TABLE.equals(request.tableName())) {
                throw new IllegalArgumentException("Unexpected table " + request.tableName());
            }
            String pk = request.expressionAttributeValues().get(":commandKey").s();
            var revisions = rows.get(pk);
            if (revisions == null || revisions.isEmpty()) {
                return QueryResponse.builder().items(List.of()).build();
            }
            List<Map<String, AttributeValue>> items;
            synchronized (revisions) {
                items = Boolean.FALSE.equals(request.scanIndexForward())
                    ? List.of(revisions.lastEntry().getValue())
                    : new ArrayList<>(revisions.values());
            }
            return QueryResponse.builder().items(items).build();
        }
    }

    private static PipelineContractDescriptor ordinaryCommandContract() {
        var types = canonicalTypes();
        return new PipelineContractDescriptor(
            3, PIPELINE_ID, CONTRACT_VERSION, "hash-ordinary", "FUNCTION", "REST", "app", false, "PIPELINE_RUNTIME",
            List.of(new PipelineBundleStepDescriptor(0, "SubmitOrder", "command", "ONE_TO_ONE",
                OrderInput.class.getName(), OrderOutput.class.getName(),
                OrderStep.class.getName(), OrderStep.class.getName(), Map.of())),
            PipelineBundleCapabilities.defaults(), types, "catalog-sync-cmd",
            org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor.empty(),
            List.of(), List.of(), List.of(),
            Map.of("schemaVersion", 1, "inputCanonicalTypeId", "OrderInput", "outputCanonicalTypeId", "OrderOutput"));
    }

    private static PipelineContractDescriptor deferredCommandContract(ScriptedCommandProvider provider) {
        var types = canonicalTypes();
        var operationId = new ConnectorOperationIdentity(
            ConnectorProviderId.of("fixture.orders"), "submit", ConnectorOperationKind.COMMAND, 1);
        var callbackSelection = new ConnectorCallbackSelection(
            ConnectorBindingName.of("orders"), operationId, 1,
            provider.operation.callbacks().getFirst(), "fixture.EndpointResolver", "fixture.Authenticator");
        var runtimeDescriptor = new LinkedHashMap<String, Object>();
        runtimeDescriptor.put("schemaVersion", 1);
        runtimeDescriptor.put("stepId", "SubmitOrder");
        runtimeDescriptor.put("inputTypeId", OrderInput.class.getName());
        runtimeDescriptor.put("outputTypeId", OrderOutput.class.getName());
        runtimeDescriptor.put("transportInputTypeId", OrderInput.class.getName());
        runtimeDescriptor.put("transportOutputTypeId", OrderCallback.class.getName());
        runtimeDescriptor.put("canonicalInputTypeId", "OrderInput");
        runtimeDescriptor.put("canonicalOutputTypeId", "OrderOutput");
        runtimeDescriptor.put("completionPayloadTypeId", "OrderCallback");
        runtimeDescriptor.put("requestAwareCompletion", true);
        runtimeDescriptor.put("completionProjectorId", "fixture.OrderProjector");
        runtimeDescriptor.put("acknowledgementAwareCommandCompletion", true);
        runtimeDescriptor.put("commandAcknowledgementTypeId", OrderOutput.class.getName());
        runtimeDescriptor.put("canonicalCommandAcknowledgementTypeId", "OrderOutput");
        runtimeDescriptor.put("callbackContract", callbackSelection.contractMetadata());
        runtimeDescriptor.put("positions", List.of(Map.of("stepIndex", 0)));
        return new PipelineContractDescriptor(
            3, PIPELINE_ID, CONTRACT_VERSION, "hash-deferred", "FUNCTION", "REST", "app", false, "PIPELINE_RUNTIME",
            List.of(new PipelineBundleStepDescriptor(0, "SubmitOrder", "command", "ONE_TO_ONE",
                OrderInput.class.getName(), OrderOutput.class.getName(),
                OrderStep.class.getName(), OrderStep.class.getName(),
                Map.of(
                    "mode", "CONNECTOR_CALLBACK",
                    "commandCompletionMode", "ACKNOWLEDGEMENT_REQUIRED",
                    "correlationStrategy", "signedResumeToken",
                    "timeout", "PT5M",
                    "idempotencyKeyFields", List.of(),
                    "runtimeDescriptor", Map.copyOf(runtimeDescriptor)))),
            PipelineBundleCapabilities.defaults(), types, "catalog-sync-cmd",
            org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor.empty(),
            List.of(), List.of(), List.of(),
            Map.of("schemaVersion", 1, "inputCanonicalTypeId", "OrderInput", "outputCanonicalTypeId", "OrderOutput"));
    }

    private static Map<String, Map<String, Object>> canonicalTypes() {
        return Map.of(
            "OrderInput", typeDef("OrderInput", OrderInput.class.getName(), "orderId"),
            "OrderOutput", typeDef("OrderOutput", OrderOutput.class.getName(), "receipt"),
            "OrderCallback", typeDef("OrderCallback", OrderCallback.class.getName(), "status"));
    }

    private static Map<String, Object> typeDef(String id, String runtimeClass, String fieldName) {
        return Map.of(
            "runtimeClass", runtimeClass,
            "definitionFingerprint", id + "-fp",
            "definition", Map.of(
                "kind", "record",
                "id", id,
                "fields", List.of(Map.of(
                    "name", fieldName,
                    "presence", "REQUIRED",
                    "nullability", "NON_NULL",
                    "type", Map.of("kind", "scalar", "id", "string")))));
    }

    private static TypedDurablePayload canonicalPayload(PipelineContractDescriptor contract, String typeId, Object value) throws Exception {
        String fingerprint = (String) contract.canonicalTypes().get(typeId).get("definitionFingerprint");
        return new TypedDurablePayload(typeId, fingerprint, contract.canonicalCatalogFingerprint(),
            JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
            PipelineJson.mapper().writeValueAsBytes(value));
    }

    private static String canonicalField(Object durableResult, String field) throws Exception {
        Object unwrapped = durableResult;
        if (unwrapped instanceof List<?> items && !items.isEmpty()) {
            unwrapped = items.getFirst();
        }
        var serialized = SerializedTransitionPayload.fromDurableValue(unwrapped);
        if (serialized.isPresent()) {
            unwrapped = new JsonTransitionPayloadCodec().decode(serialized.orElseThrow());
        }
        var canonical = TypedDurablePayload.fromDurableValue(unwrapped);
        if (canonical.isPresent()) {
            return PipelineJson.mapper().readTree(canonical.orElseThrow().payload()).path(field).asText();
        }
        if (unwrapped instanceof Map<?, ?> map && map.containsKey("payload")) {
            var nested = TypedDurablePayload.fromDurableValue(map);
            if (nested.isPresent()) {
                return PipelineJson.mapper().readTree(nested.orElseThrow().payload()).path(field).asText();
            }
        }
        return PipelineJson.mapper().valueToTree(unwrapped).path(field).asText();
    }

    @SuppressWarnings("unchecked")
    private static <T> Instance<T> emptyInstance() {
        Instance<T> instance = mock(Instance.class);
        when(instance.isUnsatisfied()).thenReturn(true);
        when(instance.stream()).thenAnswer(ignored -> Stream.empty());
        when(instance.iterator()).thenAnswer(ignored -> Collections.emptyIterator());
        return instance;
    }

    @SuppressWarnings("unchecked")
    private static <T> Instance<T> instanceOf(T item) {
        Instance<T> instance = mock(Instance.class);
        when(instance.isUnsatisfied()).thenReturn(false);
        when(instance.get()).thenReturn(item);
        when(instance.stream()).thenAnswer(ignored -> Stream.of(item));
        when(instance.iterator()).thenAnswer(ignored -> List.of(item).iterator());
        return instance;
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

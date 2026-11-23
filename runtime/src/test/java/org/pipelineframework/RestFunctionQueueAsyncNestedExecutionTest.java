package org.pipelineframework;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitCompletionDescriptor;
import org.pipelineframework.awaitable.AwaitCompletionSupport;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.command.CommandConnector;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.command.CommandEffectStatus;
import org.pipelineframework.command.CommandIdGenerator;
import org.pipelineframework.command.CommandRequest;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.PipelineInvocationSteps;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.RestTransitionWorkerProtocol;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionWireResult;
import org.pipelineframework.orchestrator.TransitionWorkerCommand;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelinePlacementDescriptor;
import org.pipelineframework.orchestrator.worker.PipelineWorkerCapability;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end verification of a REST/FUNCTION registered transition worker configured with
 * {@code pipeline.orchestrator.mode=QUEUE_ASYNC} and {@code process-loops-disabled=true}:
 * exercises an ordinary {@code kind: command} effect, nested pipeline call, nested Await
 * suspension/resume, and bounded {@code pipeline: self} recursion through the actual signed
 * {@link org.pipelineframework.orchestrator.RestTransitionWorkerResource} HTTP route.
 */
@QuarkusTest
@TestProfile(RestFunctionQueueAsyncNestedExecutionTest.Profile.class)
class RestFunctionQueueAsyncNestedExecutionTest {

    private static final String SECRET = "rf-queue-async-worker-secret";
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @TestHTTPResource(RestTransitionWorkerProtocol.EXECUTE_PATH)
    URI workerExecuteEndpoint;

    @TestHTTPResource(RestTransitionWorkerProtocol.CAPABILITIES_PATH)
    URI workerCapabilitiesEndpoint;

    @Inject
    PipelineRunner runner;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    QueueAsyncSweepLoopHost sweepLoopHost;

    @Inject
    AwaitCompletionSupport awaitSupport;

    @Inject
    PipelineStepResolver stepResolver;

    @Inject
    PipelineReleaseIdentityResolver identityResolver;

    @Test
    void executesOrdinaryCommandNestedAwaitAndSelfRecursionOverSignedRestWorkerInQueueAsyncMode() throws Exception {
        assertEquals(OrchestratorMode.QUEUE_ASYNC, orchestratorConfig.mode());
        assertTrue(orchestratorConfig.processLoopsDisabled(),
            "Registered worker must disable competing coordinator sweep/poll loops");
        assertFalse(sweepLoopHost.started(),
            "QueueAsyncSweepLoopHost must not start competing coordinator sweeps on a registered worker");

        var commandInvocations = new AtomicInteger();
        var commandStore = new InMemoryCommandEffectStore();
        var commandSupport = new CommandStepSupport(
            List.of(new RecordingCommandConnector(commandInvocations)),
            List.of(commandStore),
            orchestratorConfig);
        var commandDescriptor = new CommandDescriptor(
            "RecordInitialCommandStep",
            "test-ordinary-command",
            "java.lang.Integer",
            "java.lang.Integer",
            IntegerCommandIdGenerator.class.getName(),
            CommandDuplicatePolicy.RETURN_RECORDED,
            Map.of());

        var capturedAwaitContext = new AtomicReference<AwaitExecutionContext>();
        var awaitDescriptor = new AwaitCompletionDescriptor(
            "NestedReviewAwait",
            "java.lang.Integer",
            "java.lang.Integer",
            "ONE_TO_ONE",
            Duration.ofMinutes(5),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of(),
            "java.lang.Integer",
            "java.lang.Integer",
            v -> v,
            v -> v,
            "",
            null,
            false);

        StepOneToOne<Integer, Integer> ordinaryCommandStep = new IntStep(value ->
            commandSupport.<Integer, Integer>execute(commandDescriptor, new IntegerCommandIdGenerator(), value));

        StepOneToOne<Integer, Integer> nestedAwaitStep = new IntStep(value -> {
            if (value == 2) {
                capturedAwaitContext.set(AwaitExecutionContextHolder.get());
                capturedAwaitContext.get().invocationPathForAwait();
                return awaitSupport.awaitOneToOne(awaitDescriptor, value);
            }
            return Uni.createFrom().item(value + 1);
        });
        StepOneToOne<Integer, Integer> nestedAfterAwait = new IntStep(value -> Uni.createFrom().item(value + 10));
        List<Object> nestedDefinitionSteps = List.of(nestedAwaitStep, nestedAfterAwait);

        var rootStepsRef = new AtomicReference<List<Object>>();
        class NestedCallStep extends IntStep implements PipelineInvocationSteps.LinkedInvocation {
            private final PipelineInvocationSteps.LinkedDefinition linked;
            NestedCallStep() {
                super(value -> PipelineInvocationSteps.<Integer, Integer>oneToOne(
                    runner, "review-flow", "Run Review", -1, nestedDefinitionSteps).applyOneToOne(value));
                this.linked = new PipelineInvocationSteps.LinkedDefinition(
                    "review-flow", "Run Review", false, -1, nestedDefinitionSteps);
            }
            @Override public PipelineInvocationSteps.LinkedDefinition linkedDefinition() { return linked; }
        }
        class SelfCallStep extends IntStep implements PipelineInvocationSteps.LinkedInvocation {
            SelfCallStep() {
                super(value -> value >= 20 ? Uni.createFrom().item(value)
                    : PipelineInvocationSteps.<Integer, Integer>recursiveOneToOne(
                        runner, "$root", "Continue Review", -1, rootStepsRef.get()).applyOneToOne(value));
            }
            @Override public PipelineInvocationSteps.LinkedDefinition linkedDefinition() {
                return new PipelineInvocationSteps.LinkedDefinition("$root", "Continue Review", true, -1, rootStepsRef.get());
            }
        }
        StepOneToOne<Integer, Integer> finalizeStep = new IntStep(value -> Uni.createFrom().item(value + 100));
        List<Object> rootSteps = List.of(ordinaryCommandStep, new NestedCallStep(), new SelfCallStep(), finalizeStep);
        rootStepsRef.set(rootSteps);

        var mockStepResolver = mock(PipelineStepResolver.class);
        when(mockStepResolver.loadPipelineSteps()).thenReturn(rootSteps);
        QuarkusMock.installMockForInstance(mockStepResolver, stepResolver);

        var contract = restFunctionContract();
        var mockIdentity = mock(PipelineReleaseIdentityResolver.class);
        when(mockIdentity.validateCommandIdentity(any(), isNull())).thenReturn(Optional.empty());
        when(mockIdentity.contract()).thenReturn(contract);
        when(mockIdentity.compiledContract()).thenReturn(Optional.of(contract));
        when(mockIdentity.pipelineId(any())).thenReturn(contract.pipelineId());
        when(mockIdentity.contractVersion()).thenReturn(contract.contractVersion());
        when(mockIdentity.releaseVersion(any())).thenReturn("release-1");
        when(mockIdentity.artifactId(any())).thenReturn("app");
        when(mockIdentity.artifactDigest(any())).thenReturn("sha256:app");
        when(mockIdentity.capabilities()).thenReturn(PipelineBundleCapabilities.defaults());
        QuarkusMock.installMockForInstance(mockIdentity, identityResolver);

        var codec = new JsonTransitionPayloadCodec();
        try (HttpClient client = HttpClient.newHttpClient()) {
            // 1. Capabilities endpoint succeeds on the signed REST transition worker.
            HttpResponse<String> capResp = sendSignedGet(client, workerCapabilitiesEndpoint,
                RestTransitionWorkerProtocol.CAPABILITIES_PATH);
            assertEquals(200, capResp.statusCode(), capResp.body());
            PipelineWorkerCapability capability = PipelineJson.mapper().readValue(
                capResp.body(), PipelineWorkerCapability.class);
            assertEquals("rest", capability.providerName());
            assertEquals("rf-nested-pipeline", capability.pipelineId());

            // 2. First admitted transition: runs ordinary Command (1 -> 2), enters nested review-flow, suspends at nested Await.
            var startCommand = new TransitionWorkerCommand(
                "tenant-1", "exec-rf-1", 0, 0, ExecutionResultShape.SINGLE, 0L, "exec-rf-1:0:0",
                new ExecutionInputSnapshot(ExecutionInputShape.UNI, 1));
            var startEnvelope = TransitionCommandEnvelope.from(
                startCommand, contract.pipelineId(), contract.contractVersion(), "release-1",
                "exec-rf-1:0:0", codec.encode(startCommand.inputPayload()));
            HttpResponse<String> firstResp = sendSignedPost(client, workerExecuteEndpoint,
                RestTransitionWorkerProtocol.EXECUTE_PATH, PipelineJson.mapper().writeValueAsBytes(startEnvelope));
            assertEquals(200, firstResp.statusCode(), firstResp.body());
            TransitionWireResult firstResult = PipelineJson.mapper().readValue(firstResp.body(), TransitionWireResult.class);
            assertEquals(TransitionWorkerOutcome.WAITING_EXTERNAL, firstResult.outcome(), firstResp.body());
            assertNotNull(firstResult.awaitSuspension());
            assertEquals(1, commandInvocations.get(), "Ordinary Command should execute once before nested Await suspension");
            var recorded = commandStore.find("tenant-1", "cmd-1").await().atMost(TIMEOUT).orElseThrow();
            assertEquals(CommandEffectStatus.SUCCEEDED, recorded.status());
            assertNotNull(capturedAwaitContext.get());
            assertTrue(capturedAwaitContext.get().invocationPath().isPresent(),
                "Nested Await suspension must retain its invocation path");

            // 3. Resumed admitted transition: restores nested invocation path with projected completion value 5,
            // completes nested review-flow (+10 -> 15), triggers bounded self-call (command executes cmd-15 -> 16,
            // nested 16->17->27, self-call base case >= 20 -> 27, finalize +100 -> 127, unwind +100 -> 227)
            // WITHOUT replaying the initial command effect for input 1!
            var resumeSnapshot = new ExecutionInputSnapshot(
                ExecutionInputShape.UNI, 5, Optional.empty(), capturedAwaitContext.get().invocationPath());
            var resumeCommand = new TransitionWorkerCommand(
                "tenant-1", "exec-rf-1", 2, 0, ExecutionResultShape.SINGLE, 1L, "exec-rf-1:resume:1",
                resumeSnapshot);
            var resumeEnvelope = TransitionCommandEnvelope.from(
                resumeCommand, contract.pipelineId(), contract.contractVersion(), "release-1",
                "exec-rf-1:resume:1", codec.encode(resumeCommand.inputPayload()));
            HttpResponse<String> secondResp = sendSignedPost(client, workerExecuteEndpoint,
                RestTransitionWorkerProtocol.EXECUTE_PATH, PipelineJson.mapper().writeValueAsBytes(resumeEnvelope));
            assertEquals(200, secondResp.statusCode(), secondResp.body());
            TransitionWireResult secondResult = PipelineJson.mapper().readValue(secondResp.body(), TransitionWireResult.class);
            assertEquals(TransitionWorkerOutcome.COMPLETED, secondResult.outcome(), secondResp.body());
            assertEquals(1, secondResult.outputPayloads().size());
            Object finalOutput = codec.decode(secondResult.outputPayloads().getFirst());
            assertEquals(227, finalOutput);
            assertEquals(2, commandInvocations.get(),
                "Initial Command effect (cmd-1) must not replay on resume; only the recursive turn (cmd-15) executes");
        }
    }

    private static PipelineContractDescriptor restFunctionContract() {
        var placement = new PipelinePlacementDescriptor(
            PipelinePlacementDescriptor.CURRENT_SCHEMA_VERSION,
            List.of(
                new PipelinePlacementDescriptor.InvocationPlacement(
                    "$root", "Run Review", "review-flow", false,
                    PipelinePlacementDescriptor.LOCAL_IN_SEGMENT, "app", "application"),
                new PipelinePlacementDescriptor.InvocationPlacement(
                    "$root", "Continue Review", "$root", true,
                    PipelinePlacementDescriptor.LOCAL_IN_SEGMENT, "app", "application")),
            List.of(
                new PipelinePlacementDescriptor.ExecutionSegment(
                    "segment-root", PipelinePlacementDescriptor.ROOT_ENTRY, "",
                    "app", "application", true, List.of("$root", "review-flow"), List.of("review-flow:NestedReviewAwait")),
                new PipelinePlacementDescriptor.ExecutionSegment(
                    "segment-await-1", PipelinePlacementDescriptor.AWAIT_RESUME, "review-flow:NestedReviewAwait",
                    "app", "application", true, List.of("$root", "review-flow"), List.of("review-flow:NestedReviewAwait"))));
        return new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            "rf-nested-pipeline",
            "contract-rf-1",
            "hash-rf-1",
            "FUNCTION",
            "REST",
            "app",
            true,
            "PIPELINE_RUNTIME",
            List.of(),
            PipelineBundleCapabilities.defaults(),
            Map.of(),
            "",
            org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor.empty(),
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            placement);
    }

    private static HttpResponse<String> sendSignedPost(
        HttpClient client, URI uri, String path, byte[] body
    ) throws Exception {
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(SECRET, "POST", path, timestamp, nonce, body);
        var req = HttpRequest.newBuilder(uri)
            .timeout(TIMEOUT)
            .header("Content-Type", "application/json")
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature)
            .POST(HttpRequest.BodyPublishers.ofByteArray(body))
            .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> sendSignedGet(
        HttpClient client, URI uri, String path
    ) throws Exception {
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(SECRET, "GET", path, timestamp, nonce, new byte[0]);
        var req = HttpRequest.newBuilder(uri)
            .timeout(TIMEOUT)
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature)
            .GET()
            .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static class IntStep extends ConfigurableStep implements StepOneToOne<Integer, Integer> {
        private final java.util.function.Function<Integer, Uni<Integer>> fn;
        IntStep(java.util.function.Function<Integer, Uni<Integer>> fn) { this.fn = fn; }
        @Override public Uni<Integer> applyOneToOne(Integer input) { return fn.apply(input); }
    }

    public static final class IntegerCommandIdGenerator implements CommandIdGenerator<Integer> {
        @Override public String commandId(CommandDescriptor descriptor, Integer input) { return "cmd-" + input; }
    }

    private static final class RecordingCommandConnector implements CommandConnector<Integer, Integer> {
        private final AtomicInteger calls;
        RecordingCommandConnector(AtomicInteger calls) { this.calls = calls; }
        @Override public String command() { return "test-ordinary-command"; }
        @Override public Uni<Integer> execute(CommandRequest<Integer> request) {
            calls.incrementAndGet();
            return Uni.createFrom().item(request.input() + 1);
        }
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.devservices.enabled", "false",
                "quarkus.otel.sdk.disabled", "true",
                "pipeline.orchestrator.mode", "QUEUE_ASYNC",
                "pipeline.orchestrator.idempotency-policy", "client-key-required",
                "pipeline.orchestrator.process-loops-disabled", "true",
                "pipeline.orchestrator.worker.rest.server-enabled", "true",
                "pipeline.orchestrator.worker.rest.shared-secret", SECRET,
                "pipeline.max-recursive-depth", "3");
        }
    }
}

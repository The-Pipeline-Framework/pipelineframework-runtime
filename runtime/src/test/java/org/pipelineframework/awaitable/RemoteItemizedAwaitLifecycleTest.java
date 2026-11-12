package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.*;
import jakarta.enterprise.inject.Instance;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.AwaitTransitionAuthority;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.awaitable.store.*;
import org.pipelineframework.config.*;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** Real worker interception and authoritative stores; only the authenticated transport boundary is replaced. */
class RemoteItemizedAwaitLifecycleTest {
    record Value(String value) {}

    @Test void earlyItemCompletionsSealAndResumeInOrderWithoutWorkerStores() throws Exception {
        var config = mock(PipelineOrchestratorConfig.class, RETURNS_DEEP_STUBS);
        when(config.mode()).thenReturn(OrchestratorMode.SYNC);
        when(config.controlPlane().payloadMaterialization()).thenReturn(PipelineOrchestratorConfig.ControlPlaneConfig.PayloadMaterialization.SERIALIZED);
        when(config.worker().awaitAuthority().baseUrl()).thenReturn(Optional.of("http://coordinator"));
        when(config.worker().awaitAuthority().workerId()).thenReturn(Optional.of("worker"));
        when(config.worker().awaitAuthority().entryId()).thenReturn(Optional.of("entry"));
        when(config.workerRest().sharedSecretRef()).thenReturn(Optional.of("sys:test.secret"));
        var metadata = new HashMap<String,Object>();
        metadata.put("schemaVersion", 1); metadata.put("stepId", "Review");
        for (String field : List.of("inputTypeId", "outputTypeId", "transportInputTypeId", "transportOutputTypeId")) metadata.put(field, Value.class.getName());
        for (String field : List.of("canonicalInputTypeId", "canonicalOutputTypeId", "completionPayloadTypeId")) metadata.put(field, "Value");
        metadata.put("requestAwareCompletion", false);
        var definition = Map.<String,Object>of("runtimeClass", Value.class.getName(), "definitionFingerprint", "value-fingerprint", "definition",
            Map.of("kind", "record", "id", "Value", "fields", List.of(Map.of("name", "value", "presence", "REQUIRED", "nullability", "NON_NULL", "type", Map.of("kind", "scalar", "id", "string")))));
        var contract = new PipelineContractDescriptor(2, "pipeline", "contract", "hash", null, null, null, false, null,
            List.of(new PipelineBundleStepDescriptor(0, "Review", "internal", "ONE_TO_ONE", Value.class.getName(), Value.class.getName(), null, null,
                Map.of("runtimeDescriptor", metadata, "timeout", "PT5M", "idempotencyKeyFields", List.of(), "correlationStrategy", "interactionId", "transportType", "interaction-api"))), PipelineBundleCapabilities.defaults(), Map.of("Value", definition), "catalog");
        var release = mock(PipelineReleaseRecord.class);
        when(release.tenantId()).thenReturn("tenant"); when(release.pipelineId()).thenReturn("pipeline");
        when(release.contractVersion()).thenReturn("contract"); when(release.releaseVersion()).thenReturn("release"); when(release.contract()).thenReturn(contract);
        var releases = mock(PipelineReleaseRegistry.class);
        when(releases.get("tenant", "pipeline", "release")).thenReturn(Uni.createFrom().item(Optional.of(release)));
        @SuppressWarnings("unchecked") ExecutionRecord<Object,Object> execution = mock(ExecutionRecord.class);
        when(execution.tenantId()).thenReturn("tenant"); when(execution.executionId()).thenReturn("execution");
        when(execution.pipelineId()).thenReturn("pipeline"); when(execution.contractVersion()).thenReturn("contract"); when(execution.releaseVersion()).thenReturn("release");
        when(execution.version()).thenReturn(1L); when(execution.attempt()).thenReturn(1); when(execution.status()).thenReturn(ExecutionStatus.RUNNING);
        when(execution.leaseOwner()).thenReturn("owner"); when(execution.leaseExpiresEpochMs()).thenReturn(Long.MAX_VALUE);
        when(execution.pagingState()).thenReturn(Optional.empty()); when(execution.awaitOccurrenceCause()).thenReturn(Optional.of("occurrence"));
        var executions = mock(ExecutionStateStore.class);
        when(executions.getExecution("tenant", "execution")).thenReturn(Uni.createFrom().item(Optional.of(execution)));
        var interactions = new InMemoryAwaitInteractionStore(); var units = new InMemoryAwaitUnitStore();
        var framing = new JsonTransitionPayloadCodec();
        var resolver = new AwaitDurablePayloadResolver(); resolver.orchestratorConfig = config; resolver.executionStateStore = executions; resolver.releaseRegistry = releases;
        var server = new AwaitCoordinator(); server.interactionStores = instances(interactions); server.unitStores = instances(units);
        server.orchestratorConfig = config; server.durablePayloadResolver = resolver; server.transitionPayloadCodec = framing;
        server.resumeTokenService = new AwaitResumeTokenService("only-coordinator-signs");
        server.registeredWorkers = mock(RegisteredRestWorkerTargets.class);
        var typedCodec = new CompiledCanonicalTransitionPayloadCodec(framing, contract, "Value", "Value");
        when(server.registeredWorkers.executeAwaitValueOperation(any())).thenAnswer(call -> {
            var request = call.getArgument(0, AwaitValueOperationRequest.class);
            return Uni.createFrom().item(new AwaitValueOperationResult(request.operation(), request.operationId(), request.requestDigest(),
                typedCodec.encode(new Value(((Value) typedCodec.decode(request.requestPayload().orElseThrow())).value() + "-approved"))));
        });
        var authority = new AwaitTransitionAuthority();
        inject(authority, "executionStore", executions); inject(authority, "interactionStore", interactions);
        inject(authority, "releases", releases); inject(authority, "coordinator", server);
        var client = mock(RestAwaitLifecycleClient.class);
        when(client.register(any(), anyString(), anyString(), anyString(), any())).thenAnswer(call -> authority.registerForWorker(call.getArgument(4)));
        when(client.claimDispatch(any(), anyString(), anyString(), anyString(), any(), anyString(), anyLong())).thenAnswer(call -> authority.claimDispatch(call.getArgument(4), call.getArgument(5), call.getArgument(6)));
        when(client.report(any(), anyString(), any())).thenAnswer(call -> authority.report(call.getArgument(2)));
        when(client.sealUnit(any(), anyString(), any())).thenAnswer(call -> authority.sealUnit(call.getArgument(2)));
        when(client.resume(any(), anyString(), any())).thenAnswer(call -> authority.resume(call.getArgument(2)));
        var remote = new RemoteAwaitWorkerLifecycle(); remote.config = config; remote.framing = framing; remote.client = client;
        remote.identity = mock(PipelineReleaseIdentityResolver.class);
        when(remote.identity.compiledContract()).thenReturn(Optional.of(contract)); when(remote.identity.pipelineId(config)).thenReturn("pipeline");
        when(remote.identity.contractVersion()).thenReturn("contract"); when(remote.identity.releaseVersion(config)).thenReturn("release");
        var worker = new AwaitCoordinator(); worker.remoteLifecycle = remote;
        var dispatched = new ArrayList<String>();
        AwaitTransportAdapter<Object> adapter = new AwaitTransportAdapter<>() {
            public String type() { return "interaction-api"; }
            public Uni<AwaitDispatchResult> dispatch(AwaitDispatchRequest<Object> request) {
                var record = request.interaction();
                assertEquals(AwaitInteractionStatus.DISPATCHING, interactions.get("tenant", record.interactionId()).await().indefinitely().orElseThrow().status());
                Value input = new Value((String) ((Map<?,?>) request.payload()).get("value")); dispatched.add(input.value());
                Object canonical = framing.decode(typedCodec.encode(new Value(input.value() + "-approved")));
                return server.complete(new AwaitCompletionCommand("tenant", record.interactionId(), null, "reply", canonical, "actor", System.currentTimeMillis()))
                    .replaceWith(new AwaitDispatchResult(Map.of()));
            }
        };
        worker.adapters = instances(adapter);
        var support = new AwaitCompletionSupport(); support.orchestratorConfig = config; support.awaitCoordinator = worker;
        support.pipelineConfig = mock(PipelineConfig.class); when(support.pipelineConfig.parallelism()).thenReturn(ParallelismPolicy.SEQUENTIAL);
        var scope = new AwaitTransitionScope("tenant", "execution", "pipeline", "contract", "release", 1, "execution:0:1");
        var context = new AwaitExecutionContext("tenant", "execution", 8, AwaitContinuationMode.LIVE_IF_SUPPORTED,
            TerminalOutputOwnership.TRANSITION_WORKER, Map.of(), Optional.empty(), Optional.of("occurrence"), Optional.empty(), Optional.of(scope));
        var descriptor = new AwaitCompletionDescriptor("Review", Value.class.getName(), Value.class.getName(), Duration.ofMinutes(5), "interactionId", "interaction-api", Map.of(), List.of());
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 8));
        assertThrows(IllegalStateException.class, () -> support.awaitOneToOne(descriptor, new Value("denied")).await().indefinitely());
        verifyNoInteractions(client);
        AwaitExecutionContextHolder.set(context);
        try {
            List<Value> result = support.<Value,Value>awaitOneToOneStream(descriptor, Multi.createFrom().items(new Value("first"), new Value("second")))
                .collect().asList().await().indefinitely();
            assertEquals(List.of(new Value("first-approved"), new Value("second-approved")), result);
            assertEquals(List.of("first", "second"), dispatched);
            String unitId = new AwaitUnitOccurrence("tenant", "execution", "execution", "Review", 8, "occurrence").unitId();
            var unit = units.get("tenant", unitId).await().indefinitely().orElseThrow();
            assertTrue(unit.dispatchComplete()); assertEquals(AwaitUnitStatus.COMPLETED, unit.status());
            verify(client, times(2)).register(any(), anyString(), anyString(), anyString(), any());
            verify(client).sealUnit(any(), anyString(), any()); verify(client).resume(any(), anyString(), any());
            assertNull(worker.interactionStores); assertNull(worker.unitStores);
        } finally { AwaitExecutionContextHolder.clear(); }
    }
    @SuppressWarnings("unchecked") private static <T> Instance<T> instances(T value) {
        Instance<T> result = mock(Instance.class); when(result.isResolvable()).thenReturn(true); when(result.get()).thenReturn(value);
        when(result.iterator()).thenAnswer(call -> List.of(value).iterator()); when(result.stream()).thenAnswer(call -> java.util.stream.Stream.of(value)); return result;
    }
    private static void inject(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name); field.setAccessible(true); field.set(target, value);
    }
}

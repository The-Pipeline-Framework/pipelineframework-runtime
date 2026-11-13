package org.pipelineframework;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AwaitTransitionAuthorityTest {
    @Test void leasedWorkerCannotRegisterAtAnUncompiledPosition() {
        var f = new Fixture();
        var forged = f.request(f.scope(), f.payload("{\"approved\":true}"), 9);
        assertThrows(IllegalArgumentException.class, () -> f.authority.register(forged).await().indefinitely());
    }

    @Test void rejectsStaleForeignAndExpiredClaimsBeforeAnyRegistration() {
        var f = new Fixture();
        for (var scope : List.of(new AwaitTransitionScope("foreign", "execution", "pipeline", "contract", "release", 1, "execution:0:1"),
            new AwaitTransitionScope("tenant", "execution", "pipeline", "contract", "other-release", 1, "execution:0:1"),
            new AwaitTransitionScope("tenant", "execution", "pipeline", "contract", "release", 0, "execution:0:1"),
            new AwaitTransitionScope("tenant", "execution", "pipeline", "contract", "release", 1, "forged"))) {
            assertThrows(IllegalStateException.class, () -> f.authority.register(f.request(scope, f.payload("{\"approved\":true}"), 8)).await().indefinitely());
        }
        when(f.execution.leaseExpiresEpochMs()).thenReturn(10_000L);
        assertThrows(IllegalStateException.class, () -> f.authority.register(f.request()).await().indefinitely());
        verifyNoInteractions(f.authority.coordinator, f.authority.releases);
    }

    @Test void validatesSchemaAndOccurrenceBeforeRegistrationWithoutLoadingApplicationTypes() {
        var f = new Fixture();
        for (String json : List.of("{\"approved\":\"wrong\"}", "{}", "{\"approved\":true} {}", "{\"approved\":true,\"approved\":false}")) {
            assertThrows(IllegalArgumentException.class, () -> f.authority.register(f.request(f.scope(), f.payload(json), 8)).await().indefinitely());
        }
        var request = f.request();
        var foreignUnit = new AwaitRegistrationRequest(request.scope(), request.stepId(), request.stepIndex(), request.identityScope(),
            "foreign-unit", request.causationId(), request.itemIndex(), request.canonicalRequest(), request.assignee(), request.group(), request.invocationPath());
        assertThrows(IllegalArgumentException.class, () -> f.authority.register(foreignUnit).await().indefinitely());
        verifyNoInteractions(f.authority.coordinator);
    }

    @Test void acceptsCompiledRuntimeIndexBeyondSemanticCountAndPreservesRetainedRequestOnDuplicate() {
        var f = new Fixture();
        var first = f.authority.register(f.request()).await().indefinitely();
        assertEquals(8, first.record().stepIndex()); // The one semantic descriptor is not the generated execution index.
        assertTrue(f.authority.register(f.request()).await().indefinitely().duplicate());
        assertThrows(IllegalStateException.class, () -> f.authority.register(f.request(f.scope(), f.payload("{\"approved\":false}"), 8)).await().indefinitely());
        var stored = f.store.get("tenant", first.record().interactionId()).await().indefinitely().orElseThrow();
        assertEquals("{\"approved\":true}", new String(((TypedDurablePayload) stored.requestPayload()).payload(), StandardCharsets.UTF_8));
    }

    @Test void grantsOnlyOneFirstDispatchAndDoesNotTreatLostReplyAsRedispatch() {
        var f = new Fixture();
        var record = f.authority.register(f.request()).await().indefinitely().record();
        var simultaneousReads = new java.util.concurrent.CountDownLatch(2);
        var racingStore = spy(f.store);
        doAnswer(call -> {
            var snapshot = f.store.getMetadata("tenant", record.interactionId()).await().indefinitely();
            simultaneousReads.countDown();
            assertTrue(simultaneousReads.await(5, java.util.concurrent.TimeUnit.SECONDS));
            return Uni.createFrom().item(snapshot);
        }).when(racingStore).getMetadata("tenant", record.interactionId());
        f.authority.interactionStore = racingStore;
        ClassLoader testLoader = Thread.currentThread().getContextClassLoader();
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2, task -> new Thread(() -> {
            ClassLoader previous = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(testLoader);
            try { task.run(); }
            finally { Thread.currentThread().setContextClassLoader(previous); }
        }, "await-dispatch-race"))) {
            var attempts = java.util.stream.IntStream.range(0, 2).mapToObj(i -> CompletableFuture.supplyAsync(() -> {
                try { f.authority.claimDispatch(f.scope(), record.interactionId(), record.version()).await().indefinitely(); return true; }
                catch (IllegalStateException conflict) { return false; }
            }, executor)).toList();
            assertEquals(1, attempts.stream().filter(CompletableFuture::join).count());
        }
        var dispatching = f.store.get("tenant", record.interactionId()).await().indefinitely().orElseThrow();
        assertEquals(AwaitInteractionStatus.DISPATCHING, dispatching.status());
        assertThrows(IllegalStateException.class,
            () -> f.authority.claimDispatch(f.scope(), dispatching.interactionId(), dispatching.version()).await().indefinitely());
    }

    @Test void earlyCompletionAndExpiredDeadlineNeverGrantDispatch() {
        var f = new Fixture();
        var record = f.authority.register(f.request()).await().indefinitely().record();
        var completed = f.store.complete(new AwaitCompletionCommand("tenant", record.interactionId(), null,
            "completion", "approved", "actor", 11_000L)).await().indefinitely().record();
        assertThrows(IllegalStateException.class,
            () -> f.authority.claimDispatch(f.scope(), completed.interactionId(), completed.version()).await().indefinitely());
        var expired = new Fixture();
        var waiting = expired.authority.register(expired.request()).await().indefinitely().record();
        expired.authority.clock = Clock.fixed(Instant.ofEpochMilli(60_000), ZoneOffset.UTC);
        when(expired.execution.leaseExpiresEpochMs()).thenReturn(70_000L);
        assertThrows(IllegalStateException.class,
            () -> expired.authority.claimDispatch(expired.scope(), waiting.interactionId(), waiting.version()).await().indefinitely());
        assertEquals(AwaitInteractionStatus.WAITING, expired.store.get("tenant", waiting.interactionId()).await().indefinitely().orElseThrow().status());
    }

    @Test void rejectsForgedNestedInvocationIdentityBeforeRegistration() {
        var f = new Fixture();
        var path = org.pipelineframework.invocation.PipelineInvocationContext.root(3).atStep(8)
            .enterRecursive("nested", "Call").atStep(2).path();
        var original = f.request();
        var forged = new AwaitRegistrationRequest(original.scope(), original.stepId(), original.stepIndex(), original.identityScope(),
            original.unitId(), original.causationId(), Optional.empty(), original.canonicalRequest(), Optional.empty(), Optional.empty(),
            Optional.of(path));
        assertThrows(IllegalArgumentException.class, () -> f.authority.register(forged).await().indefinitely());
        verifyNoInteractions(f.authority.coordinator);
    }

    @Test void admittedRegistrationMayOverlapParentReclaimButStaleEntryMayNot() {
        var f = new Fixture();
        var reclaimed = mock(ExecutionRecord.class, org.mockito.AdditionalAnswers.delegatesTo(f.execution));
        when(reclaimed.version()).thenReturn(2L);
        when(f.authority.releases.get("tenant", "pipeline", "release")).thenAnswer(call -> {
            f.current.set(reclaimed); // Reclaim after the original live-parent authorization read.
            return Uni.createFrom().item(Optional.of(f.release));
        });
        assertFalse(f.authority.register(f.request()).await().indefinitely().duplicate());
        clearInvocations(f.authority.coordinator);
        assertThrows(IllegalStateException.class, () -> f.authority.register(f.request()).await().indefinitely());
        verifyNoInteractions(f.authority.coordinator);
    }

    @Test void retainedGrantAcceptsEarlyCompletionReportAfterParentAdvanceButRejectsForgedGrant() {
        var f = new Fixture();
        var registered = f.authority.register(f.request()).await().indefinitely().record();
        var grant = f.authority.claimDispatch(f.scope(), registered.interactionId(), registered.version()).await().indefinitely();
        assertEquals(grant.version(), ((Number) grant.transportMetadata().get("tpf.await.dispatchGrantVersion")).longValue());
        var completed = f.store.complete(new AwaitCompletionCommand("tenant", grant.interactionId(), null,
            "completion", "approved", "actor", 11_000L)).await().indefinitely().record();
        when(f.execution.version()).thenReturn(9L); when(f.execution.currentStepIndex()).thenReturn(99);
        when(f.execution.status()).thenReturn(ExecutionStatus.SUCCEEDED);
        when(f.authority.coordinator.applyAuthoritativeDispatchReport(any(), any())).thenAnswer(call ->
            Uni.createFrom().item(new AwaitDispatchReportResult(call.getArgument(0), false)));
        var report = new AwaitDispatchReportRequest("worker", "entry", f.scope(), grant.interactionId(), grant.version(),
            AwaitDispatchReportKind.DISPATCHED, Map.of(), Optional.empty(), Optional.empty(), Optional.empty());
        assertEquals(completed, f.authority.report(report).await().indefinitely().record());
        clearInvocations(f.authority.coordinator);
        var forged = new AwaitDispatchReportRequest("worker", "entry", f.scope(), grant.interactionId(), grant.version() + 1,
            AwaitDispatchReportKind.DISPATCHED, Map.of(), Optional.empty(), Optional.empty(), Optional.empty());
        assertThrows(IllegalStateException.class, () -> f.authority.report(forged).await().indefinitely());
        verifyNoInteractions(f.authority.coordinator);
        assertThrows(IllegalArgumentException.class, () -> new AwaitDispatchReportRequest("worker", "entry", f.scope(),
            grant.interactionId(), grant.version(), AwaitDispatchReportKind.DISPATCHED,
            Map.of("tpf.await.dispatchGrantVersion", 999), Optional.empty(), Optional.empty(), Optional.empty()));
    }

    @Test void sealRejectsCountsAndIndexesThatDifferFromAuthoritativeAdmissions() {
        var f = new Fixture();
        var scalar = f.request();
        var item = new AwaitRegistrationRequest(scalar.scope(), scalar.stepId(), scalar.stepIndex(), scalar.identityScope(),
            scalar.unitId(), scalar.causationId() + ":0", Optional.of(0), scalar.canonicalRequest(),
            Optional.empty(), Optional.empty(), Optional.empty());
        f.authority.register(item).await().indefinitely();
        var unitStore = new org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore();
        var unit = unitStore.createOrGet(new AwaitUnitCreateCommand("tenant", item.unitId(), "execution", item.stepId(),
            item.stepIndex(), "ONE_TO_ONE", 10_000, Long.MAX_VALUE)).await().indefinitely();
        when(f.authority.coordinator.getUnit("tenant", item.unitId())).thenReturn(Uni.createFrom().item(unit));
        when(f.authority.coordinator.findByUnit("tenant", item.unitId())).thenAnswer(call -> f.store.findByUnit("tenant", item.unitId()));
        var request = new AwaitWorkerUnitRequest("worker", "entry", f.scope(), item.stepId(), item.stepIndex(), item.identityScope(),
            item.unitId(), Optional.empty());
        for (int count : List.of(0, 2)) assertThrows(IllegalStateException.class,
            () -> f.authority.sealUnit(new AwaitUnitSealRequest(request, count)).await().indefinitely());
        verify(f.authority.coordinator, never()).markDispatchComplete(anyString(), anyString(), anyInt(), anyLong());
        var wrongScope = new AwaitWorkerUnitRequest("worker", "entry", f.scope(), item.stepId(), item.stepIndex(), "foreign-occurrence",
            item.unitId(), Optional.empty());
        assertThrows(IllegalStateException.class, () -> f.authority.sealUnit(new AwaitUnitSealRequest(wrongScope, 1)).await().indefinitely());
        assertFalse(unit.dispatchComplete());
    }

    private static final class Fixture {
        final AwaitTransitionAuthority authority = new AwaitTransitionAuthority();
        final InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        final ExecutionRecord<Object,Object> execution = mock(ExecutionRecord.class);
        final AtomicReference<ExecutionRecord<Object,Object>> current = new AtomicReference<>(execution);
        final PipelineReleaseRecord release = mock(PipelineReleaseRecord.class);
        Fixture() {
            authority.clock = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);
            authority.executionStore = mock(ExecutionStateStore.class);
            authority.interactionStore = store;
            authority.coordinator = mock(AwaitCoordinator.class);
            authority.releases = mock(PipelineReleaseRegistry.class);
            when(execution.tenantId()).thenReturn("tenant"); when(execution.executionId()).thenReturn("execution");
            when(execution.pipelineId()).thenReturn("pipeline"); when(execution.contractVersion()).thenReturn("contract");
            when(execution.releaseVersion()).thenReturn("release"); when(execution.version()).thenReturn(1L);
            when(execution.status()).thenReturn(ExecutionStatus.RUNNING); when(execution.leaseOwner()).thenReturn("worker");
            when(execution.leaseExpiresEpochMs()).thenReturn(20_000L); when(execution.currentStepIndex()).thenReturn(0);
            when(execution.attempt()).thenReturn(1); when(execution.pagingState()).thenReturn(Optional.empty());
            when(execution.awaitOccurrenceCause()).thenReturn(Optional.of("occurrence"));
            when(authority.executionStore.getExecution(anyString(), anyString())).thenAnswer(call -> Uni.createFrom().item(Optional.of(current.get())));
            when(release.tenantId()).thenReturn("tenant"); when(release.pipelineId()).thenReturn("pipeline");
            when(release.contractVersion()).thenReturn("contract"); when(release.releaseVersion()).thenReturn("release");
            var contract = mock(PipelineContractDescriptor.class);
            var step = mock(PipelineBundleStepDescriptor.class);
            var descriptor = new java.util.HashMap<String,Object>();
            descriptor.put("schemaVersion", 1); descriptor.put("stepId", "AwaitReview");
            descriptor.put("positions", List.of(Map.of("stepIndex", 8)));
            for (String key : List.of("inputTypeId", "outputTypeId", "transportInputTypeId", "transportOutputTypeId", "canonicalInputTypeId", "canonicalOutputTypeId", "completionPayloadTypeId")) descriptor.put(key, "Request");
            descriptor.put("requestAwareCompletion", false);
            when(step.deferredCompletion()).thenReturn(Map.of("runtimeDescriptor", descriptor, "timeout", "PT5M"));
            when(step.cardinality()).thenReturn("ONE_TO_ONE");
            when(contract.steps()).thenReturn(List.of(step)); when(contract.canonicalCatalogFingerprint()).thenReturn("catalog");
            when(contract.canonicalTypes()).thenReturn(Map.of("Request", Map.of("definitionFingerprint", "definition", "runtimeClass", "absent.application.Request",
                "definition", Map.of("kind", "record", "id", "Request", "fields", List.of(Map.of("name", "approved", "presence", "REQUIRED", "nullability", "NON_NULL", "type", Map.of("id", "bool", "kind", "scalar")))))));
            when(release.contract()).thenReturn(contract);
            when(authority.releases.get("tenant", "pipeline", "release")).thenReturn(Uni.createFrom().item(Optional.of(release)));
            when(authority.coordinator.createAuthoritativeRegistration(any(), any(), any(), any())).thenAnswer(call -> {
                AwaitRegistrationRequest request = call.getArgument(0);
                TypedDurablePayload payload = call.getArgument(2);
                return store.createOrGet(new AwaitCreateCommand("tenant", "execution", request.stepId(), request.stepIndex(), "Request", "Request",
                    request.causationId(), request.unitId(), request.unitId(), payload, null, null, "interaction-api", request.unitId(),
                    request.itemIndex().orElse(null), Map.of(), 10_000L, 60_000L, Long.MAX_VALUE));
            });
        }
        AwaitTransitionScope scope() { return new AwaitTransitionScope("tenant", "execution", "pipeline", "contract", "release", 1, "execution:0:1"); }
        SerializedTransitionPayload payload(String json) {
            return new JsonTransitionPayloadCodec().encode(new TypedDurablePayload("Request", "definition", "catalog",
                JsonDurablePayloadCodec.ENCODING, 1, json.getBytes(StandardCharsets.UTF_8)));
        }
        AwaitRegistrationRequest request() { return request(scope(), payload("{\"approved\":true}"), 8); }
        AwaitRegistrationRequest request(AwaitTransitionScope scope, SerializedTransitionPayload payload, int index) {
            String unit = new AwaitUnitOccurrence("tenant", "execution", "execution", "AwaitReview", index, "occurrence").unitId();
            return new AwaitRegistrationRequest(scope, "AwaitReview", index, "execution", unit, "execution:" + index,
                Optional.empty(), payload, Optional.empty(), Optional.empty(), Optional.empty());
        }
    }
}

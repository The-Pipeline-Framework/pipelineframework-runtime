package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.store.AwaitItemContinuationWorkStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

/** Public-action convergence contract, also inherited by the fresh Dynamo-store suite. */
class AwaitItemContinuationRecoveryTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    protected record Stores(ExecutionStateStore executions, AwaitInteractionStore interactions, AwaitUnitStore units) {}

    protected Supplier<Stores> stores() {
        Stores shared = new Stores(new InMemoryExecutionStateStore(),
            new InMemoryAwaitInteractionStore(), new InMemoryAwaitUnitStore());
        return () -> shared;
    }

    @TestFactory
    Stream<DynamicTest> admittedFactsConvergeBeforeWorkRetirement() {
        return Stream.of(CrashCut.INTERACTION_ADMITTED, CrashCut.CHILD_COMPLETED, CrashCut.CONTINUATION_RECORDED)
            .flatMap(cut -> Stream.of(false, true).map(concurrent -> DynamicTest.dynamicTest(
                cut + (concurrent ? " concurrent replay" : " sequential replay"),
                () -> recover(cut, concurrent))));
    }

    private void recover(CrashCut cut, boolean concurrent) throws Exception {
        Supplier<Stores> stores = stores();
        Seed seed = seed(stores.get());
        if (cut != CrashCut.INTERACTION_ADMITTED) {
            persistChild(stores.get(), seed);
        }
        if (cut == CrashCut.CONTINUATION_RECORDED) {
            Stores original = stores.get();
            original.units().recordItemCompleted(seed.tenant(), seed.unitId(), "item:0", seed.now()).await().atMost(TIMEOUT);
            original.units().recordItemContinuationCompleted(seed.tenant(), seed.unitId(),
                AwaitUnitRecord.continuationCompletionKey(0), seed.now()).await().atMost(TIMEOUT);
        }
        AtomicInteger workerCalls = new AtomicInteger();
        Optional<CyclicBarrier> barrier = concurrent && cut == CrashCut.INTERACTION_ADMITTED
            ? Optional.of(new CyclicBarrier(2)) : Optional.empty();
        Runtime first = runtime(stores.get(), workerCalls, barrier, 0);
        Runtime second = runtime(stores.get(), workerCalls, barrier, 0);
        if (concurrent) {
            try (var callers = Executors.newFixedThreadPool(2)) {
                CyclicBarrier start = new CyclicBarrier(2);
                var a = callers.submit(() -> { start.await(10, TimeUnit.SECONDS); return first.invoke(seed.command()); });
                var b = callers.submit(() -> { start.await(10, TimeUnit.SECONDS); return second.invoke(seed.command()); });
                AwaitItemContinuationResult firstResult = a.get(20, TimeUnit.SECONDS);
                AwaitItemContinuationResult secondResult = b.get(20, TimeUnit.SECONDS);
                for (AwaitItemContinuationResult result : List.of(firstResult, secondResult)) {
                    assertTrue(result.successful()
                            || result.disposition() == AwaitItemContinuationDisposition.RETRY
                            || result.disposition() == AwaitItemContinuationDisposition.NOT_READY,
                        () -> "concurrent replay returned a terminal failure: " + result);
                }
            }
            for (AwaitItemContinuationCommand pending : ((AwaitItemContinuationWorkStore) stores.get().interactions())
                    .findDueItemContinuations(seed.now() + 60_000, 10).await().atMost(TIMEOUT)) {
                assertTrue(runtime(stores.get(), workerCalls, Optional.empty(), 0).invoke(pending).successful(),
                    "a racing replay must converge when its durable retry is due");
            }
        } else {
            assertTrue(first.invoke(seed.command()).successful());
            assertEquals(AwaitItemContinuationDisposition.ALREADY_COMPLETED, second.invoke(seed.command()).disposition());
        }
        Stores restored = stores.get();
        AwaitUnitRecord unit = restored.units().get(seed.tenant(), seed.unitId()).await().atMost(TIMEOUT).orElseThrow();
        ExecutionRecord<Object, Object> parent = restored.executions().getExecution(
            seed.tenant(), seed.parent().executionId()).await().atMost(TIMEOUT).orElseThrow();
        ExecutionRecord<Object, Object> child = restored.executions().getExecutionByKey(
            seed.tenant(), seed.childKey()).await().atMost(TIMEOUT).orElseThrow();
        assertEquals(1, unit.completedItemCount());
        assertEquals(1, unit.completedContinuationItemCount());
        assertEquals(ExecutionStatus.SUCCEEDED, child.status());
        assertEquals(1, child.version(), "exactly one durable child-success transition");
        assertEquals(ExecutionStatus.QUEUED, parent.status());
        assertEquals(4, parent.currentStepIndex());
        assertEquals(seed.parent().version() + 1, parent.version(), "exactly one durable parent release");
        assertTrue(((AwaitItemContinuationWorkStore) restored.interactions())
            .findDueItemContinuations(seed.now() + 60_000, 10).await().atMost(TIMEOUT).isEmpty());
        assertEquals(cut == CrashCut.INTERACTION_ADMITTED ? (concurrent ? 2 : 1) : 0, workerCalls.get());
        // Another wholly fresh caller must preserve both durable versions after work has retired.
        assertTrue(runtime(stores.get(), workerCalls, Optional.empty(), 0).invoke(seed.command()).successful());
        assertEquals(parent.version(), restored.executions().getExecution(seed.tenant(), parent.executionId())
            .await().atMost(TIMEOUT).orElseThrow().version());
        assertEquals(child.version(), restored.executions().getExecutionByKey(seed.tenant(), seed.childKey())
            .await().atMost(TIMEOUT).orElseThrow().version());
    }

    @Test
    void retryAndSaturationPersistAcrossRuntimeReplacement() throws Exception {
        Supplier<Stores> stores = stores();
        Seed seed = seed(stores.get());
        AtomicInteger calls = new AtomicInteger();
        Runtime failing = runtime(stores.get(), calls, Optional.empty(), 1);
        AwaitItemContinuationResult failed = failing.invoke(seed.command());
        assertEquals(AwaitItemContinuationDisposition.RETRY, failed.disposition());
        assertTrue(failed.retryConsumesAttempt());
        AwaitItemContinuationCommand retry = due(stores.get(), failed.retryAtEpochMs());
        assertEquals(2, retry.attempt());
        Runtime saturated = runtime(stores.get(), calls, Optional.empty(), 0);
        try (var permit = saturated.executor().tryAdmit().orElseThrow()) {
            AwaitItemContinuationResult result = saturated.invoke(retry);
            assertEquals(AwaitItemContinuationDisposition.RETRY, result.disposition());
            assertFalse(result.retryConsumesAttempt());
            retry = due(stores.get(), result.retryAtEpochMs());
            assertEquals(2, retry.attempt());
        }
        assertTrue(runtime(stores.get(), calls, Optional.empty(), 0).invoke(retry).successful());
        assertEquals(2, calls.get(), "saturation must not invoke the worker");
        assertEquals(ExecutionStatus.QUEUED, stores.get().executions().getExecution(
            seed.tenant(), seed.parent().executionId()).await().atMost(TIMEOUT).orElseThrow().status());
    }

    @Test
    void failedReleaseDeliveryLeavesWorkForFreshRecovery() throws Exception {
        Supplier<Stores> stores = stores();
        Seed seed = seed(stores.get());
        AtomicInteger calls = new AtomicInteger();
        Runtime first = runtime(stores.get(), calls, Optional.empty(), 0);
        when(first.dispatcher().enqueueNow(any()))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("dispatch unavailable")));
        AwaitItemContinuationResult result = first.invoke(seed.command());
        assertEquals(AwaitItemContinuationDisposition.RETRY, result.disposition());
        AwaitItemContinuationCommand retry = due(stores.get(), result.retryAtEpochMs());
        Runtime failedReplay = runtime(stores.get(), calls, Optional.empty(), 0);
        when(failedReplay.dispatcher().enqueueNow(any()))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("still unavailable")));
        assertThrows(IllegalStateException.class, () -> failedReplay.invoke(retry));
        assertEquals(retry.attempt(), due(stores.get(), retry.nowEpochMs()).attempt());
        assertTrue(runtime(stores.get(), calls, Optional.empty(), 0).invoke(retry).successful());
        assertEquals(1, calls.get(), "recovery must use the durable child, without invoking it again");
        assertTrue(((AwaitItemContinuationWorkStore) stores.get().interactions())
            .findDueItemContinuations(seed.now() + 60_000, 10).await().atMost(TIMEOUT).isEmpty());
    }

    private AwaitItemContinuationCommand due(Stores stores, long now) {
        return ((AwaitItemContinuationWorkStore) stores.interactions()).findDueItemContinuations(now, 10)
            .await().atMost(TIMEOUT).getFirst();
    }

    private Seed seed(Stores stores) {
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        String tenant = UUID.randomUUID().toString();
        String unitId = "unit-" + tenant;
        ExecutionRecord<Object, Object> created = stores.executions().createOrGetExecution(new ExecutionCreateCommand(
            tenant, "parent", new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
            ExecutionResultShape.MATERIALIZED_MULTI, now, ttl)).await().atMost(TIMEOUT).record();
        ExecutionRecord<Object, Object> parent = stores.executions().markWaitingExternal(
            tenant, created.executionId(), created.version(), "await", unitId, 2, now).await().atMost(TIMEOUT).orElseThrow();
        stores.units().createOrGet(new AwaitUnitCreateCommand(
            tenant, unitId, parent.executionId(), "AwaitApproval", 2, "ONE_TO_ONE", now, ttl)).await().atMost(TIMEOUT);
        stores.units().markDispatchComplete(tenant, unitId, 1, now).await().atMost(TIMEOUT);
        AwaitInteractionRecord requested = stores.interactions().createOrGet(new AwaitCreateCommand(
            tenant, parent.executionId(), "AwaitApproval", 2, String.class.getName(),
            "cause", "request", "correlation", "request", "reviewer", "reviewers", "test", unitId, 0,
            now, now + 60_000, ttl)).await().atMost(TIMEOUT).record();
        AwaitInteractionRecord completed = stores.interactions().complete(new AwaitCompletionCommand(
            tenant, requested.interactionId(), requested.correlationId(), "completion", "approved", "actor", now))
            .await().atMost(TIMEOUT).record();
        AwaitUnitRecord unit = stores.units().get(tenant, unitId).await().atMost(TIMEOUT).orElseThrow();
        return new Seed(tenant, unitId, parent, completed,
            ItemContinuationKey.from(parent, unit, 0).childExecutionKey(), now);
    }

    private void persistChild(Stores stores, Seed seed) {
        var child = stores.executions().createOrGetExecution(new ExecutionCreateCommand(seed.tenant(), seed.childKey(),
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "approved"), ExecutionResultShape.MATERIALIZED_MULTI,
            seed.now(), seed.parent().ttlEpochS())).await().atMost(TIMEOUT).record();
        stores.executions().markSucceeded(seed.tenant(), child.executionId(), child.version(),
            "await-item-continuation:" + seed.unitId() + ":0", List.of("output"), seed.now()).await().atMost(TIMEOUT);
    }

    private Runtime runtime(Stores stores, AtomicInteger calls, Optional<CyclicBarrier> barrier, int failures) throws Exception {
        AwaitCoordinator await = new AwaitCoordinator();
        inject(await, "resolvedInteractionStore", stores.interactions());
        inject(await, "resolvedUnitStore", stores.units());
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig worker = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(config.worker()).thenReturn(worker);
        when(worker.maxInFlight()).thenReturn(1);
        when(worker.saturatedDelay()).thenReturn(Duration.ofMillis(10));
        TransitionWorkerExecutor executor = new TransitionWorkerExecutor(config, new PipelineInvocationRuntime());
        QueueAsyncCoordinator coordinator = new QueueAsyncCoordinator();
        coordinator.orchestratorConfig = config;
        coordinator.executionStateStore = stores.executions();
        coordinator.workDispatcher = mock(WorkDispatcher.class);
        when(coordinator.workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
        coordinator.deadLetterPublisher = mock(DeadLetterPublisher.class);
        coordinator.awaitCoordinator = await;
        coordinator.transitionWorkerExecutor = executor;
        coordinator.transitionPayloadCodec = new JsonTransitionPayloadCodec();
        coordinator.segmentBoundaryLedger = new SegmentBoundaryLedger(new InMemoryControlPlaneJournal());
        AtomicInteger remaining = new AtomicInteger(failures);
        AwaitItemContinuationHandler handler = new AwaitItemContinuationHandler() {
            @Override
            public Uni<Void> continueAwaitItem(AwaitInteractionRecord interaction, AwaitUnitRecord unit,
                int nextStep, Optional<ExecutionRecord<Object, Object>> parent, long now) {
                return Uni.createFrom().deferred(() -> {
                    calls.incrementAndGet();
                    if (remaining.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
                        return Uni.createFrom().failure(new IllegalStateException("worker unavailable"));
                    }
                    barrier.ifPresent(value -> {
                        try { value.await(10, TimeUnit.SECONDS); }
                        catch (Exception failure) { throw new IllegalStateException(failure); }
                    });
                    return coordinator.recordAwaitItemContinuation(interaction, unit, 4,
                        new ExecutionInputSnapshot(ExecutionInputShape.UNI, interaction.responsePayload()), List.of("output"), now);
                });
            }
            @Override
            public Uni<Void> releaseAwaitParentIfReady(ExecutionRecord<Object, Object> parent,
                AwaitUnitRecord unit, int nextStep, long now) {
                return coordinator.releaseItemizedAwaitParentIfReady(parent, unit, 4, now);
            }
        };
        PipelineExecutionService service = mock(PipelineExecutionService.class);
        when(service.awaitItemContinuationHandlerForControlPlane()).thenReturn(handler);
        LocalPipelineControlPlane facade = new LocalPipelineControlPlane();
        facade.queueAsyncCoordinator = coordinator;
        facade.pipelineExecutionService = service;
        return new Runtime(facade, executor, coordinator.workDispatcher);
    }

    private void inject(AwaitCoordinator target, String name, Object value) throws Exception {
        Field field = AwaitCoordinator.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private enum CrashCut { INTERACTION_ADMITTED, CHILD_COMPLETED, CONTINUATION_RECORDED }
    private record Runtime(PipelineControlPlane controlPlane, TransitionWorkerExecutor executor, WorkDispatcher dispatcher) {
        AwaitItemContinuationResult invoke(AwaitItemContinuationCommand command) {
            return controlPlane.processAwaitItemContinuation(command).await().atMost(TIMEOUT);
        }
    }
    private record Seed(String tenant, String unitId, ExecutionRecord<Object, Object> parent,
        AwaitInteractionRecord interaction, String childKey, long now) {
        AwaitItemContinuationCommand command() {
            return new AwaitItemContinuationCommand(tenant, parent.executionId(), unitId, interaction.interactionId(), 0, 1, now);
        }
    }
}

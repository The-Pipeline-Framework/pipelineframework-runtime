package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.pipelineframework.AwaitCompletionLifecycleProtocolTest.*;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.orchestrator.*;

/** One actual reissue cut: new ordinary completion before the replacement Await suspension CAS. */
class AwaitCompletionReopenedAdmissionTest {
    protected record Scenario(Supplier<Stores> stores, Supplier<AwaitUnitStore> units) {}

    protected Scenario scenario() {
        var stores = new Stores(new InMemoryExecutionStateStore(), new InMemoryAwaitInteractionStore());
        var units = new InMemoryAwaitUnitStore();
        return new Scenario(() -> stores, () -> units);
    }

    @Test
    void reissuedExecutionCannotUsePriorUnitAckToRetireNewlyAdmittedInteraction() throws Exception {
        reissuedCycle(10);
    }

    @Test
    void equalClockStillRequiresAndUsesItsOwnCausalAcknowledgement() throws Exception {
        reissuedCycle(0);
    }

    @Test
    void regressingClockDoesNotEraseCoverageOrManufactureDuration() throws Exception {
        reissuedCycle(-1);
    }

    private void reissuedCycle(long secondReleaseDelay) throws Exception {
        Scenario scenario = scenario();
        var stores = scenario.stores().get();
        var units = scenario.units().get();
        var coordinator = new AwaitCoordinator();
        field(coordinator, "resolvedInteractionStore", stores.interactions());
        field(coordinator, "resolvedUnitStore", units);
        var config = mock(PipelineOrchestratorConfig.class);
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        var policy = mock(ControlPlaneAdmissionPolicy.class);
        when(policy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
        var inputs = new ExecutionInputPolicy();
        inputs.orchestratorConfig = config;
        var reads = new ExecutionReadModel(config, inputs, stores.executions(), coordinator, policy,
            () -> "pipeline", () -> "release", JsonTransitionPayloadCodec::new);
        long now = System.currentTimeMillis();
        var created = await(stores.executions().createOrGetExecution(new ExecutionCreateCommand("tenant", "parent",
            "pipeline", "contract", "release", new ExecutionInputSnapshot(ExecutionInputShape.UNI, "request"),
            ExecutionResultShape.SINGLE, now, now / 1000 + 3600))).record();
        var descriptor = new AwaitCompletionDescriptor("AwaitApproval", String.class.getName(), String.class.getName(),
            "ONE_TO_ONE", Duration.ofMinutes(10), "interactionId", "test", Map.of(), List.of());
        var first = createInRetainedContext(coordinator, descriptor, created);
        await(stores.executions().markWaitingExternal("tenant", created.executionId(), created.version(),
            "await", first.unitId(), 2, now + 1));
        var completedFirst = await(coordinator.complete(completion(first, now + 10))).record();
        await(coordinator.recordCompletion(completedFirst, now + 11));
        var released = await(stores.executions().markAwaitCompleted("tenant", created.executionId(),
            first.unitId(), 3, now + 20)).orElseThrow();
        var firstAck = ack(stores, "tenant", created.executionId(), first.unitId()).orElseThrow();
        assertEquals(AwaitCompletionObservation.State.RELEASED,
            await(reads.getAwaitCompletionObservation("tenant", first.interactionId())).state());
        assertTrue(enumerate(stores, "tenant", 1).isEmpty());
        var sameCycleReplay = createInRetainedContext(coordinator, descriptor, released);
        assertEquals(first.interactionId(), sameCycleReplay.interactionId(),
            "terminal retry must recover the sealed occurrence's retained interaction");
        assertEquals(first.unitId(), sameCycleReplay.unitId());
        assertTrue(enumerate(stores, "tenant", 1).isEmpty());
        var succeeded = await(stores.executions().markSucceeded("tenant", created.executionId(), released.version(),
            "done", "result", now + 30)).orElseThrow();
        // This is the real native deliberate-reissue transition; it preserves execution identity
        // and revisits preceding ordinary Await steps before the explicitly targeted Command.
        var redriven = await(stores.executions().redriveTerminalExecution("tenant", created.executionId(),
            succeeded.version(), false, ExecutionRedriveIntent.REISSUE_COMMAND, Optional.of("later-command"),
            Optional.of("operator-approved reissue"), "command-reissue", now + 40)).orElseThrow();
        assertEquals(0, redriven.currentStepIndex());
        assertEquals(created.executionId(), redriven.executionId());
        assertTrue(created.awaitOccurrenceCause().isPresent());
        assertTrue(redriven.awaitOccurrenceCause().isPresent());
        assertNotEquals(created.awaitOccurrenceCause(), redriven.awaitOccurrenceCause());
        assertTrue(await(stores.executions().redriveTerminalExecution("tenant", created.executionId(),
            succeeded.version(), false, ExecutionRedriveIntent.REISSUE_COMMAND, Optional.of("later-command"),
            Optional.of("operator-approved reissue"), "command-reissue", now + 41)).isEmpty());
        assertEquals(redriven.awaitOccurrenceCause(), await(stores.executions()
            .getExecution("tenant", created.executionId())).orElseThrow().awaitOccurrenceCause(),
            "a repeated losing admission cannot change the retained reissue cause");
        var second = createInRetainedContext(coordinator, descriptor, redriven);
        assertNotEquals(first.interactionId(), second.interactionId());
        assertNotEquals(first.unitId(), second.unitId());
        assertNotEquals(first.idempotencyKey(), second.idempotencyKey());
        assertEquals(AwaitUnitStatus.WAITING_EXTERNAL, await(units.get("tenant", second.unitId())).orElseThrow().status());
        await(coordinator.complete(completion(second, now + 50)));
        var secondFact = fact(stores, second);
        assertEquals(List.of(secondFact), enumerate(stores, "tenant", 1));
        assertEquals(ExecutionStatus.QUEUED,
            await(stores.executions().getExecution("tenant", created.executionId())).orElseThrow().status());
        // Crash before the worker's replacement WAITING_EXTERNAL transition: old terminal unit
        // and immutable first ACK do not establish consequences of this distinct admission.
        // Recover through new wrappers after the admission reply is discarded.
        stores = scenario.stores().get();
        units = scenario.units().get();
        field(coordinator, "resolvedInteractionStore", stores.interactions());
        field(coordinator, "resolvedUnitStore", units);
        reads = new ExecutionReadModel(config, inputs, stores.executions(), coordinator, policy,
            () -> "pipeline", () -> "release", JsonTransitionPayloadCodec::new);
        var recoveredStores = stores;
        var observation = await(reads.getAwaitCompletionObservation("tenant", second.interactionId()));
        assertAll(
            () -> assertTrue(observation.state() == AwaitCompletionObservation.State.UNKNOWN
                    || observation.state() == AwaitCompletionObservation.State.OUTSTANDING,
                "an unrelated first ACK cannot certify the second admission: " + observation),
            () -> assertTrue(observation.admissionToReleaseLatencyMs().isEmpty()),
            () -> assertEquals(List.of(secondFact), enumerate(recoveredStores, "tenant", 1),
                "unresolved replacement admission must remain OPEN"),
            () -> assertEquals(firstAck, ack(recoveredStores, "tenant", created.executionId(), first.unitId()).orElseThrow()));
        assertTrue(ack(stores, "tenant", created.executionId(), second.unitId()).isEmpty());
        assertEquals(AwaitCompletionObservation.State.RELEASED,
            await(reads.getAwaitCompletionObservation("tenant", first.interactionId())).state(),
            "the historical occurrence keeps its own coverage after the cause advances");
        var waiting = await(stores.executions().markWaitingExternal("tenant", redriven.executionId(),
            redriven.version(), "replacement-await", second.unitId(), 2, now + 55)).orElseThrow();
        var completedSecond = await(stores.interactions().get("tenant", second.interactionId())).orElseThrow();
        await(coordinator.recordCompletion(completedSecond, now + 56));
        assertEquals(AwaitCompletionObservation.State.OUTSTANDING,
            await(reads.getAwaitCompletionObservation("tenant", second.interactionId())).state());
        assertEquals(List.of(secondFact), enumerate(stores, "tenant", 1));
        long secondReleaseAt = secondFact.firstAdmittedAtEpochMs() + secondReleaseDelay;
        await(stores.executions().markAwaitCompleted("tenant", waiting.executionId(), second.unitId(), 3,
            secondReleaseAt)).orElseThrow();
        // Discard the successful transition response and restart the read owner/store wrappers again.
        stores = scenario.stores().get();
        field(coordinator, "resolvedInteractionStore", stores.interactions());
        field(coordinator, "resolvedUnitStore", scenario.units().get());
        reads = new ExecutionReadModel(config, inputs, stores.executions(), coordinator, policy,
            () -> "pipeline", () -> "release", JsonTransitionPayloadCodec::new);
        var secondAck = ack(stores, "tenant", waiting.executionId(), second.unitId()).orElseThrow();
        assertNotEquals(firstAck.unitId(), secondAck.unitId());
        assertEquals(secondReleaseAt, secondAck.acknowledgedAtEpochMs());
        var releasedSecond = await(reads.getAwaitCompletionObservation("tenant", second.interactionId()));
        assertEquals(AwaitCompletionObservation.State.RELEASED, releasedSecond.state());
        assertEquals(Optional.of(secondAck), releasedSecond.acknowledgement());
        if (secondReleaseDelay < 0) assertTrue(releasedSecond.admissionToReleaseLatencyMs().isEmpty());
        else assertEquals(secondReleaseDelay, releasedSecond.admissionToReleaseLatencyMs().orElseThrow());
        assertTrue(enumerate(stores, "tenant", 1).isEmpty());
        assertEquals(firstAck, ack(stores, "tenant", created.executionId(), first.unitId()).orElseThrow());
        assertEquals(AwaitCompletionObservation.State.RELEASED,
            await(reads.getAwaitCompletionObservation("tenant", first.interactionId())).state());
    }

    private static AwaitInteractionRecord createInRetainedContext(AwaitCoordinator coordinator,
        AwaitCompletionDescriptor descriptor, ExecutionRecord<?, ?> record) {
        var context = new AwaitExecutionContext(record.tenantId(), record.executionId(), 2,
            AwaitContinuationMode.LIVE_IF_SUPPORTED, TerminalOutputOwnership.TRANSITION_WORKER,
            Map.of(), Optional.empty(), record.awaitOccurrenceCause());
        AwaitExecutionContextHolder.set(context);
        try {
            return await(coordinator.createOrGet(descriptor, record.tenantId(), record.executionId(), 2,
                "cause", "request", "reviewer", "reviewers")).record();
        } finally {
            AwaitExecutionContextHolder.clear();
        }
    }

    private static void field(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}

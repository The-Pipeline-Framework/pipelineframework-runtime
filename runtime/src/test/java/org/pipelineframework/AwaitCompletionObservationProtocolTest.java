package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.pipelineframework.AwaitCompletionLifecycleProtocolTest.*;

import java.lang.reflect.Field;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.orchestrator.*;

/** Actual neutral read owner and actual stores; mocks only configuration and admission policy. */
class AwaitCompletionObservationProtocolTest {
    private record Fixture(Stores stores, InMemoryAwaitUnitStore units, ExecutionReadModel readModel) {}

    private Fixture fixture() throws Exception {
        var stores = new Stores(new InMemoryExecutionStateStore(), new InMemoryAwaitInteractionStore());
        var units = new InMemoryAwaitUnitStore();
        var coordinator = new AwaitCoordinator();
        field(coordinator, "resolvedInteractionStore", stores.interactions());
        field(coordinator, "resolvedUnitStore", units);
        var config = mock(PipelineOrchestratorConfig.class);
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        var policy = mock(ControlPlaneAdmissionPolicy.class);
        when(policy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
        var inputs = new ExecutionInputPolicy();
        inputs.orchestratorConfig = config;
        var readModel = new ExecutionReadModel(config, inputs, stores.executions(), coordinator, policy,
            () -> "pipeline", () -> "release", JsonTransitionPayloadCodec::new);
        return new Fixture(stores, units, readModel);
    }

    @Test
    void unitCompletedAndRetainedAckCannotRetireMissingItemContinuationConsequences() throws Exception {
        var f = fixture();
        long now = System.currentTimeMillis();
        var parent = waiting(f.stores(), "tenant", "unit", now);
        String unit = parent.awaitUnitId();
        await(f.units().createOrGet(new AwaitUnitCreateCommand("tenant", unit, parent.executionId(),
            "AwaitApproval", 2, "ONE_TO_MANY", now, now / 1000 + 3600)));
        for (int item = 0; item < 2; item++) {
            var interaction = imported(f.stores(), "tenant", parent.executionId(), "item-" + item, unit, item,
                AwaitInteractionStatus.WAITING, now, 0);
            await(f.stores().interactions().complete(completion(interaction, now + 1)));
            await(f.units().recordItemCompleted("tenant", unit, "interaction:item-" + item, now + 2));
        }
        await(f.units().markDispatchComplete("tenant", unit, 2, now + 3));
        assertEquals(AwaitUnitStatus.COMPLETED, await(f.units().get("tenant", unit)).orElseThrow().status());
        // A retained ACK is deliberately insufficient on its own. This conservative crash/legacy
        // fixture does not claim the live owner legitimately releases before continuations finish.
        await(f.stores().executions().markAwaitCompleted("tenant", parent.executionId(), unit, 3, now + 10));
        assertEquals(AwaitCompletionObservation.State.OUTSTANDING,
            await(f.readModel().getAwaitCompletionObservation("tenant", "item-0")).state());
        assertEquals(2, enumerate(f.stores(), "tenant", 1).size());
        await(f.units().recordItemContinuationCompleted("tenant", unit,
            AwaitUnitRecord.continuationCompletionKey(0), now + 11));
        var partial = await(f.readModel().getAwaitCompletionObservation("tenant", "item-0"));
        assertEquals(AwaitCompletionObservation.State.OUTSTANDING, partial.state());
        assertTrue(partial.admissionToReleaseLatencyMs().isEmpty());
        assertEquals(2, enumerate(f.stores(), "tenant", 1).size());
        await(f.units().recordItemContinuationCompleted("tenant", unit,
            AwaitUnitRecord.continuationCompletionKey(1), now + 12));
        var observed = await(f.readModel().getAwaitCompletionObservation(" tenant ", "item-0"));
        assertEquals(AwaitCompletionObservation.State.RELEASED, observed.state());
        assertEquals(9, observed.admissionToReleaseLatencyMs().orElseThrow());
        assertEquals(1, enumerate(f.stores(), "tenant", 1).size());
        assertEquals(observed, await(f.readModel().getAwaitCompletionObservation("tenant", "item-0")));
        var page = await(f.readModel().queryAwaitCompletionObservations("tenant", 1, Optional.empty()));
        assertEquals(1, page.evaluatedEntryCount());
        assertEquals(1, page.observations().size());
        assertEquals(AwaitCompletionObservation.State.RELEASED, page.observations().getFirst().state());
        assertTrue(enumerate(f.stores(), "tenant", 1).isEmpty());
    }

    @Test
    void laterWaitingOnSameUnitMakesRetainedFirstAckInsufficient() throws Exception {
        var f = fixture();
        long now = System.currentTimeMillis();
        var parent = waiting(f.stores(), "tenant", "unit", now);
        String unit = parent.awaitUnitId();
        var interaction = imported(f.stores(), "tenant", parent.executionId(), "item", unit, 0,
            AwaitInteractionStatus.WAITING, now, 0);
        await(f.stores().interactions().complete(completion(interaction, now + 1)));
        await(f.units().createOrGet(new AwaitUnitCreateCommand("tenant", unit, parent.executionId(),
            "AwaitApproval", 2, "ONE_TO_MANY", now, now / 1000 + 3600)));
        await(f.units().recordItemCompleted("tenant", unit, "interaction:item", now + 2));
        await(f.units().recordItemContinuationCompleted("tenant", unit, AwaitUnitRecord.continuationCompletionKey(0), now + 3));
        await(f.units().markDispatchComplete("tenant", unit, 1, now + 4));
        var released = await(f.stores().executions().markAwaitCompleted("tenant", parent.executionId(), unit, 3, now + 10))
            .orElseThrow();
        var firstAck = ack(f.stores(), "tenant", parent.executionId(), unit).orElseThrow();
        await(f.stores().executions().markWaitingExternal("tenant", parent.executionId(), released.version(),
            "redrive-await", unit, 2, now + 20));
        var observed = await(f.readModel().getAwaitCompletionObservation("tenant", "item"));
        assertEquals(AwaitCompletionObservation.State.OUTSTANDING, observed.state());
        assertTrue(observed.admissionToReleaseLatencyMs().isEmpty());
        assertEquals(1, enumerate(f.stores(), "tenant", 1).size());
        assertEquals(firstAck, ack(f.stores(), "tenant", parent.executionId(), unit).orElseThrow());
    }

    @Test
    void terminalFailureHasNoResumeLatencyAndOnlyConvergedUnitCanRetire() throws Exception {
        var f = fixture();
        long now = System.currentTimeMillis();
        var parent = waiting(f.stores(), "tenant", "unit", now);
        String unit = parent.awaitUnitId();
        var interaction = imported(f.stores(), "tenant", parent.executionId(), "item", unit, 0,
            AwaitInteractionStatus.WAITING, now, 0);
        await(f.stores().interactions().complete(completion(interaction, now + 1)));
        await(f.units().createOrGet(new AwaitUnitCreateCommand("tenant", unit, parent.executionId(),
            "AwaitApproval", 2, "ONE_TO_MANY", now, now / 1000 + 3600)));
        await(f.stores().executions().markTerminalFailure("tenant", parent.executionId(), parent.version(),
            ExecutionStatus.FAILED, "timeout", "AWAIT_TIMEOUT", "another item timed out", now + 10));
        var pending = await(f.readModel().getAwaitCompletionObservation("tenant", "item"));
        assertEquals(AwaitCompletionObservation.State.OUTSTANDING, pending.state());
        assertTrue(pending.admissionToReleaseLatencyMs().isEmpty());
        assertEquals(1, enumerate(f.stores(), "tenant", 1).size());
        await(f.units().markTerminal("tenant", unit, AwaitUnitStatus.TIMED_OUT, now + 11));
        var terminal = await(f.readModel().getAwaitCompletionObservation("tenant", "item"));
        assertEquals(AwaitCompletionObservation.State.TERMINAL_FAILED, terminal.state());
        assertEquals(AwaitReleaseDisposition.TERMINAL_FAILED, terminal.acknowledgement().orElseThrow().disposition());
        assertTrue(terminal.admissionToReleaseLatencyMs().isEmpty());
        assertTrue(enumerate(f.stores(), "tenant", 1).isEmpty());
        assertEquals(terminal, await(f.readModel().getAwaitCompletionObservation("tenant", "item")));
    }

    @Test
    void completedLegacyImportIsExplicitlyUnknownEvenWhenOpenQueryIsEmpty() throws Exception {
        var f = fixture();
        long now = System.currentTimeMillis();
        imported(f.stores(), "tenant", "parent", "legacy", "unit", 0, AwaitInteractionStatus.COMPLETED, now, 0);
        var observed = await(f.readModel().getAwaitCompletionObservation("tenant", "legacy"));
        assertEquals(AwaitCompletionObservation.State.UNKNOWN, observed.state());
        assertTrue(observed.admission().isEmpty());
        assertTrue(observed.acknowledgement().isEmpty());
        assertTrue(observed.admissionToReleaseLatencyMs().isEmpty());
        var page = await(f.readModel().queryAwaitCompletionObservations("tenant", 1, Optional.empty()));
        assertTrue(page.observations().isEmpty());
        assertTrue(page.nextCursor().isEmpty());
        assertEquals(observed, await(f.readModel().getAwaitCompletionObservation("tenant", "legacy")));
    }

    @Test
    void newAdmissionIntoLegacyScopeRemainsUnknownAndOpen() throws Exception {
        var f = fixture();
        long now = System.currentTimeMillis();
        var interaction = imported(f.stores(), "tenant", "legacy-parent", "legacy-admitted", "legacy-unit", 0,
            AwaitInteractionStatus.WAITING, now, 0);
        await(f.stores().interactions().complete(completion(interaction, now + 1)));
        var admission = fact(f.stores(), interaction);
        var observed = await(f.readModel().getAwaitCompletionObservation("tenant", interaction.interactionId()));
        assertEquals(AwaitCompletionObservation.State.UNKNOWN, observed.state());
        assertEquals(Optional.of(admission), observed.admission());
        assertTrue(observed.acknowledgement().isEmpty());
        assertTrue(observed.admissionToReleaseLatencyMs().isEmpty());
        assertEquals(java.util.List.of(admission), enumerate(f.stores(), "tenant", 1),
            "a new clock cannot manufacture legacy causal coverage or retire OPEN work");
    }

    private static void field(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}

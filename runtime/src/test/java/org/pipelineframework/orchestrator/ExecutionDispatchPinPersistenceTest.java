package org.pipelineframework.orchestrator;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitUnitOccurrence;
import static org.junit.jupiter.api.Assertions.*;

class ExecutionDispatchPinPersistenceTest {
    static final ExecutionDispatchPin PIN = new ExecutionDispatchPin("entry-original", "deployment-original");

    @Test
    void memoryTransitionsAndDuplicateAdmissionKeepOriginalDispatchSelection() {
        var store = new InMemoryExecutionStateStore();
        var command = command("dispatch-pin", PIN);
        var created = store.createOrGetExecution(command).await().indefinitely().record();
        var duplicate = store.createOrGetExecution(command("dispatch-pin", new ExecutionDispatchPin("other", "other-operation")))
            .await().indefinitely();
        assertTrue(duplicate.duplicate());
        assertEquals(created.executionId(), duplicate.record().executionId());
        expect(duplicate.record());
        exerciseTransitions(store, created);
    }

    @Test
    void uncertainOutcomeRetainsPinWithoutAutomaticRetry() {
        var store = new InMemoryExecutionStateStore();
        var created = store.createOrGetExecution(command("uncertain-pin", PIN)).await().indefinitely().record();
        long now = System.currentTimeMillis();
        var claimed = expect(store.claimLease(created.tenantId(), created.executionId(), "owner", now, 10_000).await().indefinitely().orElseThrow());
        var unknown = expect(store.markRemoteOutcomeUnknown(claimed.tenantId(), claimed.executionId(), claimed.version(),
            "unknown", "REMOTE_OUTCOME_UNKNOWN", "reply lost", now + 1).await().indefinitely().orElseThrow());
        assertTrue(store.claimLease(unknown.tenantId(), unknown.executionId(), "replacement", now + 20_000, 1000)
            .await().indefinitely().isEmpty());
    }

    static ExecutionCreateCommand command(String key, ExecutionDispatchPin pin) {
        long now = System.currentTimeMillis();
        return new ExecutionCreateCommand("tenant", key, "pipeline", "contract", "release-a", "original",
            ExecutionResultShape.SINGLE, Optional.empty(), 0, Optional.empty(), now, now / 1000 + 3600, Optional.of(pin));
    }

    /** Exercises actual provider transitions against a constant independently selected entry. */
    static void exerciseTransitions(ExecutionStateStore store, ExecutionRecord<Object, Object> initial) {
        expect(initial);
        String tenant = initial.tenantId();
        String id = initial.executionId();
        long now = System.currentTimeMillis();
        var current = expect(store.claimLease(tenant, id, "owner", now, 10_000).await().indefinitely().orElseThrow());
        current = expect(store.renewLease(tenant, id, current.version(), "owner", now + 1, 10_000).await().indefinitely().orElseThrow());
        current = expect(store.scheduleRetry(tenant, id, current.version(), 1, now + 2, "retry", "retryable", "retry",
            now + 1).await().indefinitely().orElseThrow());
        current = expect(store.claimLease(tenant, id, "owner-2", now + 3, 10_000).await().indefinitely().orElseThrow());
        var unit = new AwaitUnitOccurrence(tenant, id, id, "Wait", 0, current.awaitOccurrenceCause().orElseThrow()).unitId();
        current = expect(store.markWaitingExternal(tenant, id, current.version(), "wait", unit, 0, now + 4)
            .await().indefinitely().orElseThrow());
        current = expect(store.markAwaitCompleted(tenant, id, unit, 1, now + 5).await().indefinitely().orElseThrow());
        current = expect(store.claimLease(tenant, id, "owner-3", now + 6, 10_000).await().indefinitely().orElseThrow());
        unit = new AwaitUnitOccurrence(tenant, id, id, "Items", 1, current.awaitOccurrenceCause().orElseThrow()).unitId();
        current = expect(store.markWaitingExternal(tenant, id, current.version(), "items", unit, 1, now + 7)
            .await().indefinitely().orElseThrow());
        current = expect(store.markAwaitItemContinuationsCompleted(tenant, id, unit, 2, "resumed", now + 8)
            .await().indefinitely().orElseThrow());
        current = expect(store.claimLease(tenant, id, "owner-4", now + 9, 10_000).await().indefinitely().orElseThrow());
        current = expect(store.markTerminalFailure(tenant, id, current.version(), ExecutionStatus.FAILED,
            "failed", "failure", "controlled failure", now + 10).await().indefinitely().orElseThrow());
        current = expect(store.redriveTerminalExecution(tenant, id, current.version(), true, "redrive", now + 11)
            .await().indefinitely().orElseThrow());
        current = expect(store.claimLease(tenant, id, "owner-5", now + 12, 10_000).await().indefinitely().orElseThrow());
        expect(store.markSucceeded(tenant, id, current.version(), "success", "result", now + 13)
            .await().indefinitely().orElseThrow());
        expect(store.getExecution(tenant, id).await().indefinitely().orElseThrow());
    }

    private static ExecutionRecord<Object, Object> expect(ExecutionRecord<Object, Object> record) {
        assertEquals(Optional.of(PIN), record.dispatchPin());
        return record;
    }
}

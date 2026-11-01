package org.pipelineframework.orchestrator;

import java.util.HashSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.awaitable.AwaitUnitOccurrence;
import static org.junit.jupiter.api.Assertions.*;

class AwaitCycleAdvanceTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void reenteringTheSameBoundaryGetsANewCycleAndStaleCompletionsCannotReleaseIt(boolean itemized) {
        var store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        var record = store.createOrGetExecution(new ExecutionCreateCommand("tenant", "request", "input",
            ExecutionResultShape.SINGLE, now, now / 1000 + 600)).await().indefinitely().record();
        var previousUnits = new HashSet<String>();
        var invocation = org.pipelineframework.invocation.PipelineInvocationContext.root(3);
        for (int cycle = 0; cycle < 3; cycle++) {
            invocation = invocation.enterRecursive("$root", "$recur");
            var occurrence = new AwaitUnitOccurrence("tenant", record.executionId(), record.executionId(),
                "Review", 4, record.awaitOccurrenceCause().orElseThrow()).withInvocationPath(invocation.path());
            var waiting = store.markWaitingExternal("tenant", record.executionId(), record.version(),
                "transition-" + cycle, occurrence.unitId(), 4, ++now).await().indefinitely().orElseThrow();
            for (String stale : previousUnits) {
                assertTrue(release(store, record.executionId(), stale, itemized, ++now).isEmpty());
            }
            var released = release(store, record.executionId(), occurrence.unitId(), itemized, ++now).orElseThrow();
            assertEquals(occurrence.occurrenceCause(), released.awaitOccurrenceCause().orElseThrow());
            assertTrue(release(store, record.executionId(), occurrence.unitId(), itemized, ++now).isEmpty());
            assertEquals(released.awaitOccurrenceCause(), store.getExecution("tenant", record.executionId())
                .await().indefinitely().orElseThrow().awaitOccurrenceCause());
            assertTrue(previousUnits.add(waiting.awaitUnitId()));
            record = released;
        }
    }

    private java.util.Optional<ExecutionRecord<Object, Object>> release(InMemoryExecutionStateStore store,
        String executionId, String unitId, boolean itemized, long now) {
        return (itemized
            ? store.markAwaitItemContinuationsCompleted("tenant", executionId, unitId, 5, java.util.List.of("reviewed"), now)
            : store.markAwaitCompleted("tenant", executionId, unitId, 5, now)).await().indefinitely();
    }

}

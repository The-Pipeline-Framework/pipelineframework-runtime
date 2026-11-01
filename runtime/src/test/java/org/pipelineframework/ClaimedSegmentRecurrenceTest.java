package org.pipelineframework;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitUnitOccurrence;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.invocation.PipelineInvocationContext;
import org.pipelineframework.orchestrator.*;
import static org.junit.jupiter.api.Assertions.*;

class ClaimedSegmentRecurrenceTest {
  @Test
  void workerEnvelopeKeepsAdmissionPolicyAndResumedInvocationPathTogether() {
    var policy = new PipelineContext("release-a", "REPLAY", "BYPASS");
    var original = new ExecutionInputSnapshot(ExecutionInputShape.RAW, "request", policy);
    var store = new InMemoryExecutionStateStore();
    long now = System.currentTimeMillis();
    var record = store.createOrGetExecution(new ExecutionCreateCommand("tenant", "request", original,
        ExecutionResultShape.SINGLE, now, now / 1000 + 600)).await().indefinitely().record();
    var path = PipelineInvocationContext.root(3).enterRecursive("$root", "$recur").path();
    var unit = new AwaitUnitOccurrence("tenant", record.executionId(), record.executionId(), "Review", 4,
        record.awaitOccurrenceCause().orElseThrow()).withInvocationPath(path);
    store.markWaitingExternal("tenant", record.executionId(), record.version(), "await", unit.unitId(), 4, ++now)
        .await().indefinitely().orElseThrow();
    var queued = store.markAwaitCompleted("tenant", record.executionId(), unit.unitId(), 5, ++now)
        .await().indefinitely().orElseThrow();
    var resumed = new ExecutionInputSnapshot(ExecutionInputShape.RAW, Map.of("answer", "yes"),
        Optional.empty(), Optional.of(path));
    var codec = new JsonTransitionPayloadCodec();
    var command = ClaimedSegment.from(queued).transitionCommand(resumed, codec).toCommand(codec);
    var input = assertInstanceOf(ExecutionInputSnapshot.class, command.inputPayload());
    assertEquals(Optional.of(policy), input.pipelineContext());
    assertEquals(Optional.of(path), input.invocationPath());
    assertEquals(Map.of("answer", "yes"), input.payload());
    assertEquals(5, command.currentStepIndex());
  }
}

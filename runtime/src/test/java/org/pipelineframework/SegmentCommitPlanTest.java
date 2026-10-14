package org.pipelineframework;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentCommitPlanTest {

  @Test
  void completedResultShapeValidationRejectsMultipleSingleItems() {
    ClaimedSegment segment = ClaimedSegment.from(record("exec-single", ExecutionResultShape.SINGLE));

    IllegalStateException error = assertThrows(IllegalStateException.class, () ->
        SegmentCommitPlan.from(segment, TransitionResultEnvelope.completedInProcess(List.of("a", "b"))));

    assertTrue(error.getMessage().contains("SINGLE result shape"));
    assertTrue(error.getMessage().contains("exec-single"));
  }

  @Test
  void completedPlanKeepsOutputItemsAndTerminalPlan() {
    ClaimedSegment segment = ClaimedSegment.from(record("exec-multi", ExecutionResultShape.MATERIALIZED_MULTI));

    CompletedSegment completed = assertInstanceOf(CompletedSegment.class,
        SegmentCommitPlan.from(segment, TransitionResultEnvelope.completedInProcess(List.of("a", "b"))));

    assertEquals(List.of("a", "b"), completed.outputItems());
    assertEquals("a", completed.terminalPublication().checkpointPayload().orElseThrow());
  }

  @Test
  void nullWorkerResultBecomesFailedPlan() {
    ClaimedSegment segment = ClaimedSegment.from(record("exec-null", ExecutionResultShape.SINGLE));

    FailedSegment failed = assertInstanceOf(FailedSegment.class, SegmentCommitPlan.from(segment, null));

    assertTrue(failed.failure().getMessage().contains("PipelineTransitionWorker returned null result"));
  }

  @Test
  void awaitSuspensionBecomesSuspendedPlan() {
    ClaimedSegment segment = ClaimedSegment.from(record("exec-await", ExecutionResultShape.SINGLE));
    TransitionAwaitSuspension suspension = new TransitionAwaitSuspension("tenant-1", "exec-await", "unit-1", 2);

    SuspendedSegment suspended = assertInstanceOf(SuspendedSegment.class,
        SegmentCommitPlan.from(segment, TransitionResultEnvelope.waiting(suspension)));

    assertEquals("unit-1", suspended.suspension().unitId());
  }

  @Test
  void failureResultPreservesFailureMetadata() {
    ClaimedSegment segment = ClaimedSegment.from(record("exec-failed", ExecutionResultShape.SINGLE));

    FailedSegment failed = assertInstanceOf(FailedSegment.class,
        SegmentCommitPlan.from(segment, TransitionResultEnvelope.failed(new IllegalArgumentException("boom"))));

    assertTrue(failed.failure().getMessage().contains(IllegalArgumentException.class.getName()));
    assertTrue(failed.failure().getMessage().contains("boom"));
  }

  @Test
  void terminalInputPassThroughUsesCoordinatorMaterializedInput() {
    JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();
    ExecutionInputSnapshot input = new ExecutionInputSnapshot(
        ExecutionInputShape.MULTI,
        List.of(codec.encode("a"), codec.encode("b")));
    ClaimedSegment segment = ClaimedSegment.from(record(
        "exec-terminal",
        ExecutionResultShape.MATERIALIZED_MULTI,
        input));

    CompletedSegment completed = assertInstanceOf(CompletedSegment.class,
        SegmentCommitPlan.from(segment, TransitionResultEnvelope.completedTerminalInputPassthrough(), codec));

    assertEquals(List.of("a", "b"), completed.outputItems());
    assertEquals(List.of("a", "b"), completed.terminalPublication().decodedOutputItems(codec));
  }

  @Test
  void terminalInputPassThroughDecodesSingleSerializedInput() {
    JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();
    ExecutionInputSnapshot input = new ExecutionInputSnapshot(ExecutionInputShape.UNI, codec.encode("one"));
    ClaimedSegment segment = ClaimedSegment.from(record("exec-terminal-single", ExecutionResultShape.SINGLE, input));

    CompletedSegment completed = assertInstanceOf(CompletedSegment.class,
        SegmentCommitPlan.from(segment, TransitionResultEnvelope.completedTerminalInputPassthrough(), codec));

    assertEquals(List.of("one"), completed.outputItems());
  }

  @Test
  void importedSuspensionMustCarryTheStoredCauseAndExactPageScope() {
    long now = System.currentTimeMillis();
    var store = new org.pipelineframework.orchestrator.InMemoryExecutionStateStore();
    var owner = store.createOrGetExecution(new org.pipelineframework.orchestrator.ExecutionCreateCommand(
        "tenant-1", "causal", "pipeline-a", "contract-a", "release-a",
        new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"), ExecutionResultShape.SINGLE,
        now, now / 1000 + 3600)).await().indefinitely().record();
    var segment = ClaimedSegment.from(owner);
    String cause = owner.awaitOccurrenceCause().orElseThrow();
    String current = new org.pipelineframework.awaitable.AwaitUnitOccurrence("tenant-1", owner.executionId(),
        owner.executionId(), "Await", 2, cause).unitId();
    assertInstanceOf(SuspendedSegment.class, SegmentCommitPlan.from(segment,
        TransitionResultEnvelope.waiting(new TransitionAwaitSuspension("tenant-1", owner.executionId(), current, 2))));
    for (String invalid : List.of("legacy-unit",
        new org.pipelineframework.awaitable.AwaitUnitOccurrence("tenant-1", owner.executionId(), owner.executionId(),
            "Await", 2, "previous-cause").unitId(),
        new org.pipelineframework.awaitable.AwaitUnitOccurrence("tenant-1", owner.executionId(), owner.executionId() + ":page:1",
            "Await", 2, cause).unitId())) {
      assertThrows(IllegalStateException.class, () -> SegmentCommitPlan.from(segment,
          TransitionResultEnvelope.waiting(new TransitionAwaitSuspension("tenant-1", owner.executionId(), invalid, 2))));
    }
    assertThrows(IllegalStateException.class, () -> SegmentCommitPlan.from(segment,
        TransitionResultEnvelope.waiting(new TransitionAwaitSuspension("other", owner.executionId(), "legacy", 2))));
    assertEquals(owner, store.getExecution("tenant-1", owner.executionId()).await().indefinitely().orElseThrow(),
        "invalid worker results must be rejected before any parent projection mutation");
  }

  private static ExecutionRecord<Object, Object> record(String executionId, ExecutionResultShape resultShape) {
    return record(executionId, resultShape, "input");
  }

  private static ExecutionRecord<Object, Object> record(
      String executionId,
      ExecutionResultShape resultShape,
      Object input) {
    return new ExecutionRecord<>(
        "tenant-1",
        executionId,
        "key-" + executionId,
        "pipeline-a",
        "contract-a",
        "release-a",
        resultShape,
        ExecutionStatus.QUEUED,
        0L,
        0,
        0,
        null,
        0L,
        0L,
        null,
        input,
        null,
        null,
        null,
        null,
        1L,
        1L,
        99999999L);
  }
}

package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PipelineStepExecutionFailureTest {

  @Test
  void rejectsMissingFailuresAtEveryEntryPoint() {
    assertThrows(NullPointerException.class, () -> PipelineStepExecutionFailure.at(1, null));
    assertThrows(NullPointerException.class, () -> PipelineStepExecutionFailure.atRoot(1, null));
    assertThrows(NullPointerException.class, () -> PipelineStepExecutionFailure.source(null));
  }

  @Test
  void preservesOriginatingRootStepIndexWhileStillReplacingNestedLocalIndex() {
    IllegalStateException cause = new IllegalStateException("boom");
    Throwable nested = PipelineStepExecutionFailure.at(4, cause);
    Throwable wrappedAtRoot = PipelineStepExecutionFailure.atRoot(1, nested);
    assertEquals(1, PipelineStepExecutionFailure.stepIndex(wrappedAtRoot));
    assertSame(cause, PipelineStepExecutionFailure.source(wrappedAtRoot));

    Throwable propagatedThroughDownstreamRootStep = PipelineStepExecutionFailure.atRoot(3, wrappedAtRoot);
    assertEquals(1, PipelineStepExecutionFailure.stepIndex(propagatedThroughDownstreamRootStep));
    assertSame(cause, PipelineStepExecutionFailure.source(propagatedThroughDownstreamRootStep));
  }
}

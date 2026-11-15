package org.pipelineframework;

import java.util.Objects;
import org.pipelineframework.orchestrator.TransitionResultEnvelope.PairedResult;

record PairedFailedSegment(ClaimedSegment segment, PairedResult result) implements SegmentCommitPlan {
  PairedFailedSegment {
    Objects.requireNonNull(segment, "segment");
    Objects.requireNonNull(result, "result").failure().orElseThrow();
  }
}

package org.pipelineframework;

import java.util.Objects;

import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.awaitable.AwaitUnitOccurrence;

record SuspendedSegment(
    ClaimedSegment segment,
    TransitionAwaitSuspension suspension) implements SegmentCommitPlan {

  SuspendedSegment {
    Objects.requireNonNull(segment, "segment must not be null");
    Objects.requireNonNull(suspension, "suspension must not be null");
    var parent = segment.record();
    if (!parent.tenantId().equals(suspension.tenantId()) || !parent.executionId().equals(suspension.executionId())) {
      throw new IllegalStateException("Foreign Await suspension");
    }
    var occurrence = AwaitUnitOccurrence.fromUnitId(suspension.unitId());
    if (parent.awaitOccurrenceCause().isPresent()) {
      String scope = parent.executionId() + parent.pagingState().map(page -> ":page:" + page.pageIndex()).orElse("");
      if (occurrence.isEmpty() || !occurrence.orElseThrow().occurrenceCause().equals(parent.awaitOccurrenceCause().orElseThrow())
          || !occurrence.orElseThrow().identityScope().equals(scope)) {
        throw new IllegalStateException("Await suspension lacks the retained execution occurrence");
      }
    } else if (occurrence.isPresent()) {
      throw new IllegalStateException("Legacy execution cannot bind a fresh Await occurrence");
    }

  }
}

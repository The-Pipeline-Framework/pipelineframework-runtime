package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.pipelineframework.awaitable.AwaitCompletionCursor;

/** Bounded observations of OPEN projections. Exhaustion is not a cross-page snapshot or a legacy-data census. */
public record AwaitCompletionObservationPage(
    List<AwaitCompletionObservation> observations, Optional<AwaitCompletionCursor> nextCursor,
    int evaluatedEntryCount
) {
    public AwaitCompletionObservationPage {
        observations = List.copyOf(observations);
        Objects.requireNonNull(nextCursor, "nextCursor");
        if (evaluatedEntryCount < observations.size() || evaluatedEntryCount > 100 || evaluatedEntryCount < 0) {
            throw new IllegalArgumentException("Observation page exceeds its evaluated bound");
        }
    }
}

package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;
import org.pipelineframework.awaitable.AwaitInteractionCursor;

/** One bounded current-unit observation. Absence of a checkpoint never releases its parent. */
public record AwaitSemanticSelection(
    Optional<AwaitSemanticCheckpoint> checkpoint,
    Optional<AwaitInteractionCursor> nextCursor,
    boolean cursorReset
) {
    public AwaitSemanticSelection(Optional<AwaitSemanticCheckpoint> checkpoint, Optional<AwaitInteractionCursor> nextCursor) {
        this(checkpoint, nextCursor, false);
    }

    public AwaitSemanticSelection {
        checkpoint = Objects.requireNonNull(checkpoint, "checkpoint");
        nextCursor = Objects.requireNonNull(nextCursor, "nextCursor");
        if (cursorReset && (checkpoint.isPresent() || nextCursor.isPresent())) {
            throw new IllegalArgumentException("A reset observation must restart without a checkpoint or cursor");
        }
    }
}

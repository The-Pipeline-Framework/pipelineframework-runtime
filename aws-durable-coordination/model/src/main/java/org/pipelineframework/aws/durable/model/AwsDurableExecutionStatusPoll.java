package org.pipelineframework.aws.durable.model;

import java.util.Optional;
import java.util.Objects;
import org.pipelineframework.awaitable.AwaitInteractionCursor;

public record AwsDurableExecutionStatusPoll(String status, Optional<AwsDurableAwaitIdentity> awaitIdentity,
    Optional<AwaitInteractionCursor> awaitSelectionCursor) {
    public AwsDurableExecutionStatusPoll(String status, Optional<AwsDurableAwaitIdentity> awaitIdentity) {
        this(status, awaitIdentity, Optional.empty());
    }
    public AwsDurableExecutionStatusPoll(String status) {
        this(status, Optional.empty());
    }

    public AwsDurableExecutionStatusPoll {
        awaitSelectionCursor = Objects.requireNonNull(awaitSelectionCursor, "awaitSelectionCursor");
        awaitIdentity = Objects.requireNonNull(awaitIdentity, "awaitIdentity");
        status = AwsDurableValidation.required(status, "status");
    }

    public boolean succeeded() {
        return "SUCCEEDED".equals(status);
    }

    public boolean terminal() {
        return switch (status) {
            case "SUCCEEDED", "FAILED", "DLQ", "REMOTE_OUTCOME_UNKNOWN" -> true;
            default -> false;
        };
    }
}

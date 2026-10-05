package org.pipelineframework.aws.durable.model;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record AwsDurableActionResponse(
    Optional<AwsDurableExecutionCheckpoint> checkpoint,
    boolean duplicate,
    Optional<String> executionStatus,
    Optional<String> resultJson,
    Optional<AwsDurableAwaitIdentity> awaitIdentity,
    Optional<AwsDurableAwaitCheckpoint> awaitCheckpoint,
    List<AwsDurableAwaitCheckpoint> awaitCheckpoints,
    boolean bindingCreated
) {
    public AwsDurableActionResponse {
        checkpoint = Objects.requireNonNull(checkpoint, "checkpoint");
        executionStatus = AwsDurableValidation.optional(executionStatus, "executionStatus");
        resultJson = AwsDurableValidation.optional(resultJson, "resultJson");
        awaitIdentity = Objects.requireNonNull(awaitIdentity, "awaitIdentity");
        awaitCheckpoint = Objects.requireNonNull(awaitCheckpoint, "awaitCheckpoint");
        awaitCheckpoints = List.copyOf(Objects.requireNonNull(awaitCheckpoints, "awaitCheckpoints"));
    }

    public static AwsDurableActionResponse submitted(AwsDurableExecutionCheckpoint checkpoint, boolean duplicate) {
        return new AwsDurableActionResponse(
            Optional.of(checkpoint), duplicate, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            List.of(), false);
    }

    public static AwsDurableActionResponse status(String status) {
        return new AwsDurableActionResponse(
            Optional.empty(), false, Optional.of(AwsDurableValidation.required(status, "status")),
            Optional.empty(), Optional.empty(), Optional.empty(), List.of(), false);
    }

    public static AwsDurableActionResponse result(String resultJson) {
        return new AwsDurableActionResponse(
            Optional.empty(), false, Optional.empty(),
            Optional.of(AwsDurableValidation.required(resultJson, "resultJson")), Optional.empty(), Optional.empty(),
            List.of(), false);
    }

    public static AwsDurableActionResponse pendingAwait(AwsDurableAwaitIdentity identity) {
        return new AwsDurableActionResponse(
            Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.of(identity), Optional.empty(),
            List.of(), false);
    }

    public static AwsDurableActionResponse awaitCheckpoint(AwsDurableAwaitCheckpoint checkpoint) {
        return new AwsDurableActionResponse(
            Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.of(Objects.requireNonNull(checkpoint, "checkpoint")), List.of(), false);
    }

    public static AwsDurableActionResponse awaitCheckpoints(List<AwsDurableAwaitCheckpoint> checkpoints) {
        return new AwsDurableActionResponse(
            Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            checkpoints, false);
    }

    public static AwsDurableActionResponse bound(boolean created) {
        return new AwsDurableActionResponse(
            Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            List.of(), created);
    }
}

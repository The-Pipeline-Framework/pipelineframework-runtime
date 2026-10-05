package org.pipelineframework.awsproof.model;

import java.util.Objects;
import java.util.Optional;

public record ProofActionResponse(
    Optional<ProofExecutionCheckpoint> checkpoint,
    Optional<String> executionStatus,
    Optional<String> resultJson,
    Optional<ProofAwaitIdentity> awaitIdentity,
    boolean bindingCreated
) {
    public ProofActionResponse {
        checkpoint = Objects.requireNonNull(checkpoint, "checkpoint");
        executionStatus = ProofValidation.optional(executionStatus, "executionStatus");
        resultJson = ProofValidation.optional(resultJson, "resultJson");
        awaitIdentity = Objects.requireNonNull(awaitIdentity, "awaitIdentity");
    }

    public static ProofActionResponse submitted(ProofExecutionCheckpoint checkpoint) {
        return new ProofActionResponse(
            Optional.of(checkpoint), Optional.empty(), Optional.empty(), Optional.empty(), false);
    }

    public static ProofActionResponse status(String status) {
        return new ProofActionResponse(
            Optional.empty(), Optional.of(ProofValidation.required(status, "status")),
            Optional.empty(), Optional.empty(), false);
    }

    public static ProofActionResponse result(String resultJson) {
        return new ProofActionResponse(
            Optional.empty(), Optional.empty(),
            Optional.of(ProofValidation.required(resultJson, "resultJson")), Optional.empty(), false);
    }

    public static ProofActionResponse pendingAwait(ProofAwaitIdentity identity) {
        return new ProofActionResponse(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(identity), false);
    }

    public static ProofActionResponse bound(boolean created) {
        return new ProofActionResponse(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), created);
    }
}

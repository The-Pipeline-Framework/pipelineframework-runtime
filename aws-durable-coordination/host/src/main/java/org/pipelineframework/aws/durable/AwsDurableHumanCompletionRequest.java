package org.pipelineframework.aws.durable;

import java.util.Objects;
import org.pipelineframework.orchestrator.TypedDurablePayload;

/** Canonical completion input for the trusted Action host; carries no token or transport credentials. */
public record AwsDurableHumanCompletionRequest(String idempotencyKey, TypedDurablePayload responsePayload, String actor) {
    public AwsDurableHumanCompletionRequest {
        if (idempotencyKey == null || idempotencyKey.isBlank()) throw new IllegalArgumentException("idempotencyKey is required");
        Objects.requireNonNull(responsePayload, "responsePayload");
        if (actor == null || actor.isBlank()) throw new IllegalArgumentException("actor is required");
    }
}

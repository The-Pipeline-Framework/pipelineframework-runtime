package org.pipelineframework.awsproof.model;

import java.util.Objects;

public record ProofCallbackBinding(
    ProofAwaitIdentity awaitIdentity,
    String providerExecutionName,
    String providerExecutionArn,
    String providerCallbackId,
    ProofBindingStatus status,
    long createdAtEpochMs,
    long expiresAtEpochS
) {
    public ProofCallbackBinding {
        awaitIdentity = Objects.requireNonNull(awaitIdentity, "awaitIdentity");
        providerExecutionName = ProofValidation.required(providerExecutionName, "providerExecutionName");
        providerExecutionArn = ProofValidation.required(providerExecutionArn, "providerExecutionArn");
        providerCallbackId = ProofValidation.required(providerCallbackId, "providerCallbackId");
        status = Objects.requireNonNull(status, "status");
        if (createdAtEpochMs < 0 || expiresAtEpochS < 0) {
            throw new IllegalArgumentException("binding timestamps must be non-negative");
        }
    }

    public String partitionKey() {
        return awaitIdentity.tenantId() + "#" + awaitIdentity.interactionId();
    }

    public String sortKey() {
        return generationSortKey(awaitIdentity.generation());
    }

    public static String generationSortKey(long generation) {
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
        return "GEN#" + String.format(java.util.Locale.ROOT, "%020d", generation);
    }
}

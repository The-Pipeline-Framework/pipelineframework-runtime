package org.pipelineframework.awsproof.model;

import java.util.Objects;

/** Provider-owned callback capability registered before a TPF Await identity exists. */
public record ProofCallbackRegistration(
    ProofDriverCheckpoint checkpoint,
    String providerExecutionName,
    String providerExecutionArn,
    String providerCallbackId,
    long createdAtEpochMs,
    long expiresAtEpochS
) {
    public ProofCallbackRegistration {
        checkpoint = Objects.requireNonNull(checkpoint, "checkpoint");
        providerExecutionName = ProofValidation.required(providerExecutionName, "providerExecutionName");
        providerExecutionArn = ProofValidation.required(providerExecutionArn, "providerExecutionArn");
        providerCallbackId = ProofValidation.required(providerCallbackId, "providerCallbackId");
        if (createdAtEpochMs < 0) {
            throw new IllegalArgumentException("createdAtEpochMs must be non-negative");
        }
        if (expiresAtEpochS <= 0) {
            throw new IllegalArgumentException("expiresAtEpochS must be positive");
        }
    }

    public String partitionKey() {
        return checkpoint.tenantId() + "#" + checkpoint.executionId();
    }

    public String sortKey() {
        return registrationSortKey(checkpoint.generation());
    }

    public ProofCallbackBinding bind(ProofAwaitIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        if (!checkpoint.tenantId().equals(identity.tenantId())
            || !checkpoint.executionId().equals(identity.executionId())
            || checkpoint.generation() != identity.generation()) {
            throw new IllegalArgumentException("Await identity does not belong to callback registration");
        }
        return new ProofCallbackBinding(
            identity,
            providerExecutionName,
            providerExecutionArn,
            providerCallbackId,
            ProofBindingStatus.OPEN,
            createdAtEpochMs,
            expiresAtEpochS);
    }

    public static String registrationSortKey(long generation) {
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
        return String.format(java.util.Locale.ROOT, "REGISTRATION#%020d", generation);
    }
}

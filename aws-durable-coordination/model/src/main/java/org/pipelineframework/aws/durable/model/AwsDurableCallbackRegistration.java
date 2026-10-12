package org.pipelineframework.aws.durable.model;

import java.util.Objects;
import java.util.Optional;

/** Disposable provider callback capability, pinned to an observed semantic Await in schema version 3. */
public record AwsDurableCallbackRegistration(
    AwsDurableDriverCheckpoint checkpoint,
    String providerExecutionName,
    String providerExecutionArn,
    String providerCallbackId,
    long createdAtEpochMs,
    long expiresAtEpochS,
    Optional<AwsDurableAwaitIdentity> awaitIdentity
) {
    public AwsDurableCallbackRegistration(AwsDurableDriverCheckpoint checkpoint, String providerExecutionName,
        String providerExecutionArn, String providerCallbackId, long createdAtEpochMs, long expiresAtEpochS) {
        this(checkpoint, providerExecutionName, providerExecutionArn, providerCallbackId,
            createdAtEpochMs, expiresAtEpochS, Optional.empty());
    }

    public AwsDurableCallbackRegistration {
        awaitIdentity = Objects.requireNonNull(awaitIdentity, "awaitIdentity");
        checkpoint = Objects.requireNonNull(checkpoint, "checkpoint");
        providerExecutionName = AwsDurableValidation.required(providerExecutionName, "providerExecutionName");
        providerExecutionArn = AwsDurableValidation.required(providerExecutionArn, "providerExecutionArn");
        providerCallbackId = AwsDurableValidation.required(providerCallbackId, "providerCallbackId");
        AwsDurableDriverCheckpoint owner = checkpoint;
        if (awaitIdentity.filter(identity -> !owner.tenantId().equals(identity.tenantId())
            || !owner.executionId().equals(identity.executionId())
            || owner.generation() != identity.generation()).isPresent()) {
            throw new IllegalArgumentException("registration identity does not belong to execution checkpoint");
        }
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
        return registrationSortKey(checkpoint.generation(), occurrence());
    }

    public AwsDurableCallbackBinding bind(AwsDurableAwaitIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        if (!checkpoint.tenantId().equals(identity.tenantId())
            || !checkpoint.executionId().equals(identity.executionId())
            || checkpoint.generation() != identity.generation()
            || occurrence() != identity.occurrence()
            || awaitIdentity.filter(pinned -> !pinned.equals(identity)).isPresent()) {
            throw new IllegalArgumentException("Await identity does not belong to callback registration");
        }
        return new AwsDurableCallbackBinding(
            identity,
            providerExecutionName,
            providerExecutionArn,
            providerCallbackId,
            AwsDurableBindingStatus.OPEN,
            createdAtEpochMs,
            expiresAtEpochS);
    }

    public long occurrence() {
        return awaitIdentity.map(AwsDurableAwaitIdentity::occurrence).orElse(1L);
    }

    public static String registrationSortKey(long generation, long occurrence) {
        if (occurrence < 1) throw new IllegalArgumentException("occurrence must be positive");
        return registrationSortKey(generation) + (occurrence == 1 ? "" :
            String.format(java.util.Locale.ROOT, "#OCCURRENCE#%020d", occurrence));
    }

    public static String registrationSortKey(long generation) {
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
        return String.format(java.util.Locale.ROOT, "REGISTRATION#%020d", generation);
    }
}

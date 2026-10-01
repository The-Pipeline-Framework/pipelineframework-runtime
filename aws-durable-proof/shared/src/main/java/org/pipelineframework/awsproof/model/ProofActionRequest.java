package org.pipelineframework.awsproof.model;

import java.util.Objects;
import java.util.Optional;

public record ProofActionRequest(
    ProofOperation operation,
    String tenantId,
    Optional<String> executionId,
    Optional<String> idempotencyKey,
    Optional<String> pipelineId,
    Optional<String> contractVersion,
    Optional<String> releaseVersion,
    Optional<String> inputJson,
    Optional<ProofAwaitIdentity> awaitIdentity,
    Optional<String> providerExecutionName,
    Optional<String> providerExecutionArn,
    Optional<String> providerCallbackId,
    Optional<Long> expectedVersion,
    Optional<String> reason,
    long generation
) {
    public ProofActionRequest {
        operation = Objects.requireNonNull(operation, "operation");
        tenantId = ProofValidation.required(tenantId, "tenantId");
        executionId = ProofValidation.optional(executionId, "executionId");
        idempotencyKey = ProofValidation.optional(idempotencyKey, "idempotencyKey");
        pipelineId = ProofValidation.optional(pipelineId, "pipelineId");
        contractVersion = ProofValidation.optional(contractVersion, "contractVersion");
        releaseVersion = ProofValidation.optional(releaseVersion, "releaseVersion");
        inputJson = ProofValidation.optional(inputJson, "inputJson");
        awaitIdentity = Objects.requireNonNull(awaitIdentity, "awaitIdentity");
        providerExecutionName = ProofValidation.optional(providerExecutionName, "providerExecutionName");
        providerExecutionArn = ProofValidation.optional(providerExecutionArn, "providerExecutionArn");
        providerCallbackId = ProofValidation.optional(providerCallbackId, "providerCallbackId");
        expectedVersion = Objects.requireNonNull(expectedVersion, "expectedVersion");
        reason = ProofValidation.optional(reason, "reason");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }

    public static ProofActionRequest submit(ProofExecutionInput input) {
        return new ProofActionRequest(
            ProofOperation.SUBMIT,
            input.tenantId(),
            Optional.empty(),
            Optional.of(input.idempotencyKey()),
            Optional.of(input.pipelineId()),
            Optional.of(input.contractVersion()),
            Optional.of(input.releaseVersion()),
            Optional.of(input.inputJson()),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            input.generation());
    }

    public static ProofActionRequest status(ProofExecutionCheckpoint checkpoint) {
        return forExecution(ProofOperation.STATUS, checkpoint);
    }

    public static ProofActionRequest result(ProofExecutionCheckpoint checkpoint) {
        return forExecution(ProofOperation.RESULT, checkpoint);
    }

    public static ProofActionRequest redrive(
        ProofExecutionCheckpoint checkpoint,
        long expectedVersion,
        String reason
    ) {
        if (expectedVersion < 0) {
            throw new IllegalArgumentException("expectedVersion must be non-negative");
        }
        ProofActionRequest base = forExecution(ProofOperation.REDRIVE, checkpoint);
        return new ProofActionRequest(
            base.operation(), base.tenantId(), base.executionId(), base.idempotencyKey(), base.pipelineId(),
            base.contractVersion(), base.releaseVersion(), base.inputJson(), base.awaitIdentity(),
            base.providerExecutionName(), base.providerExecutionArn(), base.providerCallbackId(),
            Optional.of(expectedVersion), Optional.of(ProofValidation.required(reason, "reason")), base.generation());
    }

    public static ProofActionRequest sweep(ProofExecutionCheckpoint checkpoint) {
        return forExecution(ProofOperation.SWEEP, checkpoint);
    }

    public static ProofActionRequest pendingAwait(ProofExecutionCheckpoint checkpoint) {
        return forExecution(ProofOperation.QUERY_PENDING_AWAIT, checkpoint);
    }

    public static ProofActionRequest bind(
        ProofAwaitIdentity awaitIdentity,
        String providerExecutionName,
        String providerExecutionArn,
        String providerCallbackId
    ) {
        return new ProofActionRequest(
            ProofOperation.BIND_CALLBACK,
            awaitIdentity.tenantId(),
            Optional.of(awaitIdentity.executionId()),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(awaitIdentity),
            Optional.of(ProofValidation.required(providerExecutionName, "providerExecutionName")),
            Optional.of(ProofValidation.required(providerExecutionArn, "providerExecutionArn")),
            Optional.of(ProofValidation.required(providerCallbackId, "providerCallbackId")),
            Optional.empty(),
            Optional.empty(),
            awaitIdentity.generation());
    }

    public static ProofActionRequest register(
        ProofExecutionCheckpoint checkpoint,
        String providerExecutionName,
        String providerExecutionArn,
        String providerCallbackId
    ) {
        ProofActionRequest base = forExecution(ProofOperation.REGISTER_CALLBACK, checkpoint);
        return new ProofActionRequest(
            base.operation(), base.tenantId(), base.executionId(), base.idempotencyKey(), base.pipelineId(),
            base.contractVersion(), base.releaseVersion(), base.inputJson(), base.awaitIdentity(),
            Optional.of(ProofValidation.required(providerExecutionName, "providerExecutionName")),
            Optional.of(ProofValidation.required(providerExecutionArn, "providerExecutionArn")),
            Optional.of(ProofValidation.required(providerCallbackId, "providerCallbackId")),
            base.expectedVersion(), base.reason(), base.generation());
    }

    private static ProofActionRequest forExecution(ProofOperation operation, ProofExecutionCheckpoint checkpoint) {
        return new ProofActionRequest(
            operation,
            checkpoint.tenantId(),
            Optional.of(checkpoint.executionId()),
            Optional.empty(),
            Optional.of(checkpoint.pipelineId()),
            Optional.of(checkpoint.contractVersion()),
            Optional.of(checkpoint.releaseVersion()),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            checkpoint.generation());
    }
}

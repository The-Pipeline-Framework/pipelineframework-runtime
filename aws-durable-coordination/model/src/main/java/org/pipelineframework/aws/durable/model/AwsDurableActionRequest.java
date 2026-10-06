package org.pipelineframework.aws.durable.model;

import java.util.Objects;
import java.util.Optional;

public record AwsDurableActionRequest(
    AwsDurableOperation operation,
    String tenantId,
    Optional<String> executionId,
    Optional<String> idempotencyKey,
    Optional<String> pipelineId,
    Optional<String> contractVersion,
    Optional<String> releaseVersion,
    Optional<String> inputJson,
    Optional<AwsDurableAwaitIdentity> awaitIdentity,
    Optional<String> providerExecutionName,
    Optional<String> providerExecutionArn,
    Optional<String> providerCallbackId,
    Optional<Long> expectedVersion,
    Optional<String> reason,
    long generation
) {
    public AwsDurableActionRequest {
        operation = Objects.requireNonNull(operation, "operation");
        tenantId = AwsDurableValidation.required(tenantId, "tenantId");
        executionId = AwsDurableValidation.optional(executionId, "executionId");
        idempotencyKey = AwsDurableValidation.optional(idempotencyKey, "idempotencyKey");
        pipelineId = AwsDurableValidation.optional(pipelineId, "pipelineId");
        contractVersion = AwsDurableValidation.optional(contractVersion, "contractVersion");
        releaseVersion = AwsDurableValidation.optional(releaseVersion, "releaseVersion");
        inputJson = AwsDurableValidation.optional(inputJson, "inputJson");
        awaitIdentity = Objects.requireNonNull(awaitIdentity, "awaitIdentity");
        providerExecutionName = AwsDurableValidation.optional(providerExecutionName, "providerExecutionName");
        providerExecutionArn = AwsDurableValidation.optional(providerExecutionArn, "providerExecutionArn");
        providerCallbackId = AwsDurableValidation.optional(providerCallbackId, "providerCallbackId");
        expectedVersion = Objects.requireNonNull(expectedVersion, "expectedVersion");
        reason = AwsDurableValidation.optional(reason, "reason");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }

    public static AwsDurableActionRequest submit(AwsDurableExecutionInput input) {
        return new AwsDurableActionRequest(
            AwsDurableOperation.SUBMIT,
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

    public static AwsDurableActionRequest status(AwsDurableDriverCheckpoint checkpoint) {
        return forExecution(AwsDurableOperation.STATUS, checkpoint);
    }

    public static AwsDurableActionRequest result(AwsDurableDriverCheckpoint checkpoint) {
        return forExecution(AwsDurableOperation.RESULT, checkpoint);
    }

    public static AwsDurableActionRequest redrive(
        AwsDurableDriverCheckpoint checkpoint,
        long expectedVersion,
        String reason
    ) {
        if (expectedVersion < 0) {
            throw new IllegalArgumentException("expectedVersion must be non-negative");
        }
        AwsDurableActionRequest base = forExecution(AwsDurableOperation.REDRIVE, checkpoint);
        return new AwsDurableActionRequest(
            base.operation(), base.tenantId(), base.executionId(), base.idempotencyKey(), base.pipelineId(),
            base.contractVersion(), base.releaseVersion(), base.inputJson(), base.awaitIdentity(),
            base.providerExecutionName(), base.providerExecutionArn(), base.providerCallbackId(),
            Optional.of(expectedVersion), Optional.of(AwsDurableValidation.required(reason, "reason")), base.generation());
    }

    public static AwsDurableActionRequest sweep(AwsDurableDriverCheckpoint checkpoint) {
        return forExecution(AwsDurableOperation.SWEEP, checkpoint);
    }

    public static AwsDurableActionRequest pendingAwait(AwsDurableDriverCheckpoint checkpoint) {
        return forExecution(AwsDurableOperation.QUERY_PENDING_AWAIT, checkpoint);
    }

    public static AwsDurableActionRequest awaitCheckpoint(AwsDurableAwaitIdentity identity) {
        return new AwsDurableActionRequest(
            AwsDurableOperation.READ_AWAIT_CHECKPOINT,
            identity.tenantId(),
            Optional.of(identity.executionId()),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(identity),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            identity.generation());
    }

    public static AwsDurableActionRequest executionAwaits(AwsDurableDriverCheckpoint checkpoint) {
        return forExecution(AwsDurableOperation.READ_EXECUTION_AWAITS, checkpoint);
    }

    public static AwsDurableActionRequest bind(
        AwsDurableAwaitIdentity awaitIdentity,
        String providerExecutionName,
        String providerExecutionArn,
        String providerCallbackId
    ) {
        return new AwsDurableActionRequest(
            AwsDurableOperation.BIND_CALLBACK,
            awaitIdentity.tenantId(),
            Optional.of(awaitIdentity.executionId()),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(awaitIdentity),
            Optional.of(AwsDurableValidation.required(providerExecutionName, "providerExecutionName")),
            Optional.of(AwsDurableValidation.required(providerExecutionArn, "providerExecutionArn")),
            Optional.of(AwsDurableValidation.required(providerCallbackId, "providerCallbackId")),
            Optional.empty(),
            Optional.empty(),
            awaitIdentity.generation());
    }

    public static AwsDurableActionRequest register(
        AwsDurableDriverCheckpoint checkpoint,
        String providerExecutionName,
        String providerExecutionArn,
        String providerCallbackId
    ) {
        AwsDurableActionRequest base = forExecution(AwsDurableOperation.REGISTER_CALLBACK, checkpoint);
        return new AwsDurableActionRequest(
            base.operation(), base.tenantId(), base.executionId(), base.idempotencyKey(), base.pipelineId(),
            base.contractVersion(), base.releaseVersion(), base.inputJson(), base.awaitIdentity(),
            Optional.of(AwsDurableValidation.required(providerExecutionName, "providerExecutionName")),
            Optional.of(AwsDurableValidation.required(providerExecutionArn, "providerExecutionArn")),
            Optional.of(AwsDurableValidation.required(providerCallbackId, "providerCallbackId")),
            base.expectedVersion(), base.reason(), base.generation());
    }

    private static AwsDurableActionRequest forExecution(AwsDurableOperation operation, AwsDurableDriverCheckpoint checkpoint) {
        return new AwsDurableActionRequest(
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

package org.pipelineframework.aws.durable.model;

import java.util.Objects;
import java.util.Optional;
import org.pipelineframework.awaitable.AwaitInteractionCursor;

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
    long generation,
    Optional<AwaitInteractionCursor> awaitSelectionCursor,
    Optional<org.pipelineframework.awaitable.AwaitCompletionCursor> completionCursor,
    Optional<Integer> completionPageLimit
) {
    public AwsDurableActionRequest(
        AwsDurableOperation operation, String tenantId, Optional<String> executionId, Optional<String> idempotencyKey,
        Optional<String> pipelineId, Optional<String> contractVersion, Optional<String> releaseVersion,
        Optional<String> inputJson, Optional<AwsDurableAwaitIdentity> awaitIdentity,
        Optional<String> providerExecutionName, Optional<String> providerExecutionArn, Optional<String> providerCallbackId,
        Optional<Long> expectedVersion, Optional<String> reason, long generation,
        Optional<AwaitInteractionCursor> awaitSelectionCursor) {
        this(operation, tenantId, executionId, idempotencyKey, pipelineId, contractVersion, releaseVersion, inputJson,
            awaitIdentity, providerExecutionName, providerExecutionArn, providerCallbackId, expectedVersion, reason,
            generation, awaitSelectionCursor, Optional.empty(), Optional.empty());
    }

    public AwsDurableActionRequest(
        AwsDurableOperation operation, String tenantId, Optional<String> executionId, Optional<String> idempotencyKey,
        Optional<String> pipelineId, Optional<String> contractVersion, Optional<String> releaseVersion,
        Optional<String> inputJson, Optional<AwsDurableAwaitIdentity> awaitIdentity,
        Optional<String> providerExecutionName, Optional<String> providerExecutionArn, Optional<String> providerCallbackId,
        Optional<Long> expectedVersion, Optional<String> reason, long generation) {
        this(operation, tenantId, executionId, idempotencyKey, pipelineId, contractVersion, releaseVersion, inputJson,
            awaitIdentity, providerExecutionName, providerExecutionArn, providerCallbackId, expectedVersion, reason,
            generation, Optional.empty());
    }

    public AwsDurableActionRequest withAwaitSelectionCursor(Optional<AwaitInteractionCursor> cursor) {
        return new AwsDurableActionRequest(operation, tenantId, executionId, idempotencyKey, pipelineId, contractVersion,
            releaseVersion, inputJson, awaitIdentity, providerExecutionName, providerExecutionArn, providerCallbackId,
            expectedVersion, reason, generation, cursor, completionCursor, completionPageLimit);
    }

    public AwsDurableActionRequest withCompletionSelection(
        int limit, Optional<org.pipelineframework.awaitable.AwaitCompletionCursor> cursor) {
        return new AwsDurableActionRequest(operation, tenantId, executionId, idempotencyKey, pipelineId, contractVersion,
            releaseVersion, inputJson, awaitIdentity, providerExecutionName, providerExecutionArn, providerCallbackId,
            expectedVersion, reason, generation, awaitSelectionCursor, cursor, Optional.of(limit));
    }

    public static AwsDurableActionRequest completionQuery(String tenantId, int limit,
        Optional<org.pipelineframework.awaitable.AwaitCompletionCursor> cursor) {
        return new AwsDurableActionRequest(AwsDurableOperation.QUERY_AWAIT_COMPLETIONS, tenantId,
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), 1)
            .withCompletionSelection(limit, cursor);
    }

    public static AwsDurableActionRequest completionObservation(AwsDurableAwaitIdentity identity) {
        var base = awaitCheckpoint(identity);
        return new AwsDurableActionRequest(AwsDurableOperation.READ_AWAIT_COMPLETION, base.tenantId(), base.executionId(),
            base.idempotencyKey(), base.pipelineId(), base.contractVersion(), base.releaseVersion(), base.inputJson(),
            base.awaitIdentity(), base.providerExecutionName(), base.providerExecutionArn(), base.providerCallbackId(),
            base.expectedVersion(), base.reason(), base.generation());
    }

    public static AwsDurableActionRequest awaitResumed(AwsDurableAwaitIdentity identity,
        AwsDurableDriverCheckpoint checkpoint, String providerExecutionArn) {
        Objects.requireNonNull(identity, "identity");
        var base = forExecution(AwsDurableOperation.RECORD_AWAIT_RESUMED, checkpoint);
        return new AwsDurableActionRequest(AwsDurableOperation.RECORD_AWAIT_RESUMED,
            base.tenantId(), base.executionId(), base.idempotencyKey(), base.pipelineId(),
            base.contractVersion(), base.releaseVersion(), Optional.empty(), Optional.of(identity), Optional.empty(),
            Optional.of(providerExecutionArn), Optional.empty(), Optional.empty(), Optional.empty(), base.generation());
    }

    public AwsDurableActionRequest {
        completionCursor = Objects.requireNonNull(completionCursor, "completionCursor");
        completionPageLimit = Objects.requireNonNull(completionPageLimit, "completionPageLimit");
        completionPageLimit.ifPresent(value -> {
            if (value < 1 || value > 100) throw new IllegalArgumentException("Completion page limit must be between 1 and 100");
        });
        awaitSelectionCursor = Objects.requireNonNull(awaitSelectionCursor, "awaitSelectionCursor");
        operation = Objects.requireNonNull(operation, "operation");
        tenantId = AwsDurableValidation.required(tenantId, "tenantId");
        if (completionCursor.isPresent()) completionCursor.orElseThrow().requireScope(tenantId);
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

    public static AwsDurableActionRequest registerAdmission(AwsDurableExecutionInput input, String name, String arn) {
        return new AwsDurableActionRequest(AwsDurableOperation.REGISTER_EXECUTION, input.tenantId(),
            input.resumeExecutionId(), Optional.of(input.idempotencyKey()), Optional.of(input.pipelineId()),
            Optional.of(input.contractVersion()), Optional.of(input.releaseVersion()), Optional.empty(), Optional.empty(),
            Optional.of(name), Optional.of(arn), Optional.empty(), Optional.empty(), Optional.empty(), input.generation());
    }

    public static AwsDurableActionRequest registerExecution(AwsDurableDriverCheckpoint checkpoint, String name, String arn) {
        var base = forExecution(AwsDurableOperation.REGISTER_EXECUTION, checkpoint);
        return new AwsDurableActionRequest(base.operation(), base.tenantId(), base.executionId(), base.idempotencyKey(),
            base.pipelineId(), base.contractVersion(), base.releaseVersion(), base.inputJson(), base.awaitIdentity(),
            Optional.of(name), Optional.of(arn), base.providerCallbackId(), base.expectedVersion(), base.reason(), base.generation());
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

    public static AwsDurableActionRequest register(
        AwsDurableDriverCheckpoint checkpoint, AwsDurableAwaitIdentity identity,
        String providerExecutionName, String providerExecutionArn, String providerCallbackId) {
        AwsDurableActionRequest base = register(checkpoint, providerExecutionName, providerExecutionArn, providerCallbackId);
        return new AwsDurableActionRequest(base.operation(), base.tenantId(), base.executionId(), base.idempotencyKey(),
            base.pipelineId(), base.contractVersion(), base.releaseVersion(), base.inputJson(), Optional.of(identity),
            base.providerExecutionName(), base.providerExecutionArn(), base.providerCallbackId(),
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

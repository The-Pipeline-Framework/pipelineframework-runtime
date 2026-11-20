package org.pipelineframework.orchestrator;

/** Exact retained interaction projection; contains no callback credentials or transport metadata. */
public record AwaitInteractionView(AwaitSemanticCheckpoint checkpoint, TypedDurablePayload requestPayload,
    String completionPayloadTypeId, long deadlineEpochMs, String transportType, boolean commandCallback) {}

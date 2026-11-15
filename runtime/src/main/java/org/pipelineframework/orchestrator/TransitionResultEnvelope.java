package org.pipelineframework.orchestrator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Runtime-local transition result. Remote transports exchange {@link TransitionWireResult}.
 *
 * @param outcome transition outcome
 * @param outputPayloads serialized output payloads
 * @param awaitSuspension await suspension metadata when waiting externally
 * @param failure failure metadata when failed
 */
public record TransitionResultEnvelope(
    TransitionWorkerOutcome outcome,
    List<SerializedTransitionPayload> outputPayloads,
    TransitionAwaitSuspension awaitSuspension,
    TransitionFailureEnvelope failure,
    @JsonIgnore List<?> decodedOutputItems,
    boolean terminalOutputPublished,
    boolean terminalInputPassthrough,
    Optional<PagedTransitionCompletion> pageCompletion,
    @JsonIgnore Optional<PairedResult> pairedResult) {

    /** Local coordinator context retained from the authenticated request. */
    public record PairedResult(NativeWorkerReleaseBinding.Attempt attempt, Optional<PortableFailure> failure) {
        public PairedResult {
            Objects.requireNonNull(attempt, "attempt");
            Objects.requireNonNull(failure, "failure");
        }
    }

    /** Validated outcome data with the logical step resolved against the pinned Contract. */
    public record PortableFailure(Code code, int stepIndex) {
        public PortableFailure {
            Objects.requireNonNull(code, "code");
            if (stepIndex < 0) {
                throw new IllegalArgumentException("Portable failure requires a Contract step index");
            }
        }

        public enum Code {
            TRANSIENT_EXHAUSTED("Local retry budget exhausted"),
            BUSINESS_REJECTION("Business function rejected input"),
            UNCLASSIFIED("Unclassified business function failure");

            private final String diagnostic;

            Code(String diagnostic) {
                this.diagnostic = diagnostic;
            }

            public String diagnostic() {
                return diagnostic;
            }
        }
    }

    public TransitionResultEnvelope(
        TransitionWorkerOutcome outcome,
        List<SerializedTransitionPayload> outputPayloads,
        TransitionAwaitSuspension awaitSuspension,
        TransitionFailureEnvelope failure,
        List<?> decodedOutputItems,
        boolean terminalOutputPublished,
        boolean terminalInputPassthrough,
        Optional<PagedTransitionCompletion> pageCompletion) {
        this(outcome, outputPayloads, awaitSuspension, failure, decodedOutputItems,
            terminalOutputPublished, terminalInputPassthrough, pageCompletion, Optional.empty());
    }
    public TransitionResultEnvelope(
        TransitionWorkerOutcome outcome,
        List<SerializedTransitionPayload> outputPayloads,
        TransitionAwaitSuspension awaitSuspension,
        TransitionFailureEnvelope failure,
        List<?> decodedOutputItems,
        boolean terminalOutputPublished,
        boolean terminalInputPassthrough) {
        this(outcome, outputPayloads, awaitSuspension, failure, decodedOutputItems,
            terminalOutputPublished, terminalInputPassthrough, Optional.empty());
    }
    public TransitionResultEnvelope(
        TransitionWorkerOutcome outcome,
        List<SerializedTransitionPayload> outputPayloads,
        TransitionAwaitSuspension awaitSuspension,
        TransitionFailureEnvelope failure) {
        this(outcome, outputPayloads, awaitSuspension, failure, null, false, false);
    }

    public TransitionResultEnvelope(
        TransitionWorkerOutcome outcome,
        List<SerializedTransitionPayload> outputPayloads,
        TransitionAwaitSuspension awaitSuspension,
        TransitionFailureEnvelope failure,
        List<?> decodedOutputItems) {
        this(outcome, outputPayloads, awaitSuspension, failure, decodedOutputItems, false, false);
    }

    public TransitionResultEnvelope {
        Objects.requireNonNull(outcome, "TransitionResultEnvelope.outcome must not be null");
        outputPayloads = outputPayloads == null ? List.of() : List.copyOf(outputPayloads);
        decodedOutputItems = decodedOutputItems == null ? null : List.copyOf(decodedOutputItems);
        pageCompletion = Optional.ofNullable(pageCompletion).orElseGet(Optional::empty);
        pairedResult = Optional.ofNullable(pairedResult).orElseGet(Optional::empty);
        if (pairedResult.isPresent()) {
            boolean pairedFailure = pairedResult.orElseThrow().failure().isPresent();
            if (awaitSuspension != null || failure != null || decodedOutputItems != null
                || terminalOutputPublished || terminalInputPassthrough || pageCompletion.isPresent()
                || (pairedFailure && (outcome != TransitionWorkerOutcome.FAILED || !outputPayloads.isEmpty()))
                || (!pairedFailure && (outcome != TransitionWorkerOutcome.COMPLETED || outputPayloads.size() != 1))) {
                throw new IllegalArgumentException("Invalid paired transition result");
            }
        }
        if (outcome == TransitionWorkerOutcome.WAITING_EXTERNAL && awaitSuspension == null) {
            throw new IllegalArgumentException("WAITING_EXTERNAL transition envelope requires awaitSuspension");
        }
        if (outcome == TransitionWorkerOutcome.FAILED && failure == null && pairedResult.isEmpty()) {
            throw new IllegalArgumentException("FAILED transition envelope requires failure");
        }
        if (outcome == TransitionWorkerOutcome.COMPLETED && !outputPayloads.isEmpty() && decodedOutputItems != null) {
            throw new IllegalArgumentException("COMPLETED transition envelope must not include both encoded and decoded outputs");
        }
        if (outcome == TransitionWorkerOutcome.COMPLETED && awaitSuspension != null) {
            throw new IllegalArgumentException("COMPLETED transition envelope must not include awaitSuspension");
        }
        if (outcome == TransitionWorkerOutcome.COMPLETED && failure != null) {
            throw new IllegalArgumentException("COMPLETED transition envelope must not include failure");
        }
        if (outcome != TransitionWorkerOutcome.COMPLETED && terminalOutputPublished) {
            throw new IllegalArgumentException("Only COMPLETED transition envelopes may mark terminal output as published");
        }
        if (outcome != TransitionWorkerOutcome.COMPLETED && terminalInputPassthrough) {
            throw new IllegalArgumentException("Only COMPLETED transition envelopes may retain terminal input");
        }
        if (outcome != TransitionWorkerOutcome.COMPLETED && pageCompletion.isPresent()) {
            throw new IllegalArgumentException("Only COMPLETED transition envelopes may include page completion");
        }
        if (terminalOutputPublished && terminalInputPassthrough) {
            throw new IllegalArgumentException("A terminal transition cannot publish output and retain terminal input");
        }
        if (terminalInputPassthrough && (!outputPayloads.isEmpty() || decodedOutputItems != null)) {
            throw new IllegalArgumentException(
                "A terminal input passthrough transition must not include output items");
        }
        if (outcome == TransitionWorkerOutcome.WAITING_EXTERNAL && (!outputPayloads.isEmpty() || failure != null || decodedOutputItems != null)) {
            throw new IllegalArgumentException("WAITING_EXTERNAL transition envelope must only include awaitSuspension");
        }
        if (outcome == TransitionWorkerOutcome.FAILED && (!outputPayloads.isEmpty() || awaitSuspension != null || decodedOutputItems != null)) {
            throw new IllegalArgumentException("FAILED transition envelope must only include failure");
        }
    }

    /**
     * Creates a completed envelope by encoding decoded output items.
     *
     * @param codec payload codec
     * @param outputItems decoded output items
     * @return completed envelope
     */
    public static TransitionResultEnvelope completed(TransitionPayloadCodec codec, List<?> outputItems) {
        return completed(codec, outputItems, false);
    }

    public static TransitionResultEnvelope completed(
        TransitionPayloadCodec codec,
        List<?> outputItems,
        boolean terminalOutputPublished) {
        Objects.requireNonNull(codec, "codec must not be null");
        List<SerializedTransitionPayload> encoded = outputItems == null
            ? List.of()
            : outputItems.stream().map(codec::encode).toList();
        return new TransitionResultEnvelope(
            TransitionWorkerOutcome.COMPLETED,
            encoded,
            null,
            null,
            null,
            terminalOutputPublished,
            false);
    }

    /**
     * Creates a completed in-process envelope with decoded output items.
     *
     * @param outputItems decoded output items
     * @return completed envelope
     */
    public static TransitionResultEnvelope completedInProcess(List<?> outputItems) {
        return completedInProcess(outputItems, false);
    }

    public static TransitionResultEnvelope completedInProcess(List<?> outputItems, boolean terminalOutputPublished) {
        return new TransitionResultEnvelope(
            TransitionWorkerOutcome.COMPLETED,
            List.of(),
            null,
            null,
            outputItems == null ? List.of() : outputItems,
            terminalOutputPublished,
            false);
    }

    /** Adds provider-validated page progress to an otherwise completed result. */
    public TransitionResultEnvelope withPageCompletion(PagedTransitionCompletion completion) {
        if (outcome != TransitionWorkerOutcome.COMPLETED) {
            throw new IllegalStateException("page completion requires a completed transition");
        }
        return new TransitionResultEnvelope(outcome, outputPayloads, awaitSuspension, failure,
            decodedOutputItems, terminalOutputPublished, terminalInputPassthrough,
            Optional.of(Objects.requireNonNull(completion, "completion must not be null")), pairedResult);
    }

    /**
     * Creates a completed envelope for a terminal transition whose input is already materialized
     * at the coordinator. The worker performs no work and does not copy the materialized payload
     * across the worker boundary.
     *
     * @return completed terminal pass-through envelope
     */
    public static TransitionResultEnvelope completedTerminalInputPassthrough() {
        return new TransitionResultEnvelope(
            TransitionWorkerOutcome.COMPLETED,
            List.of(),
            null,
            null,
            null,
            false,
            true);
    }

    /**
     * Creates a waiting envelope.
     *
     * @param awaitSuspension await suspension metadata
     * @return waiting envelope
     */
    public static TransitionResultEnvelope waiting(TransitionAwaitSuspension awaitSuspension) {
        return new TransitionResultEnvelope(
            TransitionWorkerOutcome.WAITING_EXTERNAL,
            List.of(),
            awaitSuspension,
            null);
    }

    /**
     * Creates a failed envelope.
     *
     * @param failure transition failure
     * @return failed envelope
     */
    public static TransitionResultEnvelope failed(Throwable failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        return new TransitionResultEnvelope(
            TransitionWorkerOutcome.FAILED,
            List.of(),
            null,
            TransitionFailureRuntimeAdapter.from(failure, -1));
    }

    public static TransitionResultEnvelope failed(Throwable failure, int failedStepIndex) {
        return new TransitionResultEnvelope(
            TransitionWorkerOutcome.FAILED,
            List.of(),
            null,
            TransitionFailureRuntimeAdapter.from(failure, failedStepIndex),
            null,
            false,
            false);
    }

    public static TransitionResultEnvelope failed(TransitionFailureEnvelope failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        return new TransitionResultEnvelope(
            TransitionWorkerOutcome.FAILED,
            List.of(),
            null,
            failure);
    }

    static TransitionResultEnvelope pairedCompleted(NativeWorkerReleaseBinding.Attempt attempt,
            SerializedTransitionPayload output) {
        return new TransitionResultEnvelope(TransitionWorkerOutcome.COMPLETED, List.of(output), null, null, null,
            false, false, Optional.empty(), Optional.of(new PairedResult(attempt, Optional.empty())));
    }

    static TransitionResultEnvelope pairedFailed(NativeWorkerReleaseBinding.Attempt attempt, PortableFailure failure) {
        return new TransitionResultEnvelope(TransitionWorkerOutcome.FAILED, List.of(), null, null, null,
            false, false, Optional.empty(), Optional.of(new PairedResult(attempt, Optional.of(failure))));
    }

    /**
     * Converts this runtime-local result to its portable transport representation.
     *
     * @return portable transition result
     */
    public TransitionWireResult toWireResult() {
        if (pairedResult.flatMap(PairedResult::failure).isPresent()) {
            throw new IllegalStateException("A portable failure requires its paired result profile");
        }
        if (decodedOutputItems != null) {
            throw new IllegalStateException(
                "A transition result with decoded output items cannot cross a remote worker boundary");
        }
        return new TransitionWireResult(
            outcome,
            outputPayloads,
            awaitSuspension,
            failure,
            terminalOutputPublished,
            terminalInputPassthrough,
            pageCompletion);
    }

    /**
     * Converts a portable transport result to the runtime-local representation.
     *
     * @param result portable transition result
     * @return runtime-local transition result
     */
    public static TransitionResultEnvelope fromWireResult(TransitionWireResult result) {
        Objects.requireNonNull(result, "result must not be null");
        return new TransitionResultEnvelope(
            result.outcome(),
            result.outputPayloads(),
            result.awaitSuspension(),
            result.failure(),
            null,
            result.terminalOutputPublished(),
            result.terminalInputPassthrough(),
            result.pageCompletion());
    }

    /**
     * Reconstructs the runtime exception represented by a failed result.
     *
     * @return runtime failure
     */
    public RuntimeException failureException() {
        if (outcome != TransitionWorkerOutcome.FAILED) {
            throw new IllegalStateException("Only FAILED transition results contain a runtime failure");
        }
        if (pairedResult.isPresent()) {
            throw new IllegalStateException("A portable failure carries a coordinator outcome");
        }
        return TransitionFailureRuntimeAdapter.toException(failure);
    }

    /**
     * Decodes completed output payloads.
     *
     * @param codec payload codec
     * @return decoded output items
     */
    public List<?> decodeOutputItems(TransitionPayloadCodec codec) {
        Objects.requireNonNull(codec, "codec must not be null");
        if (decodedOutputItems != null) {
            return decodedOutputItems;
        }
        return outputPayloads.stream().map(codec::decode).toList();
    }

    /**
     * Returns output items in the representation the coordinator should persist.
     * In-process workers can carry decoded Java objects. Remote workers return
     * portable payload envelopes, which the coordinator must not eagerly decode
     * when it may not own the application classes.
     *
     * @return decoded local outputs or serialized remote outputs
     */
    public List<?> coordinatorOutputItems() {
        return decodedOutputItems != null ? decodedOutputItems : outputPayloads;
    }
}

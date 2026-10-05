package org.pipelineframework.orchestrator;

import java.util.Optional;

/**
 * Structured result returned to the loop or provider host after one continuation attempt.
 */
public record AwaitItemContinuationResult(
    AwaitItemContinuationCommand command,
    AwaitItemContinuationDisposition disposition,
    long retryAtEpochMs,
    Optional<String> failureMessage) {

    public AwaitItemContinuationResult {
        if (command == null) {
            throw new IllegalArgumentException("command must not be null");
        }
        if (disposition == null) {
            throw new IllegalArgumentException("disposition must not be null");
        }
        if (retryAtEpochMs < 0) {
            throw new IllegalArgumentException("retryAtEpochMs must be non-negative");
        }
        failureMessage = failureMessage == null ? Optional.empty() : failureMessage;
    }

    public static AwaitItemContinuationResult completed(AwaitItemContinuationCommand command) {
        return new AwaitItemContinuationResult(
            command, AwaitItemContinuationDisposition.COMPLETED, 0, Optional.empty());
    }

    public static AwaitItemContinuationResult alreadyCompleted(AwaitItemContinuationCommand command) {
        return new AwaitItemContinuationResult(
            command, AwaitItemContinuationDisposition.ALREADY_COMPLETED, 0, Optional.empty());
    }

    public static AwaitItemContinuationResult notReady(AwaitItemContinuationCommand command) {
        return new AwaitItemContinuationResult(
            command, AwaitItemContinuationDisposition.NOT_READY, 0, Optional.empty());
    }

    public static AwaitItemContinuationResult retry(
        AwaitItemContinuationCommand command,
        long retryAtEpochMs,
        Throwable failure) {
        return new AwaitItemContinuationResult(
            command,
            AwaitItemContinuationDisposition.RETRY,
            retryAtEpochMs,
            Optional.ofNullable(failure == null ? null : failure.getMessage()));
    }

    public static AwaitItemContinuationResult terminalFailure(
        AwaitItemContinuationCommand command,
        Throwable failure) {
        return new AwaitItemContinuationResult(
            command,
            AwaitItemContinuationDisposition.TERMINAL_FAILURE,
            0,
            Optional.ofNullable(failure == null ? null : failure.getMessage()));
    }

    public boolean successful() {
        return disposition == AwaitItemContinuationDisposition.COMPLETED
            || disposition == AwaitItemContinuationDisposition.ALREADY_COMPLETED;
    }
}

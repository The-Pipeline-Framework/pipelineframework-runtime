package org.pipelineframework.awaitable;

/** A pure Command completion projector rejects a callback that contradicts trusted identity. */
public final class CommandCompletionRejectedException extends IllegalArgumentException
    implements AwaitCompletionAdmissionFailure {

    public CommandCompletionRejectedException(String message) {
        super(message);
    }
}

package org.pipelineframework.command;

/** Conflicting or stale native recovery evidence; it never grants another dispatch. */
public final class CommandRecoveryConflictException extends CommandEffectStoreException {
    public CommandRecoveryConflictException(String message) {
        super(message);
    }
}

package org.pipelineframework.command;

/**
 * Retryable observation barrier for a Command that remains in progress.
 * This can mean another invocation owns the command, or a native provider reports that
 * its original operation is still pending. During dispatch, preserving DISPATCHING requires
 * a recovery-capable store/provider and an exact retained original-attempt binding.
 * Such a pending observation permits later inquiry, not another effect dispatch; execution
 * retry scheduling and its finite budget remain the hosting application's responsibility.
 */
public class CommandInProgressException extends RuntimeException {
    public CommandInProgressException(String message) {
        super(message);
    }
}

package org.pipelineframework.orchestrator;

/** Outcome of one bounded itemized-Await continuation attempt. */
public enum AwaitItemContinuationDisposition {
    COMPLETED,
    ALREADY_COMPLETED,
    NOT_READY,
    RETRY,
    TERMINAL_FAILURE
}

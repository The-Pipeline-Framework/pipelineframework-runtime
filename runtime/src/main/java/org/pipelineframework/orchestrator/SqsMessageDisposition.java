package org.pipelineframework.orchestrator;

/**
 * Tells an SQS event host whether one message was durably handled.
 */
public enum SqsMessageDisposition {
    ACKNOWLEDGE,
    RETRY
}

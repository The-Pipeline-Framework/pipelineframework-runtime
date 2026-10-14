package org.pipelineframework.aws.durable;

import java.time.Duration;
import java.util.Objects;

/** Deployment-time guardrails for the AWS Durable service limits used by this host. */
public record AwsDurableHostLimits(
    int estimatedOperations,
    long estimatedWrittenStateBytes,
    Duration executionTimeout,
    Duration completedHistoryRetention,
    Duration workerActionTimeout,
    Duration sqsVisibilityTimeout
) {
    public static final int MAX_OPERATIONS = 3_000;
    public static final long MAX_WRITTEN_STATE_BYTES = 100L * 1024L * 1024L;

    public AwsDurableHostLimits {
        if (estimatedOperations <= 0 || estimatedOperations > MAX_OPERATIONS) {
            throw new IllegalArgumentException("estimatedOperations must be between 1 and 3000");
        }
        if (estimatedWrittenStateBytes <= 0 || estimatedWrittenStateBytes > MAX_WRITTEN_STATE_BYTES) {
            throw new IllegalArgumentException("estimatedWrittenStateBytes must be between 1 and 100 MiB");
        }
        executionTimeout = positive(executionTimeout, "executionTimeout");
        completedHistoryRetention = positive(completedHistoryRetention, "completedHistoryRetention");
        workerActionTimeout = positive(workerActionTimeout, "workerActionTimeout");
        sqsVisibilityTimeout = positive(sqsVisibilityTimeout, "sqsVisibilityTimeout");
        if (executionTimeout.compareTo(Duration.ofSeconds(31_622_400)) > 0) {
            throw new IllegalArgumentException("executionTimeout must not exceed 31622400 seconds");
        }
        if (completedHistoryRetention.compareTo(Duration.ofDays(90)) > 0) {
            throw new IllegalArgumentException("completedHistoryRetention must not exceed 90 days");
        }
        if (workerActionTimeout.compareTo(Duration.ofSeconds(900)) > 0) {
            throw new IllegalArgumentException("workerActionTimeout must not exceed 900 seconds");
        }
        if (sqsVisibilityTimeout.compareTo(workerActionTimeout.multipliedBy(6)) < 0) {
            throw new IllegalArgumentException("sqsVisibilityTimeout must allow six times workerActionTimeout");
        }
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}

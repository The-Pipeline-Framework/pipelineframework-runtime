package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class AwsDurableHostLimitsTest {
    @Test
    void rejectsOperationAndStateBudgetsBeyondProviderLimits() {
        assertThatThrownBy(() -> limits(3_001, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limits(1, AwsDurableHostLimits.MAX_WRITTEN_STATE_BYTES + 1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsVisibilityThatCannotContainOneWorkerAction() {
        assertThatThrownBy(() -> new AwsDurableHostLimits(
            100, 1_024, Duration.ofDays(1), Duration.ofDays(7), Duration.ofSeconds(30), Duration.ofSeconds(30)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("sqsVisibilityTimeout");
    }

    private static AwsDurableHostLimits limits(int operations, long bytes) {
        return new AwsDurableHostLimits(
            operations,
            bytes,
            Duration.ofDays(30),
            Duration.ofDays(30),
            Duration.ofSeconds(30),
            Duration.ofMinutes(2));
    }
}

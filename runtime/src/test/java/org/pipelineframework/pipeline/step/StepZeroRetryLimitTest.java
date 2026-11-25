package org.pipelineframework.pipeline.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.StepManyToOne;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.StepOneToOne;

/**
 * Verifies that {@code retryLimit(0)} disables step-level retries cleanly across all four
 * reactive step cardinalities without triggering Mutiny's {@code atMost(0)} validation error.
 */
class StepZeroRetryLimitTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Test
    void stepOneToOneAllowsZeroRetryLimit() {
        AtomicInteger calls = new AtomicInteger();
        var step = new ZeroRetryOneToOne(calls, false);
        assertEquals("ok:a", step.apply(Uni.createFrom().item("a")).await().atMost(TIMEOUT));
        assertEquals(1, calls.get());

        AtomicInteger failCalls = new AtomicInteger();
        var failing = new ZeroRetryOneToOne(failCalls, true);
        IllegalStateException err = assertThrows(IllegalStateException.class,
            () -> failing.apply(Uni.createFrom().item("a")).await().atMost(TIMEOUT));
        assertEquals("boom-1to1", err.getMessage());
        assertEquals(1, failCalls.get());
    }

    @Test
    void stepOneToManyAllowsZeroRetryLimit() {
        AtomicInteger calls = new AtomicInteger();
        var step = new ZeroRetryOneToMany(calls, false);
        assertEquals(List.of("a-1", "a-2"),
            step.apply(Uni.createFrom().item("a")).collect().asList().await().atMost(TIMEOUT));
        assertEquals(1, calls.get());

        AtomicInteger failCalls = new AtomicInteger();
        var failing = new ZeroRetryOneToMany(failCalls, true);
        IllegalStateException err = assertThrows(IllegalStateException.class,
            () -> failing.apply(Uni.createFrom().item("a")).collect().asList().await().atMost(TIMEOUT));
        assertEquals("boom-1toN", err.getMessage());
        assertEquals(1, failCalls.get());
    }

    @Test
    void stepManyToOneAllowsZeroRetryLimit() {
        AtomicInteger calls = new AtomicInteger();
        var step = new ZeroRetryManyToOne(calls, false);
        assertEquals("a,b",
            step.apply(Multi.createFrom().items("a", "b")).await().atMost(TIMEOUT));
        assertEquals(1, calls.get());

        AtomicInteger failCalls = new AtomicInteger();
        var failing = new ZeroRetryManyToOne(failCalls, true);
        IllegalStateException err = assertThrows(IllegalStateException.class,
            () -> failing.apply(Multi.createFrom().items("a", "b")).await().atMost(TIMEOUT));
        assertEquals("boom-Nto1", err.getMessage());
        assertEquals(1, failCalls.get());
    }

    @Test
    void stepManyToManyAllowsZeroRetryLimit() {
        AtomicInteger calls = new AtomicInteger();
        var step = new ZeroRetryManyToMany(calls, false);
        assertEquals(List.of("A", "B"),
            step.apply(Multi.createFrom().items("a", "b")).collect().asList().await().atMost(TIMEOUT));
        assertEquals(1, calls.get());

        AtomicInteger failCalls = new AtomicInteger();
        var failing = new ZeroRetryManyToMany(failCalls, true);
        IllegalStateException err = assertThrows(IllegalStateException.class,
            () -> failing.apply(Multi.createFrom().items("a", "b")).collect().asList().await().atMost(TIMEOUT));
        assertEquals("boom-NtoN", err.getMessage());
        assertEquals(1, failCalls.get());
    }

    private static StepConfig zeroRetryConfig() {
        return new StepConfig().retryLimit(0);
    }

    private static final class ZeroRetryOneToOne extends ConfigurableStep implements StepOneToOne<String, String> {
        private final AtomicInteger calls;
        private final boolean fail;

        private ZeroRetryOneToOne(AtomicInteger calls, boolean fail) {
            this.calls = calls;
            this.fail = fail;
            initialiseWithConfig(zeroRetryConfig());
        }

        @Override
        public Uni<String> applyOneToOne(String in) {
            calls.incrementAndGet();
            return fail
                ? Uni.createFrom().failure(new IllegalStateException("boom-1to1"))
                : Uni.createFrom().item("ok:" + in);
        }
    }

    private static final class ZeroRetryOneToMany extends ConfigurableStep implements StepOneToMany<String, String> {
        private final AtomicInteger calls;
        private final boolean fail;

        private ZeroRetryOneToMany(AtomicInteger calls, boolean fail) {
            this.calls = calls;
            this.fail = fail;
            initialiseWithConfig(zeroRetryConfig());
        }

        @Override
        public Multi<String> applyOneToMany(String in) {
            calls.incrementAndGet();
            return fail
                ? Multi.createFrom().failure(new IllegalStateException("boom-1toN"))
                : Multi.createFrom().items(in + "-1", in + "-2");
        }
    }

    private static final class ZeroRetryManyToOne extends ConfigurableStep implements StepManyToOne<String, String> {
        private final AtomicInteger calls;
        private final boolean fail;

        private ZeroRetryManyToOne(AtomicInteger calls, boolean fail) {
            this.calls = calls;
            this.fail = fail;
            initialiseWithConfig(zeroRetryConfig());
        }

        @Override
        public Uni<String> applyReduce(Multi<String> input) {
            calls.incrementAndGet();
            return fail
                ? Uni.createFrom().failure(new IllegalStateException("boom-Nto1"))
                : input.collect().asList().map(items -> String.join(",", items));
        }
    }

    private static final class ZeroRetryManyToMany extends ConfigurableStep implements StepManyToMany<String, String> {
        private final AtomicInteger calls;
        private final boolean fail;

        private ZeroRetryManyToMany(AtomicInteger calls, boolean fail) {
            this.calls = calls;
            this.fail = fail;
            initialiseWithConfig(zeroRetryConfig());
        }

        @Override
        public Multi<String> applyTransform(Multi<String> input) {
            calls.incrementAndGet();
            return fail
                ? Multi.createFrom().failure(new IllegalStateException("boom-NtoN"))
                : input.onItem().transform(String::toUpperCase);
        }
    }
}

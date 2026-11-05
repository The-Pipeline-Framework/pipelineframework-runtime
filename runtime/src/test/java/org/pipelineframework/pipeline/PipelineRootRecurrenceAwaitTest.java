package org.pipelineframework.pipeline;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.inject.Inject;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.invocation.PipelineInvocationSteps;
import org.pipelineframework.invocation.PipelineRecursionLimitExceededException;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.step.StepOneToOne;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(PipelineRootRecurrenceAwaitTest.LimitOne.class)
class PipelineRootRecurrenceAwaitTest {
    @Inject PipelineRunner runner;
    @Inject org.pipelineframework.config.PipelineConfig pipelineConfig;

    public static class LimitOne implements QuarkusTestProfile {
        @Override public Map<String, String> getConfigOverrides() {
            return Map.of("pipeline.max-recursive-depth", "1");
        }
    }

    @AfterEach void clearContext() { AwaitExecutionContextHolder.clear(); }

    @Test
    void resumesAtTheLocalRootIndexWithoutReplayingEffectsOrResettingDepth() {
        var effects = new AtomicInteger();
        var captured = new AtomicReference<AwaitExecutionContext>();
        StepOneToOne<Integer, Integer> advance = new IntegerStep(value -> Uni.createFrom().item(() -> {
            effects.incrementAndGet();
            return value + 1;
        })) { };
        StepOneToOne<Integer, Integer> suspend = new IntegerStep(value -> {
            if (value == 2) {
                captured.set(AwaitExecutionContextHolder.get());
                return Uni.createFrom().failure(new Suspended());
            }
            return Uni.createFrom().item(value);
        }) { };
        var body = new AtomicReference<List<Object>>();
        class Continue extends IntegerStep implements PipelineInvocationSteps.LinkedInvocation {
            Continue() {
                super(value -> value >= 3 ? Uni.createFrom().item(value)
                    : PipelineInvocationSteps.<Integer, Integer>recursiveOneToOne(
                    runner, "$root", "Continue", -1, body.get()).applyOneToOne(value));
            }
            @Override public PipelineInvocationSteps.LinkedDefinition linkedDefinition() {
                return new PipelineInvocationSteps.LinkedDefinition("$root", "Continue", true, -1, body.get());
            }
        }
        var recur = new Continue();
        body.set(List.of(advance, suspend, recur,
            new IntegerStep(value -> Uni.createFrom().item(value + 10))));
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 0));
        assertInstanceOf(Suspended.class, rootCause(assertThrows(RuntimeException.class,
            () -> ((Uni<?>) runner.run(Uni.createFrom().item(0), body.get())).await().atMost(Duration.ofSeconds(5)))));
        assertEquals(2, effects.get());
        assertEquals(2, captured.get().currentStepIndex());
        assertEquals(1, captured.get().invocationPath().orElseThrow().stepIndex());
        var codec = new JsonTransitionPayloadCodec();
        var snapshot = new ExecutionInputSnapshot(ExecutionInputShape.UNI, 2, Optional.empty(),
            captured.get().invocationPath());
        var restored = assertInstanceOf(ExecutionInputSnapshot.class, codec.decode(codec.encode(snapshot)));
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 3,
            AwaitContinuationMode.DURABLE_HANDOFF, TerminalOutputOwnership.TRANSITION_WORKER,
            Map.of(), Optional.empty(), Optional.empty(), restored.invocationPath()));
        assertInstanceOf(PipelineRecursionLimitExceededException.class, rootCause(assertThrows(RuntimeException.class,
            () -> ((Uni<?>) runner.runFromStep(Uni.createFrom().item(restored.payload()), body.get(), 3))
                .await().atMost(Duration.ofSeconds(5)))));
        assertEquals(2, effects.get());
        int originalLimit = pipelineConfig.maxRecursiveDepth();
        try {
            pipelineConfig.maxRecursiveDepth(3);
            assertEquals(33, ((Uni<?>) runner.runFromStep(
                Uni.createFrom().item(restored.payload()), body.get(), 3))
                .await().atMost(Duration.ofSeconds(5)));
            assertEquals(3, effects.get());
        } finally {
            pipelineConfig.maxRecursiveDepth(originalLimit);
        }
    }

    private Throwable rootCause(Throwable failure) {
        while (failure.getCause() != null) { failure = failure.getCause(); }
        return failure;
    }

    private static class IntegerStep extends org.pipelineframework.step.ConfigurableStep
        implements StepOneToOne<Integer, Integer> {
        private final java.util.function.Function<Integer, Uni<Integer>> operation;
        private IntegerStep(java.util.function.Function<Integer, Uni<Integer>> operation) {
            this.operation = operation;
        }
        @Override public Uni<Integer> applyOneToOne(Integer input) { return operation.apply(input); }
    }

    private static final class Suspended extends NonRetryableException {
        private Suspended() { super("Await test boundary"); }
    }
}

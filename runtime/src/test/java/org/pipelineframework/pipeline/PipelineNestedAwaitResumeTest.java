package org.pipelineframework.pipeline;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.invocation.PipelineInvocationSteps;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.step.StepOneToOne;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class PipelineNestedAwaitResumeTest {
    @Inject PipelineRunner runner;
    @AfterEach void clear() { AwaitExecutionContextHolder.clear(); }

    @Test
    void restoresTwoNestedCallersAndRunsTheirRemainingStepsWithoutReplayingEarlierEffects() {
        var before = new AtomicInteger();
        var captured = new AtomicReference<AwaitExecutionContext>();
        List<Object> leaf = List.of(new IntegerStep(value -> {
            before.incrementAndGet();
            return Uni.createFrom().item(value + 1);
        }), suspend(captured), plus(10));
        List<Object> middle = List.of(new Call(runner, "leaf", "Ask", leaf), plus(100));
        List<Object> root = List.of(new Call(runner, "middle", "Negotiate", middle), plus(1000));
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 0));
        assertSuspended(() -> run(1, root, 0));
        assertEquals(1, before.get());
        assertEquals(2, captured.get().invocationPath().orElseThrow().frames().size());
        assertTrue(captured.get().invocationPath().orElseThrow().frames().stream().noneMatch(frame -> frame.recursive()));
        restore(captured.get(), 2);
        // Reconstruct the invocation objects: resume must use the persisted positions and compiled structure.
        List<Object> rebuilt = List.of(new Call(runner, "middle", "Negotiate",
            List.of(new Call(runner, "leaf", "Ask", leaf), plus(100))), plus(1000));
        assertEquals(1112, run(2, rebuilt, 1));
        assertEquals(1, before.get());
    }

    @Test
    void callsiteBoundAdapterResumesChildWithAnEmptyRootRange() {
        var captured = new AtomicReference<AwaitExecutionContext>();
        List<Object> child = List.of(suspend(captured), plus(10));
        var call = PipelineInvocationSteps.<Integer, Integer>oneToOne(runner, "child", "Ask", -1, child);
        var linkage = assertInstanceOf(PipelineInvocationSteps.LinkedInvocation.class, call).linkedDefinition();
        assertEquals("child", linkage.definitionId());
        assertEquals("Ask", linkage.callsiteId());
        assertFalse(linkage.recursive());
        assertFalse(PipelineInvocationSteps.oneToOne(runner, child)
            instanceof PipelineInvocationSteps.LinkedInvocation);
        assertFalse(PipelineInvocationSteps.oneToOne(runner, "child", -1, child)
            instanceof PipelineInvocationSteps.LinkedInvocation);
        var recursiveAdapter = org.pipelineframework.invocation.PipelineInvocationContextHolder.call(
            org.pipelineframework.invocation.PipelineInvocationContext.root(3),
            () -> PipelineInvocationSteps.recursiveOneToOne(runner, "child", "Again", -1, child));
        assertFalse(recursiveAdapter instanceof PipelineInvocationSteps.LinkedInvocation);
        List<Object> root = List.of(call);
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 0));
        assertSuspended(() -> run(1, root, 0));
        restore(captured.get(), 1);
        var resumed = runner.runFromStepUntilWithContext(Uni.createFrom().item(1), root, 1, 1);
        assertEquals(11, ((Uni<?>) resumed.result()).await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void canSuspendAgainWhileReturningThroughTheParent() {
        var first = new AtomicReference<AwaitExecutionContext>();
        var second = new AtomicReference<AwaitExecutionContext>();
        List<Object> leaf = List.of(suspend(first), plus(10));
        List<Object> middle = List.of(new Call(runner, "leaf", "Ask", leaf), suspend(second), plus(100));
        List<Object> root = List.of(new Call(runner, "middle", "Negotiate", middle), plus(1000));
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 0));
        assertSuspended(() -> run(1, root, 0));
        restore(first.get(), 1);
        assertSuspended(() -> run(1, root, 1));
        assertEquals(1, second.get().invocationPath().orElseThrow().frames().size());
        assertEquals(1, second.get().invocationPath().orElseThrow().stepIndex());
        restore(second.get(), 11);
        assertEquals(1111, run(11, root, 1));
    }

    @Test
    void rejectsAChangedCallsiteInsteadOfResumingAnotherChild() {
        var captured = new AtomicReference<AwaitExecutionContext>();
        List<Object> child = List.of(suspend(captured), plus(10));
        List<Object> root = List.of(new Call(runner, "child", "Ask", child), plus(100));
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 0));
        assertSuspended(() -> run(1, root, 0));
        restore(captured.get(), 1);
        var failure = assertThrows(IllegalStateException.class, () -> run(1,
            List.of(new Call(runner, "different", "Ask", child), plus(100)), 1));
        assertTrue(failure.getMessage().contains("pinned pipeline callsite"));
    }

    private void restore(AwaitExecutionContext captured, int value) {
        var codec = new JsonTransitionPayloadCodec();
        var snapshot = new ExecutionInputSnapshot(ExecutionInputShape.UNI, value, Optional.empty(), captured.invocationPath());
        var restored = assertInstanceOf(ExecutionInputSnapshot.class, codec.decode(codec.encode(snapshot)));
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", captured.currentStepIndex() + 1,
            AwaitContinuationMode.DURABLE_HANDOFF, TerminalOutputOwnership.TRANSITION_WORKER,
            Map.of(), Optional.empty(), Optional.empty(), restored.invocationPath()));
    }

    private Object run(int input, List<Object> steps, int start) {
        return ((Uni<?>) runner.runFromStep(Uni.createFrom().item(input), steps, start)).await().atMost(Duration.ofSeconds(5));
    }

    private void assertSuspended(org.junit.jupiter.api.function.Executable operation) {
        Throwable failure = assertThrows(RuntimeException.class, operation);
        while (failure.getCause() != null) failure = failure.getCause();
        assertInstanceOf(Suspended.class, failure);
    }

    private IntegerStep plus(int amount) { return new IntegerStep(value -> Uni.createFrom().item(value + amount)); }
    private IntegerStep suspend(AtomicReference<AwaitExecutionContext> captured) {
        return new IntegerStep(value -> {
            captured.set(AwaitExecutionContextHolder.get());
            captured.get().invocationPathForAwait();
            return Uni.createFrom().failure(new Suspended());
        });
    }

    private static class IntegerStep extends ConfigurableStep implements StepOneToOne<Integer, Integer> {
        private final Function<Integer, Uni<Integer>> operation;
        IntegerStep(Function<Integer, Uni<Integer>> operation) { this.operation = operation; }
        @Override public Uni<Integer> applyOneToOne(Integer value) { return operation.apply(value); }
    }

    private static final class Call extends IntegerStep implements PipelineInvocationSteps.LinkedInvocation {
        private final PipelineInvocationSteps.LinkedDefinition definition;
        Call(PipelineRunner runner, String id, String callsite, List<Object> children) {
            super(value -> PipelineInvocationSteps.<Integer, Integer>oneToOne(runner, id, callsite, -1, children)
                .applyOneToOne(value));
            definition = new PipelineInvocationSteps.LinkedDefinition(id, callsite, false, -1, children);
        }
        @Override public PipelineInvocationSteps.LinkedDefinition linkedDefinition() { return definition; }
    }

    private static final class Suspended extends NonRetryableException {
        Suspended() { super("Await test boundary"); }
    }
}

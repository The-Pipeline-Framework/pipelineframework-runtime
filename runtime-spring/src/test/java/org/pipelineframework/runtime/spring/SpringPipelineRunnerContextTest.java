package org.pipelineframework.runtime.spring;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.pipelineframework.runtime.core.PipelineRunnerCore;
import org.pipelineframework.runtime.core.PipelineUnaryStep;
import org.pipelineframework.runtime.core.TpfExecutionContext;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.junit.jupiter.api.Assertions.*;

class SpringPipelineRunnerContextTest {
    private final ApplicationContextRunner contexts = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(SpringRuntimeAdaptersAutoConfiguration.class));

    @Test
    void restoresEachRunsContextAfterAsyncCompletionAndLeavesCompletionThreadUntouched() {
        contexts.run(application -> {
            var first = new CompletableFuture<String>();
            var second = new CompletableFuture<String>();
            PipelineUnaryStep<String, String> gate = input -> input.equals("one") ? first : second;
            PipelineUnaryStep<String, String> read = input -> CompletableFuture.completedFuture(
                input + ":" + TpfExecutionContext.versionTag().orElseThrow()
                    + ":" + TpfExecutionContext.replayMode().orElseThrow()
                    + ":" + TpfExecutionContext.cachePolicy().orElseThrow());
            var runner = new SpringPipelineRunner(new PipelineRunnerCore(), List.of(gate, read));
            CompletableFuture<Object> one;
            CompletableFuture<Object> two;
            try (var scope = TpfExecutionContext.withHeaders("v1", "r1", "c1")) {
                one = runner.run("one").toCompletableFuture();
            }
            try (var scope = TpfExecutionContext.withHeaders("v2", "r2", "c2")) {
                two = runner.run("two").toCompletableFuture();
            }
            try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
                executor.submit(() -> {
                    try (var scope = TpfExecutionContext.withHeaders("worker", "worker", "worker")) {
                        second.complete("two");
                        first.complete("one");
                        assertEquals(Optional.of("worker"), TpfExecutionContext.versionTag());
                    }
                }).get(5, TimeUnit.SECONDS);
            }
            assertEquals("one:v1:r1:c1", one.get(5, TimeUnit.SECONDS));
            assertEquals("two:v2:r2:c2", two.get(5, TimeUnit.SECONDS));
            assertTrue(TpfExecutionContext.versionTag().isEmpty());
        });
    }

    @Test
    void restoresPreviousContextWhenStepThrows() {
        contexts.run(application -> {
            PipelineUnaryStep<String, String> failing = input -> {
                throw new IllegalStateException("failure");
            };
            var runner = new SpringPipelineRunner(new PipelineRunnerCore(), List.of(failing));
            try (var scope = TpfExecutionContext.withHeaders("caller", "r", "c")) {
                assertThrows(java.util.concurrent.CompletionException.class,
                    () -> runner.run("input").toCompletableFuture().join());
                assertEquals(Optional.of("caller"), TpfExecutionContext.versionTag());
            }
            assertTrue(TpfExecutionContext.capture().versionTag().isEmpty());
        });
    }
}

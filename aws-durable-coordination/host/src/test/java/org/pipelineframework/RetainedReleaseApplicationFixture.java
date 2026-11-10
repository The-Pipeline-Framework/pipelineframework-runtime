package org.pipelineframework;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.step.ConfigFactory;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.telemetry.PipelineTelemetry;

/** Two deterministic authored application versions; the execution service, runner and step executor are real. */
public final class RetainedReleaseApplicationFixture {
    public static PipelineExecutionService application(PipelineOrchestratorConfig config,
        PipelineContractDescriptor contract, String release) throws Exception {
        List<Object> authored = List.of(new ReleaseTransform(release));
        // Fixture-generated bean selection; this is the same authored step list the compiled order supplies.
        var service = new PipelineExecutionService() {
            @Override List<Object> loadPipelineSteps() { return authored; }
        };
        var stepConfig = new SmallRyeConfigBuilder().withMapping(PipelineStepConfig.class).build()
            .getConfigMapping(PipelineStepConfig.class);
        var pipelineConfig = new PipelineConfig();
        var telemetry = new PipelineTelemetry(stepConfig);
        var runner = new PipelineRunner();
        runner.pipelineConfig = pipelineConfig;
        runner.configFactory = new ConfigFactory();
        runner.stepOrderer = new PipelineStepOrderer();
        runner.parallelismPolicyResolver = new PipelineParallelismPolicyResolver();
        runner.cacheSupportFactory = new PipelineCacheSupportFactory(new Empty<>(), new Empty<>(), "bypass-cache");
        runner.stepExecutor = new PipelineStepExecutor();
        runner.runTelemetry = telemetry;
        runner.stepTelemetry = telemetry;
        service.pipelineRunner = runner;
        service.pipelineConfig = pipelineConfig;
        service.configFactory = new ConfigFactory();
        service.pipelineStepConfig = stepConfig;
        service.executionInputPolicy = new ExecutionInputPolicy();
        service.executionHooks = new ExecutionHooks();
        service.orchestratorConfig = config;
        service.releaseIdentityResolver = new PipelineReleaseIdentityResolver();
        set(service.releaseIdentityResolver, "cachedContract", Optional.of(contract));
        Field health = PipelineExecutionService.class.getDeclaredField("startupHealthState");
        health.setAccessible(true);
        @SuppressWarnings("unchecked")
        var state = (AtomicReference<PipelineExecutionService.StartupHealthState>) health.get(service);
        state.set(PipelineExecutionService.StartupHealthState.HEALTHY);
        return service;
    }

    public static final class ReleaseTransform extends ConfigurableStep implements StepOneToOne<String, String> {
        private final String release;
        public ReleaseTransform(String release) { this.release = release; }
        @Override public Uni<String> applyOneToOne(String input) { return Uni.createFrom().item(release + ":" + input.toUpperCase(java.util.Locale.ROOT)); }
    }

    private static void set(Object object, String name, Object value) throws Exception {
        Field field = object.getClass().getDeclaredField(name); field.setAccessible(true); field.set(object, value);
    }
    private static final class Empty<T> implements Instance<T> {
        @Override public Instance<T> select(Annotation... qualifiers) { return this; }
        @Override public <U extends T> Instance<U> select(Class<U> type, Annotation... qualifiers) { return new Empty<>(); }
        @Override public <U extends T> Instance<U> select(TypeLiteral<U> type, Annotation... qualifiers) { return new Empty<>(); }
        @Override public boolean isUnsatisfied() { return true; }
        @Override public boolean isAmbiguous() { return false; }
        @Override public void destroy(T instance) {}
        @Override public Handle<T> getHandle() { throw new IllegalStateException("No configured cache provider"); }
        @Override public Iterable<? extends Handle<T>> handles() { return List.of(); }
        @Override public java.util.Iterator<T> iterator() { return java.util.Collections.emptyIterator(); }
        @Override public T get() { throw new IllegalStateException("No configured cache provider"); }
    }
}

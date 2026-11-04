package org.pipelineframework.aws.durable.fixture;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.command.*;
import org.pipelineframework.connector.*;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

/** Typed runtime fixture following the native Command callback boundary. No workflow is mocked. */
@ApplicationScoped @Unremovable
public class WaitService extends ConfigurableStep implements StepOneToOne<Request, Prepared>, CommandStep {
    @Inject CommandDeferredCompletionSupport deferred;
    @Inject AwaitCompletionDescriptorRegistry registry;
    @Inject InMemoryCommandEffectStore effects;
    @Inject PipelineOrchestratorConfig config;
    private Provider provider = new Provider();
    private CommandStepSupport commands;
    private CommandDescriptor command;
    private AwaitCompletionDescriptor completion;
    private volatile Optional<AwaitContinuationMode> observedMode = Optional.empty();
    private volatile Optional<String> observedFailure = Optional.empty();
    private volatile boolean executionContextPresent;

    @PostConstruct void initialize() {
        var binding = ConnectorBindingName.of("fixture-job");
        var operation = new ConnectorOperationIdentity(ConnectorProviderId.of("test.scalar"), "start", ConnectorOperationKind.COMMAND, 1);
        var bindings = ConnectorBindingRegistry.fromProviders(List.of(new ConnectorBindingDefinition(binding,
            operation.providerId(), 1, ConnectorConfigurationDocument.empty())), List.of(provider));
        bindings.start(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        provider = (Provider) bindings.providerInstances().getFirst();
        commands = new CommandStepSupport(new ConnectorRegistry(List.of()), bindings, List.of(effects), config);
        command = CommandDescriptor.nativeCommand("WaitForScalarAnswer", new NativeCommandSelector(Optional.of(binding),
            operation, 1, CommandPolicy.none()), Request.class.getName(), Pending.class.getName(), "fixture-id",
            CommandDuplicatePolicy.RETURN_RECORDED, Map.of());
        var callback = new ConnectorCallbackSelection(binding, operation, 1, provider.operation.callbacks().getFirst(),
            "test.EndpointResolver", "test.Authenticator");
        completion = registry.register(new AwaitCompletionDescriptor("WaitForScalarAnswer", Request.class.getName(),
            Prepared.class.getName(), "ONE_TO_ONE", Duration.ofMinutes(1), "signedResumeToken", "", Map.of(), List.of(),
            Request.class.getName(), Answer.class.getName(), Function.identity(), Function.identity(),
            AnswerProjector.class.getName(), (input, answer, metadata) -> new AnswerProjector().project((Request) input,
                (Answer) answer, metadata), true, Optional.of(callback)));
    }

    @Override public Uni<Prepared> applyOneToOne(Request request) {
        observedMode = Optional.ofNullable(AwaitExecutionContextHolder.get()).map(AwaitExecutionContext::continuationMode);
        executionContextPresent = PipelineExecutionContextHolder.get().isPresent();
        try {
            return deferred.<Request, Prepared>execute(completion, request, ignored -> URI.create("https://fixture.invalid/"),
                callback -> commands.execute(command, (descriptor, input) -> "fixture:" + ((Request) input).id(), request, callback))
                .onFailure().invoke(failure -> observedFailure = Optional.of(failure.toString()));
        } catch (RuntimeException failure) {
            observedFailure = Optional.of(failure.toString());
            throw failure;
        }
    }

    public int dispatches() { return provider.operation.calls.get(); }
    public Optional<AwaitContinuationMode> observedMode() { return observedMode; }
    public Optional<String> observedFailure() { return observedFailure; }
    public boolean executionContextPresent() { return executionContextPresent; }

    public static final class Provider implements ConnectorProvider<Void> {
        private final Operation operation = new Operation();
        @Override public ConnectorProviderId id() { return ConnectorProviderId.of("test.scalar"); }
        @Override public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
        @Override public Collection<? extends ConnectorOperation> operations() { return List.of(operation); }
    }
    public record Configuration() {}
    /** External provider substitute acknowledges immediately and never submits a completion itself. */
    public static final class Operation implements CommandOperation<Request, Configuration, Pending> {
        private final AtomicInteger calls = new AtomicInteger();
        @Override public String id() { return "start"; }
        @Override public Optional<ConnectorConfigSchema<Configuration>> configurationSchema() {
            return Optional.of(ConnectorConfigSchema.record(Configuration.class, "test.scalar.start", 1));
        }
        @Override public List<ConnectorOperationCallbackDescriptor> callbacks() {
            return List.of(new ConnectorOperationCallbackDescriptor("completed",
                new ConnectorOperationTypeContract(Answer.class.getName(), Optional.empty()), true));
        }
        @Override public CompletionStage<CommandOutcome<Pending>> dispatch(CommandInvocation<Request, Configuration> invocation) {
            invocation.callbackContext().orElseThrow();
            calls.incrementAndGet();
            return CompletableFuture.completedStage(new CommandOutcome.Succeeded<>(new Pending(invocation.input().id()),
                CommandConfirmation.none(), List.of()));
        }
    }
}

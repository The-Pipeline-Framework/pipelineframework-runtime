package org.pipelineframework.awaitable;

import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.function.Function;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.command.CommandEffectStatus;
import org.pipelineframework.command.CommandOutcomeException;
import org.pipelineframework.command.CommandRetryableEffectException;
import org.pipelineframework.connector.ConnectorCallbackContext;
import org.pipelineframework.connector.ProviderCallbackEndpointResolver;
import org.pipelineframework.connector.ProviderCallbackRequest;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

/** Orders durable completion registration around the existing Command execution path. */
@ApplicationScoped
public class CommandDeferredCompletionSupport {
    public static final String CALLBACK_PATH = "pipeline/callbacks/";
    @Inject AwaitCoordinator coordinator;
    @Inject AwaitResumeTokenService tokens;
    @Inject PipelineOrchestratorConfig config;
    @ConfigProperty(name = "pipeline.callback.allow-http", defaultValue = "false")
    boolean allowHttp;

    public <I, O> Uni<O> execute(AwaitCompletionDescriptor descriptor, I input,
        ProviderCallbackEndpointResolver endpointResolver, Function<ConnectorCallbackContext, Uni<?>> command) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(endpointResolver, "endpointResolver");
        Objects.requireNonNull(command, "command");
        if (config.mode() != OrchestratorMode.QUEUE_ASYNC
            && !coordinator.acceptsRemoteAwaitContext(AwaitExecutionContextHolder.get())) {
            return Uni.createFrom().failure(new IllegalStateException("Command callback requires QUEUE_ASYNC"));
        }
        PipelineExecutionContext execution = PipelineExecutionContextHolder.get()
            .orElseThrow(() -> new IllegalStateException("Command callback requires execution context"));
        AwaitExecutionContext context = Optional.ofNullable(AwaitExecutionContextHolder.get())
            .map(current -> new AwaitExecutionContext(current.tenantId(), current.executionId(),
                current.currentStepIndex(), current.continuationMode(), current.terminalOutputOwnership(),
                current.traceMetadata(), current.pageContext(), current.awaitOccurrenceCause(), current.invocationPath(), current.transitionScope()))
            .orElseGet(() -> new AwaitExecutionContext(execution.tenantId(), execution.executionId(),
                execution.currentStepIndex(), AwaitContinuationMode.LIVE_IF_SUPPORTED,
                TerminalOutputOwnership.TRANSITION_WORKER, Map.of(), Optional.empty()));
        Uni<AwaitWorkerRegistrationResult> registrationResult = context.transitionScope().isPresent()
            ? coordinator.registerCommandCompletionWithAuthority(descriptor, context, input)
            : coordinator.registerCommandCompletion(descriptor, context, input)
                .map(created -> new AwaitWorkerRegistrationResult(created, Optional.empty()));
        return registrationResult.chain(registration -> {
                var created = registration.creation();
                AwaitInteractionRecord record = created.record();
                if (record.status().terminal() || record.status() == AwaitInteractionStatus.DISPATCHED) {
                    return finish(new CommandCompletionSettlementResult(record,
                        record.status() == AwaitInteractionStatus.COMPLETED
                            && "dispatch".equals(record.transportMetadata().get("completionDelivery"))), context);
                }
                if (context.transitionScope().isPresent()
                    && (record.status() == AwaitInteractionStatus.DISPATCHING
                        || record.status() == AwaitInteractionStatus.COMPLETION_OBSERVED)) {
                    return suspended(record, context);
                }
                ConnectorCallbackContext callback;
                try {
                    String token = context.transitionScope().isPresent()
                        ? registration.callbackToken().orElseThrow(() -> new IllegalStateException("Coordinator callback token is absent"))
                        : tokens.sign(record, record.createdAtEpochMs());
                    var selected = descriptor.callback().orElseThrow();
                    var policy = allowHttp ? ConnectorCallbackContext.UriPolicy.LOCAL_HTTP
                        : ConnectorCallbackContext.UriPolicy.HTTPS_ONLY;
                    var base = new ConnectorCallbackContext(selected.callback().id(), endpointResolver.resolve(
                        new ProviderCallbackRequest(record.tenantId(), record.interactionId(),
                            selected.operation(), selected.callback().id())), policy).callbackUri();
                    if (base.getRawQuery() != null) {
                        throw new IllegalArgumentException("callback base URI must not contain a query");
                    }
                    String prefix = base.toASCIIString();
                    callback = new ConnectorCallbackContext(selected.callback().id(), java.net.URI.create(
                        prefix + (prefix.endsWith("/") ? "" : "/") + CALLBACK_PATH + token), policy);
                } catch (RuntimeException failure) {
                    return Uni.createFrom().failure(new IllegalStateException("Unable to resolve Command callback endpoint"));
                }
                Uni<Optional<AwaitInteractionRecord>> claimed = record.status() == AwaitInteractionStatus.WAITING
                    ? (context.transitionScope().isPresent() ? coordinator.claimCommandDispatch(record, context)
                        : coordinator.claimCommandDispatch(record)) : Uni.createFrom().item(Optional.of(record));
                return claimed.chain(claim -> {
                    if (claim.isEmpty()) {
                        return suspended(record, context);
                    }
                    return invoke(execution, command, callback)
                        .onItem().transform(acknowledgement -> descriptor.acknowledgementAwareCommandCompletion()
                            && acknowledgement == null
                            ? new DispatchResult(CommandDispatchSettlement.AMBIGUOUS, Optional.empty(), Optional.empty())
                            : new DispatchResult(CommandDispatchSettlement.SUCCEEDED, Optional.empty(),
                                descriptor.acknowledgementAwareCommandCompletion()
                                    ? Optional.ofNullable(acknowledgement) : Optional.empty()))
                        .onFailure(failure -> !(failure instanceof org.pipelineframework.command.CommandInProgressException))
                        .recoverWithItem(failure -> new DispatchResult(classify(failure), Optional.of(failure), Optional.empty()))
                        .chain(result -> (context.transitionScope().isPresent()
                            ? coordinator.settleCommandDispatch(claim.orElseThrow(), result.settlement(), result.acknowledgement(), context)
                            : coordinator.settleCommandDispatch(claim.orElseThrow(), result.settlement(), result.acknowledgement()))
                            .chain(settled -> {
                                if (settled.record().status() == AwaitInteractionStatus.WAITING) {
                                    return Uni.createFrom().<O>failure(result.failure().orElseThrow());
                                }
                                return this.<O>finish(settled, context);
                            }))
                        .onFailure(org.pipelineframework.command.CommandInProgressException.class)
                        .recoverWithUni(ignored -> this.<O>suspended(claim.orElseThrow(), context));
                });
            });
    }

    private Uni<?> invoke(PipelineExecutionContext execution, Function<ConnectorCallbackContext, Uni<?>> command,
        ConnectorCallbackContext callback) {
        Optional<PipelineExecutionContext> previous = PipelineExecutionContextHolder.get();
        PipelineExecutionContextHolder.set(execution);
        try {
            return Objects.requireNonNull(command.apply(callback), "Command returned no asynchronous result");
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        } finally {
            previous.ifPresentOrElse(PipelineExecutionContextHolder::set, PipelineExecutionContextHolder::clear);
        }
    }

    private CommandDispatchSettlement classify(Throwable failure) {
        if (failure instanceof CommandOutcomeException outcome && outcome.status() == CommandEffectStatus.AMBIGUOUS) {
            return CommandDispatchSettlement.AMBIGUOUS;
        }
        if (failure instanceof CommandOutcomeException outcome && outcome.status() == CommandEffectStatus.USER_ACTION_REQUIRED) {
            return CommandDispatchSettlement.USER_ACTION_REQUIRED;
        }
        if (CommandRetryableEffectException.find(failure).isPresent()
            || failure instanceof org.pipelineframework.command.CommandRetryableOutcomeException) {
            return CommandDispatchSettlement.RETRYABLE;
        }
        return CommandDispatchSettlement.TERMINAL;
    }

    @SuppressWarnings("unchecked")
    private <O> Uni<O> finish(CommandCompletionSettlementResult result, AwaitExecutionContext context) {
        AwaitInteractionRecord record = result.record();
        if (context.transitionScope().isPresent()) {
            if (record.status().terminal() && record.status() != AwaitInteractionStatus.COMPLETED) {
                return Uni.createFrom().failure(new AwaitInteractionTerminalException("Command completion " + record.status()));
            }
            return result.completedByDispatch() ? Uni.createFrom().item(() -> (O) coordinator.resumePayload(record))
                : suspended(record, context);
        }
        if (record.status().terminal() && record.status() != AwaitInteractionStatus.COMPLETED) {
            return coordinator.recordCompletion(record, System.currentTimeMillis())
                .chain(() -> coordinator.releaseAdmission(record))
                .chain(() -> Uni.createFrom().failure(new AwaitInteractionTerminalException(
                    "Command completion " + record.status() + ": " + record.transportMetadata().getOrDefault("completionFailure", "effect-failed"))));
        }
        if (result.completedByDispatch()) {
            return coordinator.recordCompletion(record, System.currentTimeMillis())
                .chain(() -> coordinator.releaseAdmission(record))
                .replaceWith(() -> (O) coordinator.resumePayload(record));
        }
        return suspended(record, context);
    }

    private <O> Uni<O> suspended(AwaitInteractionRecord record, AwaitExecutionContext context) {
        return Uni.createFrom().failure(new AwaitSuspendedException(context.tenantId(), context.executionId(),
            record.unitId(), context.currentStepIndex()));
    }

    private record DispatchResult(CommandDispatchSettlement settlement, Optional<Throwable> failure,
        Optional<Object> acknowledgement) { }
}

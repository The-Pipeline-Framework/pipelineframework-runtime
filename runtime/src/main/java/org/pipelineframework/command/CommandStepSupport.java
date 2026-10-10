package org.pipelineframework.command;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.CommandReference;
import org.pipelineframework.connector.CommandRecoveryBinding;
import org.pipelineframework.connector.CommandRecoveryTarget;
import org.pipelineframework.connector.CommandReconciliationInvocation;
import org.pipelineframework.connector.CommandReconciliationResult;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorConfigurationBinder;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRegistry;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.step.NonRetryableException;

/**
 * Runtime bridge used by generated command step beans.
 */
@ApplicationScoped
public class CommandStepSupport {
    private final org.pipelineframework.connector.ConnectorOperationInvocationCoordinator invocationCoordinator =
        new org.pipelineframework.connector.ConnectorOperationInvocationCoordinator();

    @Inject
    Instance<CommandEffectStore> stores;

    @Inject
    ConnectorRegistry connectorRegistry;

    @Inject
    ConnectorBindingRegistry connectorBindingRegistry;

    @Inject
    ConnectorRuntimeContext connectorRuntimeContext;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    private Collection<CommandEffectStore> fixedStores;
    private ConnectorRegistry fixedConnectorRegistry;
    private ConnectorBindingRegistry fixedConnectorBindingRegistry;
    private ConnectorRuntimeContext fixedConnectorRuntimeContext;

    public CommandStepSupport() {
    }

    public CommandStepSupport(
        Collection<CommandConnector<?, ?>> connectors,
        Collection<CommandEffectStore> stores,
        PipelineOrchestratorConfig orchestratorConfig
    ) {
        this.fixedConnectorRegistry = LegacyCommandConnectorProvider.createRegistry(
            List.of(), connectors == null ? List.of() : connectors);
        this.fixedConnectorBindingRegistry = ConnectorBindingRegistry.empty();
        this.fixedConnectorRuntimeContext = ConnectorRuntimeContext.empty();
        this.fixedStores = stores == null ? List.of() : stores;
        this.orchestratorConfig = orchestratorConfig;
    }

    public CommandStepSupport(
        ConnectorRegistry registry,
        Collection<CommandEffectStore> stores,
        PipelineOrchestratorConfig orchestratorConfig
    ) {
        this.fixedConnectorRegistry = java.util.Objects.requireNonNull(registry, "connector registry must not be null");
        this.fixedConnectorBindingRegistry = ConnectorBindingRegistry.empty();
        this.fixedConnectorRuntimeContext = ConnectorRuntimeContext.empty();
        this.fixedStores = stores == null ? List.of() : stores;
        this.orchestratorConfig = orchestratorConfig;
    }

    public CommandStepSupport(
        ConnectorRegistry registry,
        ConnectorBindingRegistry bindingRegistry,
        Collection<CommandEffectStore> stores,
        PipelineOrchestratorConfig orchestratorConfig
    ) {
        this.fixedConnectorRegistry = java.util.Objects.requireNonNull(registry, "connector registry must not be null");
        this.fixedConnectorBindingRegistry = java.util.Objects.requireNonNull(
            bindingRegistry, "connector binding registry must not be null");
        this.fixedConnectorRuntimeContext = ConnectorRuntimeContext.empty();
        this.fixedStores = stores == null ? List.of() : stores;
        this.orchestratorConfig = orchestratorConfig;
    }

    public <I, O> Uni<O> execute(
        Uni<CommandDescriptor> descriptor,
        CommandIdGenerator<? super I> commandIdGenerator,
        I input
    ) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (commandIdGenerator == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("commandIdGenerator must not be null"));
        }
        PipelineExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        }
        return descriptor.onItem().transformToUni(
            resolved -> execute(resolved, commandIdGenerator, input, context, false));
    }

    public <I, O> Uni<O> execute(
        CommandDescriptor descriptor,
        CommandIdGenerator<? super I> commandIdGenerator,
        I input
    ) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (commandIdGenerator == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("commandIdGenerator must not be null"));
        }
        PipelineExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException e) {
            return Uni.createFrom().failure(e);
        }
        return execute(descriptor, commandIdGenerator, input, context, false);
    }

    /**
     * Deliberately retries an existing logical Command effect whose latest attempt is
     * {@link CommandEffectStatus#FAILED_RETRYABLE}.
     */
    public <I, O> Uni<O> retry(
        Uni<CommandDescriptor> descriptor,
        CommandIdGenerator<? super I> commandIdGenerator,
        I input
    ) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (commandIdGenerator == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("commandIdGenerator must not be null"));
        }
        PipelineExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        }
        return descriptor.onItem().transformToUni(
            resolved -> execute(resolved, commandIdGenerator, input, context, true));
    }

    /**
     * Deliberately retries an existing logical Command effect whose latest attempt is
     * {@link CommandEffectStatus#FAILED_RETRYABLE}.
     */
    public <I, O> Uni<O> retry(
        CommandDescriptor descriptor,
        CommandIdGenerator<? super I> commandIdGenerator,
        I input
    ) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (commandIdGenerator == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("commandIdGenerator must not be null"));
        }
        PipelineExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        }
        return execute(descriptor, commandIdGenerator, input, context, true);
    }

    private <I, O> Uni<O> execute(
        CommandDescriptor descriptor,
        CommandIdGenerator<? super I> commandIdGenerator,
        I input,
        PipelineExecutionContext context,
        boolean deliberateRetry
    ) {
        return execute(descriptor, commandIdGenerator, input, context, deliberateRetry, Optional.empty());
    }

    /** Executes an ordinary native Command with transient framework callback authority. */
    public <I, O> Uni<O> execute(CommandDescriptor descriptor, CommandIdGenerator<? super I> generator,
        I input, org.pipelineframework.connector.ConnectorCallbackContext callback) {
        return executeWithCallback(descriptor, generator, input, callback, false);
    }

    /** Deliberate retry retains the same registered completion interaction. */
    public <I, O> Uni<O> retry(CommandDescriptor descriptor, CommandIdGenerator<? super I> generator,
        I input, org.pipelineframework.connector.ConnectorCallbackContext callback) {
        return executeWithCallback(descriptor, generator, input, callback, true);
    }

    private <I, O> Uni<O> executeWithCallback(CommandDescriptor descriptor, CommandIdGenerator<? super I> generator,
        I input, org.pipelineframework.connector.ConnectorCallbackContext callback, boolean deliberateRetry) {
        try {
            java.util.Objects.requireNonNull(generator, "commandIdGenerator must not be null");
            if (descriptor.nativeSelector().isEmpty() || descriptor.nativeSelector().orElseThrow().binding().isEmpty()) {
                throw new IllegalArgumentException("callback completion requires an application-bound native Command");
            }
            return execute(descriptor, generator, input, captureExecutionContext(), deliberateRetry, Optional.of(callback));
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        }
    }

    private <I, O> Uni<O> execute(
        CommandDescriptor descriptor,
        CommandIdGenerator<? super I> commandIdGenerator,
        I input,
        PipelineExecutionContext context,
        boolean deliberateRetry,
        Optional<org.pipelineframework.connector.ConnectorCallbackContext> callbackContext
    ) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        String commandId;
        try {
            commandId = commandIdGenerator.commandId(descriptor, input);
        } catch (Throwable failure) {
            return Uni.createFrom().failure(failure);
        }
        if (commandId == null || commandId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                "Command id generator " + descriptor.commandIdGenerator() + " returned a blank command id"));
        }
        if (!commandId.equals(commandId.trim())) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                "Command id generator " + descriptor.commandIdGenerator()
                    + " returned a command id with leading or trailing whitespace"));
        }
        CommandRequest<I> request = new CommandRequest<>(
            descriptor, commandId, commandId, CommandRequest.newAttemptId(), input, context, descriptor.config(), callbackContext);
        CommandEffectStore store = selectStore();
        return store.find(context.tenantId(), request.commandId())
            .onItem().transformToUni(existing -> handleExistingOrExecute(
                existing, store, request, deliberateRetry));
    }

    private <I, O> Uni<O> handleExistingOrExecute(
        Optional<CommandEffectRecord> existing,
        CommandEffectStore store,
        CommandRequest<I> request,
        boolean deliberateRetry
    ) {
        if (existing.isPresent()) {
            CommandEffectRecord record = existing.get();
            if (record.status() == CommandEffectStatus.SUCCEEDED) {
                boolean recordedAttempt = CommandReexecutionScope.claimRecorded(
                    request.commandId(), record.currentAttempt().attemptId());
                Optional<CommandReexecutionScope.Claim> claim = recordedAttempt
                    ? Optional.empty()
                    : CommandReexecutionScope.claimAttempt(
                        request.commandId(), record.currentAttempt().occurrenceId());
                if (claim.isPresent()) {
                    CommandReexecutionScope.Claim admitted = claim.orElseThrow();
                    if (admitted.admission().purpose() != CommandAttemptPurpose.REISSUE) {
                        return Uni.createFrom().failure(new IllegalStateException(
                            "Command retry cannot append an attempt from SUCCEEDED state"));
                    }
                    if (!store.supportsAttempt(CommandAttemptPurpose.REISSUE)) {
                        return Uni.createFrom().failure(new UnsupportedOperationException(
                            "Command reissue requires a CommandEffectStore that persists occurrence history"));
                    }
                    CommandRequest<I> reissueRequest = new CommandRequest<>(
                        request.descriptor(),
                        request.commandId(),
                        admitted.occurrenceId(),
                        admitted.attemptId(),
                        request.input(),
                        request.executionContext(),
                        request.config(), request.callbackContext());
                    return reissueRequest.descriptor().nativeSelector().isPresent()
                        ? executeNative(
                            store,
                            reissueRequest,
                            reissueRequest.descriptor().nativeSelector().orElseThrow(),
                            Optional.of(admitted.admission()))
                        : executeLegacy(store, reissueRequest, Optional.of(admitted.admission()));
                }
                if (request.descriptor().duplicatePolicy() == CommandDuplicatePolicy.FAIL) {
                    CommandEffectMetrics.recordDuplicate(request.descriptor(), "rejected");
                    return Uni.createFrom().failure(new NonRetryableException(
                        "Duplicate command completion for commandId " + request.commandId()));
                }
                @SuppressWarnings("unchecked")
                O recorded = (O) record.output();
                CommandRecordedDuplicateMarker.mark(recorded);
                CommandEffectMetrics.recordDuplicate(request.descriptor(), "returned_recorded");
                CommandEffectMetrics.recordAdmission(request.descriptor(), "replay");
                return Uni.createFrom().item(recorded);
            }
            if (record.status() == CommandEffectStatus.PENDING || record.status() == CommandEffectStatus.DISPATCHING) {
                CommandReexecutionScope.claimRecorded(
                    request.commandId(), record.currentAttempt().attemptId());
                CommandEffectMetrics.recordDuplicate(request.descriptor(), "in_progress");
                if (recoveryEligible(store, request, record)) {
                    return recoverNative(store, request, record);
                }
                return Uni.createFrom().failure(new CommandInProgressException(
                    "Command already in progress for commandId " + request.commandId()));
            }
            if (record.status() == CommandEffectStatus.FAILED_RETRYABLE) {
                Optional<CommandReexecutionScope.Claim> scopedClaim = deliberateRetry
                    ? Optional.empty()
                    : CommandReexecutionScope.claimAttempt(
                        request.commandId(), record.currentAttempt().occurrenceId());
                if (deliberateRetry || scopedClaim.isPresent()) {
                    CommandAttemptAdmission admission = scopedClaim
                        .map(CommandReexecutionScope.Claim::admission)
                        .orElseGet(CommandAttemptAdmission::retry);
                    if (admission.purpose() != CommandAttemptPurpose.RETRY) {
                        return Uni.createFrom().failure(new IllegalStateException(
                            "Command reissue cannot append an attempt from FAILED_RETRYABLE state"));
                    }
                    CommandRequest<I> retryRequest = scopedClaim
                        .map(admitted -> new CommandRequest<>(
                            request.descriptor(),
                            request.commandId(),
                            admitted.occurrenceId(),
                            admitted.attemptId(),
                            request.input(),
                            request.executionContext(),
                            request.config(), request.callbackContext()))
                        .orElseGet(() -> new CommandRequest<>(
                            request.descriptor(),
                            request.commandId(),
                            record.currentAttempt().occurrenceId(),
                            request.attemptId(),
                            request.input(),
                            request.executionContext(),
                            request.config(), request.callbackContext()));
                    if (record.currentAttempt().attemptId().equals(retryRequest.attemptId())) {
                        return Uni.createFrom().failure(new CommandRetryableOutcomeException(
                            "retry-admission-already-attempted"));
                    }
                    if (!store.supportsAttempt(CommandAttemptPurpose.RETRY)) {
                        return Uni.createFrom().failure(new UnsupportedOperationException(
                            "deliberate Command retry requires a CommandEffectStore that persists attempt history"));
                    }
                    return retryRequest.descriptor().nativeSelector().isPresent()
                        ? executeNative(
                            store,
                            retryRequest,
                            retryRequest.descriptor().nativeSelector().orElseThrow(),
                            Optional.of(admission))
                        : executeLegacy(store, retryRequest, Optional.of(admission));
                }
                return Uni.createFrom().failure(CommandRetryableEffectException.mark(
                    request.commandId(), new CommandRetryableOutcomeException(recordedOutcomeCode(record))));
            }
            if (record.status() == CommandEffectStatus.DLQ
                || record.status() == CommandEffectStatus.AMBIGUOUS
                || record.status() == CommandEffectStatus.USER_ACTION_REQUIRED) {
                CommandReexecutionScope.claimAttempt(
                    request.commandId(), record.currentAttempt().occurrenceId());
                if (record.status() == CommandEffectStatus.AMBIGUOUS && recoveryEligible(store, request, record)) {
                    return recoverNative(store, request, record);
                }
                return Uni.createFrom().failure(new CommandOutcomeException(record.status(), recordedOutcomeCode(record)));
            }
            return Uni.createFrom().failure(new IllegalStateException(
                "Command effect " + request.commandId() + " has unsupported retained state " + record.status()));
        }
        if (deliberateRetry) {
            return Uni.createFrom().failure(new IllegalStateException(
                "No existing command effect found to retry for commandId " + request.commandId()));
        }
        return request.descriptor().nativeSelector().isPresent()
            ? executeNative(store, request, request.descriptor().nativeSelector().orElseThrow(), Optional.empty())
            : executeLegacy(store, request, Optional.empty());
    }

    private <I, O> Uni<O> executeLegacy(
        CommandEffectStore store,
        CommandRequest<I> request,
        Optional<CommandAttemptAdmission> attemptAdmission
    ) {
        LegacyCommandConnectorProvider.LegacyCommandOperation operation;
        try {
            operation = requireLegacyOperation(request.descriptor().command());
        } catch (IllegalStateException failure) {
            return Uni.createFrom().failure(failure);
        }
        long effectStartNanos = CommandEffectMetrics.startNanos();
        return beginDispatch(store, request, attemptAdmission)
            .onItem().<O>transformToUni(ignored -> this.<I, O>dispatchLegacyConnector(operation, request)
                .onFailure(failure -> !isNonRetryable(failure))
                .transform(failure -> CommandRetryableEffectException.mark(request.commandId(), failure))
                .onItem().transformToUni(output -> store.markSucceeded(
                        request.executionContext().tenantId(),
                        request.commandId(),
                        request.attemptId(),
                        output,
                        System.currentTimeMillis())
                    .invoke(record -> CommandEffectMetrics.recordTerminalTransition(
                        request.descriptor(), CommandEffectStatus.SUCCEEDED, effectStartNanos))
                    .replaceWith(output))
                .onFailure().call(failure -> recordFailure(
                    store, request, failure, System.currentTimeMillis(), effectStartNanos).replaceWithVoid()));
    }

    private <I, O> Uni<O> executeNative(
        CommandEffectStore store,
        CommandRequest<I> request,
        NativeCommandSelector selector,
        Optional<CommandAttemptAdmission> attemptAdmission
    ) {
        if (!store.supportsNativeOutcomeSnapshots()) {
            return Uni.createFrom().failure(new IllegalStateException(
                "native command operation " + selector.operationIdentity()
                    + " requires a CommandEffectStore that persists outcome snapshots"));
        }
        long effectStartNanos = CommandEffectMetrics.startNanos();
        return activateBinding(selector)
            .onItem().transformToUni(ignored -> executeActivatedNative(
                store, request, selector, effectStartNanos, attemptAdmission));
    }

    private <I, O> Uni<O> executeActivatedNative(
        CommandEffectStore store,
        CommandRequest<I> request,
        NativeCommandSelector selector,
        long effectStartNanos,
        Optional<CommandAttemptAdmission> attemptAdmission
    ) {
        CommandOperation<?, ?, ?> operation;
        try {
            operation = selector.binding().isPresent()
                ? requireBoundCommandOperation(selector)
                : requireRegistry().requireCommandOperation(
                    selector.operationIdentity(), selector.providerMajorVersion(), selector.policy());
        } catch (IllegalStateException | IllegalArgumentException failure) {
            return Uni.createFrom().failure(failure);
        }
        if (attemptAdmission.isPresent() && !operation.capabilities().retryRedriveSupported()) {
            return Uni.createFrom().failure(new IllegalStateException(
                "native command operation " + selector.operationIdentity()
                    + " does not support deliberate retry/redrive"));
        }
        ConnectorConfigurationDocument configuration = new ConnectorConfigurationDocument(request.config());
        Object boundConfiguration;
        ConnectorConfigurationSnapshot snapshot;
        try {
            ConnectorConfigSchema<?> schema = operation.configurationSchema().orElseThrow(() -> new IllegalStateException(
                "native command operation " + selector.operationIdentity() + " does not declare a configuration schema"));
            boundConfiguration = ConnectorConfigurationBinder.bind(
                schema, configuration, "native command operation " + selector.operationIdentity());
            snapshot = ConnectorConfigurationSnapshot.from(schema, configuration, false);
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        }
        return activateProviderFirst(selector)
            .onItem().transformToUni(ignored -> recoveryBinding(operation, store, request, selector, boundConfiguration, snapshot)
                .onItem().transformToUni(binding -> bindingForAttempt(store, request, attemptAdmission, binding)
                    .onItem().transformToUni(retainedBinding -> beginDispatch(store, request, attemptAdmission, retainedBinding)))
                .onItem().<O>transformToUni(dispatched -> dispatchNativeWithOutcome(
                    store, request, selector, operation, boundConfiguration, snapshot, effectStartNanos)))
            .map(value -> (O) value);
    }

    private <I> Uni<Void> beginDispatch(
        CommandEffectStore store,
        CommandRequest<I> request,
        Optional<CommandAttemptAdmission> attemptAdmission
    ) {
        return beginDispatch(store, request, attemptAdmission, Optional.empty());
    }

    private <I> Uni<Void> beginDispatch(
        CommandEffectStore store, CommandRequest<I> request,
        Optional<CommandAttemptAdmission> attemptAdmission, Optional<CommandRecoveryBinding> binding
    ) {
        // Each effect transition records its own wall-clock time so the store can show dispatch/write duration.
        Uni<CommandEffectRecord> admitted = attemptAdmission
            .map(admission -> binding
                .map(value -> store.createAttempt(request, admission, value, System.currentTimeMillis()))
                .orElseGet(() -> store.createAttempt(request, admission, System.currentTimeMillis())))
            .orElseGet(() -> binding
                .map(value -> store.createPending(request, value, System.currentTimeMillis()))
                .orElseGet(() -> store.createPending(request, System.currentTimeMillis())));
        return admitted
            .invoke(ignored -> CommandEffectMetrics.recordAdmission(
                request.descriptor(),
                attemptAdmission.map(value -> value.purpose().name().toLowerCase(java.util.Locale.ROOT))
                    .orElse("initial")))
            .invoke(ignored -> CommandEffectMetrics.recordTransition(
                request.descriptor(),
                CommandEffectStatus.PENDING))
            .onItem().transformToUni(ignored -> binding
                .map(value -> store.claimPendingDispatch(value, System.currentTimeMillis()))
                .orElseGet(() -> store.markDispatching(
                request.executionContext().tenantId(),
                request.commandId(),
                request.attemptId(),
                System.currentTimeMillis())))
            .invoke(ignored -> CommandEffectMetrics.recordTransition(
                request.descriptor(),
                CommandEffectStatus.DISPATCHING))
            .replaceWithVoid();
    }

    private static boolean recoveryEligible(CommandEffectStore store, CommandRequest<?> request, CommandEffectRecord record) {
        return store.supportsRecovery() && request.descriptor().nativeSelector().isPresent()
            && request.callbackContext().isEmpty() && record.currentAttempt().recoveryBinding().isPresent();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Uni<Optional<CommandRecoveryBinding>> recoveryBinding(
        CommandOperation operation, CommandEffectStore store, CommandRequest<?> request,
        NativeCommandSelector selector, Object configuration, ConnectorConfigurationSnapshot snapshot
    ) {
        // Callback destinations are not captured by this recovery contract. Never guess a binding.
        if (!store.supportsRecovery() || !operation.capabilities().reconciliationSupported()
            || request.callbackContext().isPresent()) {
            return Uni.createFrom().item(Optional.empty());
        }
        var binding = selector.binding().orElseGet(() ->
            org.pipelineframework.connector.ConnectorBindingName.of(selector.operationIdentity().providerId().value()));
        CommandInvocation invocation = nativeInvocation(request, selector, configuration);
        CompletionStage<Optional<CommandRecoveryTarget>> target = invocationCoordinator.invoke(binding, operation,
            () -> java.util.concurrent.CompletableFuture.completedFuture(operation.recoveryTarget(invocation)));
        return Uni.createFrom().completionStage(target).map(value -> value.map(destination ->
            CommandRecoverySupport.binding(request, selector, snapshot, destination)));
    }

    private Uni<Optional<CommandRecoveryBinding>> bindingForAttempt(
        CommandEffectStore store, CommandRequest<?> request, Optional<CommandAttemptAdmission> admission,
        Optional<CommandRecoveryBinding> binding
    ) {
        if (admission.isEmpty() || binding.isEmpty()) {
            return Uni.createFrom().item(binding);
        }
        // Existing effect history retains the original typed input, not a second per-attempt payload.
        // A deliberate effect with changed input remains legal but cannot gain recovery by inference.
        return store.find(request.executionContext().tenantId(), request.commandId()).map(record ->
            record.filter(value -> new CommandEffectRecordCodec().digest(value.input(), request.descriptor().inputType())
                .equals(binding.orElseThrow().inputDigest())).isPresent() ? binding : Optional.empty());
    }

    private <I, O> Uni<O> recoverNative(CommandEffectStore store, CommandRequest<I> fresh, CommandEffectRecord record) {
        CommandRecoveryBinding retained = record.currentAttempt().recoveryBinding().orElseThrow();
        CommandRequest<I> request = new CommandRequest<>(fresh.descriptor(), fresh.commandId(),
            retained.occurrenceId(), retained.attemptId(), fresh.input(), fresh.executionContext(), fresh.config(), fresh.callbackContext());
        NativeCommandSelector selector = request.descriptor().nativeSelector().orElseThrow();
        return activateBinding(selector).onItem().transformToUni(ignored -> {
            CommandOperation<?, ?, ?> operation = selector.binding().isPresent() ? requireBoundCommandOperation(selector)
                : requireRegistry().requireCommandOperation(selector.operationIdentity(), selector.providerMajorVersion(), selector.policy());
            if (!operation.capabilities().reconciliationSupported()) {
                return Uni.createFrom().failure(new CommandInProgressException("Command provider does not support recovery"));
            }
            ConnectorConfigSchema<?> schema = operation.configurationSchema().orElseThrow();
            ConnectorConfigurationDocument document = new ConnectorConfigurationDocument(request.config());
            Object configuration = ConnectorConfigurationBinder.bind(schema, document, "native Command recovery");
            ConnectorConfigurationSnapshot snapshot = ConnectorConfigurationSnapshot.from(schema, document, false);
            return activateProviderFirst(selector).onItem().transformToUni(active ->
                recoveryBinding(operation, store, request, selector, configuration, snapshot)
                    .onItem().transformToUni(proposed -> {
                        if (proposed.isEmpty()) {
                            return Uni.createFrom().failure(new CommandInProgressException("Command target recovery binding is unavailable"));
                        }
                        CommandRecoverySupport.verify(record, request, proposed.orElseThrow());
                        if (record.status() == CommandEffectStatus.PENDING) {
                            return store.claimPendingDispatch(retained, System.currentTimeMillis())
                                .onItem().<O>transformToUni(claimed -> dispatchNativeWithOutcome(
                                    store, request, selector, operation, configuration, snapshot, CommandEffectMetrics.startNanos()));
                        }
                        return this.<I, O>inquireNative(store, request, record, selector, operation, configuration, snapshot);
                    }));
        });
    }

    private <I, O> Uni<O> dispatchNativeWithOutcome(
        CommandEffectStore store, CommandRequest<I> request, NativeCommandSelector selector,
        CommandOperation<?, ?, ?> operation, Object configuration, ConnectorConfigurationSnapshot snapshot,
        long effectStartNanos
    ) {
        return dispatchNative(operation, request, selector, configuration)
            .onFailure(CommandStepSupport::isCancellation)
            .recoverWithItem(new CommandOutcome.Ambiguous<>("provider-dispatch-cancelled", List.of()))
            .onFailure(failure -> !isNonRetryable(failure))
            .transform(failure -> CommandRetryableEffectException.mark(request.commandId(), failure))
            .onItem().<O>transformToUni(outcome -> applyNativeOutcome(store, request, selector, snapshot,
                operation.capabilities(), selector.policy(), outcome, effectStartNanos))
            .onFailure(CommandRetryableOutcomeException.class)
            .transform(failure -> CommandRetryableEffectException.mark(request.commandId(), failure))
            .onFailure().call(failure -> isTypedOutcomeFailure(failure) || isStoreFailure(failure)
                ? Uni.createFrom().voidItem() : recordFailure(store, request, failure, System.currentTimeMillis(), effectStartNanos).replaceWithVoid());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <I, O> Uni<O> inquireNative(
        CommandEffectStore store, CommandRequest<I> request, CommandEffectRecord record, NativeCommandSelector selector,
        CommandOperation operation, Object configuration, ConnectorConfigurationSnapshot snapshot
    ) {
        CommandRecoveryBinding expected = record.currentAttempt().recoveryBinding().orElseThrow();
        var invocation = new CommandReconciliationInvocation(nativeInvocation(request, selector, configuration), expected);
        CompletionStage<CommandReconciliationResult<Object>> stage = invocationCoordinator.invoke(expected.binding(), operation,
            () -> operation.reconcile(invocation));
        return Uni.createFrom().completionStage(stage).onItem().transformToUni(result -> {
            if (!(result instanceof CommandReconciliationResult.ConfirmedSucceeded<Object> confirmed)) {
                String code = result instanceof CommandReconciliationResult.Unresolved<?> unresolved
                    ? unresolved.code() : "invalid-provider-response";
                return Uni.createFrom().failure(new CommandInProgressException("Command reconciliation unresolved: " + code));
            }
            if (!expected.equals(confirmed.binding())
                || !operation.capabilities().durableReferenceKinds().contains(confirmed.receipt().kind())
                || !confirmed.outcome().references().contains(confirmed.receipt())
                || confirmationBarrier(selector.policy(), confirmed.outcome().confirmation()).isPresent()) {
                return Uni.createFrom().failure(new CommandRecoveryConflictException("Provider receipt binding/confirmation is insufficient or conflicting"));
            }
            Object output = confirmed.outcome().output();
            long now = System.currentTimeMillis();
            CommandReconciliationReceipt receipt = new CommandReconciliationReceipt(confirmed.receipt(),
                new CommandEffectRecordCodec().digest(output, request.descriptor().outputType()), now);
            CommandOutcomeSnapshot outcome = snapshot(selector, snapshot, operation.capabilities(), CommandEffectStatus.SUCCEEDED,
                confirmed.outcome().code(), confirmed.outcome().flags(), confirmed.outcome().confirmation(), confirmed.outcome().references());
            return Uni.createFrom().deferred(() -> store.reconcileSucceeded(expected, record.status(), output, outcome, receipt, now))
                .replaceWith((O) output)
                .onFailure(CommandStepSupport::isTransitionConflict)
                .recoverWithUni(failure -> matchingSuccessOrConflict(store, request, output, outcome));
        });
    }

    private <O> Uni<O> matchingSuccessOrConflict(
        CommandEffectStore store, CommandRequest<?> request, Object output, CommandOutcomeSnapshot snapshot
    ) {
        return store.find(request.executionContext().tenantId(), request.commandId()).onItem().transformToUni(current -> {
            if (current.isPresent() && CommandRecoverySupport.sameSuccess(current.orElseThrow(), request, output, snapshot)) {
                @SuppressWarnings("unchecked") O recorded = (O) current.orElseThrow().output();
                return Uni.createFrom().item(recorded);
            }
            return Uni.createFrom().failure(new CommandRecoveryConflictException("Concurrent or late Command outcome conflicts with retained authority"));
        });
    }

    private static boolean isTransitionConflict(Throwable failure) {
        return failure instanceof CommandEffectConflictException || failure instanceof IllegalStateException;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static CommandInvocation nativeInvocation(CommandRequest<?> request, NativeCommandSelector selector, Object configuration) {
        var binding = selector.binding().orElseGet(() ->
            org.pipelineframework.connector.ConnectorBindingName.of(selector.operationIdentity().providerId().value()));
        return new CommandInvocation(request.input(), configuration, commandOutputType(request.descriptor()),
            connectorExecutionContext(request, selector, binding), Optional.of(new org.pipelineframework.connector.CommandDispatchIdentity(
                request.commandId(), request.occurrenceId(), request.attemptId())), request.callbackContext());
    }

    private static boolean isStoreFailure(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof CommandEffectStoreException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <I> Uni<CommandOutcome<Object>> dispatchNative(
        CommandOperation<?, ?, ?> operation,
        CommandRequest<I> request,
        NativeCommandSelector selector,
        Object boundConfiguration
    ) {
        CommandOperation raw = operation;
        org.pipelineframework.connector.ConnectorBindingName binding = selector.binding().orElseGet(() ->
            org.pipelineframework.connector.ConnectorBindingName.of(selector.operationIdentity().providerId().value()));
        CompletionStage<CommandOutcome<Object>> stage = invocationCoordinator.invoke(binding, raw, () ->
            raw.dispatch(new org.pipelineframework.connector.CommandInvocation<>(
                request.input(),
                boundConfiguration,
                commandOutputType(request.descriptor()),
                connectorExecutionContext(request, selector, binding),
                Optional.of(new org.pipelineframework.connector.CommandDispatchIdentity(
                    request.commandId(), request.occurrenceId(), request.attemptId())), request.callbackContext())));
        return Uni.createFrom().completionStage(stage)
            .onFailure().transform(CommandStepSupport::unwrapTransportFailure);
    }

    private static Class<?> commandOutputType(CommandDescriptor descriptor) {
        try {
            return Class.forName(
                descriptor.outputType(),
                true,
                org.pipelineframework.config.pipeline.PipelineResources.resolveClassLoader());
        } catch (ClassNotFoundException failure) {
            throw new IllegalStateException(
                "Command output type has no generated Java class: " + descriptor.outputType(), failure);
        }
    }

    private <O> Uni<O> applyNativeOutcome(
        CommandEffectStore store,
        CommandRequest<?> request,
        NativeCommandSelector selector,
        ConnectorConfigurationSnapshot configuration,
        CommandCapabilities capabilities,
        CommandPolicy policy,
        CommandOutcome<Object> outcome,
        long effectStartNanos
    ) {
        if (outcome == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "native command operation " + selector.operationIdentity() + " returned a null outcome"));
        }
        if (outcome instanceof CommandOutcome.Succeeded<Object> succeeded) {
            Optional<ConfirmationBarrier> barrier = confirmationBarrier(policy, succeeded.confirmation());
            if (barrier.isPresent()) {
                ConfirmationBarrier resolved = barrier.orElseThrow();
                CommandOutcomeSnapshot snapshot = snapshot(
                    selector, configuration, capabilities, resolved.status(), resolved.code(), succeeded.flags(),
                    succeeded.confirmation(), succeeded.references());
                CommandOutcomeException failure = new CommandOutcomeException(resolved.status(), resolved.code());
                return Uni.createFrom().deferred(() -> store.markOutcome(
                        request.executionContext().tenantId(), request.commandId(), request.attemptId(),
                        resolved.status(), failure, snapshot,
                        System.currentTimeMillis()))
                    .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(
                        request.descriptor(), resolved.status(), effectStartNanos))
                    .onFailure().recoverWithUni(conflict -> guardLateFailure(store, request, conflict))
                    .onItem().transformToUni(ignored -> Uni.createFrom().failure(failure));
            }
            CommandOutcomeSnapshot snapshot = snapshot(
                selector, configuration, capabilities, CommandEffectStatus.SUCCEEDED, succeeded.code(), succeeded.flags(),
                succeeded.confirmation(), succeeded.references());
            @SuppressWarnings("unchecked")
            O output = (O) succeeded.output();
            return Uni.createFrom().deferred(() -> store.markSucceeded(
                    request.executionContext().tenantId(), request.commandId(), request.attemptId(),
                    output, snapshot, System.currentTimeMillis()))
                .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(
                    request.descriptor(), CommandEffectStatus.SUCCEEDED, effectStartNanos))
                .replaceWith(output)
                .onFailure(CommandStepSupport::isTransitionConflict)
                .recoverWithUni(failure -> store.find(request.executionContext().tenantId(), request.commandId())
                    .onItem().transformToUni(current -> current.isPresent() && current.orElseThrow().currentAttempt().recoveryBinding().isPresent()
                        ? matchingSuccessOrConflict(store, request, output, snapshot)
                        : Uni.createFrom().failure(failure)));
        }
        CommandEffectStatus status = outcomeStatus(outcome);
        CommandOutcomeSnapshot snapshot = snapshot(
            selector, configuration, capabilities, status, outcome.code(), outcome.flags(), outcome.confirmation(), outcome.references());
        Throwable failure = status == CommandEffectStatus.FAILED_RETRYABLE
            ? new CommandRetryableOutcomeException(outcome.code())
            : new CommandOutcomeException(status, outcome.code());
        return Uni.createFrom().deferred(() -> store.markOutcome(
                request.executionContext().tenantId(), request.commandId(), request.attemptId(),
                status, failure, snapshot, System.currentTimeMillis()))
            .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(request.descriptor(), status, effectStartNanos))
            .onFailure().recoverWithUni(conflict -> guardLateFailure(store, request, conflict))
            .onItem().transformToUni(ignored -> Uni.createFrom().failure(failure));
    }

    private static CommandOutcomeSnapshot snapshot(
        NativeCommandSelector selector,
        ConnectorConfigurationSnapshot configuration,
        CommandCapabilities capabilities,
        CommandEffectStatus status,
        String code,
        java.util.Set<String> flags,
        CommandConfirmation confirmation,
        List<CommandReference> references
    ) {
        java.util.Set<String> declared = capabilities.durableReferenceKinds();
        List<CommandReference> safeReferences = references.stream()
            .filter(reference -> declared.contains(reference.kind()))
            .toList();
        return new CommandOutcomeSnapshot(
            selector.operationIdentity(), selector.providerMajorVersion(), configuration, status, code, flags,
            confirmation.machineConfirmation(), confirmation.userConfirmed(), safeReferences);
    }

    private static Optional<ConfirmationBarrier> confirmationBarrier(
        CommandPolicy policy,
        CommandConfirmation achieved
    ) {
        Optional<CommandMachineConfirmation> requiredMachine = policy.minimumMachineConfirmation();
        if (requiredMachine.isPresent() && !achieved.machineConfirmation().satisfies(requiredMachine.orElseThrow())) {
            return Optional.of(new ConfirmationBarrier(
                CommandEffectStatus.AMBIGUOUS,
                "machine-confirmation-insufficient"));
        }
        if (policy.requireUserConfirmation() && !achieved.userConfirmed()) {
            return Optional.of(new ConfirmationBarrier(
                CommandEffectStatus.USER_ACTION_REQUIRED,
                "user-confirmation-required"));
        }
        return Optional.empty();
    }

    private static CommandEffectStatus outcomeStatus(CommandOutcome<?> outcome) {
        if (outcome instanceof CommandOutcome.RetryableFailure<?>) {
            return CommandEffectStatus.FAILED_RETRYABLE;
        }
        if (outcome instanceof CommandOutcome.TerminalFailure<?>) {
            return CommandEffectStatus.DLQ;
        }
        if (outcome instanceof CommandOutcome.Ambiguous<?>) {
            return CommandEffectStatus.AMBIGUOUS;
        }
        if (outcome instanceof CommandOutcome.UserActionRequired<?>) {
            return CommandEffectStatus.USER_ACTION_REQUIRED;
        }
        throw new IllegalStateException("unsupported native command outcome " + outcome.getClass().getName());
    }

    private Uni<CommandEffectRecord> recordFailure(
        CommandEffectStore store,
        CommandRequest<?> request,
        Throwable failure,
        long nowEpochMs,
        long effectStartNanos
    ) {
        if (isNonRetryable(failure)) {
            return Uni.createFrom().deferred(() -> store.markDlq(
                request.executionContext().tenantId(),
                request.commandId(),
                request.attemptId(),
                failure,
                nowEpochMs))
                .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(
                    request.descriptor(),
                    CommandEffectStatus.DLQ,
                    effectStartNanos))
                .onFailure().recoverWithUni(conflict -> guardLateFailure(store, request, conflict));
        }
        return Uni.createFrom().deferred(() -> store.markFailed(
            request.executionContext().tenantId(),
            request.commandId(),
            request.attemptId(),
            failure,
            nowEpochMs))
            .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(
                request.descriptor(),
                CommandEffectStatus.FAILED_RETRYABLE,
                effectStartNanos))
            .onFailure().recoverWithUni(conflict -> guardLateFailure(store, request, conflict));
    }

    private Uni<CommandEffectRecord> guardLateFailure(CommandEffectStore store, CommandRequest<?> request, Throwable failure) {
        return store.find(request.executionContext().tenantId(), request.commandId()).onItem().transformToUni(current ->
            Uni.createFrom().failure(current.isPresent() && current.orElseThrow().currentAttempt().recoveryBinding().isPresent()
                ? new CommandRecoveryConflictException("Late Command failure cannot overwrite retained recovery authority") : failure));
    }

    private boolean isNonRetryable(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof NonRetryableException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isTypedOutcomeFailure(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof CommandOutcomeException || current instanceof CommandRetryableOutcomeException) {
                return true;
            }
            Throwable cause = current.getCause();
            current = cause == current ? null : cause;
        }
        return false;
    }

    private static String recordedOutcomeCode(CommandEffectRecord record) {
        return record.outcome()
            .map(CommandOutcomeSnapshot::outcomeCode)
            .orElseGet(() -> switch (record.status()) {
                case FAILED_RETRYABLE -> "recorded-retryable-failure";
                case DLQ -> "recorded-terminal-failure";
                case AMBIGUOUS -> "recorded-ambiguous";
                case USER_ACTION_REQUIRED -> "recorded-user-action-required";
                default -> "recorded-command-effect";
            });
    }

    private static Throwable unwrapTransportFailure(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static boolean isCancellation(Throwable failure) {
        return unwrapTransportFailure(failure) instanceof CancellationException;
    }

    private record ConfirmationBarrier(CommandEffectStatus status, String code) {
    }

    private PipelineExecutionContext captureExecutionContext() {
        if (orchestratorConfig == null || orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            throw new IllegalStateException("Command steps require pipeline.orchestrator.mode=QUEUE_ASYNC.");
        }
        PipelineExecutionContext context = PipelineExecutionContextHolder.get().orElse(null);
        if (context == null) {
            throw new IllegalStateException("Command step executed without queue-async execution context.");
        }
        return context;
    }

    private LegacyCommandConnectorProvider.LegacyCommandOperation requireLegacyOperation(String command) {
        return LegacyCommandConnectorProvider.requireOperation(requireRegistry(), command);
    }

    private ConnectorRegistry requireRegistry() {
        ConnectorRegistry registry = fixedConnectorRegistry != null ? fixedConnectorRegistry : connectorRegistry;
        if (registry == null) {
            throw new IllegalStateException("connector registry is not available for command execution");
        }
        return registry;
    }

    private CommandOperation<?, ?, ?> requireBoundCommandOperation(NativeCommandSelector selector) {
        ConnectorBindingRegistry registry = requireBindingRegistry();
        org.pipelineframework.connector.ConnectorBindingName binding = selector.binding().orElseThrow();
        org.pipelineframework.connector.ConnectorProvider<?> provider = registry.requireProvider(binding);
        if (!provider.id().equals(selector.operationIdentity().providerId())
            || provider.version().major() != selector.providerMajorVersion()) {
            throw new IllegalStateException(
                "connector binding '" + binding.value() + "' resolves provider " + provider.id().value()
                    + " v" + provider.version().major() + " but command descriptor requires "
                    + selector.operationIdentity().providerId().value() + " v" + selector.providerMajorVersion());
        }
        return registry.requireCommandOperation(
            binding,
            selector.operationIdentity().operationId(),
            selector.operationIdentity().majorVersion(),
            selector.policy());
    }

    private Uni<Void> activateBinding(NativeCommandSelector selector) {
        if (selector.binding().isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        try {
            return Uni.createFrom().completionStage(requireBindingRegistry().activate(
                selector.binding().orElseThrow(), requireConnectorRuntimeContext()));
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        }
    }

    private Uni<Void> activateProviderFirst(NativeCommandSelector selector) {
        if (selector.binding().isPresent()) {
            return Uni.createFrom().voidItem();
        }
        return Uni.createFrom().completionStage(
            requireRegistry().activate(selector.operationIdentity().providerId(), requireConnectorRuntimeContext()));
    }

    private ConnectorBindingRegistry requireBindingRegistry() {
        ConnectorBindingRegistry registry = fixedConnectorBindingRegistry != null
            ? fixedConnectorBindingRegistry
            : connectorBindingRegistry;
        if (registry == null) {
            throw new IllegalStateException("connector binding registry is not available for command execution");
        }
        return registry;
    }

    private ConnectorRuntimeContext requireConnectorRuntimeContext() {
        ConnectorRuntimeContext context = fixedConnectorRuntimeContext != null
            ? fixedConnectorRuntimeContext
            : connectorRuntimeContext;
        if (context == null) {
            throw new IllegalStateException("connector runtime context is not available for command execution");
        }
        return context;
    }

    private static ConnectorExecutionContext connectorExecutionContext(
        CommandRequest<?> request,
        NativeCommandSelector selector,
        org.pipelineframework.connector.ConnectorBindingName binding
    ) {
        PipelineExecutionContext context = request.executionContext();
        return ConnectorExecutionContext.managed(
            context.tenantId(),
            context.executionId(),
            context.pipelineId(),
            context.contractVersion(),
            context.releaseVersion(),
            request.descriptor().stepId(),
            new org.pipelineframework.connector.ConnectorInvocationTarget(binding, selector.operationIdentity()),
            context.correlationId(),
            context.traceId(),
            Optional.empty());
    }

    private <I, O> Uni<O> dispatchLegacyConnector(
        LegacyCommandConnectorProvider.LegacyCommandOperation operation,
        CommandRequest<I> request
    ) {
        return Uni.createFrom().<O>completionStage(operation.dispatchOutput(request));
    }

    /**
     * Returns the single configured effect store. Command v1 does not support store routing;
     * multiple stores are treated as a misconfiguration rather than silently picking one.
     */
    private CommandEffectStore selectStore() {
        CommandEffectStore fixedStore = selectSingleStore(fixedStores);
        if (fixedStore != null) {
            return fixedStore;
        }
        CommandEffectStore injectedStore = selectSingleStore(stores);
        if (injectedStore != null) {
            return injectedStore;
        }
        throw new IllegalStateException("No CommandEffectStore configured for command step");
    }

    private CommandEffectStore selectSingleStore(Iterable<CommandEffectStore> candidates) {
        if (candidates == null) {
            return null;
        }
        CommandEffectStore selected = null;
        for (CommandEffectStore store : candidates) {
            if (store == null) {
                continue;
            }
            if (selected != null) {
                throw new IllegalStateException(
                    "Multiple CommandEffectStore instances configured; command steps support a single effect store");
            }
            selected = store;
        }
        return selected;
    }
}

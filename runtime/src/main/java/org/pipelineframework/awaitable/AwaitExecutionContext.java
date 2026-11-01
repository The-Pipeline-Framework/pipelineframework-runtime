package org.pipelineframework.awaitable;

import java.util.Map;
import java.util.Optional;
import org.pipelineframework.orchestrator.PagedTransitionContext;

/** Internal queue-async execution context used by generated await steps. */
public final class AwaitExecutionContext {
    private final String tenantId;
    private final String executionId;
    private final AwaitContinuationMode continuationMode;
    private final TerminalOutputOwnership terminalOutputOwnership;
    private final Map<String, Object> traceMetadata;
    private final Optional<PagedTransitionContext> pageContext;
    private final Optional<String> awaitOccurrenceCause;
    private final Optional<org.pipelineframework.invocation.PipelineInvocationPath> invocationPath;
    private int currentStepIndex;

    public AwaitExecutionContext(String tenantId, String executionId, int currentStepIndex) {
        this(tenantId, executionId, currentStepIndex, AwaitContinuationMode.LIVE_IF_SUPPORTED,
            TerminalOutputOwnership.TRANSITION_WORKER, Map.of(), Optional.empty());
    }

    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        int currentStepIndex,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership
    ) {
        this(tenantId, executionId, currentStepIndex, continuationMode, terminalOutputOwnership,
            Map.of(), Optional.empty());
    }

    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        int currentStepIndex,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership,
        Map<String, Object> traceMetadata
    ) {
        this(tenantId, executionId, currentStepIndex, continuationMode, terminalOutputOwnership,
            traceMetadata, Optional.empty());
    }

    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        int currentStepIndex,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership,
        Map<String, Object> traceMetadata,
        Optional<PagedTransitionContext> pageContext
    ) {
        this(tenantId, executionId, currentStepIndex, continuationMode, terminalOutputOwnership,
            traceMetadata, pageContext, Optional.empty());
    }

    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        int currentStepIndex,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership,
        Map<String, Object> traceMetadata,
        Optional<PagedTransitionContext> pageContext,
        Optional<String> awaitOccurrenceCause
    ) {
        this(tenantId, executionId, currentStepIndex, continuationMode, terminalOutputOwnership, traceMetadata, pageContext, awaitOccurrenceCause, Optional.empty());
    }

    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        int currentStepIndex,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership,
        Map<String, Object> traceMetadata,
        Optional<PagedTransitionContext> pageContext,
        Optional<String> awaitOccurrenceCause,
        Optional<org.pipelineframework.invocation.PipelineInvocationPath> invocationPath
    ) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be non-negative");
        }
        this.invocationPath = java.util.Objects.requireNonNull(invocationPath, "invocationPath");
        this.tenantId = tenantId;
        this.executionId = executionId;
        this.currentStepIndex = currentStepIndex;
        this.continuationMode = java.util.Objects.requireNonNull(continuationMode, "continuationMode must not be null");
        this.terminalOutputOwnership = java.util.Objects.requireNonNull(
            terminalOutputOwnership, "terminalOutputOwnership must not be null");
        this.traceMetadata = Map.copyOf(java.util.Objects.requireNonNull(traceMetadata, "traceMetadata must not be null"));
        this.pageContext = java.util.Objects.requireNonNull(pageContext, "pageContext must not be null");
        this.awaitOccurrenceCause = java.util.Objects.requireNonNull(
            awaitOccurrenceCause, "awaitOccurrenceCause must not be null");
        this.awaitOccurrenceCause.ifPresent(value -> {
            if (value.isBlank()) throw new IllegalArgumentException("awaitOccurrenceCause must not be blank");
        });
    }

    public Optional<org.pipelineframework.invocation.PipelineInvocationPath> invocationPath() { return invocationPath; }

    /** Validates durable resume support before an Await creates state or dispatches. */
    public Optional<org.pipelineframework.invocation.PipelineInvocationPath> invocationPathForAwait() {
        invocationPath.ifPresent(path -> {
            if (path.frames().stream().anyMatch(frame ->
                !"$root".equals(frame.definitionId()) || !"$recur".equals(frame.callsiteId()))) {
                throw new IllegalStateException("Await supports only root recurrence invocation paths");
            }
        });
        return invocationPath.filter(path -> !path.frames().isEmpty());
    }

    public String tenantId() { return tenantId; }

    public String executionId() { return executionId; }

    public int currentStepIndex() { return currentStepIndex; }

    public AwaitContinuationMode continuationMode() { return continuationMode; }

    public TerminalOutputOwnership terminalOutputOwnership() { return terminalOutputOwnership; }

    public Map<String, Object> traceMetadata() { return traceMetadata; }

    public Optional<PagedTransitionContext> pageContext() { return pageContext; }

    public Optional<String> awaitOccurrenceCause() { return awaitOccurrenceCause; }

    /** Stable identity prefix shared by every item and effect in this execution page. */
    public String identityScope() {
        return executionId + pageContext.map(page -> ":page:" + page.pageIndex()).orElse("");
    }

    public void currentStepIndex(int currentStepIndex) {
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be non-negative");
        }
        this.currentStepIndex = currentStepIndex;
    }
}

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
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be non-negative");
        }
        this.tenantId = tenantId;
        this.executionId = executionId;
        this.currentStepIndex = currentStepIndex;
        this.continuationMode = java.util.Objects.requireNonNull(continuationMode, "continuationMode must not be null");
        this.terminalOutputOwnership = java.util.Objects.requireNonNull(
            terminalOutputOwnership, "terminalOutputOwnership must not be null");
        this.traceMetadata = Map.copyOf(java.util.Objects.requireNonNull(traceMetadata, "traceMetadata must not be null"));
        this.pageContext = java.util.Objects.requireNonNull(pageContext, "pageContext must not be null");
    }

    public String tenantId() { return tenantId; }

    public String executionId() { return executionId; }

    public int currentStepIndex() { return currentStepIndex; }

    public AwaitContinuationMode continuationMode() { return continuationMode; }

    public TerminalOutputOwnership terminalOutputOwnership() { return terminalOutputOwnership; }

    public Map<String, Object> traceMetadata() { return traceMetadata; }

    public Optional<PagedTransitionContext> pageContext() { return pageContext; }

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

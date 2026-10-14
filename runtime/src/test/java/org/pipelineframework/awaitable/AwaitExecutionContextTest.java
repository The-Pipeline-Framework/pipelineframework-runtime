package org.pipelineframework.awaitable;

import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.orchestrator.PagedTransitionContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AwaitExecutionContextTest {

    @Test
    void constructsWithValidFields() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-123", 2);

        assertEquals("tenant1", context.tenantId());
        assertEquals("exec-123", context.executionId());
        assertEquals(2, context.currentStepIndex());
        assertEquals(AwaitContinuationMode.LIVE_IF_SUPPORTED, context.continuationMode());
        assertEquals(TerminalOutputOwnership.TRANSITION_WORKER, context.terminalOutputOwnership());
    }

    @Test
    void acceptsZeroStepIndex() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-123", 0);

        assertEquals(0, context.currentStepIndex());
    }

    @Test
    void rejectsBlankTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext("", "exec-123", 0));
    }

    @Test
    void rejectsWhitespaceTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext("   ", "exec-123", 0));
    }

    @Test
    void rejectsNullTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext(null, "exec-123", 0));
    }

    @Test
    void rejectsBlankExecutionId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext("tenant1", "", 0));
    }

    @Test
    void rejectsNullExecutionId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext("tenant1", null, 0));
    }

    @Test
    void rejectsNegativeStepIndex() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext("tenant1", "exec-123", -1));
    }

    @Test
    void updatesStepIndexMutation() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-123", 0);
        context.currentStepIndex(5);

        assertEquals(5, context.currentStepIndex());
    }

    @Test
    void rejectsNegativeStepIndexOnMutation() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-123", 0);

        assertThrows(IllegalArgumentException.class, () -> context.currentStepIndex(-1));
    }

    @Test
    void allowsIncrementingStepIndexRepeatedly() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-123", 0);
        context.currentStepIndex(1);
        context.currentStepIndex(2);
        context.currentStepIndex(10);

        assertEquals(10, context.currentStepIndex());
    }

    @Test
    void retainsAwaitCauseSeparatelyFromCommandIdentity() {
        var page = new PagedTransitionContext(2, "snapshot", Optional.empty(), 20);
        AwaitExecutionContext context = new AwaitExecutionContext("tenant", "exec", 4,
            AwaitContinuationMode.DURABLE_HANDOFF, TerminalOutputOwnership.TRANSITION_WORKER,
            Map.of(), Optional.of(page), Optional.of("reissue-v7-command"));

        assertEquals("exec:page:2", context.identityScope());
        assertEquals(Optional.of("reissue-v7-command"), context.awaitOccurrenceCause());
        assertEquals(Optional.empty(), new AwaitExecutionContext("tenant", "exec", 0).awaitOccurrenceCause());
    }
}

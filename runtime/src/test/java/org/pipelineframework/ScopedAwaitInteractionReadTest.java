package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.Optional;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.orchestrator.*;

class ScopedAwaitInteractionReadTest {
    private final PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    private final ExecutionStateStore store = mock(ExecutionStateStore.class);
    private final AwaitCoordinator awaits = mock(AwaitCoordinator.class);
    private final ControlPlaneAdmissionPolicy policy = mock(ControlPlaneAdmissionPolicy.class);
    private final ExecutionSemanticCheckpoint pin = new ExecutionSemanticCheckpoint("tenant", "execution", "pipeline", "contract", "A1", ExecutionStatus.WAITING_EXTERNAL);
    private final ExecutionRecord<Object, Object> parent = mock(ExecutionRecord.class);
    private final AwaitInteractionRecord interaction = mock(AwaitInteractionRecord.class);
    private final ExecutionReadModel reads;

    ScopedAwaitInteractionReadTest() {
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(policy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
        var input = new ExecutionInputPolicy(); input.orchestratorConfig = config;
        reads = new ExecutionReadModel(config, input, store, awaits, policy, () -> "pipeline", () -> "A2", JsonTransitionPayloadCodec::new);
        when(parent.tenantId()).thenReturn("tenant"); when(parent.executionId()).thenReturn("execution");
        when(parent.pipelineId()).thenReturn("pipeline"); when(parent.contractVersion()).thenReturn("contract");
        when(parent.releaseVersion()).thenReturn("A1"); when(parent.status()).thenReturn(ExecutionStatus.WAITING_EXTERNAL);
        when(store.getExecution("tenant", "execution")).thenReturn(Uni.createFrom().item(Optional.of(parent)));
        when(interaction.tenantId()).thenReturn("tenant"); when(interaction.executionId()).thenReturn("execution");
        when(interaction.interactionId()).thenReturn("interaction"); when(interaction.correlationId()).thenReturn("correlation");
        when(interaction.unitId()).thenReturn("unit"); when(interaction.stepId()).thenReturn("review");
        when(interaction.status()).thenReturn(AwaitInteractionStatus.WAITING);
        when(interaction.transportType()).thenReturn("interaction-api"); when(interaction.deadlineEpochMs()).thenReturn(12000L);
        when(awaits.getInteraction("tenant", "interaction")).thenReturn(Uni.createFrom().item(Optional.of(interaction)));
    }

    @Test void exactRetainedA1ReadAfterA2ContainsOnlySafeCanonicalFields() throws Exception {
        var canonical = new TypedDurablePayload("Pending", "type-sha", "catalog-sha", JsonDurablePayloadCodec.ENCODING, 1, "{\"value\":42}".getBytes());
        when(awaits.canonicalRequestPayload(interaction)).thenReturn(canonical);
        when(awaits.canonicalCompletionPayloadTypeId(interaction)).thenReturn("Decision");
        var observed = reads.getAwaitInteractionView(pin, "interaction").await().indefinitely().orElseThrow();
        assertEquals("A1", observed.checkpoint().releaseVersion()); assertSame(canonical, observed.requestPayload());
        assertEquals("Decision", observed.completionPayloadTypeId()); assertEquals(12000, observed.deadlineEpochMs());
        assertEquals("interaction-api", observed.transportType()); assertFalse(observed.commandCallback());
        var json = org.pipelineframework.config.pipeline.PipelineJson.mapper().valueToTree(observed);
        assertFalse(json.has("transportMetadata")); assertFalse(json.has("resumeToken")); assertFalse(json.has("assignee"));
        verify(awaits).getInteraction("tenant", "interaction"); verify(awaits).canonicalRequestPayload(interaction);
        verify(awaits).canonicalCompletionPayloadTypeId(interaction); verifyNoMoreInteractions(awaits);
    }

    @ParameterizedTest @ValueSource(strings = {"tenant", "execution", "pipeline", "contract", "release"})
    void foreignParentPinsFailBeforeInteractionOrPayloadRead(String field) {
        switch (field) {
            case "tenant" -> when(parent.tenantId()).thenReturn("foreign");
            case "execution" -> when(parent.executionId()).thenReturn("foreign");
            case "pipeline" -> when(parent.pipelineId()).thenReturn("foreign");
            case "contract" -> when(parent.contractVersion()).thenReturn("foreign");
            case "release" -> when(parent.releaseVersion()).thenReturn("foreign");
        }
        assertThrows(IllegalStateException.class, () -> reads.getAwaitInteractionView(pin, "interaction").await().indefinitely());
        verifyNoInteractions(awaits);
    }

    @ParameterizedTest @ValueSource(strings = {"tenant", "execution", "interaction"})
    void foreignInteractionPointReadNeverExportsPayload(String field) {
        switch (field) {
            case "tenant" -> when(interaction.tenantId()).thenReturn("foreign");
            case "execution" -> when(interaction.executionId()).thenReturn("foreign");
            case "interaction" -> when(interaction.interactionId()).thenReturn("foreign");
        }
        assertThrows(IllegalStateException.class, () -> reads.getAwaitInteractionView(pin, "interaction").await().indefinitely());
        verify(awaits, never()).canonicalRequestPayload(any());
    }

    @Test void absenceAndDeniedAdmissionDoNotScanOrCreateInteractions() {
        when(awaits.getInteraction("tenant", "interaction")).thenReturn(Uni.createFrom().item(Optional.empty()));
        assertTrue(reads.getAwaitInteractionView(pin, "interaction").await().indefinitely().isEmpty());
        clearInvocations(awaits, store);
        when(policy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.deny("denied", "no read"));
        assertThrows(ControlPlaneAdmissionException.class, () -> reads.getAwaitInteractionView(pin, "interaction").await().indefinitely());
        verifyNoInteractions(awaits, store);
    }
}

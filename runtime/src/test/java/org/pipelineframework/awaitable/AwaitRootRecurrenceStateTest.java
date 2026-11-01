package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.invocation.PipelineInvocationContext;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AwaitRootRecurrenceStateTest {
    @AfterEach void clearContext() { AwaitExecutionContextHolder.clear(); }

    @Test
    void repeatedLiveVisitsUseTheExplicitInvocationContextWithoutAnAmbientWorker() {
        var interactions = new InMemoryAwaitInteractionStore();
        var units = new InMemoryAwaitUnitStore();
        var descriptor = new AwaitCompletionDescriptor("Review", Map.class.getName(), Map.class.getName(),
            Duration.ofMinutes(5), "interactionId", "interaction-api", Map.of(), List.of("id"));
        var coordinator = coordinator(interactions, units, descriptor);
        var invocation = PipelineInvocationContext.root(3);
        var seen = new java.util.HashSet<String>();
        for (int turn = 0; turn < 3; turn++) {
            invocation = invocation.enterRecursive("$root", "$recur");
            var context = new AwaitExecutionContext("tenant", "execution", 1,
                AwaitContinuationMode.LIVE_IF_SUPPORTED, TerminalOutputOwnership.TRANSITION_WORKER,
                Map.of(), Optional.empty(), Optional.of("initial-v1"), Optional.of(invocation.path()));
            var created = coordinator.createOrGet(descriptor, "tenant", "execution", "execution", 1,
                "request", Map.of("id", "request"), "owner", "owners", Map.of(), Optional.of(context))
                .await().indefinitely();
            assertFalse(created.duplicate());
            assertTrue(seen.add(created.record().unitId()));
            var occurrence = AwaitUnitOccurrence.fromUnitId(created.record().unitId()).orElseThrow();
            assertEquals("initial-v1", occurrence.occurrenceCause());
            assertTrue(occurrence.invocationFingerprint().isPresent());
            coordinator.complete(new AwaitCompletionCommand("tenant", created.record().interactionId(),
                created.record().correlationId(), "completion-" + turn, Map.of("answer", "yes"),
                "owner", System.currentTimeMillis())).await().indefinitely();
        }
    }

    @Test
    void completionRetainsTheInvocationPathThroughStoreAndWorkerEnvelope() {
        var interactions = new InMemoryAwaitInteractionStore();
        var units = new InMemoryAwaitUnitStore();
        var descriptor = new AwaitCompletionDescriptor("Review", Map.class.getName(), Map.class.getName(),
            Duration.ofMinutes(5), "interactionId", "interaction-api", Map.of(), List.of("id"));
        var coordinator = coordinator(interactions, units, descriptor);
        var path = PipelineInvocationContext.root(3).enterRecursive("$root", "$recur").path();
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 1,
            AwaitContinuationMode.DURABLE_HANDOFF, TerminalOutputOwnership.TRANSITION_WORKER,
            Map.of(), Optional.empty(), Optional.empty(), Optional.of(path)));
        var created = coordinator.createOrGet(descriptor, "tenant", "execution", 1, "request",
            Map.of("id", "request"), "owner", "owners").await().indefinitely().record();
        assertEquals(Optional.of(path), created.invocationPath());
        AwaitExecutionContextHolder.clear();
        var completed = coordinator.complete(new AwaitCompletionCommand("tenant", created.interactionId(),
            created.correlationId(), "completion", Map.of("answer", "yes"), "owner", System.currentTimeMillis()))
            .await().indefinitely().record();
        assertEquals(Optional.of(path), completed.invocationPath());
        // A fresh coordinator has no in-process invocation state.
        var restarted = coordinator(interactions, units, descriptor);
        var input = assertInstanceOf(ExecutionInputSnapshot.class,
            restarted.loadResumeInput("tenant", created.unitId()).await().indefinitely());
        var codec = new JsonTransitionPayloadCodec();
        var restored = assertInstanceOf(ExecutionInputSnapshot.class, codec.decode(codec.encode(input)));
        assertEquals(Map.of("answer", "yes"), restored.payload());
        assertEquals(Optional.of(path), restored.invocationPath());
        assertEquals(Map.of("answer", "yes"), restarted.loadResumePayload("tenant", created.unitId())
            .await().indefinitely());
    }

    private AwaitCoordinator coordinator(InMemoryAwaitInteractionStore interactions, InMemoryAwaitUnitStore units,
        AwaitCompletionDescriptor descriptor) {
        var coordinator = new AwaitCoordinator();
        coordinator.interactionStores = instances(List.<AwaitInteractionStore>of(interactions));
        coordinator.unitStores = instances(List.<AwaitUnitStore>of(units));
        coordinator.descriptorFactory = new AwaitCompletionDescriptorRegistry();
        coordinator.descriptorFactory.register(descriptor);
        coordinator.resumeTokenService = new AwaitResumeTokenService("root-recurrence-test-secret");
        return coordinator;
    }

    @SuppressWarnings("unchecked")
    private <T> Instance<T> instances(List<T> values) {
        Instance<T> instance = mock(Instance.class);
        when(instance.stream()).thenAnswer(ignored -> values.stream());
        return instance;
    }
}

package org.pipelineframework.aws.durable;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.*;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AwsDurableHostTelemetryTest {
    @Test
    void emfContainsActualCountBoundedDimensionsAndOnlyHashedCorrelation() throws Exception {
        var messages = new ArrayList<String>();
        var telemetry = new AwsDurableHostTelemetry(Clock.fixed(Instant.ofEpochMilli(123456), ZoneOffset.UTC), messages::add);
        telemetry.record(AwsDurableHostTelemetry.Stage.WAKEUP, AwsDurableHostTelemetry.Outcome.STALE, "private-tenant\nprivate-input", 7);
        var event = new ObjectMapper().readTree(messages.getFirst());
        assertEquals(123456, event.path("_aws").path("Timestamp").asLong());
        var directive = event.path("_aws").path("CloudWatchMetrics").get(0);
        assertEquals("TPF/AwsDurable", directive.path("Namespace").asText());
        assertEquals("[[\"Stage\",\"Outcome\"]]", directive.path("Dimensions").toString());
        assertEquals("HostOutcomeCount", directive.path("Metrics").get(0).path("Name").asText());
        assertEquals(1, event.path("HostOutcomeCount").asInt());
        assertEquals(7, event.path("Generation").asLong());
        assertTrue(event.path("CorrelationHash").asText().matches("[a-f0-9]{64}"));
        assertFalse(messages.getFirst().contains("private-"));
    }

    @Test
    void telemetrySinkFailureCannotChangeCallbackAcknowledgement() {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var callbacks = mock(AwsDurableCallbackClient.class);
        var binding = binding(); var identity = binding.awaitIdentity();
        when(bindings.find("tenant", "interaction", 2, 1)).thenReturn(Optional.of(binding));
        when(bindings.isCurrentGeneration("tenant", "execution", 2)).thenReturn(true);
        var telemetry = new AwsDurableHostTelemetry(Clock.systemUTC(), ignored -> { throw new IllegalStateException("logs unavailable"); });
        var service = new AwsDurableWakeupService(reader(), bindings, callbacks, ignored -> false, telemetry);
        assertEquals(AwsDurableWakeupDisposition.ACKNOWLEDGE, service.wake(identity));
        verify(bindings).recordDelivered(binding, "CALLBACK_ACCEPTED");
    }

    @Test
    void unknownProviderStateEmitsUnknownAndKeepsRetryWithoutReplacement() throws Exception {
        var messages = new ArrayList<String>();
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var callbacks = mock(AwsDurableCallbackClient.class);
        var replacement = mock(AwsDurableReplacementStarter.class);
        var binding = binding();
        when(bindings.find("tenant", "interaction", 2, 1)).thenReturn(Optional.of(binding));
        when(bindings.isCurrentGeneration("tenant", "execution", 2)).thenReturn(true);
        doThrow(ResourceNotFoundException.builder().message("uncertain").build()).when(callbacks).sendSuccessObserved(eq(binding), any());
        when(callbacks.state(binding)).thenReturn(AwsDurableProviderCallbackState.UNKNOWN);
        var service = new AwsDurableWakeupService(reader(), bindings, callbacks, replacement,
            new AwsDurableHostTelemetry(Clock.systemUTC(), messages::add));
        assertEquals(AwsDurableWakeupDisposition.RETRY, service.wake(binding.awaitIdentity()));
        verifyNoInteractions(replacement);
        assertEquals("PROVIDER_UNKNOWN", new ObjectMapper().readTree(messages.getFirst()).path("Outcome").asText());
    }

    @Test
    void replacementAndRepairFailuresEmitRetryEvidenceAndPreserveTheOriginalFailure() throws Exception {
        var messages = new ArrayList<String>();
        var telemetry = new AwsDurableHostTelemetry(Clock.systemUTC(), messages::add);
        var failure = new IllegalStateException("private provider failure");
        var replacement = new AwsDurableReplacementService((tenant, interaction) -> { throw failure; },
            mock(AwsDurableExecutionStarter.class), mock(AwsDurableCallbackBindingRepository.class), telemetry);
        assertSame(failure, assertThrows(IllegalStateException.class, () -> replacement.startReplacement(binding())));
        var repair = new AwsDurableTerminalReconciler(mock(AwsDurableCallbackBindingRepository.class),
            mock(org.pipelineframework.orchestrator.PipelineControlPlane.class), mock(AwsDurableExecutionStarter.class),
            ignored -> { throw failure; }, java.time.Duration.ofSeconds(1), telemetry);
        assertSame(failure, assertThrows(IllegalStateException.class, () -> repair.reconcile("private-provider-arn")));
        var mapper = new ObjectMapper();
        assertEquals(3, messages.size());
        assertEquals("REPLACEMENT", mapper.readTree(messages.get(0)).path("Stage").asText());
        assertEquals("FAILED", mapper.readTree(messages.get(0)).path("Outcome").asText());
        assertEquals("REPAIR", mapper.readTree(messages.get(1)).path("Stage").asText());
        assertEquals("REPAIR_AGE_UNKNOWN", mapper.readTree(messages.get(1)).path("Outcome").asText());
        assertFalse(mapper.readTree(messages.get(1)).has("RepairAgeMilliseconds"));
        assertEquals("REPAIR", mapper.readTree(messages.get(2)).path("Stage").asText());
        assertEquals("FAILED", mapper.readTree(messages.get(2)).path("Outcome").asText());
        for (String message : messages) {
            assertFalse(message.contains("private"));
        }
        var wakeup = new AwsDurableWakeupService((tenant, interaction) -> { throw failure; },
            mock(AwsDurableCallbackBindingRepository.class), mock(AwsDurableCallbackClient.class), ignored -> false, telemetry);
        assertSame(failure, assertThrows(IllegalStateException.class, () -> wakeup.wake(binding().awaitIdentity())));
        assertEquals("TRANSIENT_FAILURE", mapper.readTree(messages.get(3)).path("Outcome").asText());
    }

    private AwsDurableAwaitCheckpointReader reader() {
        return (tenant, interaction) -> Optional.of(new AwsDurableAwaitCheckpoint(tenant, "execution", interaction,
            "correlation", "unit", "step", "COMPLETED", "pipeline", "contract", "release"));
    }

    private AwsDurableCallbackBinding binding() {
        return new AwsDurableCallbackBinding(new AwsDurableAwaitIdentity("tenant", "execution", "interaction", "correlation", 2),
            "provider", "arn:aws:lambda:eu-west-1:123456789012:function:driver:1/durable-execution/provider", "callback-private",
            AwsDurableBindingStatus.OPEN, 100, 1000);
    }
}

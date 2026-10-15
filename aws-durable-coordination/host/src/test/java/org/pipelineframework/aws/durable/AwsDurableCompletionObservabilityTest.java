package org.pipelineframework.aws.durable;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.orchestrator.AwaitCompletionObservation;
import org.pipelineframework.orchestrator.AwaitCompletionObservationPage;
import org.pipelineframework.orchestrator.PipelineControlPlane;

class AwsDurableCompletionObservabilityTest {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

    @Test
    void completionStatesUseBoundedDimensionsAndOnlyNativeReleaseLatency() throws Exception {
        var messages = new ArrayList<String>();
        var telemetry = telemetry(messages);
        telemetry.recordCompletion(observation(AwaitCompletionObservation.State.OUTSTANDING, OptionalLong.empty()));
        telemetry.recordCompletion(observation(AwaitCompletionObservation.State.RELEASED, OptionalLong.of(37)));
        telemetry.recordCompletion(observation(AwaitCompletionObservation.State.TERMINAL_FAILED, OptionalLong.empty()));
        telemetry.recordCompletion(observation(AwaitCompletionObservation.State.UNKNOWN, OptionalLong.empty()));

        assertEquals(4, messages.size());
        assertEquals(List.of("OUTSTANDING", "RELEASED", "TERMINAL_FAILED", "UNKNOWN"), messages.stream()
            .map(this::read).map(event -> event.path("Outcome").asText()).toList());
        JsonNode released = read(messages.get(1));
        assertEquals("[[\"Stage\",\"Outcome\"]]", released.path("_aws").path("CloudWatchMetrics")
            .get(0).path("Dimensions").toString());
        assertTrue(released.path("_aws").path("CloudWatchMetrics").get(0).path("Metrics").toString()
            .contains("AdmissionToReleaseLatencyMilliseconds"));
        assertEquals(37, released.path("AdmissionToReleaseLatencyMilliseconds").asLong());
        for (int index : List.of(0, 2, 3)) {
            JsonNode event = read(messages.get(index));
            assertEquals(1, event.path("CompletionObservationCount").asInt());
            assertFalse(event.has("AdmissionToReleaseLatencyMilliseconds"));
            assertFalse(event.path("_aws").path("CloudWatchMetrics").get(0).path("Metrics").toString()
                .contains("AdmissionToReleaseLatencyMilliseconds"));
        }
        for (String message : messages) {
            assertEquals("COMPLETION", read(message).path("Stage").asText());
            assertTrue(read(message).path("CorrelationHash").asText().matches("[a-f0-9]{64}"));
            assertFalse(message.contains("tenant-secret"));
            assertFalse(message.contains("interaction-secret"));
        }
    }

    @Test
    void boundedPageTelemetryReportsPartialCoverageAndNeverClaimsGlobalInventory() throws Exception {
        var messages = new ArrayList<String>();
        var page = new AwaitCompletionObservationPage(
            List.of(observation(AwaitCompletionObservation.State.OUTSTANDING, OptionalLong.empty())),
            Optional.of(new org.pipelineframework.awaitable.AwaitCompletionCursor("tenant-secret", "opaque-cursor")), 2);

        telemetry(messages).recordCompletionPage("tenant-secret", page);

        assertEquals(2, messages.size()); // One row observation and one page-coverage event.
        JsonNode coverage = read(messages.getLast());
        assertEquals("BOUNDED_PAGE", coverage.path("Outcome").asText());
        assertEquals("BOUNDED_PAGE_ONLY_NOT_GLOBAL_INVENTORY", coverage.path("Coverage").asText());
        assertEquals(2, coverage.path("CompletionObservationEvaluatedEntries").asInt());
        assertEquals(1, coverage.path("CompletionObservationReturnedEntries").asInt());
        assertEquals(1, coverage.path("CompletionObservationHasMorePages").asInt());
        assertFalse(messages.getLast().contains("tenant-secret"));
        assertFalse(messages.getLast().contains("opaque-cursor"));
    }

    @Test
    void telemetryFailureCannotChangeNativeCompletionActionResult() throws Exception {
        PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        var observation = observation(AwaitCompletionObservation.State.RELEASED, OptionalLong.of(37));
        when(controlPlane.getAwaitCompletionObservation("tenant-secret", "interaction-secret"))
            .thenReturn(Uni.createFrom().item(observation));
        var actions = new AwsDurableControlPlaneActions(controlPlane,
            mock(AwsDurableCallbackBindingRepository.class), input -> input, mapper,
            Duration.ofSeconds(5), Duration.ofDays(1), telemetry(new ArrayList<>(), ignored -> {
                throw new IllegalStateException("telemetry unavailable");
            }));
        var identity = new AwsDurableAwaitIdentity("tenant-secret", "execution-secret", "interaction-secret",
            "correlation-secret", 1);

        var response = actions.invoke(AwsDurableActionRequest.completionObservation(identity));

        assertTrue(response.resultJson().isPresent());
        assertTrue(response.resultJson().orElseThrow().contains("RELEASED"));
        verify(controlPlane).getAwaitCompletionObservation("tenant-secret", "interaction-secret");
    }

    @Test
    void queryActionLogsTheTypedPageAndReturnsTheSameBoundedResult() throws Exception {
        PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        var page = new AwaitCompletionObservationPage(
            List.of(observation(AwaitCompletionObservation.State.OUTSTANDING, OptionalLong.empty())),
            Optional.empty(), 1);
        when(controlPlane.queryAwaitCompletionObservations("tenant-secret", 25, Optional.empty()))
            .thenReturn(Uni.createFrom().item(page));
        var messages = new ArrayList<String>();
        var actions = new AwsDurableControlPlaneActions(controlPlane,
            mock(AwsDurableCallbackBindingRepository.class), input -> input, mapper,
            Duration.ofSeconds(5), Duration.ofDays(1), telemetry(messages));

        var response = actions.invoke(AwsDurableActionRequest.completionQuery("tenant-secret", 25, Optional.empty()));

        assertTrue(response.resultJson().orElseThrow().contains("OUTSTANDING"));
        verify(controlPlane).queryAwaitCompletionObservations("tenant-secret", 25, Optional.empty());
        assertEquals(2, messages.size());
        assertEquals("BOUNDED_PAGE", read(messages.getLast()).path("Outcome").asText());
        assertEquals("BOUNDED_PAGE_ONLY_NOT_GLOBAL_INVENTORY", read(messages.getLast()).path("Coverage").asText());
    }

    @Test
    void legacyActionRequestWithoutCompletionPagingFieldsDefaultsToNoCursorAndNoPageLimit() throws Exception {
        String legacyJson = """
            {"operation":"QUERY_AWAIT_COMPLETIONS","tenantId":"tenant-secret","generation":1}
            """;

        var request = mapper.readValue(legacyJson, AwsDurableActionRequest.class);

        assertEquals(Optional.empty(), request.completionCursor());
        assertEquals(Optional.empty(), request.completionPageLimit());
        assertEquals(Optional.empty(), request.awaitSelectionCursor());
    }

    private AwsDurableHostTelemetry telemetry(List<String> messages) {
        return telemetry(messages, messages::add);
    }

    private AwsDurableHostTelemetry telemetry(List<String> messages, java.util.function.Consumer<String> sink) {
        return new AwsDurableHostTelemetry(Clock.fixed(Instant.ofEpochMilli(123456), ZoneOffset.UTC), sink);
    }

    private AwaitCompletionObservation observation(AwaitCompletionObservation.State state, OptionalLong latency) {
        return new AwaitCompletionObservation("tenant-secret", "interaction-secret", Optional.empty(), Optional.empty(),
            state, latency);
    }

    private JsonNode read(String message) {
        try {
            return mapper.readTree(message);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}

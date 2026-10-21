package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

class SqsHostManagedResponseTest {
    @Test
    void dispatchesSignedRequestWithoutInventingAResponseQueue() throws Exception {
        SqsClient client = mock(SqsClient.class);
        var descriptor = descriptor(hostManaged("https://sqs.us-east-2.amazonaws.com/123456789012/requests"));
        var result = dispatch(adapter(client, false), descriptor);
        var capture = org.mockito.ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(client).sendMessage(capture.capture());
        var request = capture.getValue();
        var body = PipelineJson.mapper().readTree(request.messageBody());
        assertEquals("https://sqs.us-east-2.amazonaws.com/123456789012/requests", request.queueUrl());
        assertEquals("exec-1", body.path("executionId").asText());
        assertEquals("interaction-1", body.path("interactionId").asText());
        assertEquals("corr-1", body.path("correlationId").asText());
        assertEquals("p-1", body.path("requestPayload").path("paymentId").asText());
        assertFalse(body.path("resumeToken").asText().isBlank());
        assertEquals("HOST_MANAGED", result.metadata().get("responseMode"));
        assertFalse(result.metadata().containsKey("responseQueueUrl"));
        assertEquals("HOST_MANAGED", body.path("transportMetadata").path("responseMode").asText());
        assertFalse(body.path("transportMetadata").has("responseQueueUrl"));
    }

    @Test
    void hostManagedResponseNeverClaimsALocalLiveWindow() {
        var adapter = adapter(mock(SqsClient.class), false);
        var descriptor = descriptor(hostManaged("http://sqs.local/requests"));
        assertFalse(adapter.supportsLiveAwaitWindow(descriptor));
        assertEquals("sqs://http://sqs.local/requests", adapter.admissionEndpoint(descriptor).orElseThrow());
    }

    @Test
    void enabledLocalPollerRejectsHostManagedBeforeSendingAnything() {
        SqsClient client = mock(SqsClient.class);
        var adapter = adapter(client, true);
        var descriptor = descriptor(hostManaged("http://sqs.local/requests"));
        assertThrows(IllegalStateException.class, () -> dispatch(adapter, descriptor));
        assertThrows(IllegalStateException.class, () -> adapter.supportsLiveAwaitWindow(descriptor));
        assertThrows(IllegalStateException.class, () -> adapter.admissionEndpoint(descriptor));
        verifyNoInteractions(client);
    }

    @Test
    void modeAndPhysicalResponseQueueAreMutuallyExclusive() {
        assertInvalid(Map.of("request", Map.of("queueUrl", "http://sqs.local/requests"),
            "response", Map.of("mode", "HOST_MANAGED", "queueUrl", "http://sqs.local/responses")), "response");
    }

    @Test
    void unknownOrMisspelledModeCannotSelectHostManagedDelivery() {
        for (String mode : List.of("QUEUE", "host_managed", "", "OTHER")) {
            assertInvalid(Map.of("request", Map.of("queueUrl", "http://sqs.local/requests"),
                "response", Map.of("mode", mode)), "mode");
        }
    }

    @Test
    void hostManagedStillRequiresAnExplicitStandardRequestQueue() {
        assertInvalid(Map.of("request", Map.of(), "response", Map.of("mode", "HOST_MANAGED")), "request.queueUrl");
        for (String url : List.of("http://sqs.local/requests.fifo", "http://sqs.local/requests.fifo?x=1#fragment")) {
            assertInvalid(hostManaged(url), "standard queues only");
        }
    }

    @Test
    void legacyPhysicalQueueMetadataStaysUnchanged() {
        var result = dispatch(adapter(mock(SqsClient.class), false), descriptor(Map.of(
            "request", Map.of("queueUrl", "http://sqs.local/requests"),
            "response", Map.of("queueUrl", "http://sqs.local/responses"))));
        assertEquals(Set.of("adapter", "requestQueueUrl", "responseQueueUrl", "dispatchedAtEpochMs"), result.metadata().keySet());
        assertEquals("http://sqs.local/responses", result.metadata().get("responseQueueUrl"));
    }

    @Test
    void explicitModeSurvivesTheTransportConfigurationJsonBoundary() throws Exception {
        var original = descriptor(hostManaged("http://sqs.local/requests"));
        Map<String, Object> decoded = PipelineJson.mapper().readValue(
            PipelineJson.mapper().writeValueAsBytes(original.transportConfig()),
            new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        assertEquals(original.transportConfig(), decoded);
        assertEquals("HOST_MANAGED", dispatch(adapter(mock(SqsClient.class), false), descriptor(decoded)).metadata().get("responseMode"));
    }

    private static void assertInvalid(Map<String, Object> config, String message) {
        SqsClient client = mock(SqsClient.class);
        var error = assertThrows(IllegalArgumentException.class, () -> dispatch(adapter(client, false), descriptor(config)));
        assertTrue(error.getMessage().contains(message), error.getMessage());
        verifyNoInteractions(client);
    }

    private static Map<String, Object> hostManaged(String requestQueue) {
        return Map.of("request", Map.of("queueUrl", requestQueue), "response", Map.of("mode", "HOST_MANAGED"));
    }

    private static AwaitTransportAdapter.AwaitDispatchResult dispatch(SqsAwaitTransportAdapter adapter, AwaitCompletionDescriptor descriptor) {
        return adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(descriptor, interaction(), Map.of("paymentId", "p-1")))
            .await().atMost(Duration.ofSeconds(5));
    }

    private static SqsAwaitTransportAdapter adapter(SqsClient client, boolean poller) {
        var config = mock(PipelineOrchestratorConfig.class);
        var sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.sqs()).thenReturn(sqs);
        when(sqs.region()).thenReturn(Optional.empty());
        when(sqs.endpointOverride()).thenReturn(Optional.empty());
        var adapter = new SqsAwaitTransportAdapter(client, config,
            () -> new SqsAwaitTransportAdapter.SqsLiveAwaitWindowConfig(poller, Optional.of("http://sqs.local/local-poller")));
        adapter.resumeTokenService = new AwaitResumeTokenService("disclosed-host-managed-test-key");
        return adapter;
    }

    private static AwaitCompletionDescriptor descriptor(Map<String, Object> config) {
        return new AwaitCompletionDescriptor("PaymentProvider", "com.example.PaymentRecord", "com.example.PaymentStatus",
            Duration.ofMinutes(10), "signedResumeToken", "sqs", config, List.of("paymentId"));
    }

    private static AwaitInteractionRecord interaction() {
        return new AwaitInteractionRecord("tenant-1", "exec-1", "PaymentProvider", 1, "com.example.PaymentStatus",
            "interaction-1", "corr-1", "cause-1", "idem-1", 1L, AwaitInteractionStatus.DISPATCHING,
            Map.of("paymentId", "p-1"), null, null, null, null, "sqs", Map.of(),
            99_999_999_999L, 1_000L, 2_000L, 99_999L);
    }
}

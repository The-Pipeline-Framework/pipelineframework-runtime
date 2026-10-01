package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqsTransitionWorkerActionTest {

    private final JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
    private PipelineExecutionService executionService;
    private SqsClient client;
    private PipelineOrchestratorConfig.SqsWorkerConfig workerConfig;
    private SqsTransitionWorkerAction action;

    @BeforeEach
    void setUp() {
        executionService = mock(PipelineExecutionService.class);
        client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        workerConfig = mock(PipelineOrchestratorConfig.SqsWorkerConfig.class);
        PipelineOrchestratorConfig.SqsConfig sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.workerSqs()).thenReturn(workerConfig);
        when(config.sqs()).thenReturn(sqs);
        when(workerConfig.serverEnabled()).thenReturn(true);
        when(workerConfig.responseQueueUrl()).thenReturn(Optional.of("https://sqs.local/response"));
        when(workerConfig.requestTimeout()).thenReturn(Duration.ofSeconds(1));
        when(workerConfig.signatureTolerance()).thenReturn(Duration.ofMinutes(2));
        when(workerConfig.sharedSecret()).thenReturn(Optional.of("worker-secret"));
        when(workerConfig.sharedSecretRef()).thenReturn(Optional.empty());
        when(sqs.region()).thenReturn(Optional.empty());
        when(sqs.endpointOverride()).thenReturn(Optional.empty());
        action = new SqsTransitionWorkerAction(
            config,
            executionService,
            new LocalControlPlaneSecretResolver(),
            client);
    }

    @Test
    void acknowledgesMalformedAndUnauthenticatedRequestsWithoutExecuting() {
        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            handle(Message.builder().messageId("bad-json").body("{bad-json").build()));
        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            handle(requestMessage("bad-signature", envelope(), false)));

        verify(executionService, never()).executePortableTransition(any());
        verify(client, never()).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void publishesSuccessfulResponseBeforeAcknowledging() {
        TransitionCommandEnvelope envelope = envelope();
        when(executionService.executePortableTransition(envelope))
            .thenReturn(Uni.createFrom().item(TransitionResultEnvelope.completed(payloadCodec, List.of("ok"))));

        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            handle(requestMessage("request-1", envelope, true)));

        verify(client).sendMessage(argThat((SendMessageRequest request) ->
            request.queueUrl().equals("https://sqs.local/response")
                && decodeResponse(request.messageBody()).resultEnvelope().contains("\"outcome\":\"COMPLETED\"")));
    }

    @Test
    void publishesFailedExecutionOutcomeBeforeAcknowledging() {
        TransitionCommandEnvelope envelope = envelope();
        when(executionService.executePortableTransition(envelope))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("boom")));

        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            handle(requestMessage("request-failed", envelope, true)));

        verify(client).sendMessage(argThat((SendMessageRequest request) ->
            decodeResponse(request.messageBody()).resultEnvelope().contains("\"outcome\":\"FAILED\"")));
    }

    @Test
    void publishesFailedOutcomeWhenExecutionTimesOut() {
        TransitionCommandEnvelope envelope = envelope();
        when(workerConfig.requestTimeout()).thenReturn(Duration.ofMillis(10));
        when(executionService.executePortableTransition(envelope)).thenReturn(Uni.createFrom().nothing());

        assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
            handle(requestMessage("request-timeout", envelope, true)));

        verify(client).sendMessage(argThat((SendMessageRequest request) ->
            decodeResponse(request.messageBody()).resultEnvelope().contains("\"outcome\":\"FAILED\"")));
    }

    @Test
    void retriesResponseFailureWithoutRecordingNonce() {
        TransitionCommandEnvelope envelope = envelope();
        Message request = requestMessage("request-retry", envelope, true);
        when(executionService.executePortableTransition(envelope))
            .thenReturn(
                Uni.createFrom().item(TransitionResultEnvelope.completed(payloadCodec, List.of("ok"))),
                Uni.createFrom().item(TransitionResultEnvelope.completed(payloadCodec, List.of("ok"))));
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenThrow(new IllegalStateException("send failed"))
            .thenReturn(null);

        assertEquals(SqsMessageDisposition.RETRY, handle(request));
        assertEquals(SqsMessageDisposition.ACKNOWLEDGE, handle(request));

        verify(executionService, times(2)).executePortableTransition(envelope);
    }

    private SqsMessageDisposition handle(Message message) {
        return action.handle(SqsInboundMessage.from(message)).await().atMost(Duration.ofSeconds(5));
    }

    private TransitionCommandEnvelope envelope() {
        return TransitionEnvelopeFixtures.envelope(payloadCodec);
    }

    private static Message requestMessage(
        String requestId,
        TransitionCommandEnvelope envelope,
        boolean signed
    ) {
        try {
            String commandJson = PipelineJson.mapper().writeValueAsString(envelope);
            String timestamp = Instant.now().toString();
            String nonce = UUID.randomUUID().toString();
            String signature = signed
                ? TransitionWorkerSignature.sign(
                    "worker-secret",
                    SqsTransitionWorkerProtocol.SIGNATURE_METHOD,
                    SqsTransitionWorkerProtocol.REQUEST_SIGNATURE_PATH,
                    timestamp,
                    nonce,
                    SqsTransitionWorkerProtocol.signedBytes(requestId, commandJson))
                : "bad-signature";
            SqsTransitionWorkerRequest request = new SqsTransitionWorkerRequest(
                requestId,
                SqsTransitionWorkerProtocol.PROTOCOL_VERSION,
                SqsTransitionWorkerProtocol.PAYLOAD_ENCODING,
                commandJson,
                timestamp,
                nonce,
                signature);
            return Message.builder()
                .messageId(requestId)
                .body(PipelineJson.mapper().writeValueAsString(request))
                .build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static SqsTransitionWorkerResponse decodeResponse(String body) {
        try {
            return PipelineJson.mapper().readValue(body, SqsTransitionWorkerResponse.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}

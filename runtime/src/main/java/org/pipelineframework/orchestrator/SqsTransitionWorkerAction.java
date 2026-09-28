package org.pipelineframework.orchestrator;

import java.net.URI;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Processes one authenticated SQS transition-worker request and publishes its response.
 */
@ApplicationScoped
public class SqsTransitionWorkerAction {

    private static final Logger LOG = Logger.getLogger(SqsTransitionWorkerAction.class);
    private static final ObjectMapper JSON = PipelineJson.mapper();

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineExecutionService executionService;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    private volatile SqsClient client;
    private final TransitionWorkerNonceReplayGuard nonceReplayGuard = new TransitionWorkerNonceReplayGuard();

    public SqsTransitionWorkerAction() {
    }

    SqsTransitionWorkerAction(
        PipelineOrchestratorConfig orchestratorConfig,
        PipelineExecutionService executionService,
        ControlPlaneSecretResolver secretResolver,
        SqsClient client
    ) {
        this.orchestratorConfig = orchestratorConfig;
        this.executionService = executionService;
        this.secretResolver = secretResolver;
        this.client = client;
    }

    @PostConstruct
    void validateActionConfig() {
        if (!orchestratorConfig.workerSqs().serverEnabled()) {
            return;
        }
        if (orchestratorConfig.workerSqs().responseQueueUrl().filter(value -> !value.isBlank()).isEmpty()) {
            throw new IllegalStateException(
                "pipeline.orchestrator.worker.sqs.response-queue-url is required when "
                    + "pipeline.orchestrator.worker.sqs.server-enabled=true");
        }
        WorkerSecretSupport.validationError(
            orchestratorConfig.workerSqs().sharedSecret(),
            orchestratorConfig.workerSqs().sharedSecretRef(),
            "pipeline.orchestrator.worker.sqs.shared-secret",
            "pipeline.orchestrator.worker.sqs.shared-secret-ref",
            "pipeline.orchestrator.worker.sqs.server-enabled=true")
            .ifPresent(message -> {
                throw new IllegalStateException(message);
            });
    }

    public Uni<SqsMessageDisposition> handle(SqsInboundMessage message) {
        Objects.requireNonNull(message, "message");
        SqsTransitionWorkerRequest request;
        try {
            request = JSON.readValue(message.body().orElse(""), SqsTransitionWorkerRequest.class);
        } catch (Exception e) {
            LOG.warnf(e,
                "Dropping malformed SQS transition worker request id=%s",
                message.messageId().orElse("<missing>"));
            return Uni.createFrom().item(SqsMessageDisposition.ACKNOWLEDGE);
        }
        if (!authenticate(request, false)) {
            LOG.warnf("Dropping unauthenticated SQS transition worker request id=%s requestId=%s",
                message.messageId().orElse("<missing>"),
                request.requestId());
            return Uni.createFrom().item(SqsMessageDisposition.ACKNOWLEDGE);
        }

        TransitionCommandEnvelope command;
        try {
            command = JSON.readValue(request.commandEnvelope(), TransitionCommandEnvelope.class);
        } catch (Exception e) {
            LOG.warnf(e, "Dropping malformed SQS transition worker command requestId=%s", request.requestId());
            return Uni.createFrom().item(SqsMessageDisposition.ACKNOWLEDGE);
        }

        return executionService.executePortableTransition(command)
            .ifNoItem().after(orchestratorConfig.workerSqs().requestTimeout()).fail()
            .onFailure().invoke(failure -> LOG.errorf(
                failure,
                "Failed processing SQS transition worker requestId=%s executionId=%s",
                request.requestId(),
                command.executionId()))
            .onFailure().recoverWithItem(TransitionResultEnvelope::failed)
            .onItem().transformToUni(result -> publishResponse(request, command, result));
    }

    private Uni<SqsMessageDisposition> publishResponse(
        SqsTransitionWorkerRequest request,
        TransitionCommandEnvelope command,
        TransitionResultEnvelope result
    ) {
        return Uni.createFrom().item(() -> {
            sendResponse(request.requestId(), result);
            recordAuthenticatedNonce(request);
            return SqsMessageDisposition.ACKNOWLEDGE;
        })
            .runSubscriptionOn(Infrastructure.getDefaultExecutor())
            .onFailure().invoke(failure -> LOG.errorf(
                failure,
                "Failed sending SQS transition worker response requestId=%s executionId=%s",
                request.requestId(),
                command.executionId()))
            .onFailure().recoverWithItem(SqsMessageDisposition.RETRY);
    }

    private void sendResponse(String requestId, TransitionResultEnvelope result) {
        try {
            String resultJson = JSON.writeValueAsString(result.toWireResult());
            String timestamp = Instant.now().toString();
            String nonce = UUID.randomUUID().toString();
            String signature = TransitionWorkerSignature.sign(
                sharedSecret(),
                SqsTransitionWorkerProtocol.SIGNATURE_METHOD,
                SqsTransitionWorkerProtocol.RESPONSE_SIGNATURE_PATH,
                timestamp,
                nonce,
                SqsTransitionWorkerProtocol.signedBytes(requestId, resultJson));
            SqsTransitionWorkerResponse response = new SqsTransitionWorkerResponse(
                requestId,
                SqsTransitionWorkerProtocol.PROTOCOL_VERSION,
                SqsTransitionWorkerProtocol.PAYLOAD_ENCODING,
                resultJson,
                timestamp,
                nonce,
                signature);
            sqsClient().sendMessage(SendMessageRequest.builder()
                .queueUrl(responseQueueUrl())
                .messageBody(JSON.writeValueAsString(response))
                .build());
        } catch (Exception e) {
            throw new TransitionWorkerFailureException("Failed sending SQS transition worker response", e);
        }
    }

    private boolean authenticate(SqsTransitionWorkerRequest request, boolean recordNonce) {
        if (!SqsTransitionWorkerProtocol.PROTOCOL_VERSION.equals(request.protocolVersion())
            || !SqsTransitionWorkerProtocol.PAYLOAD_ENCODING.equals(request.commandEncoding())) {
            return false;
        }
        long timestampEpochMs;
        try {
            timestampEpochMs = TransitionWorkerSignature.parseTimestamp(request.timestamp());
        } catch (IllegalArgumentException e) {
            return false;
        }
        long now = System.currentTimeMillis();
        long toleranceMs = Math.max(0L, orchestratorConfig.workerSqs().signatureTolerance().toMillis());
        if (Math.abs(now - timestampEpochMs) > toleranceMs) {
            return false;
        }
        String expected = TransitionWorkerSignature.sign(
            sharedSecret(),
            SqsTransitionWorkerProtocol.SIGNATURE_METHOD,
            SqsTransitionWorkerProtocol.REQUEST_SIGNATURE_PATH,
            request.timestamp(),
            request.nonce(),
            SqsTransitionWorkerProtocol.signedBytes(request.requestId(), request.commandEnvelope()));
        if (!TransitionWorkerSignature.matches(expected, request.signature())) {
            return false;
        }
        if (recordNonce) {
            return nonceReplayGuard.accept(request.nonce(), timestampEpochMs, now, toleranceMs);
        }
        return !nonceReplayGuard.seen(request.nonce(), now, toleranceMs);
    }

    private void recordAuthenticatedNonce(SqsTransitionWorkerRequest request) {
        if (!authenticate(request, true)) {
            LOG.warnf("SQS transition worker request nonce was already recorded requestId=%s", request.requestId());
        }
    }

    private String responseQueueUrl() {
        return orchestratorConfig.workerSqs().responseQueueUrl()
            .filter(url -> !url.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.worker.sqs.response-queue-url is required"));
    }

    private String sharedSecret() {
        return WorkerSecretSupport.resolve(
            orchestratorConfig.workerSqs().sharedSecret(),
            orchestratorConfig.workerSqs().sharedSecretRef(),
            secretResolver,
            "pipeline.orchestrator.worker.sqs.shared-secret",
            "pipeline.orchestrator.worker.sqs.shared-secret-ref");
    }

    private SqsClient sqsClient() {
        SqsClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            if (client == null) {
                var builder = SqsClient.builder();
                builder.httpClientBuilder(UrlConnectionHttpClient.builder());
                orchestratorConfig.sqs().region()
                    .filter(region -> !region.isBlank())
                    .ifPresent(region -> builder.region(Region.of(region)));
                orchestratorConfig.sqs().endpointOverride()
                    .filter(endpoint -> !endpoint.isBlank())
                    .ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
                client = builder.build();
            }
            return client;
        }
    }

    @PreDestroy
    void shutdown() {
        SqsClient activeClient = client;
        if (activeClient != null) {
            activeClient.close();
        }
        client = null;
    }
}

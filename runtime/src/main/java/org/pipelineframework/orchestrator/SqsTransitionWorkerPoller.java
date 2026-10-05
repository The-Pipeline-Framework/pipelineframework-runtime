package org.pipelineframework.orchestrator;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import io.quarkus.runtime.StartupEvent;
import org.jboss.logging.Logger;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * SQS request/reply poller for transition worker commands.
 */
@ApplicationScoped
public class SqsTransitionWorkerPoller {

    private static final Logger LOG = Logger.getLogger(SqsTransitionWorkerPoller.class);
    private static final int MAX_MESSAGES_PER_POLL = 1;
    private static final int WAIT_TIME_SECONDS = 1;
    private static final Duration ERROR_POLL_DELAY = Duration.ofMillis(500);

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    SqsTransitionWorkerAction transitionWorkerAction;

    private volatile SqsClient client;
    private volatile ExecutorService pollExecutor;
    private volatile Future<?> pollFuture;
    private volatile boolean running;

    @PostConstruct
    void validateServerConfig() {
        if (orchestratorConfig.processLoopsDisabled()
            || !orchestratorConfig.workerSqs().serverEnabled()) {
            return;
        }
        if (orchestratorConfig.workerSqs().requestQueueUrl().filter(value -> !value.isBlank()).isEmpty()) {
            throw new IllegalStateException(
                "pipeline.orchestrator.worker.sqs.request-queue-url is required when "
                    + "pipeline.orchestrator.worker.sqs.server-enabled=true");
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
        if (visibilityTimeoutSeconds() < requestTimeoutSeconds()) {
            throw new IllegalStateException("pipeline.orchestrator.worker.sqs.visibility-timeout must be "
                + "greater than or equal to pipeline.orchestrator.worker.sqs.request-timeout");
        }
    }

    public SqsTransitionWorkerPoller() {
    }

    SqsTransitionWorkerPoller(
        PipelineOrchestratorConfig orchestratorConfig,
        SqsTransitionWorkerAction transitionWorkerAction,
        SqsClient client
    ) {
        this.orchestratorConfig = orchestratorConfig;
        this.transitionWorkerAction = transitionWorkerAction;
        this.client = client;
    }

    synchronized void onStartup(@Observes StartupEvent event) {
        if (orchestratorConfig.processLoopsDisabled() || !enabled() || running) {
            return;
        }
        running = true;
        pollExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual()
            .name("tpf-sqs-transition-worker-poller-", 0)
            .factory());
        pollFuture = pollExecutor.submit(() -> {
            sleep(orchestratorConfig.workerSqs().pollStartDelay());
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    pollOnce();
                } catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) {
                        LOG.debug("SQS transition worker poller interrupted; stopping.");
                        break;
                    }
                    LOG.error("SQS transition worker poll failed.", e);
                    sleep(ERROR_POLL_DELAY);
                }
            }
        });
        LOG.infof("SQS transition worker poller enabled requestQueueUrl=%s", requestQueueUrl());
    }

    int pollOnce() {
        if (!enabled()) {
            return 0;
        }
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
            .queueUrl(requestQueueUrl())
            .maxNumberOfMessages(MAX_MESSAGES_PER_POLL)
            .waitTimeSeconds(WAIT_TIME_SECONDS)
            .visibilityTimeout(visibilityTimeoutSeconds())
            .build();
        List<Message> messages = sqsClient().receiveMessage(request).messages();
        for (Message message : messages) {
            handleMessage(message);
        }
        return messages.size();
    }

    private void handleMessage(Message message) {
        if (message == null || message.receiptHandle() == null) {
            return;
        }
        try {
            SqsMessageDisposition disposition = transitionWorkerAction.handle(SqsInboundMessage.from(message))
                .await().indefinitely();
            if (disposition == SqsMessageDisposition.ACKNOWLEDGE) {
                deleteRequest(message.receiptHandle());
            }
        } catch (RuntimeException e) {
            LOG.errorf(e, "Failed invoking SQS transition-worker action id=%s", message.messageId());
        }
    }

    private void deleteRequest(String receiptHandle) {
        sqsClient().deleteMessage(DeleteMessageRequest.builder()
            .queueUrl(requestQueueUrl())
            .receiptHandle(receiptHandle)
            .build());
    }

    private boolean enabled() {
        return orchestratorConfig != null
            && orchestratorConfig.workerSqs().serverEnabled()
            && orchestratorConfig.workerSqs().requestQueueUrl().filter(url -> !url.isBlank()).isPresent()
            && orchestratorConfig.workerSqs().responseQueueUrl().filter(url -> !url.isBlank()).isPresent()
            && WorkerSecretSupport.validationError(
                orchestratorConfig.workerSqs().sharedSecret(),
                orchestratorConfig.workerSqs().sharedSecretRef(),
                "pipeline.orchestrator.worker.sqs.shared-secret",
                "pipeline.orchestrator.worker.sqs.shared-secret-ref",
                "pipeline.orchestrator.worker.sqs.server-enabled=true").isEmpty();
    }

    private String requestQueueUrl() {
        return orchestratorConfig.workerSqs().requestQueueUrl()
            .filter(url -> !url.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.worker.sqs.request-queue-url is required"));
    }

    private int visibilityTimeoutSeconds() {
        long seconds = Math.max(1L, orchestratorConfig.workerSqs().visibilityTimeout().toSeconds());
        return (int) Math.min(Integer.MAX_VALUE, seconds);
    }

    private long requestTimeoutSeconds() {
        return Math.max(1L, orchestratorConfig.workerSqs().requestTimeout().toSeconds());
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
        running = false;
        Future<?> activePollFuture = pollFuture;
        if (activePollFuture != null) {
            activePollFuture.cancel(true);
        }
        ExecutorService activePollExecutor = pollExecutor;
        if (activePollExecutor != null) {
            activePollExecutor.shutdownNow();
            try {
                if (!activePollExecutor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                    LOG.warn("SQS transition worker poller did not terminate within 30 seconds.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for SQS transition worker poller shutdown.");
            }
        }
        SqsClient activeClient = client;
        if (activeClient != null) {
            activeClient.close();
        }
        client = null;
        pollExecutor = null;
        pollFuture = null;
    }

    private static void sleep(Duration delay) {
        try {
            Thread.sleep(Math.max(0L, delay.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.worker.PipelineWorkerCapability;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.invocation.TransportBoundaryDescriptor;
import org.pipelineframework.invocation.TransportBoundaryInvocation;

/**
 * REST client adapter for transition workers.
 */
@ApplicationScoped
public class RestPipelineTransitionWorker implements PipelineTransitionWorker, TransportBoundaryInvocation {

    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final ObjectReader PAIRED_CAPABILITIES = JSON.readerFor(PipelineWorkerCapability.class)
        .with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
        .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final Logger LOG = Logger.getLogger(RestPipelineTransitionWorker.class);
    private static final AtomicInteger THREAD_SEQUENCE = new AtomicInteger();
    private static final int MAX_PAIRED_REQUEST_BYTES = 16 * 1024;
    private static final int MAX_PAIRED_RESPONSE_BYTES = 4096;
    private static final TransportBoundaryDescriptor BOUNDARY =
        new TransportBoundaryDescriptor("rest", "transition-worker.execute");

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    @Inject
    PipelineInvocationRuntime invocationRuntime;

    private final ExecutorService blockingExecutor = Executors.newCachedThreadPool(task -> {
        Thread thread = new Thread(task, "rest-transition-worker-client-" + THREAD_SEQUENCE.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });
    private final ScheduledThreadPoolExecutor deadlineWarningExecutor = newDeadlineWarningExecutor();

    private volatile HttpClient httpClient;
    private volatile HttpClient pairedHttpClient;

    public RestPipelineTransitionWorker() {
        this(null);
    }

    RestPipelineTransitionWorker(HttpClient httpClient) {
        this(httpClient, null);
    }

    RestPipelineTransitionWorker(HttpClient httpClient, PipelineInvocationRuntime invocationRuntime) {
        this(httpClient, Optional.empty(), invocationRuntime);
    }

    RestPipelineTransitionWorker(HttpClient httpClient, Optional<HttpClient> pairedHttpClient,
            PipelineInvocationRuntime invocationRuntime) {
        this.httpClient = httpClient;
        pairedHttpClient.ifPresent(client -> this.pairedHttpClient = client);
        this.invocationRuntime = invocationRuntime;
    }

    private static ScheduledThreadPoolExecutor newDeadlineWarningExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, "rest-transition-worker-deadline-warning-" + THREAD_SEQUENCE.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    @Override
    public Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command) {
        var runtime = invocationRuntime();
        return Uni.createFrom().deferred(() -> {
            Executor signalContext = subscriberSignalContext();
            return runtime.invokeTransportUni(this, () ->
                Uni.createFrom().deferred(() -> executeRequest(command, signalContext)));
        });
    }

    /** Execute against an environment-owned target without changing the legacy global endpoint. */
    public Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command, URI endpoint, String secretRef) {
        var runtime = invocationRuntime();
        return Uni.createFrom().deferred(() -> {
            Executor signalContext = subscriberSignalContext();
            return runtime.invokeTransportUni(this, () -> Uni.createFrom().deferred(() ->
                executeRequest(command, endpoint, targetSecret(secretRef), signalContext)));
        });
    }

    /**
     * Dispatches an explicitly paired native attempt only when the durable store reserves its current lease.
     * An empty result means the attempt was not authorized to send.
     */
    public Uni<Optional<TransitionResultEnvelope>> executeTransition(NativeWorkerReleaseBinding.Attempt attempt,
            String leaseOwner, PairedTransitionSettlementStore store) {
        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(store, "store");
        var runtime = invocationRuntime();
        return Uni.createFrom().deferred(() -> {
            Executor signalContext = subscriberSignalContext();
            return runtime.invokeTransportUni(this, () -> Uni.createFrom().deferred(() -> {
                var target = attempt.target();
                if (!RestTransitionWorkerProtocol.EXECUTE_PATH.equals(orchestratorConfig.workerRest().path())) {
                    throw new IllegalStateException("Paired native dispatch requires the fixed worker route");
                }
                var decoder = new PairedTransitionResultDecoder(attempt);
                decoder.validateCommand();
                String secret = targetSecret(target.sharedSecretRef());
                PairedDispatchIntent intent = PairedDispatchIntent.from(attempt, leaseOwner);
                return Uni.createFrom().completionStage(() -> CompletableFuture.supplyAsync(
                        () -> request(attempt.command(), target.endpoint(), secret, true), blockingExecutor))
                    .emitOn(signalContext)
                    .chain(request -> store.beginPairedDispatch(intent)
                        .emitOn(signalContext)
                        .chain(permit -> permit.isPresent()
                            ? executePairedRequest(request, target.endpoint(), decoder, intent, store, signalContext,
                                permit.orElseThrow())
                            : Uni.createFrom().item(Optional.empty())));
            }));
        });
    }

    private Uni<Optional<TransitionResultEnvelope>> executePairedRequest(HttpRequest request, URI endpoint,
            PairedTransitionResultDecoder decoder, PairedDispatchIntent intent, PairedTransitionSettlementStore store,
            Executor signalContext, long leaseExpiresEpochMs) {
        HttpClient client = pairedHttpClient();
        if (leaseExpiresEpochMs <= System.currentTimeMillis()) {
            // The durable intent stays outstanding even if its authorization arrived too late to send.
            return Uni.createFrom().item(Optional.empty());
        }
        long deadlineMillis = Math.max(1L, orchestratorConfig.workerRest().requestTimeout().toMillis());
        long startedAtNanos = System.nanoTime();
        String target = workerTarget(endpoint);
        ScheduledFuture<?> warning = scheduleDeadlineWarning(intent.command(), target, deadlineMillis);
        return Uni.createFrom().completionStage(() -> sendPaired(client, request, decoder, target, startedAtNanos, deadlineMillis))
            .emitOn(signalContext)
            .onItemOrFailure().invoke((ignored, failure) -> warning.cancel(false))
            .onFailure().transform(failure -> classifyFailure(failure, target, startedAtNanos, deadlineMillis))
            .onFailure(RemoteTransitionOutcomeUnknownException.class).call(failure ->
                store.settlePairedUnknown(intent).replaceWithVoid())
            .map(Optional::of);
    }

    private Uni<TransitionResultEnvelope> executeRequest(TransitionCommandEnvelope command, Executor signalContext) {
        return executeRequest(command, configuredEndpoint(), sharedSecret(), signalContext);
    }

    private Uni<TransitionResultEnvelope> executeRequest(TransitionCommandEnvelope command, URI endpoint, String secret,
                                                       Executor signalContext) {
        long deadlineMillis = Math.max(1L, orchestratorConfig.workerRest().requestTimeout().toMillis());
        long startedAtNanos = System.nanoTime();
        String target = workerTarget(endpoint);
        ScheduledFuture<?> warning = scheduleDeadlineWarning(command, target, deadlineMillis);
        return Uni.createFrom().completionStage(() -> CompletableFuture
                .supplyAsync(() -> request(command, endpoint, secret, false), blockingExecutor)
                .thenCompose(request -> httpClient().sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenApplyAsync(response -> decodeResponse(response, command), blockingExecutor)))
            .emitOn(signalContext)
            .onItemOrFailure().invoke((ignored, failure) -> warning.cancel(false))
            .onFailure().transform(failure -> classifyFailure(failure, target, startedAtNanos, deadlineMillis));
    }

    private CompletableFuture<TransitionResultEnvelope> sendPaired(HttpClient client, HttpRequest request,
            PairedTransitionResultDecoder decoder,
            String target, long startedAtNanos, long deadlineMillis) {
        return CompletableFuture.completedFuture(request)
            .thenCompose(sent -> sendBoundedPaired(client, sent, deadlineMillis))
            .thenApplyAsync(response -> decodePairedResponse(request, response, decoder), blockingExecutor)
            .exceptionally(failure -> {
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
                throw new RemoteTransitionOutcomeUnknownException("rest", target, elapsedMillis,
                    deadlineMillis, new TransitionWorkerFailureException("No validated paired worker result"));
            });
    }

    private CompletableFuture<HttpResponse<String>> sendBoundedPaired(HttpClient client, HttpRequest request,
            long deadlineMillis) {
        CompletableFuture<HttpResponse<String>> response = client.sendAsync(request, ignored -> new BoundedPairedResponse());
        return response.copy().orTimeout(deadlineMillis, TimeUnit.MILLISECONDS)
            .whenComplete((received, failure) -> {
                if (failure instanceof TimeoutException) {
                    response.cancel(true);
                }
            });
    }

    private TransitionResultEnvelope decodePairedResponse(HttpRequest request, HttpResponse<String> response,
            PairedTransitionResultDecoder decoder) {
        if (!request.uri().equals(response.uri()) || response.previousResponse().isPresent()
            || response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new TransitionWorkerFailureException("No successful response on the paired worker request channel");
        }
        return decoder.decode(response.body());
    }

    private ScheduledFuture<?> scheduleDeadlineWarning(
        TransitionCommandEnvelope command,
        String target,
        long deadlineMillis
    ) {
        long warningDelayMillis = Math.max(1L, deadlineMillis * 3L / 4L);
        return deadlineWarningExecutor.schedule(() -> LOG.warnf(
            "event=remote_transition_deadline_warning executionId=%s attempt=%d transitionKey=%s "
                + "protocol=rest target=%s elapsedMs=%d deadlineMs=%d state=running",
            command.executionId(),
            command.attempt(),
            command.transitionKey(),
            target,
            warningDelayMillis,
            deadlineMillis), warningDelayMillis, TimeUnit.MILLISECONDS);
    }

    private Throwable classifyFailure(
        Throwable failure,
        String target,
        long startedAtNanos,
        long deadlineMillis
    ) {
        Throwable unwrapped = unwrapFailure(failure);
        if (unwrapped instanceof HttpTimeoutException) {
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
            return new RemoteTransitionOutcomeUnknownException(
                "rest",
                target,
                elapsedMillis,
                deadlineMillis,
                unwrapped);
        }
        return unwrapped;
    }

    /**
     * Fetches worker release capabilities from the REST worker.
     *
     * @return remote worker capabilities
     */
    public Uni<PipelineWorkerCapability> capabilities() {
        return Uni.createFrom().deferred(() -> {
            Executor signalContext = subscriberSignalContext();
            return capabilitiesAt(configuredEndpoint(), sharedSecret(), signalContext);
        });
    }

    public Uni<PipelineWorkerCapability> capabilities(URI endpoint, String secretRef) {
        return Uni.createFrom().deferred(() -> {
            Executor signalContext = subscriberSignalContext();
            return capabilitiesAt(endpoint, targetSecret(secretRef), signalContext);
        });
    }

    /** Probe the configured native endpoint on the same direct HTTPS channel policy as paired execution. */
    public Uni<PipelineWorkerCapability> pairedCapabilities(NativeWorkerReleaseBinding binding) {
        return Uni.createFrom().deferred(() -> {
            if (!RestTransitionWorkerProtocol.CAPABILITIES_PATH.equals(orchestratorConfig.workerRest().capabilitiesPath())
                || !RestTransitionWorkerProtocol.EXECUTE_PATH.equals(orchestratorConfig.workerRest().path())) {
                throw new IllegalStateException("Paired native execution requires the fixed worker routes");
            }
            var target = binding.target();
            Executor signalContext = subscriberSignalContext();
            return capabilitiesAt(target.endpoint(), targetSecret(target.sharedSecretRef()), signalContext, true)
                .invoke(capability -> {
                    if (!binding.acceptsCapabilities(capability)) {
                        throw new IllegalStateException("Paired worker capabilities do not match its prepared Release binding");
                    }
                });
        });
    }

    private Uni<PipelineWorkerCapability> capabilitiesAt(URI endpoint, String secret, Executor signalContext) {
        return capabilitiesAt(endpoint, secret, signalContext, false);
    }

    private Uni<PipelineWorkerCapability> capabilitiesAt(URI endpoint, String secret, Executor signalContext, boolean paired) {
        return Uni.createFrom().completionStage(() -> CompletableFuture.supplyAsync(() -> capabilitiesRequest(endpoint, secret), blockingExecutor)
            .thenCompose(request -> paired
                ? sendBoundedPaired(pairedHttpClient(), request, orchestratorConfig.workerRest().requestTimeout().toMillis())
                    .thenApply(response -> {
                    if (!request.uri().equals(response.uri()) || response.previousResponse().isPresent()) {
                        throw new TransitionWorkerFailureException("Paired worker capabilities response changed channels");
                    }
                    return response;
                })
                : httpClient().sendAsync(request, HttpResponse.BodyHandlers.ofString()))
            .thenCompose(response -> CompletableFuture.supplyAsync(
                () -> decodeCapabilitiesResponse(response, paired),
                blockingExecutor)))
            .emitOn(signalContext)
            .onFailure().transform(this::unwrapFailure);
    }

    /** Preserve the server-side subscription loader only while delivering either transport signal. */
    private static Executor subscriberSignalContext() {
        ClassLoader subscriberLoader = Thread.currentThread().getContextClassLoader();
        return signal -> {
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            try {
                thread.setContextClassLoader(subscriberLoader);
                signal.run();
            } finally {
                thread.setContextClassLoader(previous);
            }
        };
    }

    @Override
    public String providerName() {
        return "rest";
    }

    @Override
    public TransportBoundaryDescriptor transportBoundary() {
        return BOUNDARY;
    }

    @Override
    public Optional<String> startupValidationError(PipelineOrchestratorConfig config) {
        if (!config.workerRest().isEnabled()) {
            return Optional.empty();
        }
        Optional<String> secretError = WorkerSecretSupport.validationError(
            config.workerRest().sharedSecret(),
            config.workerRest().sharedSecretRef(),
            "pipeline.orchestrator.worker.rest.shared-secret",
            "pipeline.orchestrator.worker.rest.shared-secret-ref",
            "pipeline.orchestrator.worker.rest.base-url is configured");
        if (secretError.isPresent()) {
            return secretError;
        }
        return config.workerRest().baseUrl()
            .flatMap(value -> {
                try {
                    URI uri = URI.create(value);
                    if (!uri.isAbsolute() || uri.getHost() == null) {
                        return Optional.of("Invalid pipeline.orchestrator.worker.rest.base-url: " + value);
                    }
                    return Optional.empty();
                } catch (IllegalArgumentException e) {
                    return Optional.of("Invalid pipeline.orchestrator.worker.rest.base-url: " + value);
                }
            });
    }

    private TransitionResultEnvelope decodeResponse(HttpResponse<String> response, TransitionCommandEnvelope command) {
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new TransitionWorkerFailureException(
                "REST transition worker returned HTTP " + response.statusCode()
                    + " for execution " + command.executionId()
                    + " with response body omitted");
        }
        try {
            TransitionWireResult result = JSON.readValue(response.body(), TransitionWireResult.class);
            if (result == null) {
                throw new TransitionWorkerFailureException(
                    "REST transition worker returned an empty result for execution "
                        + command.executionId()
                        + " with HTTP " + response.statusCode()
                        + " and response body omitted");
            }
            return TransitionResultEnvelope.fromWireResult(result);
        } catch (IOException e) {
            throw new TransitionWorkerFailureException(
                "REST transition worker returned malformed JSON for execution "
                    + command.executionId()
                    + " with HTTP " + response.statusCode()
                    + " and response body omitted",
                e);
        }
    }

    static void requireHostedAdmissionFits(PipelineReleaseRecord release, SerializedTransitionPayload payload) {
        // The two supported stores generate UUID execution IDs. Claims reuse that ID as trace ID
        // and append the root step and attempt to the transition key. Reserve the widest counters.
        String executionId = "00000000-0000-0000-0000-000000000000";
        int attempt = Integer.MAX_VALUE;
        requestBody(new TransitionCommandEnvelope(release.tenantId(), executionId, release.pipelineId(),
            release.contractVersion(), release.releaseVersion(), 0, -1, attempt, ExecutionResultShape.SINGLE,
            Long.MAX_VALUE, executionId + ":0:" + attempt, executionId, payload.payloadTypeId(),
            payload.payloadEncoding(), payload.payload()), true);
    }

    private static byte[] requestBody(TransitionCommandEnvelope command, boolean paired) {
        try {
            byte[] body = JSON.writeValueAsBytes(command);
            if (paired && body.length > MAX_PAIRED_REQUEST_BYTES) {
                throw new ExecutionAdmissionTooLargeException("Paired worker command exceeds the 16 KiB request limit");
            }
            return body;
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding REST transition worker command", e);
        }
    }

    private HttpRequest request(TransitionCommandEnvelope command, URI endpoint, String secret, boolean paired) {
        byte[] body = requestBody(command, paired);
        URI uri = workerUri(endpoint, orchestratorConfig.workerRest().path());
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            secret,
            RestTransitionWorkerProtocol.EXECUTE_METHOD,
            orchestratorConfig.workerRest().path(),
            timestamp,
            nonce,
            body);
        HttpRequest.Builder request = HttpRequest.newBuilder(uri)
            .timeout(orchestratorConfig.workerRest().requestTimeout())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature);
        var publisher = HttpRequest.BodyPublishers.ofByteArray(body);
        return paired ? request.version(HttpClient.Version.HTTP_1_1).POST(new OneShotRequestBody(publisher)).build()
            : request.POST(publisher).build();
    }

    private HttpRequest capabilitiesRequest(URI endpoint, String secret) {
        URI uri = workerUri(endpoint, orchestratorConfig.workerRest().capabilitiesPath());
        byte[] body = new byte[0];
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            secret,
            RestTransitionWorkerProtocol.CAPABILITIES_METHOD,
            orchestratorConfig.workerRest().capabilitiesPath(),
            timestamp,
            nonce,
            body);
        return HttpRequest.newBuilder(uri)
            .timeout(orchestratorConfig.workerRest().requestTimeout())
            .header("Accept", "application/json")
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature)
            .GET()
            .build();
    }

    private PipelineWorkerCapability decodeCapabilitiesResponse(HttpResponse<String> response, boolean paired) {
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new TransitionWorkerFailureException(
                "REST transition worker capabilities returned HTTP " + response.statusCode()
                    + " with response body omitted");
        }
        try {
            PipelineWorkerCapability capability = paired ? PAIRED_CAPABILITIES.readValue(response.body())
                : JSON.readValue(response.body(), PipelineWorkerCapability.class);
            if (capability == null) {
                throw new TransitionWorkerFailureException(
                    "REST transition worker capabilities returned an empty response");
            }
            return capability;
        } catch (IOException e) {
            throw new TransitionWorkerFailureException(
                "REST transition worker capabilities returned malformed JSON",
                e);
        }
    }

    private URI configuredEndpoint() {
        String baseUrl = orchestratorConfig.workerRest().baseUrl()
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.worker.rest.base-url is required for REST transition worker"));
        return URI.create(baseUrl);
    }

    private URI workerUri(URI endpoint, String path) {
        String baseUrl = endpoint.toString();
        String normalizedBase = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        return URI.create(normalizedBase + normalizedPath);
    }

    private String targetSecret(String reference) {
        return WorkerSecretSupport.resolve(Optional.empty(), Optional.of(reference), secretResolver,
            "registered REST target shared-secret", "registered REST target shared-secret-ref");
    }

    private String workerTarget(URI endpoint) {
        URI uri = workerUri(endpoint, orchestratorConfig.workerRest().path());
        String host = uri.getHost() == null ? "<unknown>" : uri.getHost();
        return uri.getScheme() + "://" + host + (uri.getPort() < 0 ? "" : ":" + uri.getPort()) + uri.getPath();
    }

    private String sharedSecret() {
        return WorkerSecretSupport.resolve(
            orchestratorConfig.workerRest().sharedSecret(),
            orchestratorConfig.workerRest().sharedSecretRef(),
            secretResolver,
            "pipeline.orchestrator.worker.rest.shared-secret",
            "pipeline.orchestrator.worker.rest.shared-secret-ref");
    }

    private HttpClient httpClient() {
        HttpClient client = httpClient;
        if (client != null) {
            return client;
        }
        synchronized (this) {
            if (httpClient == null) {
                httpClient = HttpClient.newBuilder()
                    .connectTimeout(orchestratorConfig.workerRest().connectTimeout())
                    .build();
            }
            return httpClient;
        }
    }

    private HttpClient pairedHttpClient() {
        HttpClient client = pairedHttpClient;
        if (client != null) {
            return client;
        }
        synchronized (this) {
            if (pairedHttpClient == null) {
                pairedHttpClient = HttpClient.newBuilder()
                    .connectTimeout(orchestratorConfig.workerRest().connectTimeout())
                    .proxy(HttpClient.Builder.NO_PROXY)
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();
            }
            return pairedHttpClient;
        }
    }

    private static final class OneShotRequestBody implements HttpRequest.BodyPublisher {
        private final HttpRequest.BodyPublisher body;
        private final AtomicBoolean subscribed = new AtomicBoolean();

        private OneShotRequestBody(HttpRequest.BodyPublisher body) {
            this.body = body;
        }

        @Override
        public long contentLength() {
            return body.contentLength();
        }

        @Override
        public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
            if (subscribed.compareAndSet(false, true)) {
                body.subscribe(subscriber);
                return;
            }
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override public void request(long count) { }
                @Override public void cancel() { }
            });
            subscriber.onError(new IOException("Paired transition request body cannot be sent again"));
        }
    }

    private static final class BoundedPairedResponse implements HttpResponse.BodySubscriber<String> {
        private final HttpResponse.BodySubscriber<String> body = HttpResponse.BodySubscribers.ofString(StandardCharsets.UTF_8);
        private Flow.Subscription subscription;
        private long bytes;

        @Override
        public CompletionStage<String> getBody() {
            return body.getBody();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            body.onSubscribe(subscription);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            for (ByteBuffer buffer : buffers) {
                bytes += buffer.remaining();
            }
            if (bytes > MAX_PAIRED_RESPONSE_BYTES) {
                subscription.cancel();
                body.onError(new IOException("Paired worker response exceeded its size limit"));
                return;
            }
            body.onNext(buffers);
        }

        @Override
        public void onError(Throwable error) {
            body.onError(error);
        }

        @Override
        public void onComplete() {
            body.onComplete();
        }
    }

    private PipelineInvocationRuntime invocationRuntime() {
        if (invocationRuntime == null) {
            throw new IllegalStateException("PipelineInvocationRuntime was not injected into "
                + "RestPipelineTransitionWorker.invocationRuntime");
        }
        return invocationRuntime;
    }

    private Throwable unwrapFailure(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return unwrapFailure(failure.getCause());
        }
        return failure;
    }

    @PreDestroy
    void close() {
        if (pairedHttpClient != null) {
            pairedHttpClient.shutdownNow();
        }
        deadlineWarningExecutor.shutdownNow();
        blockingExecutor.shutdownNow();
    }
}

package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.worker.PipelineWorkerCapability;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestTransitionWorkerResourceTest {

    private final JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
    private RestTransitionWorkerResource resource;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.RestWorkerConfig restConfig;
    private PipelineExecutionService executionService;
    private PipelineReleaseIdentityResolver identityResolver;

    @BeforeEach
    void setUp() {
        resource = new RestTransitionWorkerResource();
        config = mock(PipelineOrchestratorConfig.class);
        restConfig = mock(PipelineOrchestratorConfig.RestWorkerConfig.class);
        executionService = mock(PipelineExecutionService.class);
        identityResolver = mock(PipelineReleaseIdentityResolver.class);
        resource.orchestratorConfig = config;
        resource.executionService = executionService;
        resource.requestThreadContext = mock(org.eclipse.microprofile.context.ThreadContext.class);
        when(resource.requestThreadContext.currentContextExecutor()).thenReturn(Runnable::run);
        resource.identityResolver = identityResolver;
        when(config.workerRest()).thenReturn(restConfig);
        when(restConfig.path()).thenReturn("/pipeline/worker/transitions/execute");
        when(restConfig.capabilitiesPath()).thenReturn("/pipeline/worker/capabilities");
        when(restConfig.sharedSecret()).thenReturn(java.util.Optional.of("worker-secret"));
        when(restConfig.sharedSecretRef()).thenReturn(java.util.Optional.empty());
        when(restConfig.signatureTolerance()).thenReturn(Duration.ofMinutes(2));
        when(identityResolver.pipelineId(config)).thenReturn("org.example.restaurant");
        when(identityResolver.contractVersion()).thenReturn("sha256:contract");
        when(identityResolver.releaseVersion(config)).thenReturn("sha256:bundle");
        when(identityResolver.artifactId(config)).thenReturn("restaurant-approval-monolith");
        when(identityResolver.artifactDigest(config)).thenReturn("sha256:artifact");
        when(identityResolver.capabilities()).thenReturn(PipelineBundleCapabilities.defaults());
    }

    @Test
    void rejectsCallsWhenServerDisabled() {
        when(restConfig.serverEnabled()).thenReturn(false);

        Response response = resource.execute(null, null, null, new byte[0]).await().indefinitely();

        assertEquals(404, response.getStatus());
    }

    @Test
    void delegatesToLocalExecutionServiceWhenEnabled() throws Exception {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(payloadCodec, List.of("ok"));
        when(restConfig.serverEnabled()).thenReturn(true);
        when(executionService.executePortableTransition(envelope))
            .thenReturn(Uni.createFrom().item(result));

        SignedRequest request = signed(envelope);
        Response response = resource.execute(
            request.timestamp(),
            request.nonce(),
            request.signature(),
            request.body()).await().indefinitely();

        assertEquals(200, response.getStatus());
        assertEquals(TransitionWorkerOutcome.COMPLETED, ((TransitionWireResult) response.getEntity()).outcome());
    }

    @Test
    void delegatesWhenServerUsesSharedSecretReference() throws Exception {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(payloadCodec, List.of("ok"));
        when(restConfig.serverEnabled()).thenReturn(true);
        when(restConfig.sharedSecret()).thenReturn(java.util.Optional.empty());
        when(restConfig.sharedSecretRef()).thenReturn(java.util.Optional.of("sys:tpf.rest.worker.secret"));
        when(executionService.executePortableTransition(envelope))
            .thenReturn(Uni.createFrom().item(result));

        try {
            System.setProperty("tpf.rest.worker.secret", "worker-secret");
            SignedRequest request = signed(envelope);
            Response response = resource.execute(
                request.timestamp(),
                request.nonce(),
                request.signature(),
                request.body()).await().indefinitely();

            assertEquals(200, response.getStatus());
        } finally {
            System.clearProperty("tpf.rest.worker.secret");
        }
    }

    @Test
    void rejectsUnsignedRequestWhenServerEnabled() {
        when(restConfig.serverEnabled()).thenReturn(true);

        Response response = resource.execute(null, null, null, new byte[0]).await().indefinitely();

        assertEquals(401, response.getStatus());
    }

    @Test
    void startupValidationRejectsUnsupportedCustomServerPaths() {
        when(restConfig.serverEnabled()).thenReturn(true);
        when(restConfig.path()).thenReturn("/custom/execute");

        IllegalStateException error = assertThrows(IllegalStateException.class, resource::validateServerConfig);

        assertEquals(
            "REST transition worker server only supports "
                + "pipeline.orchestrator.worker.rest.path=/pipeline/worker/transitions/execute",
            error.getMessage());
    }

    @Test
    void returnsSignedWorkerCapabilitiesWhenEnabled() {
        when(restConfig.serverEnabled()).thenReturn(true);
        SignedRequest request = signedCapability();

        Response response = resource.capabilities(
            request.timestamp(),
            request.nonce(),
            request.signature()).await().indefinitely();

        assertEquals(200, response.getStatus());
        PipelineWorkerCapability capability = (PipelineWorkerCapability) response.getEntity();
        assertEquals("rest", capability.providerName());
        assertEquals("org.example.restaurant", capability.pipelineId());
        assertEquals("sha256:contract", capability.contractVersion());
        assertEquals("sha256:bundle", capability.releaseVersion());
        assertEquals("restaurant-approval-monolith", capability.artifactId());
        assertEquals("sha256:artifact", capability.artifactDigest());
    }

    @Test
    void rejectsMissingServerSecret() throws Exception {
        when(restConfig.serverEnabled()).thenReturn(true);
        when(restConfig.sharedSecret()).thenReturn(java.util.Optional.empty());
        when(restConfig.sharedSecretRef()).thenReturn(java.util.Optional.empty());
        SignedRequest request = signed(envelope());

        Response response = resource.execute(
            request.timestamp(),
            request.nonce(),
            request.signature(),
            request.body()).await().indefinitely();

        assertEquals(503, response.getStatus());
    }

    @Test
    void rejectsReplayedNonce() throws Exception {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(payloadCodec, List.of("ok"));
        when(restConfig.serverEnabled()).thenReturn(true);
        when(executionService.executePortableTransition(envelope))
            .thenReturn(Uni.createFrom().item(result));
        SignedRequest request = signed(envelope);

        Response first = resource.execute(
            request.timestamp(),
            request.nonce(),
            request.signature(),
            request.body()).await().indefinitely();
        Response replay = resource.execute(
            request.timestamp(),
            request.nonce(),
            request.signature(),
            request.body()).await().indefinitely();

        assertEquals(200, first.getStatus());
        assertEquals(401, replay.getStatus());
    }


    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void asynchronousResponseRestoresTheOriginatingRequestContext(boolean failed) throws Exception {
        var context = new ThreadLocal<String>();
        context.set("original-request");
        java.util.concurrent.Executor captured = task -> {
            String previous = context.get();
            context.set("original-request");
            try { task.run(); } finally {
                if (previous == null) context.remove(); else context.set(previous);
            }
        };
        when(resource.requestThreadContext.currentContextExecutor()).thenReturn(captured);
        var completed = new java.util.concurrent.CompletableFuture<TransitionResultEnvelope>();
        var command = envelope();
        when(restConfig.serverEnabled()).thenReturn(true);
        when(executionService.executePortableTransition(command))
            .thenReturn(Uni.createFrom().completionStage(completed));
        SignedRequest request = signed(command);
        try {
            var subscriber = resource.execute(request.timestamp(), request.nonce(), request.signature(), request.body())
                .onItemOrFailure().invoke((response, failure) -> assertEquals("original-request", context.get()))
                .subscribe().withSubscriber(io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
            var completion = java.util.concurrent.CompletableFuture.runAsync(() -> {
                context.set("unrelated-provider-request");
                if (failed) completed.completeExceptionally(new IllegalStateException("provider-failure"));
                else completed.complete(TransitionResultEnvelope.completed(payloadCodec, List.of("ok")));
                assertEquals("unrelated-provider-request", context.get());
                context.remove();
            });
            completion.join();
            if (failed) subscriber.awaitFailure().assertFailedWith(IllegalStateException.class, "provider-failure");
            else assertEquals(200, subscriber.awaitItem().getItem().getStatus());
            assertEquals("original-request", context.get());
        } finally { context.remove(); }
    }


    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void responseReturnsToOriginatingVertxContext(boolean failed) throws Exception {
        var vertx = io.vertx.core.Vertx.vertx();
        try {
            var base = (io.vertx.core.impl.ContextInternal) vertx.getOrCreateContext();
            var origin = base.duplicate();
            var provider = base.duplicate();
            org.junit.jupiter.api.Assertions.assertNotSame(origin, provider);
            var completed = new java.util.concurrent.CompletableFuture<TransitionResultEnvelope>();
            var observed = new java.util.concurrent.CompletableFuture<io.vertx.core.Context>();
            var subscribed = new java.util.concurrent.CompletableFuture<Void>();
            var command = envelope();
            when(restConfig.serverEnabled()).thenReturn(true);
            when(executionService.executePortableTransition(command))
                .thenReturn(Uni.createFrom().completionStage(completed));
            SignedRequest request = signed(command);
            origin.runOnContext(ignored -> {
                resource.execute(request.timestamp(), request.nonce(), request.signature(), request.body())
                    .subscribe().with(
                        response -> observed.complete(io.vertx.core.Vertx.currentContext()),
                        failure -> observed.complete(io.vertx.core.Vertx.currentContext()));
                subscribed.complete(null);
            });
            subscribed.get(10, java.util.concurrent.TimeUnit.SECONDS);
            provider.runOnContext(ignored -> {
                if (failed) completed.completeExceptionally(new IllegalStateException("provider-failure"));
                else completed.complete(TransitionResultEnvelope.completed(payloadCodec, List.of("ok")));
            });
            assertEquals(origin, observed.get(10, java.util.concurrent.TimeUnit.SECONDS));
        } finally {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    @Test
    void responseContextRestorationPreservesCancellation() throws Exception {
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        var command = envelope();
        when(restConfig.serverEnabled()).thenReturn(true);
        when(executionService.executePortableTransition(command)).thenReturn(
            Uni.createFrom().<TransitionResultEnvelope>nothing().onCancellation().invoke(() -> cancelled.set(true)));
        SignedRequest request = signed(command);
        var subscriber = resource.execute(request.timestamp(), request.nonce(), request.signature(), request.body())
            .subscribe().withSubscriber(io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.cancel();
        org.junit.jupiter.api.Assertions.assertTrue(cancelled.get());
    }

    private TransitionCommandEnvelope envelope() {
        return TransitionEnvelopeFixtures.envelope(payloadCodec);
    }

    private SignedRequest signed(TransitionCommandEnvelope envelope) throws Exception {
        byte[] body = PipelineJson.mapper().writeValueAsBytes(envelope);
        String timestamp = Instant.now().toString();
        String nonce = java.util.UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            "worker-secret",
            "POST",
            "/pipeline/worker/transitions/execute",
            timestamp,
            nonce,
            body);
        return new SignedRequest(timestamp, nonce, signature, body);
    }

    private SignedRequest signedCapability() {
        byte[] body = new byte[0];
        String timestamp = Instant.now().toString();
        String nonce = java.util.UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            "worker-secret",
            "GET",
            "/pipeline/worker/capabilities",
            timestamp,
            nonce,
            body);
        return new SignedRequest(timestamp, nonce, signature, body);
    }

    private record SignedRequest(String timestamp, String nonce, String signature, byte[] body) {
    }
}

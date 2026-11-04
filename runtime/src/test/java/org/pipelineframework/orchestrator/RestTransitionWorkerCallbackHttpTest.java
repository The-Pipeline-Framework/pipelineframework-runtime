package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.vertx.http.runtime.CurrentVertxRequest;
import io.smallrye.common.annotation.NonBlocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;

@QuarkusTest
@TestProfile(RestTransitionWorkerCallbackHttpTest.Profile.class)
class RestTransitionWorkerCallbackHttpTest {
    private static final String SECRET = "callback-test-secret";
    private static final String CALLBACK_PATH = "/tpf-test/complete-worker";
    private static final String WORKER_CONTEXT_KEY = RestTransitionWorkerCallbackHttpTest.class.getName();
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @TestHTTPResource(RestTransitionWorkerProtocol.EXECUTE_PATH)
    URI workerEndpoint;

    @TestHTTPResource(CALLBACK_PATH)
    URI callbackEndpoint;

    @Inject
    PipelineExecutionService executionService;

    @Inject
    CallbackState callback;

    @Test
    void workerResponseReturnsToItsRequestAfterProviderRequestEnds() throws Exception {
        var codec = new JsonTransitionPayloadCodec();
        var envelope = TransitionEnvelopeFixtures.envelope(codec);
        var wireResult = TransitionResultEnvelope.completed(codec, List.of("ok")).toWireResult();
        var result = mock(TransitionResultEnvelope.class);
        when(result.toWireResult()).thenAnswer(ignored -> {
            callback.encodingRequest.complete(callback.currentRequest.getCurrent());
            callback.encodedOnWorkerContext.complete(Boolean.TRUE.equals(Vertx.currentContext().getLocal(WORKER_CONTEXT_KEY)));
            return wireResult;
        });
        callback.resultToDeliver = result;
        var mockService = mock(PipelineExecutionService.class);
        when(mockService.executePortableTransition(envelope)).thenAnswer(ignored -> {
            Vertx.currentContext().putLocal(WORKER_CONTEXT_KEY, Boolean.TRUE);
            return Uni.createFrom().<TransitionResultEnvelope>emitter(emitter -> {
                callback.result.thenAccept(emitter::complete);
                callback.workerRequest.complete(callback.currentRequest.getCurrent());
            });
        });
        QuarkusMock.installMockForInstance(mockService, executionService);

        byte[] body = PipelineJson.mapper().writeValueAsBytes(envelope);
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            SECRET, "POST", RestTransitionWorkerProtocol.EXECUTE_PATH, timestamp, nonce, body);
        var request = HttpRequest.newBuilder(workerEndpoint)
            .timeout(TIMEOUT)
            .header("Content-Type", "application/json")
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature)
            .POST(HttpRequest.BodyPublishers.ofByteArray(body))
            .build();

        try (HttpClient client = HttpClient.newHttpClient()) {
            var pendingResponse = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
            RoutingContext origin = callback.workerRequest.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            var providerResponse = client.send(HttpRequest.newBuilder(callbackEndpoint)
                .timeout(TIMEOUT).POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.discarding());
            assertEquals(204, providerResponse.statusCode());
            var response = pendingResponse.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertEquals(200, response.statusCode(), response.body());
            assertEquals(TransitionWorkerOutcome.COMPLETED,
                PipelineJson.mapper().readValue(response.body(), TransitionWireResult.class).outcome());
            assertEquals(RestTransitionWorkerProtocol.EXECUTE_PATH,
                response.headers().firstValue("X-Worker-Request-Path").orElseThrow());
            assertNotSame(origin, callback.providerRequest.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
            assertSame(origin, callback.encodingRequest.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
            assertEquals(true, callback.encodedOnWorkerContext.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS),
                "The worker must encode on its own Vert.x context");
            assertSame(origin, callback.responseRequest.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
        }
    }

    @Singleton
    public static class CallbackState {
        private final CompletableFuture<RoutingContext> workerRequest = new CompletableFuture<>();
        private final CompletableFuture<RoutingContext> providerRequest = new CompletableFuture<>();
        private final CompletableFuture<Boolean> encodedOnWorkerContext = new CompletableFuture<>();
        private final CompletableFuture<RoutingContext> encodingRequest = new CompletableFuture<>();
        private final CompletableFuture<RoutingContext> responseRequest = new CompletableFuture<>();
        private final CompletableFuture<TransitionResultEnvelope> result = new CompletableFuture<>();

        private volatile TransitionResultEnvelope resultToDeliver;

        @Inject
        CurrentVertxRequest currentRequest;

        @ServerResponseFilter
        void readWorkerRequest(ContainerRequestContext request, ContainerResponseContext response) {
            if (RestTransitionWorkerProtocol.EXECUTE_PATH.equals(request.getUriInfo().getRequestUri().getPath())) {
                RoutingContext activeRequest = currentRequest.getCurrent();
                response.getHeaders().putSingle("X-Worker-Request-Path", activeRequest.request().path());
                responseRequest.complete(activeRequest);
            }
        }
    }

    @Path(CALLBACK_PATH)
    @RequestScoped
    public static class ProviderCallback {
        @Inject
        CallbackState callback;

        private Context providerContext;

        @POST
        @NonBlocking
        public Response complete() {
            providerContext = Vertx.currentContext();
            callback.providerRequest.complete(callback.currentRequest.getCurrent());
            return Response.noContent().build();
        }

        @PreDestroy
        void deliverAfterRequest() {
            if (providerContext != null) {
                providerContext.runOnContext(ignored -> callback.result.complete(callback.resultToDeliver));
            }
        }
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.devservices.enabled", "false",
                "quarkus.otel.sdk.disabled", "true",
                "pipeline.orchestrator.worker.rest.server-enabled", "true",
                "pipeline.orchestrator.worker.rest.shared-secret", SECRET);
        }
    }
}

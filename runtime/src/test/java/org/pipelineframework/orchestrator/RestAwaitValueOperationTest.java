package org.pipelineframework.orchestrator;

import com.sun.net.httpserver.HttpServer;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitValueOperation;
import org.pipelineframework.awaitable.AwaitValueOperationExecutor;
import org.pipelineframework.awaitable.AwaitValueOperationRequest;
import org.pipelineframework.awaitable.AwaitValueOperationResult;
import org.pipelineframework.awaitable.CommandCompletionRejectedException;
import org.pipelineframework.config.pipeline.PipelineJson;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Real HTTP framing and authentication; application projection is isolated behind its tested executor. */
class RestAwaitValueOperationTest {
    private RestTransitionWorkerResource resource;
    private RestPipelineTransitionWorker client;
    private HttpClient http;
    private HttpServer server;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.RestWorkerConfig rest;
    private AwaitValueOperationExecutor executor;
    private URI endpoint;
    private final AtomicInteger operationCalls = new AtomicInteger();
    private UnaryOperator<String> responseTransform = UnaryOperator.identity();

    @BeforeEach
    void setUp() throws Exception {
        config = mock(PipelineOrchestratorConfig.class);
        rest = mock(PipelineOrchestratorConfig.RestWorkerConfig.class);
        when(config.workerRest()).thenReturn(rest);
        when(rest.serverEnabled()).thenReturn(true);
        when(rest.path()).thenReturn(RestTransitionWorkerProtocol.EXECUTE_PATH);
        when(rest.capabilitiesPath()).thenReturn(RestTransitionWorkerProtocol.CAPABILITIES_PATH);
        when(rest.sharedSecret()).thenReturn(Optional.of("test-worker-secret"));
        when(rest.sharedSecretRef()).thenReturn(Optional.empty());
        when(rest.signatureTolerance()).thenReturn(Duration.ofMinutes(2));
        when(rest.requestTimeout()).thenReturn(Duration.ofSeconds(5));
        resource = new RestTransitionWorkerResource();
        resource.orchestratorConfig = config;
        resource.executionService = mock(org.pipelineframework.PipelineExecutionService.class);
        resource.requestThreadContext = mock(org.eclipse.microprofile.context.ThreadContext.class);
        when(resource.requestThreadContext.currentContextExecutor()).thenReturn(Runnable::run);
        resource.identityResolver = mock(PipelineReleaseIdentityResolver.class);
        when(resource.identityResolver.pipelineId(config)).thenReturn("application-a");
        when(resource.identityResolver.contractVersion()).thenReturn("contract-a");
        when(resource.identityResolver.releaseVersion(config)).thenReturn("release-a1");
        when(resource.identityResolver.artifactId(config)).thenReturn("worker-a1");
        when(resource.identityResolver.artifactDigest(config)).thenReturn("sha256:artifact-a1");
        when(resource.identityResolver.capabilities()).thenReturn(PipelineBundleCapabilities.defaults());
        executor = mock(AwaitValueOperationExecutor.class);
        resource.awaitValueOperations = executor;
        when(executor.supported()).thenReturn(true);
        when(executor.execute(any())).thenAnswer(call -> {
            AwaitValueOperationRequest request = call.getArgument(0);
            return Uni.createFrom().item(new AwaitValueOperationResult(request.operation(), request.operationId(),
                request.requestDigest(), new SerializedTransitionPayload("canonical-result", "json", "{\"answer\":42}")));
        });
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext(RestTransitionWorkerProtocol.CAPABILITIES_PATH, exchange -> {
            var headers = exchange.getRequestHeaders();
            try (Response response = resource.capabilities(headers.getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER),
                headers.getFirst(TransitionWorkerSignature.NONCE_HEADER),
                headers.getFirst(TransitionWorkerSignature.SIGNATURE_HEADER)).await().atMost(Duration.ofSeconds(5))) {
                byte[] bytes = PipelineJson.mapper().writeValueAsBytes(response.getEntity());
                exchange.sendResponseHeaders(response.getStatus(), bytes.length);
                exchange.getResponseBody().write(bytes);
            } finally { exchange.close(); }
        });
        server.createContext(RestTransitionWorkerProtocol.AWAIT_VALUE_OPERATION_PATH, exchange -> {
            operationCalls.incrementAndGet();
            var headers = exchange.getRequestHeaders();
            try (Response response = resource.executeAwaitValueOperation(
                headers.getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER),
                headers.getFirst(TransitionWorkerSignature.NONCE_HEADER),
                headers.getFirst(TransitionWorkerSignature.SIGNATURE_HEADER),
                exchange.getRequestBody().readAllBytes()).await().atMost(Duration.ofSeconds(5))) {
                byte[] bytes = responseTransform.apply(PipelineJson.mapper().writeValueAsString(response.getEntity()))
                    .getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(response.getStatus(), bytes.length);
                exchange.getResponseBody().write(bytes);
            } finally { exchange.close(); }
        });
        server.start();
        endpoint = URI.create("http://localhost:" + server.getAddress().getPort());
        when(rest.baseUrl()).thenReturn(Optional.of(endpoint.toString()));
        http = HttpClient.newHttpClient();
        client = new RestPipelineTransitionWorker(http);
        client.orchestratorConfig = config;
    }

    @AfterEach
    void close() {
        client.close();
        http.close();
        server.stop(0);
    }

    @Test
    void signedHttpRoundTripPreservesAllOperationFieldsAndResultBinding() {
        var request = request("release-a1");
        var result = client.executeAwaitValueOperation(request).await().atMost(Duration.ofSeconds(5));
        assertTrue(result.matches(request));
        assertEquals("{\"answer\":42}", result.value().payload());
        verify(executor).execute(request);
        assertEquals(1, operationCalls.get());
    }

    @Test
    void scopedEndpointUsesSelectedSecretReferenceInsteadOfGlobalSecret() {
        System.setProperty("tpf.r1.test.worker.secret", "test-worker-secret");
        when(rest.sharedSecret()).thenReturn(Optional.of("different-global-secret"));
        // Server and client configurations are separate in production.
        var serverConfig = mock(PipelineOrchestratorConfig.class);
        var serverRest = mock(PipelineOrchestratorConfig.RestWorkerConfig.class);
        when(serverConfig.workerRest()).thenReturn(serverRest);
        when(serverRest.serverEnabled()).thenReturn(true);
        when(serverRest.sharedSecret()).thenReturn(Optional.of("test-worker-secret"));
        when(serverRest.sharedSecretRef()).thenReturn(Optional.empty());
        when(serverRest.signatureTolerance()).thenReturn(Duration.ofMinutes(2));
        when(resource.identityResolver.pipelineId(serverConfig)).thenReturn("application-a");
        when(resource.identityResolver.releaseVersion(serverConfig)).thenReturn("release-a1");
        when(resource.identityResolver.artifactId(serverConfig)).thenReturn("worker-a1");
        when(resource.identityResolver.artifactDigest(serverConfig)).thenReturn("sha256:artifact-a1");
        resource.orchestratorConfig = serverConfig;
        try {
            assertTrue(client.executeAwaitValueOperation(request("release-a1"), endpoint,
                "sys:tpf.r1.test.worker.secret").await().atMost(Duration.ofSeconds(5)).matches(request("release-a1")));
        } finally { System.clearProperty("tpf.r1.test.worker.secret"); }
    }

    @Test
    void unsupportedCapabilityAndWrongReleaseNeverSendOperation() {
        when(executor.supported()).thenReturn(false);
        assertThrows(TransitionWorkerFailureException.class,
            () -> client.executeAwaitValueOperation(request("release-a1")).await().atMost(Duration.ofSeconds(5)));
        when(executor.supported()).thenReturn(true);
        assertThrows(TransitionWorkerFailureException.class,
            () -> client.executeAwaitValueOperation(request("release-a2")).await().atMost(Duration.ofSeconds(5)));
        assertEquals(0, operationCalls.get());
        verify(executor, never()).execute(any());
    }

    @Test
    void responseScopeMismatchAndTrailingJsonAreRejected() {
        responseTransform = body -> body.replace("operation-1", "foreign-operation");
        assertThrows(TransitionWorkerFailureException.class,
            () -> client.executeAwaitValueOperation(request("release-a1")).await().atMost(Duration.ofSeconds(5)));
        responseTransform = body -> body.replace(request("release-a1").requestDigest(), "sha256:foreign-request");
        assertThrows(TransitionWorkerFailureException.class,
            () -> client.executeAwaitValueOperation(request("release-a1")).await().atMost(Duration.ofSeconds(5)));
        responseTransform = body -> body + " {}";
        assertThrows(TransitionWorkerFailureException.class,
            () -> client.executeAwaitValueOperation(request("release-a1")).await().atMost(Duration.ofSeconds(5)));
        assertEquals(3, operationCalls.get(), "negative oracle must reach the value-operation boundary");
    }

    @Test
    void workerFailureAfterCapabilityProbeDoesNotRetryOrAcceptAValue() {
        server.removeContext(RestTransitionWorkerProtocol.AWAIT_VALUE_OPERATION_PATH);
        server.createContext(RestTransitionWorkerProtocol.AWAIT_VALUE_OPERATION_PATH, exchange -> {
            operationCalls.incrementAndGet();
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        assertThrows(TransitionWorkerFailureException.class,
            () -> client.executeAwaitValueOperation(request("release-a1")).await().atMost(Duration.ofSeconds(5)));
        assertEquals(1, operationCalls.get());
        verify(executor, never()).execute(any());
    }

    @Test
    void typedCommandRejectionRetainsItsMeaningWithoutApplicationErrorDetails() {
        doReturn(Uni.createFrom().failure(new CommandCompletionRejectedException("private application identity detail")))
            .when(executor).execute(any());
        var failure = assertThrows(CommandCompletionRejectedException.class,
            () -> client.executeAwaitValueOperation(request("release-a1")).await().atMost(Duration.ofSeconds(5)));
        assertFalse(failure.getMessage().contains("private application"));
        assertEquals(1, operationCalls.get());
    }

    @Test
    void malformedOrForeignRejectionCannotDiscardCompletionEvidence() {
        doReturn(Uni.createFrom().failure(new CommandCompletionRejectedException("identity mismatch")))
            .when(executor).execute(any());
        responseTransform = body -> body.replace(request("release-a1").requestDigest(), "sha256:foreign-request");
        assertThrows(TransitionWorkerFailureException.class,
            () -> client.executeAwaitValueOperation(request("release-a1")).await().atMost(Duration.ofSeconds(5)));
        responseTransform = body -> body.replace("COMMAND_COMPLETION_REJECTED", "UNKNOWN_CODE");
        assertThrows(TransitionWorkerFailureException.class,
            () -> client.executeAwaitValueOperation(request("release-a1")).await().atMost(Duration.ofSeconds(5)));
        responseTransform = body -> body + " {}";
        assertThrows(TransitionWorkerFailureException.class,
            () -> client.executeAwaitValueOperation(request("release-a1")).await().atMost(Duration.ofSeconds(5)));
        assertEquals(3, operationCalls.get(), "negative oracle must reach the value-operation boundary");
    }

    @Test
    void ordinaryValidationFailureIsNotAnIdentityRejection() {
        doReturn(Uni.createFrom().failure(new IllegalArgumentException("bad schema"))).when(executor).execute(any());
        assertThrows(TransitionWorkerFailureException.class,
            () -> client.executeAwaitValueOperation(request("release-a1")).await().atMost(Duration.ofSeconds(5)));
        assertEquals(1, operationCalls.get(), "negative oracle must reach the value-operation boundary");
    }

    @Test
    void signedDirectCallCannotBypassUnsupportedCapability() throws Exception {
        when(executor.supported()).thenReturn(false);
        byte[] body = PipelineJson.mapper().writeValueAsBytes(request("release-a1"));
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign("test-worker-secret", "POST",
            RestTransitionWorkerProtocol.AWAIT_VALUE_OPERATION_PATH, timestamp, nonce, body);
        assertEquals(404, resource.executeAwaitValueOperation(timestamp, nonce, signature, body)
            .await().indefinitely().getStatus());
        verify(executor, never()).execute(any());
    }

    @Test
    void signatureBindsPathAndBytesAndNonceCannotBeReplayed() throws Exception {
        byte[] body = PipelineJson.mapper().writeValueAsBytes(request("release-a1"));
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String wrongPathSignature = TransitionWorkerSignature.sign("test-worker-secret", "POST",
            RestTransitionWorkerProtocol.EXECUTE_PATH, timestamp, nonce, body);
        assertEquals(401, resource.executeAwaitValueOperation(timestamp, nonce, wrongPathSignature, body)
            .await().indefinitely().getStatus());
        String signature = TransitionWorkerSignature.sign("test-worker-secret", "POST",
            RestTransitionWorkerProtocol.AWAIT_VALUE_OPERATION_PATH, timestamp, nonce, body);
        assertEquals(401, resource.executeAwaitValueOperation(timestamp, nonce, signature, new byte[0])
            .await().indefinitely().getStatus());
        assertEquals(200, resource.executeAwaitValueOperation(timestamp, nonce, signature, body)
            .await().indefinitely().getStatus());
        assertEquals(401, resource.executeAwaitValueOperation(timestamp, nonce, signature, body)
            .await().indefinitely().getStatus());
        verify(executor, times(1)).execute(any());
    }

    @Test
    void disabledUnsignedExpiredAndMalformedRequestsNeverExecute() throws Exception {
        when(rest.serverEnabled()).thenReturn(false);
        assertEquals(404, resource.executeAwaitValueOperation(null, null, null, new byte[0]).await().indefinitely().getStatus());
        when(rest.serverEnabled()).thenReturn(true);
        assertEquals(401, resource.executeAwaitValueOperation(null, null, null, new byte[0]).await().indefinitely().getStatus());
        byte[] body = (PipelineJson.mapper().writeValueAsString(request("release-a1")) + " {}").getBytes(StandardCharsets.UTF_8);
        for (var timestamp : new String[]{Instant.now().minusSeconds(600).toString(), Instant.now().toString()}) {
            String nonce = UUID.randomUUID().toString();
            String signature = TransitionWorkerSignature.sign("test-worker-secret", "POST",
                RestTransitionWorkerProtocol.AWAIT_VALUE_OPERATION_PATH, timestamp, nonce, body);
            int expected = Instant.parse(timestamp).isBefore(Instant.now().minusSeconds(300)) ? 401 : 400;
            assertEquals(expected, resource.executeAwaitValueOperation(timestamp, nonce, signature, body)
                .await().indefinitely().getStatus());
        }
        verify(executor, never()).execute(any());
    }

    private static AwaitValueOperationRequest request(String release) {
        return new AwaitValueOperationRequest(AwaitValueOperation.NORMALIZE_AND_PROJECT_COMPLETION,
            "operation-1", "tenant-a", "execution-a", "interaction-a", "unit-a", "PrepareService", 3,
            "application-a", "contract-a", release, Optional.of("projector-a1"), Optional.of("sha256:callback"),
            Optional.of(new SerializedTransitionPayload("request", "json", "{\"id\":\"request-a\"}")),
            Optional.empty(), Optional.of(new SerializedTransitionPayload("completion", "json", "{\"accepted\":true}")),
            "actor-a", 1234);
    }
}

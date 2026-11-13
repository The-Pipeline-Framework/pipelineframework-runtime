package org.pipelineframework.orchestrator;

import com.sun.net.httpserver.HttpServer;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.AwaitTransitionAuthority;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** Actual HTTP, signing, nonce replay and response framing; lifecycle semantics have a separate owner suite. */
class RestAwaitLifecycleTest {
    private RestAwaitLifecycleResource resource;
    private RestAwaitLifecycleClient client;
    private HttpServer server;
    private java.util.concurrent.ExecutorService serverExecutor;
    private HttpClient raw;
    private URI origin;
    private AwaitRegistrationRequest registration;
    private AwaitInteractionRecord record;
    private AwaitTransitionAuthority authority;
    private PipelineOrchestratorConfig config;
    private final AtomicInteger calls = new AtomicInteger();
    private UnaryOperator<String> responseTransform = UnaryOperator.identity();
    private boolean dropReply;
    private static final String SECRET = "worker-a-retained-secret";

    @BeforeEach void setUp() throws Exception {
        config = mock(PipelineOrchestratorConfig.class);
        var worker = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        var rest = mock(PipelineOrchestratorConfig.RestWorkerConfig.class);
        when(config.worker()).thenReturn(worker); when(config.workerRest()).thenReturn(rest);
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(worker.targetingMode()).thenReturn("registered-rest"); when(worker.allowLoopbackHttp()).thenReturn(true);
        when(rest.signatureTolerance()).thenReturn(Duration.ofMinutes(1));
        when(rest.connectTimeout()).thenReturn(Duration.ofSeconds(2)); when(rest.requestTimeout()).thenReturn(Duration.ofSeconds(3));
        resource = new RestAwaitLifecycleResource(); resource.config = config;
        resource.workers = mock(RegisteredRestWorkerTargets.class);
        resource.secrets = mock(ControlPlaneSecretResolver.class);
        when(resource.secrets.resolve("env:worker-a")).thenReturn(SECRET);
        authority = mock(AwaitTransitionAuthority.class); resource.authority = authority;
        var scope = new AwaitTransitionScope("tenant-a", "execution", "pipeline", "contract", "release-a1", 1, "transition");
        registration = new AwaitRegistrationRequest(scope, "AwaitReview", 8, "execution", "unit", "execution:8",
            Optional.empty(), new SerializedTransitionPayload("canonical", "json", "{}"), Optional.empty(), Optional.empty(), Optional.empty(), Map.of());
        when(resource.workers.lifecycleCredentialRef(any(), anyString(), anyString())).thenAnswer(call -> {
            AwaitTransitionScope requested = call.getArgument(0);
            return scope.equals(requested) && "worker-a".equals(call.getArgument(1)) && "entry-a".equals(call.getArgument(2))
                ? Uni.createFrom().item("env:worker-a") : Uni.createFrom().failure(new IllegalStateException("Unbound worker"));
        });
        var store = new InMemoryAwaitInteractionStore(); long now = System.currentTimeMillis();
        record = store.createOrGet(new AwaitCreateCommand("tenant-a", "execution", "AwaitReview", 8, "Output", "Output", "execution:8",
            "idempotency", "correlation", Map.of(), null, null, "interaction-api", "unit", null, Map.of(), now, now + 60_000, Long.MAX_VALUE))
            .await().indefinitely().record();
        when(authority.registerForWorker(any())).thenReturn(Uni.createFrom().item(new AwaitWorkerRegistrationResult(
            new AwaitCreateResult(record, false), Optional.of("opaque-coordinator-callback-token"))));
        when(authority.claimDispatch(any(), anyString(), anyLong())).thenAnswer(call ->
            store.markDispatching("tenant-a", record.interactionId(), record.version(), System.currentTimeMillis())
                .map(claimed -> claimed.orElseThrow(() -> new IllegalStateException("Grant consumed"))));
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        serverExecutor = java.util.concurrent.Executors.newFixedThreadPool(2, task -> new Thread(() -> {
            var prior = Thread.currentThread().getContextClassLoader(); Thread.currentThread().setContextClassLoader(loader);
            try { task.run(); } finally { Thread.currentThread().setContextClassLoader(prior); }
        }, "await-lifecycle-http"));
        server.setExecutor(serverExecutor);
        server.createContext(RestAwaitLifecycleProtocol.ROOT, exchange -> {
            calls.incrementAndGet(); var headers = exchange.getRequestHeaders(); byte[] body = exchange.getRequestBody().readAllBytes();
            String timestamp = headers.getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER);
            String nonce = headers.getFirst(TransitionWorkerSignature.NONCE_HEADER);
            String signature = headers.getFirst(TransitionWorkerSignature.SIGNATURE_HEADER);
            var pending = switch (exchange.getRequestURI().getPath()) {
                case RestAwaitLifecycleProtocol.REGISTRATION_PATH -> resource.register(timestamp, nonce, signature, body);
                case RestAwaitLifecycleProtocol.DISPATCH_CLAIM_PATH -> resource.claim(timestamp, nonce, signature, body);
                case RestAwaitLifecycleProtocol.REPORT_PATH -> resource.report(timestamp, nonce, signature, body);
                case RestAwaitLifecycleProtocol.UNIT_PREPARE_PATH -> resource.prepareUnit(timestamp, nonce, signature, body);
                case RestAwaitLifecycleProtocol.UNIT_SEAL_PATH -> resource.sealUnit(timestamp, nonce, signature, body);
                case RestAwaitLifecycleProtocol.UNIT_SUSPENSION_PATH -> resource.suspension(timestamp, nonce, signature, body);
                case RestAwaitLifecycleProtocol.UNIT_RESUME_PATH -> resource.resume(timestamp, nonce, signature, body);
                default -> throw new IllegalArgumentException("Unexpected path");
            };
            try (Response response = pending.await().atMost(Duration.ofSeconds(5))) {
                if (dropReply) { exchange.close(); return; }
                byte[] bytes = responseTransform.apply(PipelineJson.mapper().writeValueAsString(response.getEntity())).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json"); exchange.sendResponseHeaders(response.getStatus(), bytes.length);
                exchange.getResponseBody().write(bytes);
            } finally { exchange.close(); }
        });
        server.start(); origin = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        client = new RestAwaitLifecycleClient(); client.config = config; client.secrets = resource.secrets;
        raw = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
    }

    @AfterEach void close() {
        if (server != null) server.stop(0);
        if (serverExecutor != null) serverExecutor.close();
        if (client != null) client.close();
        if (raw != null) raw.close();
    }

    @Test void signedRegistrationForwardsOnlyCoordinatorIssuedTokenAndScopedCreation() {
        var result = client.register(origin, "env:worker-a", "worker-a", "entry-a", registration).await().indefinitely();
        assertEquals(record.interactionId(), result.creation().record().interactionId());
        assertEquals(Optional.of("opaque-coordinator-callback-token"), result.callbackToken());
        verify(authority).registerForWorker(registration); assertEquals(1, calls.get());
    }

    @Test void signedDispatchGrantIsConsumedOnceAndNeverResent() {
        var grant = client.claimDispatch(origin, "env:worker-a", "worker-a", "entry-a", registration.scope(), record.interactionId(), record.version()).await().indefinitely();
        assertEquals(AwaitInteractionStatus.DISPATCHING, grant.status());
        assertThrows(IllegalStateException.class, () -> client.claimDispatch(origin, "env:worker-a", "worker-a", "entry-a", registration.scope(), record.interactionId(), record.version()).await().indefinitely());
        verify(authority, times(2)).claimDispatch(registration.scope(), record.interactionId(), record.version());
        assertEquals(2, calls.get());
    }

    @Test void wrongSignatureWrongEntryAndReplayNeverReachAuthority() throws Exception {
        byte[] body = body(); String now = Instant.now().toString(); String nonce = UUID.randomUUID().toString();
        assertEquals(401, signed(body, "wrong-secret", now, nonce).statusCode());
        assertEquals(200, signed(body, SECRET, now, nonce).statusCode());
        assertEquals(401, signed(body, SECRET, now, nonce).statusCode());
        verify(authority, times(1)).registerForWorker(any());
        clearInvocations(authority);
        byte[] foreign = PipelineJson.mapper().writeValueAsBytes(new AwaitWorkerRegistrationRequest("worker-a", "foreign-entry", registration));
        assertEquals(401, signed(foreign, SECRET, now, UUID.randomUUID().toString()).statusCode());
        verifyNoInteractions(authority);
    }

    @Test void absentStaleAndCrossScopeSignaturesFailBeforeLifecycleMutation() throws Exception {
        byte[] body = body();
        var unsigned = HttpRequest.newBuilder(origin.resolve(RestAwaitLifecycleProtocol.REGISTRATION_PATH)).POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
        assertEquals(401, raw.send(unsigned, HttpResponse.BodyHandlers.ofString()).statusCode());
        assertEquals(401, signed(body, SECRET, Instant.now().minusSeconds(120).toString(), UUID.randomUUID().toString()).statusCode());
        byte[] foreign = new String(body, java.nio.charset.StandardCharsets.UTF_8).replace("tenant-a", "tenant-b").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(401, signed(foreign, SECRET, Instant.now().toString(), UUID.randomUUID().toString()).statusCode());
        verifyNoInteractions(authority);
    }

    @Test void disabledResourceAndMalformedOrInjectedAuthorityFieldsDoNotInvokeHelper() throws Exception {
        byte[] injected = new String(body(), java.nio.charset.StandardCharsets.UTF_8).replaceFirst("\\{", "{\"callbackToken\":\"forged\",").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(400, signed(injected, SECRET, Instant.now().toString(), UUID.randomUUID().toString()).statusCode());
        when(config.mode()).thenReturn(OrchestratorMode.SYNC);
        assertEquals(404, signed(body(), SECRET, Instant.now().toString(), UUID.randomUUID().toString()).statusCode());
        verifyNoInteractions(authority);
    }

    @Test void malformedOrCrossScopeSuccessIsUnknownAndNeverRetried() {
        responseTransform = ignored -> "{malformed";
        assertThrows(RemoteTransitionOutcomeUnknownException.class,
            () -> client.register(origin, "env:worker-a", "worker-a", "entry-a", registration).await().indefinitely());
        responseTransform = wire -> wire.replace("tenant-a", "tenant-b");
        assertThrows(RemoteTransitionOutcomeUnknownException.class,
            () -> client.register(origin, "env:worker-a", "worker-a", "entry-a", registration).await().indefinitely());
        assertEquals(2, calls.get());
    }

    @Test void lostDispatchReplyPreservesUnknownDispositionWithoutAutomaticResend() {
        dropReply = true;
        assertThrows(RemoteTransitionOutcomeUnknownException.class,
            () -> client.claimDispatch(origin, "env:worker-a", "worker-a", "entry-a", registration.scope(), record.interactionId(), record.version()).await().indefinitely());
        verify(authority, times(1)).claimDispatch(any(), anyString(), anyLong());
        assertEquals(1, calls.get());
    }

    @Test void retainedReportAuthenticationDoesNotRequireNewDispatchEligibility() {
        when(resource.workers.lifecycleCredentialRef(any(), anyString(), anyString()))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("Lease expired")));
        when(resource.workers.lifecycleReportCredentialRef(registration.scope(), "worker-a", "entry-a"))
            .thenReturn(Uni.createFrom().item("env:worker-a"));
        var request = new AwaitDispatchReportRequest("worker-a", "entry-a", registration.scope(), record.interactionId(),
            record.version(), AwaitDispatchReportKind.DISPATCHED, Map.of("messageId", "delivery-1"), Optional.empty(), Optional.empty(), Optional.empty());
        when(authority.report(request)).thenReturn(Uni.createFrom().item(new AwaitDispatchReportResult(record, false)));
        assertEquals(record, client.report(origin, "env:worker-a", request).await().indefinitely().record());
        verify(resource.workers, never()).lifecycleCredentialRef(any(), anyString(), anyString());
        verify(authority).report(request);
    }

    @Test void unitRoutesUseLiveAdmissionButRetainedObservationAndCheckResponseOwnership() {
        var request = new AwaitWorkerUnitRequest("worker-a", "entry-a", registration.scope(), "AwaitReview", 8,
            "execution", "unit", Optional.empty());
        var unit = new AwaitUnitRecord("tenant-a", "unit", "execution", "AwaitReview", 8, "SINGLE", 1,
            AwaitUnitStatus.WAITING_EXTERNAL, record.interactionId(), 1, 0, java.util.Set.of(), false, 1, 1, Long.MAX_VALUE);
        when(resource.workers.lifecycleReportCredentialRef(registration.scope(), "worker-a", "entry-a"))
            .thenReturn(Uni.createFrom().item("env:worker-a"));
        when(authority.prepareUnit(request)).thenReturn(Uni.createFrom().item(unit));
        when(authority.sealUnit(any())).thenReturn(Uni.createFrom().item(unit));
        when(authority.suspension(request)).thenReturn(Uni.createFrom().item(
            new TransitionAwaitSuspension("tenant-a", "execution", "unit", 8, unit, java.util.List.of(record))));
        when(authority.resume(request)).thenReturn(Uni.createFrom().item(new SerializedTransitionPayload("canonical", "json", "{}")));
        assertEquals(unit, client.prepareUnit(origin, "env:worker-a", request).await().indefinitely());
        assertEquals(unit, client.sealUnit(origin, "env:worker-a", new AwaitUnitSealRequest(request, 1)).await().indefinitely());
        assertEquals(unit, client.suspension(origin, "env:worker-a", request).await().indefinitely().unit());
        assertNotNull(client.resume(origin, "env:worker-a", request).await().indefinitely());
        verify(resource.workers, times(1)).lifecycleCredentialRef(any(), anyString(), anyString());
        verify(resource.workers, times(3)).lifecycleReportCredentialRef(any(), anyString(), anyString());
        responseTransform = wire -> wire.replace("tenant-a", "tenant-b");
        assertThrows(RemoteTransitionOutcomeUnknownException.class,
            () -> client.prepareUnit(origin, "env:worker-a", request).await().indefinitely());
    }

    private byte[] body() throws Exception { return PipelineJson.mapper().writeValueAsBytes(new AwaitWorkerRegistrationRequest("worker-a", "entry-a", registration)); }
    private HttpResponse<String> signed(byte[] body, String secret, String timestamp, String nonce) throws Exception {
        String path = RestAwaitLifecycleProtocol.REGISTRATION_PATH;
        return raw.send(HttpRequest.newBuilder(origin.resolve(path)).header("Content-Type", "application/json")
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp).header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, TransitionWorkerSignature.sign(secret, "POST", path, timestamp, nonce, body))
            .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
}

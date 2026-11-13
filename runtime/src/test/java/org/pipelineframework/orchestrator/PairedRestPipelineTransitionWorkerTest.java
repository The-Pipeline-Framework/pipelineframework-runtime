package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Uni;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.orchestrator.TransitionResultEnvelope.PortableFailure;

class PairedRestPipelineTransitionWorkerTest {
    private static final String INPUT_TYPE = "InputValue";
    private static final String OUTPUT_TYPE = "OutputValue";
    private static final String SECRET = "scoped-secret";
    private static final String OWNER = "coordinator";
    private static final Duration WAIT = Duration.ofSeconds(5);

    @TempDir Path temporary;
    private NativeWorkerReleaseBindingTest.TransportFixture fixture;
    private NativeWorkerReleaseBinding.Attempt attempt;
    private HttpClient pairedClient;
    private HttpClient legacyClient;
    private PairedTransitionSettlementStore store;
    private RestPipelineTransitionWorker worker;

    @BeforeEach
    void prepare() throws IOException {
        fixture = NativeWorkerReleaseBindingTest.transportFixture(temporary);
        attempt = attempt("{\"value\":3}");
        pairedClient = mock(HttpClient.class);
        legacyClient = mock(HttpClient.class);
        store = mock(PairedTransitionSettlementStore.class);
        when(store.beginPairedDispatch(any())).thenAnswer(ignored -> Uni.createFrom().item(OptionalLong.of(System.currentTimeMillis() + WAIT.toMillis())));
        when(store.settlePairedUnknown(any())).thenReturn(Uni.createFrom().item(Optional.empty()));
        worker = new RestPipelineTransitionWorker(legacyClient, Optional.of(pairedClient), new PipelineInvocationRuntime());
        worker.orchestratorConfig = mock(PipelineOrchestratorConfig.class);
        var rest = mock(PipelineOrchestratorConfig.RestWorkerConfig.class);
        when(worker.orchestratorConfig.workerRest()).thenReturn(rest);
        when(rest.path()).thenReturn(RestTransitionWorkerProtocol.EXECUTE_PATH);
        when(rest.requestTimeout()).thenReturn(WAIT);
        worker.secretResolver = mock(ControlPlaneSecretResolver.class);
        when(worker.secretResolver.resolve(fixture.target().sharedSecretRef())).thenReturn(SECRET);
    }

    @AfterEach
    void close() {
        if (worker != null) {
            worker.close();
        }
    }

    @Test
    void signsTheExactPinnedRequestAndAcceptsReleasedCompletedWireResult() throws Exception {
        var output = new SerializedTransitionPayload(OUTPUT_TYPE, fixture.target().profiles().payloadEncoding(),
            "{\"value\":5}");
        String releasedResult = PipelineJson.mapper().writeValueAsString(new TransitionWireResult(
            TransitionWorkerOutcome.COMPLETED, List.of(output), null, null, false, false, Optional.empty()));
        var sent = respond(200, releasedResult, Optional.empty());

        var result = worker.executeTransition(attempt, OWNER, store).await().atMost(WAIT).orElseThrow();

        var ordered = inOrder(store, pairedClient);
        ordered.verify(store).beginPairedDispatch(any());
        ordered.verify(pairedClient).sendAsync(any(HttpRequest.class),
            ArgumentMatchers.<HttpResponse.BodyHandler<String>>any());
        assertEquals(List.of(output), result.outputPayloads());
        assertSame(attempt, result.pairedResult().orElseThrow().attempt());
        assertEquals(Optional.empty(), result.pairedResult().orElseThrow().failure());
        assertEquals(TransitionWorkerOutcome.COMPLETED, result.toWireResult().outcome());
        HttpRequest request = sent.get();
        assertEquals(URI.create("https://worker.example:443" + RestTransitionWorkerProtocol.EXECUTE_PATH), request.uri());
        assertEquals(HttpClient.Version.HTTP_1_1, request.version().orElseThrow());
        var publisher = request.bodyPublisher().orElseThrow();
        byte[] rawBody = readBody(publisher).join();
        assertArrayEquals(PipelineJson.mapper().writeValueAsBytes(attempt.command()), rawBody);
        String timestamp = request.headers().firstValue(TransitionWorkerSignature.TIMESTAMP_HEADER).orElseThrow();
        String nonce = request.headers().firstValue(TransitionWorkerSignature.NONCE_HEADER).orElseThrow();
        String signature = request.headers().firstValue(TransitionWorkerSignature.SIGNATURE_HEADER).orElseThrow();
        assertEquals(TransitionWorkerSignature.sign(SECRET, request.method(), request.uri().getPath(), timestamp, nonce, rawBody),
            signature);
        assertTrue(assertThrows(CompletionException.class, () -> readBody(publisher).join()).getCause() instanceof IOException);
        verifyNoInteractions(legacyClient);
        verify(worker.secretResolver).resolve(fixture.target().sharedSecretRef());
    }

    @ParameterizedTest
    @EnumSource(PortableFailure.Code.class)
    void acceptsOnlyNormalizedPortableFailureData(PortableFailure.Code code) throws Exception {
        respond(200, PipelineJson.mapper().writeValueAsString(Map.of(
            "resultProfile", fixture.target().profiles().resultProfile(), "outcome", "FAILED",
            "failure", Map.of("code", code.name(), "stepId", "B", "diagnostic", code.diagnostic()))), Optional.empty());

        var result = worker.executeTransition(attempt, OWNER, store).await().atMost(WAIT).orElseThrow();

        assertEquals(TransitionWorkerOutcome.FAILED, result.outcome());
        assertEquals(new PortableFailure(code, 1), result.pairedResult().orElseThrow().failure().orElseThrow());
        assertThrows(IllegalStateException.class, result::toWireResult);
        assertThrows(IllegalStateException.class, result::failureException);
        verifyNoInteractions(legacyClient);
    }

    @Test
    void rejectsInvalidInputBeforeAnyRequestOrSecretLookup() {
        assertThrows(IllegalArgumentException.class,
            () -> worker.executeTransition(attempt("{\"value\":3.0}"), OWNER, store).await().atMost(WAIT));
        verifyNoInteractions(pairedClient, legacyClient, worker.secretResolver, store);
    }

    @Test
    void aDeclinedIntentDoesNotSendOrSettle() {
        when(store.beginPairedDispatch(any())).thenReturn(Uni.createFrom().item(OptionalLong.empty()));
        assertTrue(worker.executeTransition(attempt, OWNER, store).await().atMost(WAIT).isEmpty());
        verifyNoInteractions(pairedClient, legacyClient);
    }

    @Test
    void aPermitThatArrivesAfterLeaseExpiryDoesNotSend() {
        when(store.beginPairedDispatch(any())).thenReturn(Uni.createFrom().item(OptionalLong.of(1L)));
        assertTrue(worker.executeTransition(attempt, OWNER, store).await().atMost(WAIT).isEmpty());
        verifyNoInteractions(pairedClient, legacyClient);
    }

    @Test
    void anUnconfirmedReservationDoesNotSend() {
        when(store.beginPairedDispatch(any())).thenReturn(Uni.createFrom().failure(new IOException("commit reply lost")));
        assertThrows(RuntimeException.class, () -> worker.executeTransition(attempt, OWNER, store).await().atMost(WAIT));
        verifyNoInteractions(pairedClient, legacyClient);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "null",
        "{\"outcome\":\"COMPLETED\",\"outputPayloads\":[],\"terminalOutputPublished\":false,\"terminalInputPassthrough\":false}",
        "{\"outcome\":\"FAILED\",\"failure\":{\"failureClass\":\"java.lang.IllegalStateException\",\"message\":\"private\"}}",
        "{\"resultProfile\":\"tpf-polyglot-failed-result/1\",\"outcome\":\"FAILED\",\"failure\":{\"code\":\"BUSINESS_REJECTION\",\"stepId\":\"B\",\"diagnostic\":\"private\"}}",
        "{\"resultProfile\":\"tpf-polyglot-failed-result/1\",\"outcome\":\"FAILED\",\"failure\":{\"code\":\"BUSINESS_REJECTION\",\"stepId\":\"Z\",\"diagnostic\":\"Business function rejected input\"}}",
        "{\"resultProfile\":\"tpf-polyglot-failed-result/1\",\"outcome\":\"FAILED\",\"outcome\":\"FAILED\",\"failure\":{}}"
    })
    void corruptActiveResponseIsOutcomeUnknown(String body) {
        respond(200, body, Optional.empty());
        assertUnknown();
    }

    @Test
    void oversizedResponseIsOutcomeUnknown() {
        respond(200, " ".repeat(5000), Optional.empty());
        assertUnknown();
    }

    @Test
    void responseFromAnotherOriginIsOutcomeUnknown() {
        respond(200, "{}", Optional.of(URI.create("https://other.example:443")));
        assertUnknown();
    }

    @Test
    void anErrorStatusAfterSendIsOutcomeUnknown() {
        respond(503, "private", Optional.empty());
        assertUnknown();
    }

    @Test
    void lostResponseIsOutcomeUnknownWithoutResend() {
        when(pairedClient.sendAsync(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
            .thenReturn(CompletableFuture.failedFuture(new IOException("private diagnostic")));
        assertUnknown();
    }

    private NativeWorkerReleaseBinding.Attempt attempt(String payload) {
        var target = fixture.target();
        return fixture.binding().pin(new TransitionCommandEnvelope(target.tenantId(), "execution", target.pipelineId(),
            target.contractVersion(), target.releaseVersion(), 0, -1, 0, ExecutionResultShape.SINGLE, 1,
            "execution:0:0", "trace", INPUT_TYPE, target.profiles().payloadEncoding(), payload));
    }

    private AtomicReference<HttpRequest> respond(int status, String body, Optional<URI> responseUri) {
        var captured = new AtomicReference<HttpRequest>();
        when(pairedClient.sendAsync(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
            .thenAnswer(invocation -> {
                HttpRequest request = invocation.getArgument(0);
                captured.set(request);
                HttpResponse.BodyHandler<String> handler = invocation.getArgument(1);
                HttpResponse.BodySubscriber<String> subscriber = handler.apply(mock(HttpResponse.ResponseInfo.class));
                var subscription = mock(Flow.Subscription.class);
                subscriber.onSubscribe(subscription);
                subscriber.onNext(List.of(ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8))));
                subscriber.onComplete();
                return subscriber.getBody().toCompletableFuture().thenApply(received -> {
                    @SuppressWarnings("unchecked")
                    HttpResponse<String> response = mock(HttpResponse.class);
                    when(response.statusCode()).thenReturn(status);
                    when(response.uri()).thenReturn(responseUri.orElseGet(request::uri));
                    when(response.previousResponse()).thenReturn(Optional.empty());
                    when(response.body()).thenReturn(received);
                    return response;
                });
            });
        return captured;
    }

    private void assertUnknown() {
        var failure = assertThrows(RemoteTransitionOutcomeUnknownException.class,
            () -> worker.executeTransition(attempt, OWNER, store).await().atMost(WAIT));
        assertFalse(failure.getCause().getMessage().contains("private"));
        verify(pairedClient).sendAsync(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any());
        verify(store).settlePairedUnknown(any());
        verifyNoInteractions(legacyClient);
    }

    private CompletableFuture<byte[]> readBody(HttpRequest.BodyPublisher publisher) {
        var result = new CompletableFuture<byte[]>();
        publisher.subscribe(new Flow.Subscriber<>() {
            private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
                byte[] chunk = new byte[item.remaining()];
                item.get(chunk);
                bytes.writeBytes(chunk);
            }

            @Override
            public void onError(Throwable error) {
                result.completeExceptionally(error);
            }

            @Override
            public void onComplete() {
                result.complete(bytes.toByteArray());
            }
        });
        return result;
    }
}

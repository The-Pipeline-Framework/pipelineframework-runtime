package org.pipelineframework.orchestrator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniSubscriber;
import io.smallrye.mutiny.subscription.UniSubscription;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.invocation.TransportBoundaryInvocation;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.worker.PipelineWorkerCapability;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RestPipelineTransitionWorkerContextTest {
    @Test
    void timeoutSignalUsesSubscriberLoaderAndRestoresTransportThread() throws Exception {
        var parent = getClass().getClassLoader();
        try (var assembly = new URLClassLoader(new URL[0], parent);
             var subscriber = new URLClassLoader(new URL[0], parent);
             var transport = new URLClassLoader(new URL[0], parent)) {
            var response = new CompletableFuture<HttpResponse<String>>();
            var requested = new CountDownLatch(1);
            var sends = new AtomicInteger();
            var client = mock(HttpClient.class);
            when(client.<String>sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenAnswer(invocation -> {
                    sends.incrementAndGet();
                    requested.countDown();
                    return response;
                });
            var worker = worker(client);
            try {
                var result = underLoader(assembly, () -> worker.executeTransition(
                    TransitionEnvelopeFixtures.envelope(new JsonTransitionPayloadCodec())));
                var observed = subscribe(result, subscriber);
                assertTrue(requested.await(5, TimeUnit.SECONDS));
                var timeout = new HttpTimeoutException("controlled transport timeout");
                var after = new AtomicReference<ClassLoader>();
                var completion = new Thread(() -> {
                    response.completeExceptionally(timeout);
                    after.set(Thread.currentThread().getContextClassLoader());
                }, "controlled-http-completion");
                completion.setContextClassLoader(transport);
                completion.start();
                completion.join(5000);
                assertFalse(completion.isAlive());
                var signal = observed.await();
                var failure = assertInstanceOf(RemoteTransitionOutcomeUnknownException.class,
                    signal.failure().orElseThrow());
                assertSame(timeout, failure.getCause());
                assertEquals(1, sends.get());
                assertEquals(1, observed.signals.get());
                assertSame(transport, after.get());
                assertSame(subscriber, signal.loader(), "REST failure must use the subscription loader, not the transport loader");
            } finally {
                worker.close();
            }
        }
    }

    @Test
    void transportFailuresCaptureEachSubscriberNotAssemblyAndPreserveOriginalCause() throws Exception {
        var parent = getClass().getClassLoader();
        try (var assembly = new URLClassLoader(new URL[0], parent);
             var first = new URLClassLoader(new URL[0], parent);
             var second = new URLClassLoader(new URL[0], parent);
             var transport = new URLClassLoader(new URL[0], parent)) {
            for (boolean execute : List.of(false, true)) {
                for (boolean registered : List.of(false, true)) {
                    var boundary = new ControlledHttp();
                    var worker = worker(boundary.client);
                    try {
                        Uni<?> result = underLoader(assembly, () -> operation(worker, execute, registered));
                        int expectedSends = 0;
                        for (Throwable cause : List.of(new IOException("controlled connection failure"),
                                new HttpTimeoutException("controlled transport timeout"))) {
                            for (ClassLoader subscriber : List.of(first, second)) {
                                var observed = subscribe(result, subscriber);
                                var pending = boundary.request();
                                assertEquals(execute ? "POST" : "GET", pending.request().method());
                                completeUnder(transport, () -> pending.response().completeExceptionally(cause));
                                var signal = observed.await();
                                assertSame(subscriber, signal.loader());
                                var failure = signal.failure().orElseThrow();
                                if (execute && cause instanceof HttpTimeoutException) {
                                    assertInstanceOf(RemoteTransitionOutcomeUnknownException.class, failure);
                                    assertSame(cause, failure.getCause());
                                } else {
                                    assertSame(cause, failure);
                                }
                                assertEquals(++expectedSends, boundary.sends.get());
                                assertEquals(1, observed.signals.get());
                            }
                        }
                    } finally {
                        worker.close();
                    }
                }
            }
        }
    }

    @Test
    void realHttpSuccessAndDecodeFailuresUseEachSubscriptionLoader() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        var gate = new AtomicReference<HttpGate>();
        var calls = new AtomicInteger();
        var signatures = new AtomicInteger();
        server.createContext("/pipeline/worker/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            calls.incrementAndGet();
            if (exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.SIGNATURE_HEADER) != null) {
                signatures.incrementAndGet();
            }
            var current = gate.get();
            current.requested.countDown();
            try {
                if (!current.release.await(5, TimeUnit.SECONDS)) throw new IOException("HTTP fixture response was not released");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            exchange.sendResponseHeaders(200, current.body.length);
            try (var body = exchange.getResponseBody()) {
                body.write(current.body);
            }
        });
        server.start();
        var client = HttpClient.newHttpClient();
        var worker = worker(client);
        URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort());
        when(worker.orchestratorConfig.workerRest().baseUrl()).thenReturn(Optional.of(endpoint.toString()));
        when(worker.orchestratorConfig.workerRest().requestTimeout()).thenReturn(Duration.ofSeconds(10));
        var parent = getClass().getClassLoader();
        try (var assembly = new URLClassLoader(new URL[0], parent);
             var first = new URLClassLoader(new URL[0], parent);
             var second = new URLClassLoader(new URL[0], parent)) {
            int expectedCalls = 0;
            for (boolean execute : List.of(false, true)) {
                byte[] valid = execute
                    ? PipelineJson.mapper().writeValueAsBytes(TransitionResultEnvelope.completed(
                        new JsonTransitionPayloadCodec(), List.of("ok")).toWireResult())
                    : PipelineJson.mapper().writeValueAsBytes(new PipelineWorkerCapability(
                        PipelineWorkerCapability.PROTOCOL_VERSION, "rest", "pipeline", "contract", "release",
                        "app", "sha256:artifact", List.of("application/tpf-transition-envelope+json"), List.of("rest")));
                var warm = new HttpGate(valid);
                warm.release.countDown();
                gate.set(warm);
                operation(worker, execute, false).await().atMost(Duration.ofSeconds(5));
                expectedCalls++;
                for (boolean malformed : List.of(false, true)) {
                    for (boolean registered : List.of(false, true)) {
                        Uni<?> result = underLoader(assembly, () -> operation(worker, execute, registered, endpoint));
                        for (ClassLoader subscriber : List.of(first, second)) {
                            var request = new HttpGate(malformed ? "{not-json".getBytes(StandardCharsets.UTF_8) : valid);
                            gate.set(request);
                            var observed = subscribe(result, subscriber);
                            assertTrue(request.requested.await(5, TimeUnit.SECONDS));
                            request.release.countDown();
                            var signal = observed.await();
                            assertSame(subscriber, signal.loader());
                            if (malformed) {
                                assertInstanceOf(TransitionWorkerFailureException.class, signal.failure().orElseThrow());
                            } else if (execute) {
                                var envelope = assertInstanceOf(TransitionResultEnvelope.class, signal.item().orElseThrow());
                                assertEquals(TransitionWorkerOutcome.COMPLETED, envelope.outcome());
                                assertEquals(List.of("ok"), envelope.decodeOutputItems(new JsonTransitionPayloadCodec()));
                            } else {
                                var capability = assertInstanceOf(PipelineWorkerCapability.class, signal.item().orElseThrow());
                                assertEquals("pipeline", capability.pipelineId());
                                assertEquals("release", capability.releaseVersion());
                            }
                            assertEquals(++expectedCalls, calls.get());
                            assertEquals(expectedCalls, signatures.get());
                            assertEquals(1, observed.signals.get());
                        }
                    }
                }
            }
        } finally {
            worker.close();
            client.close();
            server.stop(0);
        }
    }

    @Test
    void capturesSubscriberBeforeAsynchronousInvocationBoundary() throws Exception {
        var parent = getClass().getClassLoader();
        try (var subscriber = new URLClassLoader(new URL[0], parent);
             var transport = new URLClassLoader(new URL[0], parent)) {
            var boundary = new ControlledHttp();
            var worker = worker(boundary.client);
            var runtime = new DelayedInvocationRuntime();
            worker.invocationRuntime = runtime;
            try {
                var observed = subscribe(operation(worker, true, false), subscriber);
                assertEquals(1, runtime.entries.get());
                assertEquals(0, boundary.sends.get());
                completeUnder(transport, () -> runtime.ready.complete("ready"));
                var pending = boundary.request();
                var cause = new IOException("controlled connection failure");
                completeUnder(transport, () -> pending.response().completeExceptionally(cause));
                var signal = observed.await();
                assertSame(subscriber, signal.loader());
                assertSame(cause, signal.failure().orElseThrow());
                assertEquals(1, boundary.sends.get());
                assertEquals(1, runtime.entries.get());
                assertEquals(1, runtime.terminations.get());
            } finally {
                worker.close();
            }
        }
    }

    @Test
    void cancellationSuppressesLateSignalAndRetainsOneSend() throws Exception {
        var parent = getClass().getClassLoader();
        try (var subscriber = new URLClassLoader(new URL[0], parent);
             var transport = new URLClassLoader(new URL[0], parent)) {
            var boundary = new ControlledHttp();
            var worker = worker(boundary.client);
            try {
                var observed = subscribe(operation(worker, true, false), subscriber);
                var pending = boundary.request();
                observed.subscription.get().cancel();
                completeUnder(transport, () -> pending.response().completeExceptionally(new IOException("late completion")));
                assertEquals(0, observed.signals.get());
                assertEquals(1, boundary.sends.get());
            } finally {
                worker.close();
            }
        }
    }

    @Test
    void restoresTransportLoaderWhenSubscriberThrows() throws Exception {
        var parent = getClass().getClassLoader();
        try (var subscriber = new URLClassLoader(new URL[0], parent);
             var transport = new URLClassLoader(new URL[0], parent)) {
            var boundary = new ControlledHttp();
            var worker = worker(boundary.client);
            try {
                var observed = new Observation<Object>();
                observed.throwAfterSignal = true;
                underLoader(subscriber, () -> operation(worker, false, false).subscribe().withSubscriber(observed));
                var pending = boundary.request();
                completeUnder(transport, () -> pending.response().completeExceptionally(new IOException("controlled failure")));
                assertSame(subscriber, observed.await().loader());
                assertEquals(1, observed.signals.get());
                assertEquals(1, boundary.sends.get());
            } finally {
                worker.close();
            }
        }
    }

    @Test
    void missingInvocationRuntimeStillFailsAtAssemblyBeforeAnyRequest() {
        var client = mock(HttpClient.class);
        var worker = new RestPipelineTransitionWorker(client);
        try {
            var command = TransitionEnvelopeFixtures.envelope(new JsonTransitionPayloadCodec());
            assertThrows(IllegalStateException.class, () -> worker.executeTransition(command));
            assertThrows(IllegalStateException.class, () -> worker.executeTransition(command, URI.create("http://localhost:1"), "fixture:secret"));
            verifyNoInteractions(client);
        } finally {
            worker.close();
        }
    }

    private Uni<?> operation(RestPipelineTransitionWorker worker, boolean execute, boolean registered) {
        return operation(worker, execute, registered, URI.create("http://localhost:1"));
    }

    private Uni<?> operation(RestPipelineTransitionWorker worker, boolean execute, boolean registered, URI endpoint) {
        if (execute) {
            var command = TransitionEnvelopeFixtures.envelope(new JsonTransitionPayloadCodec());
            return registered ? worker.executeTransition(command, endpoint, "fixture:secret") : worker.executeTransition(command);
        }
        return registered ? worker.capabilities(endpoint, "fixture:secret") : worker.capabilities();
    }

    private static void completeUnder(ClassLoader loader, Runnable action) throws InterruptedException {
        var restored = new AtomicReference<ClassLoader>();
        var failure = new AtomicReference<Throwable>();
        var thread = new Thread(() -> {
            try {
                action.run();
            } catch (Throwable thrown) {
                failure.set(thrown);
            } finally {
                restored.set(Thread.currentThread().getContextClassLoader());
            }
        }, "controlled-http-completion");
        thread.setContextClassLoader(loader);
        thread.start();
        thread.join(5000);
        assertFalse(thread.isAlive());
        assertNull(failure.get());
        assertSame(loader, restored.get());
    }

    private record Pending(HttpRequest request, CompletableFuture<HttpResponse<String>> response) {}

    private static final class ControlledHttp {
        private final HttpClient client = mock(HttpClient.class);
        private final LinkedBlockingQueue<Pending> requests = new LinkedBlockingQueue<>();
        private final AtomicInteger sends = new AtomicInteger();
        private ControlledHttp() {
            when(client.<String>sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenAnswer(invocation -> {
                    var response = new CompletableFuture<HttpResponse<String>>();
                    sends.incrementAndGet();
                    requests.add(new Pending(invocation.getArgument(0), response));
                    return response;
                });
        }
        private Pending request() throws InterruptedException {
            return Optional.ofNullable(requests.poll(5, TimeUnit.SECONDS)).orElseThrow();
        }
    }

    private static final class HttpGate {
        private final CountDownLatch requested = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final byte[] body;
        private HttpGate(byte[] body) { this.body = body; }
    }

    private static final class DelayedInvocationRuntime extends PipelineInvocationRuntime {
        private final CompletableFuture<String> ready = new CompletableFuture<>();
        private final AtomicInteger entries = new AtomicInteger();
        private final AtomicInteger terminations = new AtomicInteger();
        @Override public <T> Uni<T> invokeTransportUni(TransportBoundaryInvocation boundary, Supplier<Uni<T>> supplier) {
            entries.incrementAndGet();
            return Uni.createFrom().completionStage(ready)
                .chain(ignored -> super.invokeTransportUni(boundary, supplier))
                .onTermination().invoke(() -> terminations.incrementAndGet());
        }
    }

    private RestPipelineTransitionWorker worker(HttpClient client) {
        var worker = new RestPipelineTransitionWorker(client, new PipelineInvocationRuntime());
        var config = mock(PipelineOrchestratorConfig.class);
        var rest = mock(PipelineOrchestratorConfig.RestWorkerConfig.class);
        when(config.workerRest()).thenReturn(rest);
        when(rest.baseUrl()).thenReturn(Optional.of("http://localhost:1"));
        when(rest.path()).thenReturn("/pipeline/worker/transitions/execute");
        when(rest.capabilitiesPath()).thenReturn("/pipeline/worker/capabilities");
        when(rest.requestTimeout()).thenReturn(Duration.ofSeconds(1));
        when(rest.sharedSecret()).thenReturn(Optional.of("fixture-secret"));
        when(rest.sharedSecretRef()).thenReturn(Optional.empty());
        worker.orchestratorConfig = config;
        worker.secretResolver = reference -> "fixture-secret";
        return worker;
    }

    private static <T> T underLoader(ClassLoader loader, Supplier<T> action) {
        var thread = Thread.currentThread();
        var previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(loader);
            return action.get();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    private static <T> Observation<T> subscribe(Uni<T> uni, ClassLoader loader) {
        var observed = new Observation<T>();
        underLoader(loader, () -> uni.subscribe().withSubscriber(observed));
        return observed;
    }

    private record Signal<T>(Optional<T> item, Optional<Throwable> failure, ClassLoader loader) {}

    private static final class Observation<T> implements UniSubscriber<T> {
        private final CountDownLatch done = new CountDownLatch(1);
        private final AtomicReference<Signal<T>> signal = new AtomicReference<>();
        private final AtomicInteger signals = new AtomicInteger();
        private final AtomicReference<UniSubscription> subscription = new AtomicReference<>();
        private boolean throwAfterSignal;

        @Override public void onSubscribe(UniSubscription value) { subscription.set(value); }
        @Override public void onItem(T item) {
            signals.incrementAndGet();
            signal.set(new Signal<>(Optional.of(item), Optional.empty(), Thread.currentThread().getContextClassLoader()));
            done.countDown();
            if (throwAfterSignal) throw new IllegalStateException("controlled subscriber failure");
        }
        @Override public void onFailure(Throwable failure) {
            signals.incrementAndGet();
            signal.set(new Signal<>(Optional.empty(), Optional.of(failure), Thread.currentThread().getContextClassLoader()));
            done.countDown();
            if (throwAfterSignal) throw new IllegalStateException("controlled subscriber failure");
        }
        private Signal<T> await() throws InterruptedException {
            assertTrue(done.await(5, TimeUnit.SECONDS));
            return signal.get();
        }
    }
}

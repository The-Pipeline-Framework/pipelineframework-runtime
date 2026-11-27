/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.runtime.spring.polyglot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionPayloadEncoding;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;
import org.pipelineframework.runtime.core.PipelineRunnerCore;
import org.pipelineframework.runtime.core.RuntimeAdapters;
import org.pipelineframework.runtime.core.TpfExecutionContext;
import org.pipelineframework.runtime.spring.SpringPipelineRunner;
import org.pipelineframework.runtime.spring.SpringRuntimeAdaptersAutoConfiguration;
import org.pipelineframework.runtime.spring.SpringRuntimeWorkEvent;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

/**
 * End-to-end qualification of Spring-hosted polyglot {@link SpringCanonicalFunctionHostStep}
 * execution, durable Await resume, and cross-pipeline checkpoint interoperability.
 */
class SpringPolyglotParityTest {

    private static final String SHARED_SECRET = "spring-polyglot-hmac-secret";
    private static final String EXACT_DECIMAL = "99999999999999999999.123456789";
    private static final String EXACT_INT64 = "9007199254740993";

    private HttpServer server;
    private ExecutorService serverPool;

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(
            SpringPolyglotAutoConfiguration.class,
            SpringRuntimeAdaptersAutoConfiguration.class));

    @AfterEach
    void tearDown() {
        RuntimeAdapters.resetForTests();
        if (server != null) {
            server.stop(0);
            server = null;
        }
        if (serverPool != null) {
            serverPool.shutdownNow();
            serverPool = null;
        }
    }

    @Test
    void autoWiredSpringPipelineRunnerExecutesPolyglotStepsPreservingPrecisionContextAndDispatchIntent() throws Exception {
        AtomicReference<String> seenVersionTag = new AtomicReference<>();
        AtomicInteger validateCalls = new AtomicInteger();

        URI host = startServer(exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String path = exchange.getRequestURI().getPath();
            if (!verifyHmac(exchange, path, body)) {
                sendJson(exchange, 401, "{\"status\":\"ERROR\",\"code\":\"UNAUTHORIZED\"}");
                return;
            }
            JsonNode req = SpringPolyglotJson.exactMapper().readTree(body);
            if ("/tpf/functions/validate-order".equals(path)) {
                validateCalls.incrementAndGet();
                seenVersionTag.set(req.path("context").path("versionTag").asText(""));
                JsonNode payload = req.path("payload");
                String out = "{\"status\":\"OK\",\"outputTypeId\":\"EnrichedOrder\",\"payload\":{"
                    + "\"orderId\":" + payload.path("orderId").toString()
                    + ",\"amount\":" + payload.path("amount").toString()
                    + ",\"seq\":" + payload.path("seq").toString()
                    + ",\"validated\":true}}";
                sendJson(exchange, 200, out);
                return;
            }
            if ("/tpf/functions/issue-receipt".equals(path)) {
                JsonNode payload = req.path("payload");
                String out = "{\"status\":\"OK\",\"outputTypeId\":\"Receipt\",\"payload\":{"
                    + "\"receiptFor\":" + payload.path("orderId").toString()
                    + ",\"settledAmount\":" + payload.path("amount").toString()
                    + ",\"seq\":" + payload.path("seq").toString()
                    + ",\"validated\":" + payload.path("validated").asBoolean() + "}}";
                sendJson(exchange, 200, out);
                return;
            }
            sendJson(exchange, 404, "{\"status\":\"ERROR\",\"code\":\"NOT_FOUND\"}");
        });

        contextRunner
            .withPropertyValues(
                "pipeline.polyglot.enabled=true",
                "pipeline.polyglot.function-host-uri=" + host,
                "pipeline.polyglot.shared-secret=" + SHARED_SECRET,
                "pipeline.polyglot.allow-loopback-http=true",
                "pipeline.polyglot.contract-resource-path=polyglot/spring-unary-contract.json")
            .run(context -> {
                assertNullStartupFailure(context);
                SpringPipelineRunner runner = context.getBean(SpringPipelineRunner.class);
                SpringForeignDispatchIntentStore.InMemory intentStore =
                    assertInstanceOf(SpringForeignDispatchIntentStore.InMemory.class,
                        context.getBean(SpringForeignDispatchIntentStore.class));

                assertEquals(2, runner.stepCount());
                SpringCanonicalPayload input = SpringCanonicalPayload.ofJson(
                    "Order", "fp-Order", "catalog-fp-spring-1",
                    "{\"orderId\":\"ord-1\",\"amount\":" + EXACT_DECIMAL + ",\"seq\":" + EXACT_INT64 + "}");

                SpringTransitionDispatchScope scope = new SpringTransitionDispatchScope(
                    "tenant-s", "exec-1", 1L, 1, "tx-1");
                SpringCanonicalPayload output;
                try (var ctxScope = TpfExecutionContext.withHeaders("v-spring-42", "LIVE", "DEFAULT");
                     var dispatchScope = SpringTransitionDispatchScope.open(scope)) {
                    output = (SpringCanonicalPayload) runner.run(input).toCompletableFuture().get(5, TimeUnit.SECONDS);
                }

                assertEquals("Receipt", output.canonicalTypeId());
                assertEquals("fp-Receipt", output.typeExpressionFingerprint());
                assertEquals("v-spring-42", seenVersionTag.get());
                assertTrue(output.payloadJson().contains(EXACT_DECIMAL),
                    "29-digit exact decimal must survive Spring polyglot hops: " + output.payloadJson());
                assertTrue(output.payloadJson().contains(EXACT_INT64),
                    "int64 > 2^53 must survive Spring polyglot hops: " + output.payloadJson());
                assertEquals(2, intentStore.reservationCount());
                assertTrue(TpfExecutionContext.versionTag().isEmpty());

                // Replaying the same transitionKey must fail closed BEFORE hitting the foreign host
                int callsBeforeReplay = validateCalls.get();
                try (var dispatchScope = SpringTransitionDispatchScope.open(scope)) {
                    CompletionException failure = assertThrows(CompletionException.class,
                        () -> runner.run(input).toCompletableFuture().join());
                    SpringForeignStepException.OutcomeUnknown unknown =
                        assertInstanceOf(SpringForeignStepException.OutcomeUnknown.class, failure.getCause());
                    assertTrue(unknown.isOutcomeUnknown());
                    assertFalse(unknown.isRetryable());
                }
                assertEquals(callsBeforeReplay, validateCalls.get(),
                    "Duplicate pre-send intent must suppress automatic wire resend");
            });
    }

    @Test
    void enforcesHttpsByDefaultAndFailsClosedWhenSharedSecretMissing() {
        contextRunner
            .withPropertyValues(
                "pipeline.polyglot.enabled=true",
                "pipeline.polyglot.function-host-uri=http://127.0.0.1:8080",
                "pipeline.polyglot.shared-secret=" + SHARED_SECRET,
                "pipeline.polyglot.allow-loopback-http=false",
                "pipeline.polyglot.contract-resource-path=polyglot/spring-unary-contract.json")
            .run(context -> {
                assertNotNull(context.getStartupFailure());
                assertTrue(rootMessage(context.getStartupFailure()).contains("HTTPS"));
            });

        contextRunner
            .withPropertyValues(
                "pipeline.polyglot.enabled=true",
                "pipeline.polyglot.function-host-uri=https://node.example.com",
                "pipeline.polyglot.contract-resource-path=polyglot/spring-unary-contract.json")
            .run(context -> {
                assertNotNull(context.getStartupFailure());
                assertTrue(rootMessage(context.getStartupFailure()).contains("shared-secret"));
            });
    }

    @Test
    void classifiesPortableBusinessRejectionTransientAuthAndUnknownFailures() throws Exception {
        AtomicReference<String> mode = new AtomicReference<>("BUSINESS");
        URI host = startServer(exchange -> {
            exchange.getRequestBody().readAllBytes();
            switch (mode.get()) {
                case "BUSINESS" -> sendJson(exchange, 200,
                    "{\"status\":\"ERROR\",\"disposition\":\"BUSINESS_REJECTION\",\"code\":\"INVALID_ORDER\",\"message\":\"Negative total\"}");
                case "TRANSIENT" -> sendJson(exchange, 200,
                    "{\"status\":\"ERROR\",\"disposition\":\"TRANSIENT\",\"code\":\"RATE_LIMITED\",\"message\":\"Retry later\"}");
                case "AUTH" -> sendJson(exchange, 401,
                    "{\"status\":\"ERROR\",\"code\":\"UNAUTHORIZED\"}");
                default -> sendJson(exchange, 503, "upstream down");
            }
        });

        SpringCanonicalFunctionHostStep step = new SpringCanonicalFunctionHostStep(
            new SpringCanonicalFunctionHostStep.Spec(
                "validate-order", 0, "ONE_TO_ONE",
                "Order", "fp-Order", "EnrichedOrder", "fp-EnrichedOrder", "catalog-fp-spring-1",
                host, SHARED_SECRET, Duration.ofSeconds(5), Duration.ofSeconds(5),
                SpringForeignDispatchIntentStore.noop(), SpringCanonicalFunctionHostStep.DEFAULT_MAX_RESPONSE_BYTES),
            true);
        SpringCanonicalPayload input = SpringCanonicalPayload.ofJson(
            "Order", "fp-Order", "catalog-fp-spring-1", "{\"orderId\":\"o1\"}");

        mode.set("BUSINESS");
        CompletionException bizErr = assertThrows(CompletionException.class,
            () -> step.apply(input).toCompletableFuture().join());
        SpringForeignStepException.BusinessRejection biz =
            assertInstanceOf(SpringForeignStepException.BusinessRejection.class, bizErr.getCause());
        assertFalse(biz.isRetryable());
        assertEquals(SpringForeignStepException.Disposition.BUSINESS_REJECTION, biz.disposition());

        mode.set("TRANSIENT");
        CompletionException transientErr = assertThrows(CompletionException.class,
            () -> step.apply(input).toCompletableFuture().join());
        SpringForeignStepException.TransientFailure tr =
            assertInstanceOf(SpringForeignStepException.TransientFailure.class, transientErr.getCause());
        assertTrue(tr.isRetryable());

        mode.set("AUTH");
        CompletionException authErr = assertThrows(CompletionException.class,
            () -> step.apply(input).toCompletableFuture().join());
        SpringForeignStepException.AuthenticationFailure auth =
            assertInstanceOf(SpringForeignStepException.AuthenticationFailure.class, authErr.getCause());
        assertFalse(auth.isRetryable());
        assertEquals(401, auth.statusCode());

        mode.set("HTTP503");
        CompletionException unknownErr = assertThrows(CompletionException.class,
            () -> step.apply(input).toCompletableFuture().join());
        assertInstanceOf(SpringForeignStepException.OutcomeUnknown.class, unknownErr.getCause());
    }

    @Test
    void supportsOneToManyManyToOneAndTrueInterleavedFullDuplexManyToManyUnderSpring() throws Exception {
        URI host = startServer(exchange -> {
            String path = exchange.getRequestURI().getPath();
            if ("/tpf/functions/split-batch/stream".equals(path)) {
                byte[] body = exchange.getRequestBody().readAllBytes();
                if (!verifyHmac(exchange, path, body)) {
                    sendJson(exchange, 401, "{\"status\":\"ERROR\",\"code\":\"UNAUTHORIZED\"}");
                    return;
                }
                exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
                exchange.sendResponseHeaders(200, 0);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(("{\"event\":\"item\",\"outputTypeId\":\"Frame\",\"payload\":{\"seq\":1,\"amount\":" + EXACT_DECIMAL + "}}\n")
                        .getBytes(StandardCharsets.UTF_8));
                    out.write("{\"event\":\"item\",\"outputTypeId\":\"Frame\",\"payload\":{\"seq\":2,\"amount\":42.50}}\n"
                        .getBytes(StandardCharsets.UTF_8));
                    out.write("{\"event\":\"complete\"}\n".getBytes(StandardCharsets.UTF_8));
                }
                return;
            }
            if ("/tpf/functions/transform-stream/duplex".equals(path)) {
                if (!verifyHmac(exchange, path,
                    SpringCanonicalFunctionHostStep.STREAMING_BODY_DIGEST_SENTINEL.getBytes(StandardCharsets.UTF_8))) {
                    sendJson(exchange, 401, "{\"status\":\"ERROR\",\"code\":\"UNAUTHORIZED\"}");
                    return;
                }
                exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
                exchange.sendResponseHeaders(200, 0);
                try (BufferedReader reader = new BufferedReader(
                         new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));
                     OutputStream out = exchange.getResponseBody()) {
                    String line;
                    int count = 0;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) continue;
                        JsonNode frame = SpringPolyglotJson.exactMapper().readTree(line);
                        if ("complete".equals(frame.path("event").asText())) break;
                        if ("item".equals(frame.path("event").asText())) {
                            count++;
                            JsonNode p = frame.path("payload");
                            String reply = "{\"event\":\"item\",\"outputTypeId\":\"Frame\",\"payload\":{"
                                + "\"seq\":" + (p.path("seq").asLong() + 10)
                                + ",\"amount\":" + p.path("amount").toString() + "}}\n";
                            out.write(reply.getBytes(StandardCharsets.UTF_8));
                            out.flush();
                        }
                    }
                    out.write(("{\"event\":\"complete\",\"itemCount\":" + count + "}\n").getBytes(StandardCharsets.UTF_8));
                    out.flush();
                }
                return;
            }
            if ("/tpf/functions/collect-summary/collect".equals(path)) {
                if (!verifyHmac(exchange, path,
                    SpringCanonicalFunctionHostStep.STREAMING_BODY_DIGEST_SENTINEL.getBytes(StandardCharsets.UTF_8))) {
                    sendJson(exchange, 401, "{\"status\":\"ERROR\",\"code\":\"UNAUTHORIZED\"}");
                    return;
                }
                int count = 0;
                try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) continue;
                        JsonNode frame = SpringPolyglotJson.exactMapper().readTree(line);
                        if ("item".equals(frame.path("event").asText())) count++;
                    }
                }
                sendJson(exchange, 200,
                    "{\"status\":\"OK\",\"outputTypeId\":\"Summary\",\"payload\":{\"count\":" + count + "}}");
                return;
            }
            sendJson(exchange, 404, "{\"status\":\"ERROR\",\"code\":\"NOT_FOUND\"}");
        });

        contextRunner
            .withPropertyValues(
                "pipeline.polyglot.enabled=true",
                "pipeline.polyglot.function-host-uri=" + host,
                "pipeline.polyglot.shared-secret=" + SHARED_SECRET,
                "pipeline.polyglot.allow-loopback-http=true",
                "pipeline.polyglot.contract-resource-path=polyglot/spring-streaming-contract.json")
            .run(context -> {
                assertNullStartupFailure(context);
                SpringPolyglotAutoConfiguration.SpringPolyglotStepRegistry registry =
                    context.getBean(SpringPolyglotAutoConfiguration.SpringPolyglotStepRegistry.class);
                assertEquals(3, registry.orderedSteps().size());
                assertEquals(0, context.getBean(SpringPipelineRunner.class).stepCount());
            });

        SpringPolyglotProperties props = new SpringPolyglotProperties();
        props.setEnabled(true);
        props.setFunctionHostUri(host.toString());
        props.setSharedSecret(SHARED_SECRET);
        props.setAllowLoopbackHttp(true);
        props.setContractResourcePath("polyglot/spring-streaming-contract.json");

        SpringPolyglotAutoConfiguration.SpringPolyglotStepRegistry registry =
            SpringPolyglotAutoConfiguration.buildRegistry(
                props, SpringForeignDispatchIntentStore.noop(), getClass().getClassLoader());
        assertEquals(3, registry.orderedSteps().size());

        SpringCanonicalFunctionHostStep splitStep = registry.findByStepId("split-batch").orElseThrow();
        SpringCanonicalFunctionHostStep duplexStep = registry.findByStepId("transform-stream").orElseThrow();
        SpringCanonicalFunctionHostStep collectStep = registry.findByStepId("collect-summary").orElseThrow();

        // 1) ONE_TO_MANY
        List<SpringCanonicalPayload> splitItems = collectPublisher(splitStep.applyOneToMany(
            SpringCanonicalPayload.ofJson("Batch", "fp-Batch", "catalog-fp-stream-1", "{\"id\":\"b1\"}")));
        assertEquals(2, splitItems.size());
        assertTrue(splitItems.get(0).payloadJson().contains(EXACT_DECIMAL));

        // 2) True interleaved full-duplex MANY_TO_MANY: output #1 MUST arrive BEFORE input #2 is sent!
        CountDownLatch firstOutputReceived = new CountDownLatch(1);
        AtomicBoolean input2SentBeforeOutput1 = new AtomicBoolean(true);
        Flow.Publisher<SpringCanonicalPayload> controlledUpstream = sub -> sub.onSubscribe(new Flow.Subscription() {
            private final AtomicInteger step = new AtomicInteger(0);
            private final AtomicBoolean cancelled = new AtomicBoolean();

            @Override
            public void request(long n) {
                if (cancelled.get()) return;
                int s = step.getAndIncrement();
                if (s == 0) {
                    sub.onNext(splitItems.get(0));
                } else if (s == 1) {
                    Thread.startVirtualThread(() -> {
                        try {
                            boolean received = firstOutputReceived.await(3, TimeUnit.SECONDS);
                            input2SentBeforeOutput1.set(!received);
                            if (!cancelled.get()) {
                                sub.onNext(splitItems.get(1));
                                sub.onComplete();
                            }
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
            }

            @Override
            public void cancel() {
                cancelled.set(true);
            }
        });

        List<SpringCanonicalPayload> duplexOut = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> duplexDone = new CompletableFuture<>();
        duplexStep.applyManyToMany(controlledUpstream).subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(SpringCanonicalPayload item) {
                duplexOut.add(item);
                firstOutputReceived.countDown();
            }

            @Override
            public void onError(Throwable t) {
                duplexDone.completeExceptionally(t);
            }

            @Override
            public void onComplete() {
                duplexDone.complete(null);
            }
        });
        duplexDone.get(5, TimeUnit.SECONDS);
        assertFalse(input2SentBeforeOutput1.get(), "Output #1 must arrive BEFORE input #2 is emitted");
        assertEquals(2, duplexOut.size());
        assertTrue(duplexOut.get(0).payloadJson().contains("\"seq\":11"));
        assertTrue(duplexOut.get(0).payloadJson().contains(EXACT_DECIMAL));

        // 3) MANY_TO_ONE
        SpringCanonicalPayload summary = collectStep.applyManyToOne(publisherOf(duplexOut))
            .toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals("Summary", summary.canonicalTypeId());
        assertTrue(summary.payloadJson().contains("\"count\":2"));
    }

    @Test
    void durableAwaitSuspendAndIdempotentResumeUnderSpringRestoresContextAndSuppressesDuplicates() throws Exception {
        AtomicInteger postAwaitCalls = new AtomicInteger();
        AtomicReference<String> resumedVersionTag = new AtomicReference<>();

        URI host = startServer(exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String path = exchange.getRequestURI().getPath();
            if (!verifyHmac(exchange, path, body)) {
                sendJson(exchange, 401, "{\"status\":\"ERROR\",\"code\":\"UNAUTHORIZED\"}");
                return;
            }
            JsonNode req = SpringPolyglotJson.exactMapper().readTree(body);
            if ("/tpf/functions/prepare-payment".equals(path)) {
                sendJson(exchange, 200,
                    "{\"status\":\"OK\",\"outputTypeId\":\"PreparedPayment\",\"payload\":{\"id\":\"pay-1\",\"state\":\"AWAITING_WEBHOOK\"}}");
                return;
            }
            if ("/tpf/functions/finalize-payment".equals(path)) {
                postAwaitCalls.incrementAndGet();
                resumedVersionTag.set(req.path("context").path("versionTag").asText(""));
                JsonNode p = req.path("payload");
                sendJson(exchange, 200,
                    "{\"status\":\"OK\",\"outputTypeId\":\"FinalizedPayment\",\"payload\":{"
                        + "\"id\":" + p.path("id").toString()
                        + ",\"settledAmount\":" + p.path("settledAmount").toString()
                        + ",\"finalized\":true}}");
                return;
            }
            sendJson(exchange, 404, "{\"status\":\"ERROR\",\"code\":\"NOT_FOUND\"}");
        });

        contextRunner.run(context -> {
            assertNullStartupFailure(context);
            SpringAwaitResumeCoordinator coordinator = context.getBean(SpringAwaitResumeCoordinator.class);
            PipelineRunnerCore runnerCore = context.getBean(PipelineRunnerCore.class);
            SpringForeignDispatchIntentStore intentStore = context.getBean(SpringForeignDispatchIntentStore.class);

            SpringCanonicalFunctionHostStep preStep = new SpringCanonicalFunctionHostStep(
                new SpringCanonicalFunctionHostStep.Spec(
                    "prepare-payment", 0, "ONE_TO_ONE",
                    "Order", "fp-Order", "PreparedPayment", "fp-Prepared", "cat-await-1",
                    host, SHARED_SECRET, Duration.ofSeconds(5), Duration.ofSeconds(5),
                    intentStore, SpringCanonicalFunctionHostStep.DEFAULT_MAX_RESPONSE_BYTES),
                true);
            SpringCanonicalFunctionHostStep postStep = new SpringCanonicalFunctionHostStep(
                new SpringCanonicalFunctionHostStep.Spec(
                    "finalize-payment", 2, "ONE_TO_ONE",
                    "WebhookCompletion", "fp-Completion", "FinalizedPayment", "fp-Finalized", "cat-await-1",
                    host, SHARED_SECRET, Duration.ofSeconds(5), Duration.ofSeconds(5),
                    intentStore, SpringCanonicalFunctionHostStep.DEFAULT_MAX_RESPONSE_BYTES),
                true);

            SpringPipelineRunner preRunner = new SpringPipelineRunner(runnerCore, List.of(preStep));
            SpringPipelineRunner postRunner = new SpringPipelineRunner(runnerCore, List.of(postStep));

            SpringAwaitResumeCoordinator.StartResult started;
            try (var ctx = TpfExecutionContext.withHeaders("v-await-99", "LIVE", "DEFAULT")) {
                started = coordinator.startAndSuspend(
                    "tenant-a", "exec-await-1", "await-webhook", 1, Duration.ofMinutes(5),
                    preRunner,
                    SpringCanonicalPayload.ofJson("Order", "fp-Order", "cat-await-1", "{\"id\":\"pay-1\"}"))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
            assertEquals(AwaitUnitStatus.WAITING_EXTERNAL, started.unit().status());
            assertEquals(started.unit().unitId(), started.suspension().unitId());

            SpringCanonicalPayload completion = SpringCanonicalPayload.ofJson(
                "WebhookCompletion", "fp-Completion", "cat-await-1",
                "{\"id\":\"pay-1\",\"settledAmount\":" + EXACT_DECIMAL + "}");

            // Resume outside original TpfExecutionContext scope — coordinator must restore v-await-99!
            SpringAwaitResumeCoordinator.ResumeResult resumed = coordinator.admitCompletionAndResume(
                "tenant-a", started.unit().unitId(), "webhook-evt-1", completion, postRunner)
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(SpringAwaitResumeCoordinator.ResumeStatus.RESUMED, resumed.status());
            assertEquals(AwaitUnitStatus.COMPLETED, resumed.unit().orElseThrow().status());
            assertEquals("v-await-99", resumedVersionTag.get());
            assertTrue(resumed.resumedOutput().orElseThrow().payloadJson().contains(EXACT_DECIMAL));
            assertEquals(1, postAwaitCalls.get());

            // Duplicate completion delivery must be ignored idempotently without re-invoking foreign host
            SpringAwaitResumeCoordinator.ResumeResult dup = coordinator.admitCompletionAndResume(
                "tenant-a", started.unit().unitId(), "webhook-evt-1", completion, postRunner)
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(SpringAwaitResumeCoordinator.ResumeStatus.DUPLICATE_IGNORED, dup.status());
            assertEquals(1, postAwaitCalls.get());

            // Timed-out unit rejects late completion
            SpringAwaitResumeCoordinator.StartResult timedOutStart = coordinator.startAndSuspend(
                "tenant-a", "exec-await-2", "await-webhook", 1, Duration.ofMinutes(5),
                preRunner,
                SpringCanonicalPayload.ofJson("Order", "fp-Order", "cat-await-1", "{\"id\":\"pay-2\"}"))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(AwaitUnitStatus.TIMED_OUT,
                coordinator.timeoutUnit("tenant-a", timedOutStart.unit().unitId()).orElseThrow().status());
            SpringAwaitResumeCoordinator.ResumeResult late = coordinator.admitCompletionAndResume(
                "tenant-a", timedOutStart.unit().unitId(), "late-evt", completion, postRunner)
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(SpringAwaitResumeCoordinator.ResumeStatus.TERMINAL_REJECTED, late.status());
            assertEquals(1, postAwaitCalls.get());
        });
    }

    @Test
    void checkpointHandoffInteroperatesAcrossPipelinesWithFingerprintVerificationAndWorkDispatcher() throws Exception {
        URI host = startServer(exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String path = exchange.getRequestURI().getPath();
            if (!verifyHmac(exchange, path, body)) {
                sendJson(exchange, 401, "{\"status\":\"ERROR\",\"code\":\"UNAUTHORIZED\"}");
                return;
            }
            JsonNode req = SpringPolyglotJson.exactMapper().readTree(body);
            JsonNode p = req.path("payload");
            String tag = req.path("context").path("versionTag").asText("");
            sendJson(exchange, 200,
                "{\"status\":\"OK\",\"outputTypeId\":\"LedgerEntry\",\"payload\":{"
                    + "\"entryId\":\"led-1\""
                    + ",\"amount\":" + p.path("amount").toString()
                    + ",\"versionTag\":\"" + tag + "\"}}");
        });

        contextRunner.withUserConfiguration(CheckpointCaptureConfig.class).run(context -> {
            assertNullStartupFailure(context);
            SpringCheckpointHandoffCodec codec = context.getBean(SpringCheckpointHandoffCodec.class);
            CheckpointListener listener = context.getBean(CheckpointListener.class);
            PipelineRunnerCore runnerCore = context.getBean(PipelineRunnerCore.class);

            SpringCanonicalPayload upstreamOutput = SpringCanonicalPayload.ofJson(
                "Settlement", "fp-Settlement", "cat-handoff-1",
                "{\"settlementId\":\"set-1\",\"amount\":" + EXACT_DECIMAL + "}");
            SpringCheckpointHandoffCodec.CheckpointMetadata meta = new SpringCheckpointHandoffCodec.CheckpointMetadata(
                "tenant-c", "payments-pipeline", "ledger-pipeline", "ckpt-1", "v1", "rel-1", Optional.of("corr-7"));

            SerializedTransitionPayload published;
            try (var ctx = TpfExecutionContext.withHeaders("v-ckpt-7", "LIVE", "DEFAULT")) {
                published = codec.publishHandoff(meta, upstreamOutput)
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
            assertEquals(TransitionPayloadEncoding.JSON, published.payloadEncoding());
            assertEquals(1, listener.receivedWork.size());
            SerializedTransitionPayload delivered =
                assertInstanceOf(SerializedTransitionPayload.class, listener.receivedWork.get(0));

            // Downstream pipeline decodes & verifies exact fingerprints fail-closed
            SpringCheckpointHandoffCodec.DecodedCheckpointHandoff decoded = codec.decodeAndVerify(
                delivered, "Settlement", "fp-Settlement", "cat-handoff-1");
            assertEquals("payments-pipeline", decoded.metadata().sourcePipelineId());
            assertEquals("ledger-pipeline", decoded.metadata().targetPipelineId());
            assertEquals(Optional.of("v-ckpt-7"), decoded.propagatedContext().versionTag());
            assertTrue(decoded.payload().payloadJson().contains(EXACT_DECIMAL));

            // Execute downstream Spring polyglot pipeline with restored checkpoint context
            SpringCanonicalFunctionHostStep downstreamStep = new SpringCanonicalFunctionHostStep(
                new SpringCanonicalFunctionHostStep.Spec(
                    "record-ledger", 0, "ONE_TO_ONE",
                    "Settlement", "fp-Settlement", "LedgerEntry", "fp-LedgerEntry", "cat-handoff-1",
                    host, SHARED_SECRET, Duration.ofSeconds(5), Duration.ofSeconds(5),
                    SpringForeignDispatchIntentStore.noop(), SpringCanonicalFunctionHostStep.DEFAULT_MAX_RESPONSE_BYTES),
                true);
            SpringPipelineRunner downstreamRunner = new SpringPipelineRunner(runnerCore, List.of(downstreamStep));
            TpfExecutionContext.Snapshot previous = TpfExecutionContext.capture();
            SpringCanonicalPayload ledgerOut;
            try {
                TpfExecutionContext.restore(decoded.propagatedContext());
                ledgerOut = (SpringCanonicalPayload) downstreamRunner.run(decoded.payload())
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            } finally {
                TpfExecutionContext.restore(previous);
            }
            assertEquals("LedgerEntry", ledgerOut.canonicalTypeId());
            assertTrue(ledgerOut.payloadJson().contains(EXACT_DECIMAL));
            assertTrue(ledgerOut.payloadJson().contains("\"versionTag\":\"v-ckpt-7\""));

            // Fingerprint mismatches fail closed
            assertThrows(IllegalArgumentException.class, () -> codec.decodeAndVerify(
                delivered, "Settlement", "fp-WRONG", "cat-handoff-1"));
            assertThrows(IllegalArgumentException.class, () -> codec.decodeAndVerify(
                delivered, "Settlement", "fp-Settlement", "cat-WRONG"));
        });
    }

    private URI startServer(com.sun.net.httpserver.HttpHandler handler) throws IOException {
        serverPool = Executors.newCachedThreadPool();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(serverPool);
        server.createContext("/", handler);
        server.start();
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    private static boolean verifyHmac(HttpExchange exchange, String path, byte[] bodyForSignature) {
        String ts = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER);
        String nonce = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.NONCE_HEADER);
        String sig = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.SIGNATURE_HEADER);
        if (ts == null || nonce == null || sig == null) {
            return false;
        }
        String expected = TransitionWorkerSignature.sign(SHARED_SECRET, "POST", path, ts, nonce, bodyForSignature);
        return TransitionWorkerSignature.matches(expected, sig);
    }

    private static void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static List<SpringCanonicalPayload> collectPublisher(Flow.Publisher<SpringCanonicalPayload> pub) throws Exception {
        List<SpringCanonicalPayload> items = new ArrayList<>();
        CompletableFuture<Void> done = new CompletableFuture<>();
        pub.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(SpringCanonicalPayload item) {
                items.add(item);
            }

            @Override
            public void onError(Throwable t) {
                done.completeExceptionally(t);
            }

            @Override
            public void onComplete() {
                done.complete(null);
            }
        });
        done.get(5, TimeUnit.SECONDS);
        return items;
    }

    private static Flow.Publisher<SpringCanonicalPayload> publisherOf(List<SpringCanonicalPayload> items) {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private final AtomicBoolean sent = new AtomicBoolean();

            @Override
            public void request(long n) {
                if (sent.compareAndSet(false, true)) {
                    for (SpringCanonicalPayload item : items) {
                        subscriber.onNext(item);
                    }
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
                sent.set(true);
            }
        });
    }

    private static void assertNullStartupFailure(
        org.springframework.boot.test.context.assertj.AssertableApplicationContext context
    ) {
        if (context.getStartupFailure() != null) {
            throw new AssertionError("Spring context failed to start", context.getStartupFailure());
        }
    }

    private static String rootMessage(Throwable failure) {
        Throwable cur = failure;
        while (cur.getCause() != null) {
            cur = cur.getCause();
        }
        return cur.getMessage() == null ? "" : cur.getMessage();
    }

    @Configuration(proxyBeanMethods = false)
    static class CheckpointCaptureConfig {
        @Bean
        CheckpointListener checkpointListener() {
            return new CheckpointListener();
        }
    }

    static final class CheckpointListener {
        private final List<Object> receivedWork = new CopyOnWriteArrayList<>();

        @EventListener
        void onWork(SpringRuntimeWorkEvent event) {
            receivedWork.add(event.work());
        }
    }
}

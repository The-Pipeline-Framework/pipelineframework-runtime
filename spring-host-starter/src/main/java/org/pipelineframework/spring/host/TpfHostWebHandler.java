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

package org.pipelineframework.spring.host;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.JsonNode;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Spring WebFlux router and HTTP/1.1 protocol handler implementing the ADR-0092 Foreign Function
 * &amp; Tier-2 Connector Host endpoints.
 */
public final class TpfHostWebHandler {

    public static final MediaType APPLICATION_NDJSON = new MediaType("application", "x-ndjson");
    public static final String STEP_ID_HEADER = "X-TPF-Step-Id";
    public static final String STEP_INDEX_HEADER = "X-TPF-Step-Index";

    private static final byte[] STREAMING_SENTINEL_BYTES =
        TpfHostRequestAuthenticator.STREAMING_BODY_DIGEST_SENTINEL.getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMPLETE_FRAME = "{\"event\":\"complete\"}\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEADER_FLUSH_FRAME = "\n".getBytes(StandardCharsets.UTF_8);
    private static final DataBufferFactory BUFFER_FACTORY = DefaultDataBufferFactory.sharedInstance;

    private final TpfHostRegistry registry;
    private final TpfHostRequestAuthenticator authenticator;
    private final int maxRequestBytes;
    private final int maxFrameBytes;

    public TpfHostWebHandler(
        TpfHostRegistry registry,
        TpfHostRequestAuthenticator authenticator,
        int maxRequestBytes,
        int maxFrameBytes
    ) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.authenticator = Objects.requireNonNull(authenticator, "authenticator must not be null");
        if (maxRequestBytes <= 0) {
            throw new IllegalArgumentException("maxRequestBytes must be positive");
        }
        if (maxFrameBytes <= 0) {
            throw new IllegalArgumentException("maxFrameBytes must be positive");
        }
        this.maxRequestBytes = maxRequestBytes;
        this.maxFrameBytes = maxFrameBytes;
    }

    public RouterFunction<ServerResponse> routes() {
        return RouterFunctions.route()
            .GET("/tpf/health", this::handleHealth)
            .POST("/tpf/functions/{stepId}/stream", this::handleOneToMany)
            .POST("/tpf/functions/{stepId}/reduce", this::handleManyToOne)
            .POST("/tpf/functions/{stepId}/duplex", this::handleManyToMany)
            .POST("/tpf/functions/{stepId}", this::handleOneToOne)
            .POST("/tpf/connector/auth/challenge", this::handleLoopbackChallenge)
            .POST("/tpf/connector/query/{operation}", this::handleQueryConnector)
            .POST("/tpf/connector/command/{operation}/dispatch", this::handleCommandDispatch)
            .POST("/tpf/connector/command/{operation}/reconcile", this::handleCommandReconcile)
            .build();
    }

    private Mono<ServerResponse> handleHealth(ServerRequest request) {
        return jsonResponse(HttpStatus.OK, Map.of("status", "OK"));
    }

    private Mono<ServerResponse> handleOneToOne(ServerRequest request) {
        String stepId = request.pathVariable("stepId");
        return readBoundedBody(request).flatMap(bodyBytes -> {
            Optional<String> authError = verifyRequest(request, bodyBytes);
            if (authError.isPresent()) {
                return unauthorizedResponse(authError.orElseThrow());
            }
            Optional<TpfHostRegistry.OneToOneHandler> handlerOpt = registry.findOneToOne(stepId);
            if (handlerOpt.isEmpty()) {
                return jsonResponse(HttpStatus.NOT_FOUND, Map.of(
                    "status", "ERROR",
                    "disposition", "UNCLASSIFIED",
                    "code", "STEP_NOT_FOUND",
                    "message", "Unknown ONE_TO_ONE step: " + stepId));
            }
            JsonNode envelope;
            try {
                envelope = TpfHostJson.readStrictObject(bodyBytes);
            } catch (IOException malformed) {
                return badRequestResponse("Malformed JSON request envelope");
            }
            JsonNode payload = envelope.get("payload");
            if (payload == null || payload.isNull()) {
                return badRequestResponse("Missing payload field in function request");
            }
            TpfHostRegistry.OneToOneHandler entry = handlerOpt.orElseThrow();
            TpfFunctionContext ctx = buildFunctionContext(stepId, request, envelope);
            String outputTypeId = resolveOutputTypeId(entry.defaultOutputTypeId(), envelope);

            return entry.handler().apply(payload, ctx)
                .flatMap(resultPayload -> jsonResponse(HttpStatus.OK, Map.of(
                    "status", "OK",
                    "outputTypeId", outputTypeId,
                    "payload", resultPayload)))
                .onErrorResume(failure -> jsonResponse(HttpStatus.OK, portableErrorEnvelope(failure)));
        }).onErrorResume(PayloadTooLargeException.class, e -> payloadTooLargeResponse(e.getMessage()));
    }

    private Mono<ServerResponse> handleOneToMany(ServerRequest request) {
        String stepId = request.pathVariable("stepId");
        return readBoundedBody(request).flatMap(bodyBytes -> {
            Optional<String> authError = verifyRequest(request, bodyBytes);
            if (authError.isPresent()) {
                return unauthorizedResponse(authError.orElseThrow());
            }
            Optional<TpfHostRegistry.OneToManyHandler> handlerOpt = registry.findOneToMany(stepId);
            if (handlerOpt.isEmpty()) {
                return jsonResponse(HttpStatus.NOT_FOUND, Map.of(
                    "status", "ERROR",
                    "disposition", "UNCLASSIFIED",
                    "code", "STEP_NOT_FOUND",
                    "message", "Unknown ONE_TO_MANY step: " + stepId));
            }
            JsonNode envelope;
            try {
                envelope = TpfHostJson.readStrictObject(bodyBytes);
            } catch (IOException malformed) {
                return badRequestResponse("Malformed JSON request envelope");
            }
            JsonNode payload = envelope.get("payload");
            if (payload == null || payload.isNull()) {
                return badRequestResponse("Missing payload field in stream request");
            }
            TpfHostRegistry.OneToManyHandler entry = handlerOpt.orElseThrow();
            TpfFunctionContext ctx = buildFunctionContext(stepId, request, envelope);
            String outputTypeId = resolveOutputTypeId(entry.defaultOutputTypeId(), envelope);

            Flux<byte[]> frames = entry.handler().apply(payload, ctx)
                .map(item -> encodeItemFrame(outputTypeId, item))
                .concatWith(Mono.just(COMPLETE_FRAME))
                .onErrorResume(failure -> Mono.just(encodeErrorFrame(failure)));

            return ndjsonStreamResponse(frames, false);
        }).onErrorResume(PayloadTooLargeException.class, e -> payloadTooLargeResponse(e.getMessage()));
    }

    private Mono<ServerResponse> handleManyToOne(ServerRequest request) {
        String stepId = request.pathVariable("stepId");
        Optional<String> authError = verifyRequest(request, STREAMING_SENTINEL_BYTES);
        if (authError.isPresent()) {
            return drainBody(request).then(unauthorizedResponse(authError.orElseThrow()));
        }
        Optional<TpfHostRegistry.ManyToOneHandler> handlerOpt = registry.findManyToOne(stepId);
        if (handlerOpt.isEmpty()) {
            return drainBody(request).then(jsonResponse(HttpStatus.NOT_FOUND, Map.of(
                "status", "ERROR",
                "disposition", "UNCLASSIFIED",
                "code", "STEP_NOT_FOUND",
                "message", "Unknown MANY_TO_ONE step: " + stepId)));
        }
        TpfHostRegistry.ManyToOneHandler entry = handlerOpt.orElseThrow();
        TpfFunctionContext ctx = buildStreamingFunctionContext(stepId, request, entry.outputTypeId());
        Flux<JsonNode> items = decodeNdjsonInputItems(request.bodyToFlux(DataBuffer.class));

        return entry.handler().apply(items, ctx)
            .flatMap(resultPayload -> jsonResponse(HttpStatus.OK, Map.of(
                "status", "OK",
                "outputTypeId", entry.outputTypeId(),
                "payload", resultPayload)))
            .onErrorResume(failure -> jsonResponse(HttpStatus.OK, portableErrorEnvelope(failure)));
    }

    private Mono<ServerResponse> handleManyToMany(ServerRequest request) {
        String stepId = request.pathVariable("stepId");
        Optional<String> authError = verifyRequest(request, STREAMING_SENTINEL_BYTES);
        if (authError.isPresent()) {
            return drainBody(request).then(unauthorizedResponse(authError.orElseThrow()));
        }
        Optional<TpfHostRegistry.ManyToManyHandler> handlerOpt = registry.findManyToMany(stepId);
        if (handlerOpt.isEmpty()) {
            return drainBody(request).then(jsonResponse(HttpStatus.NOT_FOUND, Map.of(
                "status", "ERROR",
                "disposition", "UNCLASSIFIED",
                "code", "STEP_NOT_FOUND",
                "message", "Unknown MANY_TO_MANY step: " + stepId)));
        }
        TpfHostRegistry.ManyToManyHandler entry = handlerOpt.orElseThrow();
        TpfFunctionContext ctx = buildStreamingFunctionContext(stepId, request, entry.outputTypeId());
        Flux<JsonNode> items = decodeNdjsonInputItems(request.bodyToFlux(DataBuffer.class));

        Flux<byte[]> frames = entry.handler().apply(items, ctx)
            .map(item -> encodeItemFrame(entry.outputTypeId(), item))
            .concatWith(Mono.just(COMPLETE_FRAME))
            .onErrorResume(failure -> Mono.just(encodeErrorFrame(failure)));

        // Flush HTTP 200 response headers immediately so full-duplex clients reading response headers
        // concurrently with request streaming never block waiting for the first output item.
        return ndjsonStreamResponse(frames, true);
    }

    private Mono<ServerResponse> handleLoopbackChallenge(ServerRequest request) {
        return readBoundedBody(request).flatMap(bodyBytes -> {
            Optional<String> authError = verifyRequest(request, bodyBytes);
            if (authError.isPresent()) {
                return unauthorizedResponse(authError.orElseThrow());
            }
            JsonNode envelope;
            try {
                envelope = TpfHostJson.readStrictObject(bodyBytes);
            } catch (IOException malformed) {
                return badRequestResponse("Malformed challenge JSON");
            }
            String challenge = envelope.path("challenge").asText("");
            if (challenge.isBlank()) {
                return badRequestResponse("Missing challenge value");
            }
            String proof = authenticator.computeLoopbackChallengeProof(challenge);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "OK");
            response.put("challenge", challenge);
            response.put("proof", proof);
            return jsonResponse(HttpStatus.OK, response);
        }).onErrorResume(PayloadTooLargeException.class, e -> payloadTooLargeResponse(e.getMessage()));
    }

    private Mono<ServerResponse> handleQueryConnector(ServerRequest request) {
        String operation = request.pathVariable("operation");
        return readBoundedBody(request).flatMap(bodyBytes -> {
            Optional<String> authError = verifyRequest(request, bodyBytes);
            if (authError.isPresent()) {
                return unauthorizedResponse(authError.orElseThrow());
            }
            Optional<TpfHostRegistry.QueryHandler> handlerOpt = registry.findQuery(operation);
            if (handlerOpt.isEmpty()) {
                return jsonResponse(HttpStatus.OK, Map.of(
                    "outcome", "TERMINAL_FAILURE",
                    "code", "unknown-query-operation"));
            }
            JsonNode envelope;
            try {
                envelope = TpfHostJson.readStrictObject(bodyBytes);
            } catch (IOException malformed) {
                return badRequestResponse("Malformed query connector request");
            }
            JsonNode input = envelope.get("input");
            if (input == null || input.isNull()) {
                return badRequestResponse("Missing input field in query connector request");
            }
            Optional<String> scope = optionalText(envelope, "scope");
            Map<String, String> secrets = extractStringMap(envelope.get("secrets"));
            TpfConnectorContext ctx = new TpfConnectorContext(operation, scope, secrets);

            return handlerOpt.orElseThrow().handler().apply(input, ctx)
                .flatMap(result -> jsonResponse(HttpStatus.OK, encodeQueryResult(result)))
                .onErrorResume(failure -> jsonResponse(HttpStatus.OK, encodeQueryFailure(failure)));
        }).onErrorResume(PayloadTooLargeException.class, e -> payloadTooLargeResponse(e.getMessage()));
    }

    private Mono<ServerResponse> handleCommandDispatch(ServerRequest request) {
        String operation = request.pathVariable("operation");
        return readBoundedBody(request).flatMap(bodyBytes -> {
            Optional<String> authError = verifyRequest(request, bodyBytes);
            if (authError.isPresent()) {
                return unauthorizedResponse(authError.orElseThrow());
            }
            Optional<TpfHostRegistry.CommandHandler> handlerOpt = registry.findCommand(operation);
            if (handlerOpt.isEmpty()) {
                return jsonResponse(HttpStatus.OK, Map.of(
                    "outcome", "TERMINAL_FAILURE",
                    "code", "unknown-command-operation"));
            }
            JsonNode envelope;
            try {
                envelope = TpfHostJson.readStrictObject(bodyBytes);
            } catch (IOException malformed) {
                return badRequestResponse("Malformed command dispatch request");
            }
            JsonNode input = envelope.get("input");
            JsonNode identity = envelope.get("dispatchIdentity");
            if (input == null || input.isNull() || identity == null || !identity.isObject()) {
                return badRequestResponse("Missing input or dispatchIdentity in command dispatch request");
            }
            String commandId = identity.path("commandId").asText("");
            String occurrenceId = identity.path("occurrenceId").asText("");
            String attemptId = identity.path("attemptId").asText("");
            String idempotencyKey = identity.path("providerIdempotencyKey").asText("");
            if (commandId.isBlank() || occurrenceId.isBlank() || attemptId.isBlank() || idempotencyKey.isBlank()) {
                return badRequestResponse("Incomplete dispatchIdentity in command dispatch request");
            }
            Optional<String> scope = optionalText(envelope, "scope");
            Map<String, String> secrets = extractStringMap(envelope.get("secrets"));
            TpfCommandContext ctx = new TpfCommandContext(
                operation, commandId, occurrenceId, attemptId, idempotencyKey, scope, secrets);

            return handlerOpt.orElseThrow().handler().apply(input, ctx)
                .flatMap(result -> jsonResponse(HttpStatus.OK, encodeCommandResult(result)))
                .onErrorResume(failure -> jsonResponse(HttpStatus.OK, encodeCommandFailure(failure)));
        }).onErrorResume(PayloadTooLargeException.class, e -> payloadTooLargeResponse(e.getMessage()));
    }

    private Mono<ServerResponse> handleCommandReconcile(ServerRequest request) {
        String operation = request.pathVariable("operation");
        return readBoundedBody(request).flatMap(bodyBytes -> {
            Optional<String> authError = verifyRequest(request, bodyBytes);
            if (authError.isPresent()) {
                return unauthorizedResponse(authError.orElseThrow());
            }
            Optional<TpfHostRegistry.ReconcilerHandler> handlerOpt = registry.findReconciler(operation);
            if (handlerOpt.isEmpty()) {
                return jsonResponse(HttpStatus.OK, Map.of(
                    "outcome", "UNRESOLVED",
                    "code", "reconciler-not-registered"));
            }
            JsonNode envelope;
            try {
                envelope = TpfHostJson.readStrictObject(bodyBytes);
            } catch (IOException malformed) {
                return badRequestResponse("Malformed command reconcile request");
            }
            String recoveryTargetIdentity = envelope.path("recoveryTargetIdentity").asText("");
            JsonNode identity = envelope.get("dispatchIdentity");
            if (recoveryTargetIdentity.isBlank() || identity == null || !identity.isObject()) {
                return badRequestResponse("Missing recoveryTargetIdentity or dispatchIdentity");
            }
            String commandId = identity.path("commandId").asText("");
            String occurrenceId = identity.path("occurrenceId").asText("");
            String attemptId = identity.path("attemptId").asText("");
            String idempotencyKey = identity.path("providerIdempotencyKey").asText("");
            if (commandId.isBlank() || occurrenceId.isBlank() || attemptId.isBlank() || idempotencyKey.isBlank()) {
                return badRequestResponse("Incomplete dispatchIdentity in command reconcile request");
            }
            Map<String, String> secrets = extractStringMap(envelope.get("secrets"));
            TpfReconcileContext ctx = new TpfReconcileContext(
                operation, recoveryTargetIdentity, commandId, occurrenceId, attemptId, idempotencyKey, secrets);

            return handlerOpt.orElseThrow().handler().apply(ctx)
                .flatMap(result -> jsonResponse(HttpStatus.OK, encodeReconcileResult(result)))
                .onErrorResume(failure -> jsonResponse(HttpStatus.OK, Map.of(
                    "outcome", "UNRESOLVED",
                    "code", "reconcile-error")));
        }).onErrorResume(PayloadTooLargeException.class, e -> payloadTooLargeResponse(e.getMessage()));
    }

    private Optional<String> verifyRequest(ServerRequest request, byte[] bodyForSignature) {
        String method = request.method().name();
        String path = request.uri().getRawPath();
        String timestamp = request.headers().firstHeader(TransitionWorkerSignature.TIMESTAMP_HEADER);
        String nonce = request.headers().firstHeader(TransitionWorkerSignature.NONCE_HEADER);
        String signature = request.headers().firstHeader(TransitionWorkerSignature.SIGNATURE_HEADER);
        return authenticator.verify(method, path, timestamp, nonce, signature, bodyForSignature);
    }

    private Mono<byte[]> readBoundedBody(ServerRequest request) {
        return Mono.defer(() -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            return request.bodyToFlux(DataBuffer.class)
                .concatMap(buffer -> {
                    try {
                        int readable = buffer.readableByteCount();
                        if (readable > maxRequestBytes - out.size()) {
                            return Mono.error(new PayloadTooLargeException(
                                "Request body exceeded " + maxRequestBytes + " byte limit"));
                        }
                        byte[] chunk = new byte[readable];
                        buffer.read(chunk);
                        out.write(chunk, 0, readable);
                        return Mono.empty();
                    } finally {
                        DataBufferUtils.release(buffer);
                    }
                })
                .then(Mono.fromSupplier(out::toByteArray));
        });
    }

    private Mono<Void> drainBody(ServerRequest request) {
        return request.bodyToFlux(DataBuffer.class)
            .doOnNext(DataBufferUtils::release)
            .then();
    }

    /**
     * Incrementally splits incoming chunked NDJSON {@link DataBuffer}s into validated item payload
     * {@link JsonNode}s with strict per-frame size bounds and explicit {@code complete} sentinel
     * enforcement.
     */
    private Flux<JsonNode> decodeNdjsonInputItems(Flux<DataBuffer> body) {
        return Flux.defer(() -> {
            ByteArrayOutputStream lineBuf = new ByteArrayOutputStream(256);
            AtomicBoolean completed = new AtomicBoolean(false);

            Flux<String> lines = body.concatMap(buffer -> {
                List<String> emitted = new ArrayList<>();
                try {
                    int readable = buffer.readableByteCount();
                    byte[] bytes = new byte[readable];
                    buffer.read(bytes);
                    for (byte b : bytes) {
                        if (b == '\n') {
                            String line = lineBuf.toString(StandardCharsets.UTF_8).trim();
                            lineBuf.reset();
                            if (!line.isEmpty()) {
                                emitted.add(line);
                            }
                        } else {
                            if (lineBuf.size() >= maxFrameBytes) {
                                return Flux.error(new TpfBusinessRejectionException(
                                    "FRAME_TOO_LARGE",
                                    "NDJSON input frame exceeded " + maxFrameBytes + " byte limit"));
                            }
                            lineBuf.write(b);
                        }
                    }
                } finally {
                    DataBufferUtils.release(buffer);
                }
                return Flux.fromIterable(emitted);
            }).concatWith(Flux.defer(() -> {
                if (lineBuf.size() > 0) {
                    String trailing = lineBuf.toString(StandardCharsets.UTF_8).trim();
                    lineBuf.reset();
                    if (!trailing.isEmpty()) {
                        return Flux.just(trailing);
                    }
                }
                return Flux.empty();
            }));

            return lines.<JsonNode>handle((line, sink) -> {
                if (completed.get()) {
                    return;
                }
                JsonNode frame;
                try {
                    frame = TpfHostJson.readStrictObject(line);
                } catch (IOException malformed) {
                    sink.error(new IllegalArgumentException("Malformed NDJSON input frame", malformed));
                    return;
                }
                String event = frame.path("event").asText("");
                switch (event) {
                    case "item" -> {
                        JsonNode payload = frame.get("payload");
                        if (payload == null || payload.isNull()) {
                            sink.error(new IllegalArgumentException("NDJSON item frame missing payload"));
                            return;
                        }
                        sink.next(payload);
                    }
                    case "complete" -> {
                        completed.set(true);
                        sink.complete();
                    }
                    default -> sink.error(new IllegalArgumentException("Unknown NDJSON input event: " + event));
                }
            }).concatWith(Flux.defer(() -> completed.get()
                ? Flux.empty()
                : Flux.error(new IllegalStateException("NDJSON input stream ended without complete sentinel"))));
        });
    }

    private Mono<ServerResponse> ndjsonStreamResponse(Flux<byte[]> frames, boolean flushHeadersImmediately) {
        return ServerResponse.ok()
            .contentType(APPLICATION_NDJSON)
            .body((outputMessage, context) -> {
                Flux<Publisher<DataBuffer>> chunks = frames.map(bytes -> Mono.just(BUFFER_FACTORY.wrap(bytes)));
                if (flushHeadersImmediately) {
                    chunks = Flux.concat(
                        Mono.just(Mono.just(BUFFER_FACTORY.wrap(HEADER_FLUSH_FRAME))),
                        chunks);
                }
                return outputMessage.writeAndFlushWith(chunks);
            });
    }

    private static TpfFunctionContext buildFunctionContext(String stepId, ServerRequest request, JsonNode envelope) {
        int stepIndex = envelope.path("stepIndex").asInt(parseStepIndexHeader(request));
        String inputTypeId = envelope.path("inputTypeId").asText("");
        String outputTypeId = envelope.path("outputTypeId").asText("");
        Map<String, String> attributes = extractStringMap(envelope.get("context"));
        return new TpfFunctionContext(stepId, Math.max(0, stepIndex), inputTypeId, outputTypeId, attributes);
    }

    private static TpfFunctionContext buildStreamingFunctionContext(
        String stepId, ServerRequest request, String outputTypeId
    ) {
        int stepIndex = parseStepIndexHeader(request);
        return new TpfFunctionContext(stepId, Math.max(0, stepIndex), "", outputTypeId, Map.of());
    }

    private static int parseStepIndexHeader(ServerRequest request) {
        String raw = request.headers().firstHeader(STEP_INDEX_HEADER);
        if (raw == null || raw.isBlank()) {
            return 0;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private static String resolveOutputTypeId(Optional<String> explicitOutputTypeId, JsonNode envelope) {
        if (explicitOutputTypeId.isPresent()) {
            return explicitOutputTypeId.orElseThrow();
        }
        String fromEnvelope = envelope.path("outputTypeId").asText("");
        return fromEnvelope.isBlank() ? "unknown" : fromEnvelope;
    }

    private static byte[] encodeItemFrame(String outputTypeId, JsonNode payload) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("event", "item");
        frame.put("outputTypeId", outputTypeId);
        frame.put("payload", payload);
        return TpfHostJson.writeNdjsonLine(frame);
    }

    private static byte[] encodeErrorFrame(Throwable failure) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("event", "error");
        populateFailureFields(frame, failure);
        return TpfHostJson.writeNdjsonLine(frame);
    }

    private static Map<String, Object> portableErrorEnvelope(Throwable failure) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("status", "ERROR");
        populateFailureFields(envelope, failure);
        return envelope;
    }

    private static void populateFailureFields(Map<String, Object> target, Throwable failure) {
        if (failure instanceof TpfBusinessRejectionException rejection) {
            target.put("disposition", "BUSINESS_REJECTION");
            target.put("code", rejection.code());
            target.put("message", safeMessage(rejection.getMessage(), rejection.code()));
            return;
        }
        if (failure instanceof TpfTransientException transientFailure) {
            target.put("disposition", "TRANSIENT");
            target.put("code", transientFailure.code());
            target.put("message", safeMessage(transientFailure.getMessage(), transientFailure.code()));
            return;
        }
        target.put("disposition", "UNCLASSIFIED");
        target.put("code", "UNCLASSIFIED_ERROR");
        target.put("message", safeMessage(failure == null ? "" : failure.getMessage(), "Unhandled function failure"));
    }

    private static Map<String, Object> encodeQueryResult(TpfQueryResult<JsonNode> result) {
        Map<String, Object> map = new LinkedHashMap<>();
        switch (result) {
            case TpfQueryResult.Found<JsonNode> found -> {
                map.put("outcome", "FOUND");
                map.put("payload", found.payload());
            }
            case TpfQueryResult.NotFound<JsonNode> notFound -> {
                map.put("outcome", "NOT_FOUND");
                map.put("code", notFound.code());
            }
            case TpfQueryResult.AuthRequired<JsonNode> auth -> {
                map.put("outcome", "AUTH_REQUIRED");
                map.put("code", auth.code());
            }
            case TpfQueryResult.TemporarilyUnavailable<JsonNode> unavail -> {
                map.put("outcome", "TEMPORARILY_UNAVAILABLE");
                map.put("code", unavail.code());
            }
            case TpfQueryResult.TerminalFailure<JsonNode> term -> {
                map.put("outcome", "TERMINAL_FAILURE");
                map.put("code", term.code());
            }
        }
        return map;
    }

    private static Map<String, Object> encodeQueryFailure(Throwable failure) {
        if (failure instanceof TpfTransientException transientFailure) {
            return Map.of("outcome", "TEMPORARILY_UNAVAILABLE", "code", transientFailure.code());
        }
        if (failure instanceof TpfBusinessRejectionException rejection) {
            return Map.of("outcome", "TERMINAL_FAILURE", "code", rejection.code());
        }
        return Map.of("outcome", "TERMINAL_FAILURE", "code", "query-handler-error");
    }

    private static Map<String, Object> encodeCommandResult(TpfCommandResult<JsonNode> result) {
        Map<String, Object> map = new LinkedHashMap<>();
        switch (result) {
            case TpfCommandResult.Succeeded<JsonNode> ok -> {
                map.put("outcome", "SUCCEEDED");
                map.put("payload", ok.payload());
                ok.receiptReference().ifPresent(ref -> map.put("receiptReference", ref));
            }
            case TpfCommandResult.RetryableFailure<JsonNode> retry -> {
                map.put("outcome", "RETRYABLE_FAILURE");
                map.put("code", retry.code());
            }
            case TpfCommandResult.TerminalFailure<JsonNode> term -> {
                map.put("outcome", "TERMINAL_FAILURE");
                map.put("code", term.code());
            }
            case TpfCommandResult.Ambiguous<JsonNode> amb -> {
                map.put("outcome", "AMBIGUOUS");
                map.put("code", amb.code());
            }
        }
        return map;
    }

    private static Map<String, Object> encodeCommandFailure(Throwable failure) {
        if (failure instanceof TpfTransientException transientFailure) {
            return Map.of("outcome", "RETRYABLE_FAILURE", "code", transientFailure.code());
        }
        if (failure instanceof TpfBusinessRejectionException rejection) {
            return Map.of("outcome", "TERMINAL_FAILURE", "code", rejection.code());
        }
        return Map.of("outcome", "AMBIGUOUS", "code", "command-handler-error");
    }

    private static Map<String, Object> encodeReconcileResult(TpfReconcileResult<JsonNode> result) {
        Map<String, Object> map = new LinkedHashMap<>();
        switch (result) {
            case TpfReconcileResult.ConfirmedSucceeded<JsonNode> ok -> {
                map.put("outcome", "CONFIRMED_SUCCEEDED");
                map.put("payload", ok.payload());
                map.put("receiptReference", ok.receiptReference());
            }
            case TpfReconcileResult.Unresolved<JsonNode> unresolved -> {
                map.put("outcome", "UNRESOLVED");
                map.put("code", unresolved.code());
            }
        }
        return map;
    }

    private static Map<String, String> extractStringMap(JsonNode node) {
        if (node == null || !node.isObject()) {
            return Map.of();
        }
        Map<String, String> map = new LinkedHashMap<>();
        node.fields().forEachRemaining(entry -> {
            if (entry.getValue() != null && !entry.getValue().isNull()) {
                map.put(entry.getKey(), entry.getValue().asText());
            }
        });
        return Map.copyOf(map);
    }

    private static Optional<String> optionalText(JsonNode node, String field) {
        JsonNode child = node.get(field);
        if (child == null || child.isNull()) {
            return Optional.empty();
        }
        String text = child.asText("");
        return text.isBlank() ? Optional.empty() : Optional.of(text);
    }

    private static String safeMessage(String message, String fallback) {
        return (message == null || message.isBlank()) ? fallback : message;
    }

    private static Mono<ServerResponse> unauthorizedResponse(String reason) {
        return jsonResponse(HttpStatus.UNAUTHORIZED, Map.of(
            "status", "ERROR",
            "code", "UNAUTHORIZED",
            "message", reason));
    }

    private static Mono<ServerResponse> badRequestResponse(String reason) {
        return jsonResponse(HttpStatus.BAD_REQUEST, Map.of(
            "status", "ERROR",
            "code", "BAD_REQUEST",
            "message", reason));
    }

    private static Mono<ServerResponse> payloadTooLargeResponse(String reason) {
        return jsonResponse(HttpStatus.PAYLOAD_TOO_LARGE, Map.of(
            "status", "ERROR",
            "code", "PAYLOAD_TOO_LARGE",
            "message", reason));
    }

    private static Mono<ServerResponse> jsonResponse(HttpStatus status, Object body) {
        byte[] bytes = TpfHostJson.writeBytes(body);
        return ServerResponse.status(status)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(bytes);
    }

    private static final class PayloadTooLargeException extends RuntimeException {
        private PayloadTooLargeException(String message) {
            super(message);
        }
    }
}

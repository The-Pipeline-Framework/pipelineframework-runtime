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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.fasterxml.jackson.databind.JsonNode;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;
import org.pipelineframework.runtime.core.PipelineUnaryStep;
import org.pipelineframework.runtime.core.RuntimeAdapters;
import org.pipelineframework.runtime.core.TpfExecutionContext;

/**
 * Spring-hosted polyglot step adapter speaking the canonical authenticated HTTP/1.1 protocol
 * for {@code ONE_TO_ONE}, {@code ONE_TO_MANY}, {@code MANY_TO_ONE}, and full-duplex
 * {@code MANY_TO_MANY} foreign business functions.
 *
 * <p>Implements {@link PipelineUnaryStep} so unary polyglot steps plug directly into
 * {@link org.pipelineframework.runtime.spring.SpringPipelineRunner} while preserving
 * {@link TpfExecutionContext}, pre-send {@link SpringForeignDispatchIntentStore} durability,
 * HTTPS-by-default origin enforcement, and exact JSON decimal/int64 precision.</p>
 */
public final class SpringCanonicalFunctionHostStep
    implements PipelineUnaryStep<SpringCanonicalPayload, SpringCanonicalPayload> {

    public static final String FUNCTION_PATH_PREFIX = "/tpf/functions/";
    public static final String STEP_ID_HEADER = "X-TPF-Step-Id";
    public static final String STEP_INDEX_HEADER = "X-TPF-Step-Index";
    public static final String STREAMING_BODY_DIGEST_SENTINEL = "STREAMING-NDJSON-V1";
    public static final int DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024;
    private static final int MAX_HEADER_BYTES = 64 * 1024;
    private static final Set<String> LOOPBACK_HOSTS = Set.of("127.0.0.1", "::1", "[::1]");

    /** Immutable binding for one Spring-hosted foreign business-function step. */
    public record Spec(
        String stepId,
        int stepIndex,
        String cardinality,
        String inputCanonicalTypeId,
        String inputDefinitionFingerprint,
        String outputCanonicalTypeId,
        String outputDefinitionFingerprint,
        String catalogFingerprint,
        URI baseUri,
        String sharedSecret,
        Duration requestTimeout,
        Duration streamIdleTimeout,
        SpringForeignDispatchIntentStore dispatchIntentStore,
        int maxResponseBytes
    ) {
        public Spec {
            stepId = requireNonBlank(stepId, "stepId");
            if (stepIndex < 0) {
                throw new IllegalArgumentException("stepIndex must be non-negative");
            }
            cardinality = requireNonBlank(cardinality, "cardinality").toUpperCase(Locale.ROOT);
            inputCanonicalTypeId = requireNonBlank(inputCanonicalTypeId, "inputCanonicalTypeId");
            inputDefinitionFingerprint = requireNonBlank(inputDefinitionFingerprint, "inputDefinitionFingerprint");
            outputCanonicalTypeId = requireNonBlank(outputCanonicalTypeId, "outputCanonicalTypeId");
            outputDefinitionFingerprint = requireNonBlank(outputDefinitionFingerprint, "outputDefinitionFingerprint");
            catalogFingerprint = requireNonBlank(catalogFingerprint, "catalogFingerprint");
            Objects.requireNonNull(baseUri, "baseUri");
            sharedSecret = requireNonBlank(sharedSecret, "sharedSecret");
            requirePositive(requestTimeout, "requestTimeout");
            requirePositive(streamIdleTimeout, "streamIdleTimeout");
            Objects.requireNonNull(dispatchIntentStore, "dispatchIntentStore");
            if (maxResponseBytes <= 0) {
                throw new IllegalArgumentException("maxResponseBytes must be positive");
            }
        }
    }

    private final Spec spec;
    private final URI normalizedOrigin;
    private final HttpClient httpClient;

    public SpringCanonicalFunctionHostStep(Spec spec, boolean allowLoopbackHttp) {
        this.spec = Objects.requireNonNull(spec, "spec");
        this.normalizedOrigin = normalizeOrigin(spec.baseUri().toString(), allowLoopbackHttp);
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(spec.requestTimeout())
            .build();
    }

    public Spec spec() {
        return spec;
    }

    public URI normalizedOrigin() {
        return normalizedOrigin;
    }

    /** Validates and canonicalizes a foreign function host origin (HTTPS by default). */
    public static URI normalizeOrigin(String value, boolean allowLoopbackHttp) {
        requireNonBlank(value, "baseUri");
        URI uri;
        try {
            uri = URI.create(value.trim()).normalize();
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("Invalid foreign function host URI: " + value, failure);
        }
        String host = uri.getHost();
        boolean loopback = host != null && LOOPBACK_HOSTS.contains(host);
        boolean secure = "https".equalsIgnoreCase(uri.getScheme());
        boolean local = "http".equalsIgnoreCase(uri.getScheme()) && loopback && allowLoopbackHttp;
        if ((!secure && !local)
            || host == null
            || uri.getUserInfo() != null
            || uri.getQuery() != null
            || uri.getFragment() != null
            || (!uri.getPath().isEmpty() && !"/".equals(uri.getPath()))) {
            throw new IllegalArgumentException(
                "Foreign function host URI must be a trusted HTTPS origin or an explicit loopback HTTP origin");
        }
        int port = uri.getPort() < 0 ? (secure ? 443 : 80) : uri.getPort();
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid foreign function host port: " + port);
        }
        return URI.create(uri.getScheme().toLowerCase(Locale.ROOT) + "://" + host.toLowerCase(Locale.ROOT) + ":" + port);
    }

    @Override
    public CompletionStage<SpringCanonicalPayload> apply(SpringCanonicalPayload input) {
        TpfExecutionContext.Snapshot contextSnapshot = TpfExecutionContext.capture();
        Optional<SpringTransitionDispatchScope> dispatchScope = SpringTransitionDispatchScope.current();
        return executeBlocking(() -> invokeUnarySynchronously(input, contextSnapshot, dispatchScope));
    }

    /** Invokes a {@code ONE_TO_MANY} foreign function streaming NDJSON output frames. */
    public Flow.Publisher<SpringCanonicalPayload> applyOneToMany(SpringCanonicalPayload input) {
        TpfExecutionContext.Snapshot contextSnapshot = TpfExecutionContext.capture();
        Optional<SpringTransitionDispatchScope> dispatchScope = SpringTransitionDispatchScope.current();
        return subscriber -> {
            Objects.requireNonNull(subscriber, "subscriber");
            String path = FUNCTION_PATH_PREFIX + spec.stepId() + "/stream";
            URI targetUri = normalizedOrigin.resolve(path);
            byte[] bodyBytes;
            Optional<SpringForeignDispatchIntent> reserved;
            try {
                String canonicalJson = validateCanonicalInput(input);
                bodyBytes = buildRequestBody(canonicalJson, contextSnapshot);
                reserved = reserveDispatchIntent(dispatchScope, targetUri, bodyBytes);
            } catch (RuntimeException failure) {
                subscriber.onSubscribe(new EmptySubscription());
                subscriber.onError(failure);
                return;
            }
            new OneToManySubscription(subscriber, targetUri, path, bodyBytes, reserved).start();
        };
    }

    /** Invokes a {@code MANY_TO_ONE} foreign function consuming an upstream {@link Flow.Publisher}. */
    public CompletionStage<SpringCanonicalPayload> applyManyToOne(Flow.Publisher<SpringCanonicalPayload> inputs) {
        Objects.requireNonNull(inputs, "inputs");
        Optional<SpringTransitionDispatchScope> dispatchScope = SpringTransitionDispatchScope.current();
        CompletableFuture<SpringCanonicalPayload> result = new CompletableFuture<>();
        String path = FUNCTION_PATH_PREFIX + spec.stepId() + "/collect";
        URI targetUri = normalizedOrigin.resolve(path);

        List<SpringCanonicalPayload> buffered = new java.util.ArrayList<>();
        inputs.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription sub) {
                this.subscription = sub;
                sub.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(SpringCanonicalPayload item) {
                try {
                    validateCanonicalInput(item);
                    buffered.add(item);
                } catch (RuntimeException failure) {
                    subscription.cancel();
                    result.completeExceptionally(failure);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                result.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                if (result.isDone()) {
                    return;
                }
                executeBlocking(() -> invokeManyToOneSynchronously(buffered, targetUri, path, dispatchScope))
                    .whenComplete((payload, failure) -> {
                        if (failure != null) {
                            result.completeExceptionally(unwrap(failure));
                        } else {
                            result.complete(payload);
                        }
                    });
            }
        });
        return result;
    }

    /** Invokes a full-duplex {@code MANY_TO_MANY} foreign function over chunked HTTP/1.1 NDJSON. */
    public Flow.Publisher<SpringCanonicalPayload> applyManyToMany(Flow.Publisher<SpringCanonicalPayload> inputs) {
        Objects.requireNonNull(inputs, "inputs");
        Optional<SpringTransitionDispatchScope> dispatchScope = SpringTransitionDispatchScope.current();
        return subscriber -> {
            Objects.requireNonNull(subscriber, "subscriber");
            String path = FUNCTION_PATH_PREFIX + spec.stepId() + "/duplex";
            URI targetUri = normalizedOrigin.resolve(path);
            Optional<SpringForeignDispatchIntent> reserved;
            try {
                reserved = reserveDispatchIntent(
                    dispatchScope, targetUri, STREAMING_BODY_DIGEST_SENTINEL.getBytes(StandardCharsets.UTF_8));
            } catch (RuntimeException failure) {
                subscriber.onSubscribe(new EmptySubscription());
                subscriber.onError(failure);
                return;
            }
            DuplexSubscription duplex = new DuplexSubscription(subscriber, inputs, targetUri, path, reserved);
            duplex.start();
        };
    }

    private SpringCanonicalPayload invokeUnarySynchronously(
        SpringCanonicalPayload input,
        TpfExecutionContext.Snapshot contextSnapshot,
        Optional<SpringTransitionDispatchScope> dispatchScope
    ) {
        String canonicalJson = validateCanonicalInput(input);
        String path = FUNCTION_PATH_PREFIX + spec.stepId();
        URI targetUri = normalizedOrigin.resolve(path);
        byte[] bodyBytes = buildRequestBody(canonicalJson, contextSnapshot);

        Optional<SpringForeignDispatchIntent> reserved = reserveDispatchIntent(dispatchScope, targetUri, bodyBytes);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(targetUri)
            .timeout(spec.requestTimeout())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes));
        signHeaders(requestBuilder, path, bodyBytes);

        HttpResponse<byte[]> response;
        try {
            response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            settleDispatchIntent(reserved, SpringForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
            throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Interrupted awaiting foreign function response", Optional.of(interrupted));
        } catch (IOException transportFailure) {
            settleDispatchIntent(reserved, SpringForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
            throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Uncertain transport outcome invoking foreign function " + spec.stepId()
                    + ": " + transportFailure.getMessage(),
                Optional.of(transportFailure));
        }

        try {
            SpringCanonicalPayload output = decodeJsonResponse(response, targetUri);
            settleDispatchIntent(reserved, SpringForeignDispatchIntentStore.SettlementStatus.VALIDATED_SUCCEEDED);
            return output;
        } catch (RuntimeException failure) {
            settleDispatchIntent(reserved, settlementForFailure(failure));
            throw failure;
        }
    }

    private SpringCanonicalPayload invokeManyToOneSynchronously(
        List<SpringCanonicalPayload> items,
        URI targetUri,
        String path,
        Optional<SpringTransitionDispatchScope> dispatchScope
    ) {
        byte[] sentinel = STREAMING_BODY_DIGEST_SENTINEL.getBytes(StandardCharsets.UTF_8);
        Optional<SpringForeignDispatchIntent> reserved = reserveDispatchIntent(dispatchScope, targetUri, sentinel);
        ByteArrayOutputStream ndjson = new ByteArrayOutputStream();
        try {
            for (SpringCanonicalPayload item : items) {
                Map<String, Object> frame = new LinkedHashMap<>();
                frame.put("event", "item");
                frame.put("inputTypeId", spec.inputCanonicalTypeId());
                frame.put("payload", SpringPolyglotJson.strictReader().readTree(item.payload()));
                ndjson.write(SpringPolyglotJson.exactMapper().writeValueAsBytes(frame));
                ndjson.write('\n');
            }
            ndjson.write("{\"event\":\"complete\"}\n".getBytes(StandardCharsets.UTF_8));
        } catch (IOException serializationError) {
            settleDispatchIntent(reserved, SpringForeignDispatchIntentStore.SettlementStatus.VALIDATED_TERMINAL);
            throw new SpringForeignStepException.BusinessRejection(
                spec.stepId(), spec.stepIndex(), SpringForeignStepException.Disposition.UNCLASSIFIED,
                "Failed serializing MANY_TO_ONE request for " + spec.stepId() + ": " + serializationError.getMessage());
        }

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(targetUri)
            .timeout(spec.requestTimeout())
            .header("Content-Type", "application/x-ndjson")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(ndjson.toByteArray()));
        signHeaders(requestBuilder, path, sentinel);

        HttpResponse<byte[]> response;
        try {
            response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            settleDispatchIntent(reserved, SpringForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
            throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Interrupted awaiting MANY_TO_ONE response", Optional.of(interrupted));
        } catch (IOException transportFailure) {
            settleDispatchIntent(reserved, SpringForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
            throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Uncertain transport outcome invoking MANY_TO_ONE step " + spec.stepId()
                    + ": " + transportFailure.getMessage(),
                Optional.of(transportFailure));
        }

        try {
            SpringCanonicalPayload output = decodeJsonResponse(response, targetUri);
            settleDispatchIntent(reserved, SpringForeignDispatchIntentStore.SettlementStatus.VALIDATED_SUCCEEDED);
            return output;
        } catch (RuntimeException failure) {
            settleDispatchIntent(reserved, settlementForFailure(failure));
            throw failure;
        }
    }

    private String validateCanonicalInput(SpringCanonicalPayload input) {
        if (input == null) {
            throw new SpringForeignStepException.BusinessRejection(
                spec.stepId(), spec.stepIndex(), SpringForeignStepException.Disposition.UNCLASSIFIED,
                "Foreign function step " + spec.stepId() + " received null input");
        }
        if (!spec.inputCanonicalTypeId().equals(input.canonicalTypeId())) {
            throw new SpringForeignStepException.BusinessRejection(
                spec.stepId(), spec.stepIndex(), SpringForeignStepException.Disposition.UNCLASSIFIED,
                "Input canonical type mismatch for step " + spec.stepId()
                    + ": expected " + spec.inputCanonicalTypeId() + " but got " + input.canonicalTypeId());
        }
        if (!spec.inputDefinitionFingerprint().equals(input.typeExpressionFingerprint())) {
            throw new SpringForeignStepException.BusinessRejection(
                spec.stepId(), spec.stepIndex(), SpringForeignStepException.Disposition.UNCLASSIFIED,
                "Input definition fingerprint mismatch for step " + spec.stepId());
        }
        if (!spec.catalogFingerprint().equals(input.catalogFingerprint())) {
            throw new SpringForeignStepException.BusinessRejection(
                spec.stepId(), spec.stepIndex(), SpringForeignStepException.Disposition.UNCLASSIFIED,
                "Input catalog fingerprint mismatch for step " + spec.stepId());
        }
        if (!SpringCanonicalPayload.CANONICAL_JSON_ENCODING.equals(input.encoding())
            || input.encodingVersion() != SpringCanonicalPayload.CANONICAL_JSON_VERSION) {
            throw new SpringForeignStepException.BusinessRejection(
                spec.stepId(), spec.stepIndex(), SpringForeignStepException.Disposition.UNCLASSIFIED,
                "Unsupported canonical durable encoding for step " + spec.stepId());
        }
        try {
            JsonNode tree = SpringPolyglotJson.strictReader().readTree(input.payload());
            if (tree == null || tree.isNull()) {
                throw new IllegalArgumentException("canonical payload JSON must not be empty or null");
            }
            return SpringPolyglotJson.exactMapper().writeValueAsString(tree);
        } catch (IOException | RuntimeException invalid) {
            throw new SpringForeignStepException.BusinessRejection(
                spec.stepId(), spec.stepIndex(), SpringForeignStepException.Disposition.UNCLASSIFIED,
                "Invalid canonical input payload for step " + spec.stepId() + ": " + invalid.getMessage());
        }
    }

    private byte[] buildRequestBody(String canonicalJson, TpfExecutionContext.Snapshot contextSnapshot) {
        try {
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("stepId", spec.stepId());
            envelope.put("stepIndex", spec.stepIndex());
            envelope.put("inputTypeId", spec.inputCanonicalTypeId());
            envelope.put("outputTypeId", spec.outputCanonicalTypeId());
            envelope.put("payload", SpringPolyglotJson.exactMapper().readTree(canonicalJson));
            Map<String, String> context = new LinkedHashMap<>();
            contextSnapshot.versionTag().ifPresent(v -> context.put("versionTag", v));
            contextSnapshot.replayMode().ifPresent(v -> context.put("replayMode", v));
            contextSnapshot.cachePolicy().ifPresent(v -> context.put("cachePolicy", v));
            envelope.put("context", context);
            return SpringPolyglotJson.exactMapper().writeValueAsBytes(envelope);
        } catch (IOException failure) {
            throw new IllegalStateException("Failed serializing canonical function request for " + spec.stepId(), failure);
        }
    }

    private SpringCanonicalPayload decodeJsonResponse(HttpResponse<byte[]> response, URI targetUri) {
        int status = response.statusCode();
        if (status == 401 || status == 403) {
            throw new SpringForeignStepException.AuthenticationFailure(
                spec.stepId(), spec.stepIndex(), status,
                "Foreign function host rejected authentication for step " + spec.stepId() + " (HTTP " + status + ")");
        }
        if (status != 200) {
            throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Uncertain HTTP status " + status + " from foreign function host for step " + spec.stepId(),
                Optional.empty());
        }
        JsonNode root = readStrictObject(response.body(), targetUri);
        String outcomeStatus = root.path("status").asText("");
        return switch (outcomeStatus) {
            case "OK" -> validateCanonicalOutput(root, targetUri);
            case "ERROR" -> throw decodePortableError(root, targetUri);
            default -> throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Unknown function host status '" + outcomeStatus + "' for step " + spec.stepId(),
                Optional.empty());
        };
    }

    private SpringCanonicalPayload validateCanonicalOutput(JsonNode root, URI targetUri) {
        String outputTypeId = root.path("outputTypeId").asText("");
        if (!spec.outputCanonicalTypeId().equals(outputTypeId)) {
            throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Foreign function returned unexpected outputTypeId '" + outputTypeId
                    + "' (expected '" + spec.outputCanonicalTypeId() + "') for step " + spec.stepId(),
                Optional.empty());
        }
        JsonNode payloadNode = root.get("payload");
        if (payloadNode == null || payloadNode.isNull()) {
            throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Foreign function returned missing payload for step " + spec.stepId(),
                Optional.empty());
        }
        try {
            byte[] serialized = SpringPolyglotJson.exactMapper().writeValueAsBytes(payloadNode);
            return new SpringCanonicalPayload(
                spec.outputCanonicalTypeId(),
                spec.outputDefinitionFingerprint(),
                spec.catalogFingerprint(),
                SpringCanonicalPayload.CANONICAL_JSON_ENCODING,
                SpringCanonicalPayload.CANONICAL_JSON_VERSION,
                serialized);
        } catch (IOException failure) {
            throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Failed encoding foreign function output for step " + spec.stepId(),
                Optional.of(failure));
        }
    }

    private SpringForeignStepException decodePortableError(JsonNode root, URI targetUri) {
        String rawDisposition = root.path("disposition").asText("");
        String code = root.path("code").asText(rawDisposition);
        String detail = root.path("message").asText("");
        String message = detail.isBlank() ? code : code + ": " + detail;
        return switch (rawDisposition) {
            case "TRANSIENT" -> new SpringForeignStepException.TransientFailure(
                spec.stepId(), spec.stepIndex(), message);
            case "BUSINESS_REJECTION" -> new SpringForeignStepException.BusinessRejection(
                spec.stepId(), spec.stepIndex(), SpringForeignStepException.Disposition.BUSINESS_REJECTION, message);
            case "UNCLASSIFIED" -> new SpringForeignStepException.BusinessRejection(
                spec.stepId(), spec.stepIndex(), SpringForeignStepException.Disposition.UNCLASSIFIED, message);
            default -> new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Unknown portable failure disposition '" + rawDisposition + "' for step " + spec.stepId(),
                Optional.empty());
        };
    }

    private JsonNode readStrictObject(byte[] bytes, URI targetUri) {
        if (bytes.length > spec.maxResponseBytes()) {
            throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Foreign function host response exceeded " + spec.maxResponseBytes()
                    + " byte limit for step " + spec.stepId(),
                Optional.empty());
        }
        try {
            JsonNode root = SpringPolyglotJson.strictReader().readTree(bytes);
            if (root == null || !root.isObject()) {
                throw new SpringForeignStepException.OutcomeUnknown(
                    spec.stepId(), spec.stepIndex(), targetUri.toString(),
                    "Non-object JSON response from foreign function host for step " + spec.stepId(),
                    Optional.empty());
            }
            return root;
        } catch (IOException malformed) {
            throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Malformed JSON response from foreign function host for step " + spec.stepId(),
                Optional.of(malformed));
        }
    }

    private void signHeaders(HttpRequest.Builder builder, String path, byte[] bodyForSignature) {
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            spec.sharedSecret(), "POST", path, timestamp, nonce, bodyForSignature);
        builder
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature)
            .header(STEP_ID_HEADER, spec.stepId())
            .header(STEP_INDEX_HEADER, Integer.toString(spec.stepIndex()));
    }

    private Optional<SpringForeignDispatchIntent> reserveDispatchIntent(
        Optional<SpringTransitionDispatchScope> dispatchScope,
        URI targetUri,
        byte[] digestInput
    ) {
        if (dispatchScope.isEmpty()) {
            return Optional.empty();
        }
        SpringTransitionDispatchScope scope = dispatchScope.orElseThrow();
        SpringForeignDispatchIntent intent = new SpringForeignDispatchIntent(
            scope.tenantId(),
            scope.executionId(),
            scope.executionVersion(),
            scope.attempt(),
            scope.transitionKey(),
            spec.stepIndex(),
            spec.stepId(),
            targetUri.toString(),
            SpringForeignDispatchIntent.sha256Hex(digestInput),
            System.currentTimeMillis());
        if (spec.dispatchIntentStore().reserve(intent)
            != SpringForeignDispatchIntentStore.ReservationOutcome.RESERVED) {
            throw new SpringForeignStepException.OutcomeUnknown(
                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                "Unresolved pre-send dispatch intent already exists for step " + spec.stepId()
                    + " (transitionKey=" + scope.transitionKey() + "); automatic resend suppressed",
                Optional.empty());
        }
        return Optional.of(intent);
    }

    private void settleDispatchIntent(
        Optional<SpringForeignDispatchIntent> reserved,
        SpringForeignDispatchIntentStore.SettlementStatus status
    ) {
        reserved.ifPresent(intent -> spec.dispatchIntentStore().recordSettlement(
            intent, status, System.currentTimeMillis()));
    }

    private static SpringForeignDispatchIntentStore.SettlementStatus settlementForFailure(Throwable failure) {
        if (failure instanceof SpringForeignStepException.OutcomeUnknown) {
            return SpringForeignDispatchIntentStore.SettlementStatus.UNCERTAIN;
        }
        if (failure instanceof SpringForeignStepException.TransientFailure) {
            return SpringForeignDispatchIntentStore.SettlementStatus.VALIDATED_RETRYABLE;
        }
        if (failure instanceof SpringForeignStepException) {
            return SpringForeignDispatchIntentStore.SettlementStatus.VALIDATED_TERMINAL;
        }
        return SpringForeignDispatchIntentStore.SettlementStatus.UNCERTAIN;
    }

    private static <T> CompletionStage<T> executeBlocking(java.util.function.Supplier<T> supplier) {
        try {
            return RuntimeAdapters.executeBlocking(supplier::get, true);
        } catch (IllegalStateException noAdapters) {
            CompletableFuture<T> future = new CompletableFuture<>();
            Thread.startVirtualThread(() -> {
                try {
                    future.complete(supplier.get());
                } catch (Throwable failure) {
                    future.completeExceptionally(failure);
                }
            });
            return future;
        }
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof java.util.concurrent.CompletionException ce && ce.getCause() != null) {
            return ce.getCause();
        }
        return failure;
    }

    private Socket openSocket() throws IOException {
        String host = normalizedOrigin.getHost();
        int port = normalizedOrigin.getPort();
        int connectTimeoutMs = (int) Math.min(Integer.MAX_VALUE, Math.max(1L, spec.requestTimeout().toMillis()));
        int readTimeoutMs = (int) Math.min(Integer.MAX_VALUE, Math.max(1L, spec.streamIdleTimeout().toMillis()));
        Socket raw = new Socket();
        raw.setTcpNoDelay(true);
        raw.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        raw.setSoTimeout(readTimeoutMs);
        if (!"https".equalsIgnoreCase(normalizedOrigin.getScheme())) {
            return raw;
        }
        try {
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            SSLSocket ssl = (SSLSocket) factory.createSocket(raw, host, port, true);
            SSLParameters params = ssl.getSSLParameters();
            params.setEndpointIdentificationAlgorithm("HTTPS");
            if (!isIpLiteral(host)) {
                params.setServerNames(List.of(new SNIHostName(host)));
            }
            ssl.setSSLParameters(params);
            ssl.setSoTimeout(readTimeoutMs);
            ssl.startHandshake();
            return ssl;
        } catch (IOException | RuntimeException tlsFailure) {
            closeQuietly(raw);
            throw tlsFailure;
        }
    }

    private static boolean isIpLiteral(String host) {
        if (host.contains(":")) {
            return true;
        }
        for (int i = 0; i < host.length(); i++) {
            char c = host.charAt(i);
            if ((c < '0' || c > '9') && c != '.') {
                return false;
            }
        }
        return !host.isEmpty();
    }

    private static void closeQuietly(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Best-effort cleanup.
            }
        }
    }

    private static void writeHttpChunk(OutputStream out, byte[] data) throws IOException {
        if (data.length == 0) {
            return;
        }
        out.write((Integer.toHexString(data.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(data);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private static void writeFinalHttpChunk(OutputStream out) throws IOException {
        out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private static ResponseHeaders readResponseHeaders(BufferedInputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(256);
        int state = 0;
        while (true) {
            int b = in.read();
            if (b == -1) {
                throw new IOException("Unexpected EOF while reading HTTP response headers");
            }
            buf.write(b);
            if (buf.size() > MAX_HEADER_BYTES) {
                throw new IOException("HTTP response headers exceeded " + MAX_HEADER_BYTES + " bytes");
            }
            if ((state == 0 || state == 2) && b == '\r') {
                state++;
            } else if ((state == 1 || state == 3) && b == '\n') {
                state++;
                if (state == 4) {
                    break;
                }
            } else {
                state = (b == '\r') ? 1 : 0;
            }
        }
        String raw = buf.toString(StandardCharsets.ISO_8859_1);
        String[] lines = raw.split("\r\n");
        if (lines.length == 0) {
            throw new IOException("Missing HTTP status line");
        }
        String[] parts = lines[0].split(" ");
        if (parts.length < 2) {
            throw new IOException("Malformed HTTP status line: " + lines[0]);
        }
        int statusCode;
        try {
            statusCode = Integer.parseInt(parts[1]);
        } catch (NumberFormatException badStatus) {
            throw new IOException("Invalid HTTP status code: " + parts[1], badStatus);
        }
        boolean chunked = false;
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            if (colon > 0) {
                String name = lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT);
                String value = lines[i].substring(colon + 1).trim().toLowerCase(Locale.ROOT);
                if ("transfer-encoding".equals(name) && value.contains("chunked")) {
                    chunked = true;
                }
            }
        }
        return new ResponseHeaders(statusCode, chunked);
    }

    private record ResponseHeaders(int statusCode, boolean chunked) {
    }

    private static final class EmptySubscription implements Flow.Subscription {
        @Override
        public void request(long n) {
        }

        @Override
        public void cancel() {
        }
    }

    /** Demand-driven ONE_TO_MANY NDJSON reader over JDK HttpClient. */
    private final class OneToManySubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super SpringCanonicalPayload> downstream;
        private final URI targetUri;
        private final String path;
        private final byte[] bodyBytes;
        private final Optional<SpringForeignDispatchIntent> reserved;
        private final AtomicLong demand = new AtomicLong();
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean terminated = new AtomicBoolean();
        private final AtomicBoolean readerStarted = new AtomicBoolean();
        private final Object demandLock = new Object();

        private OneToManySubscription(
            Flow.Subscriber<? super SpringCanonicalPayload> downstream,
            URI targetUri,
            String path,
            byte[] bodyBytes,
            Optional<SpringForeignDispatchIntent> reserved
        ) {
            this.downstream = downstream;
            this.targetUri = targetUri;
            this.path = path;
            this.bodyBytes = bodyBytes;
            this.reserved = reserved;
        }

        void start() {
            downstream.onSubscribe(this);
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                cancel();
                terminateError(new IllegalArgumentException("Demand must be positive: " + n));
                return;
            }
            demand.accumulateAndGet(n, (cur, add) -> {
                long sum = cur + add;
                return sum < 0 ? Long.MAX_VALUE : sum;
            });
            synchronized (demandLock) {
                demandLock.notifyAll();
            }
            if (readerStarted.compareAndSet(false, true)) {
                Thread.startVirtualThread(this::runReader);
            }
        }

        @Override
        public void cancel() {
            if (cancelled.compareAndSet(false, true)) {
                synchronized (demandLock) {
                    demandLock.notifyAll();
                }
                if (terminated.compareAndSet(false, true)) {
                    settleDispatchIntent(reserved, SpringForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
                }
            }
        }

        private void runReader() {
            HttpRequest.Builder req = HttpRequest.newBuilder(targetUri)
                .timeout(spec.requestTimeout())
                .header("Content-Type", "application/json")
                .header("Accept", "application/x-ndjson")
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes));
            signHeaders(req, path, bodyBytes);
            try {
                HttpResponse<InputStream> response = httpClient.send(req.build(), HttpResponse.BodyHandlers.ofInputStream());
                int status = response.statusCode();
                if (status == 401 || status == 403) {
                    response.body().close();
                    terminateError(new SpringForeignStepException.AuthenticationFailure(
                        spec.stepId(), spec.stepIndex(), status,
                        "Foreign function host rejected authentication for step " + spec.stepId() + " (HTTP " + status + ")"));
                    return;
                }
                if (status != 200) {
                    response.body().close();
                    terminateError(new SpringForeignStepException.OutcomeUnknown(
                        spec.stepId(), spec.stepIndex(), targetUri.toString(),
                        "Uncertain HTTP status " + status + " from ONE_TO_MANY step " + spec.stepId(),
                        Optional.empty()));
                    return;
                }
                try (InputStream body = response.body()) {
                    ByteArrayOutputStream lineBuf = new ByteArrayOutputStream();
                    boolean sawComplete = false;
                    while (!cancelled.get()) {
                        int b = body.read();
                        if (b == -1) {
                            if (lineBuf.size() > 0 && handleFrame(lineBuf.toByteArray())) {
                                sawComplete = true;
                            }
                            break;
                        }
                        if (b == '\n') {
                            byte[] line = lineBuf.toByteArray();
                            lineBuf.reset();
                            if (handleFrame(line)) {
                                sawComplete = true;
                                break;
                            }
                        } else if (b != '\r') {
                            if (lineBuf.size() >= spec.maxResponseBytes()) {
                                throw new SpringForeignStepException.OutcomeUnknown(
                                    spec.stepId(), spec.stepIndex(), targetUri.toString(),
                                    "ONE_TO_MANY NDJSON frame exceeded " + spec.maxResponseBytes() + " bytes",
                                    Optional.empty());
                            }
                            lineBuf.write(b);
                        }
                    }
                    if (!cancelled.get()) {
                        if (!sawComplete) {
                            terminateError(new SpringForeignStepException.OutcomeUnknown(
                                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                                "Truncated ONE_TO_MANY stream without complete sentinel for step " + spec.stepId(),
                                Optional.empty()));
                        } else if (terminated.compareAndSet(false, true)) {
                            settleDispatchIntent(reserved, SpringForeignDispatchIntentStore.SettlementStatus.VALIDATED_SUCCEEDED);
                            downstream.onComplete();
                        }
                    }
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                terminateError(new SpringForeignStepException.OutcomeUnknown(
                    spec.stepId(), spec.stepIndex(), targetUri.toString(),
                    "Interrupted reading ONE_TO_MANY stream", Optional.of(interrupted)));
            } catch (IOException ioError) {
                if (!cancelled.get()) {
                    terminateError(new SpringForeignStepException.OutcomeUnknown(
                        spec.stepId(), spec.stepIndex(), targetUri.toString(),
                        "Uncertain transport failure reading ONE_TO_MANY stream: " + ioError.getMessage(),
                        Optional.of(ioError)));
                }
            } catch (RuntimeException failure) {
                terminateError(failure);
            }
        }

        private boolean handleFrame(byte[] lineBytes) throws InterruptedException {
            String trimmed = new String(lineBytes, StandardCharsets.UTF_8).trim();
            if (trimmed.isEmpty()) {
                return false;
            }
            JsonNode frame = readStrictObject(trimmed.getBytes(StandardCharsets.UTF_8), targetUri);
            String event = frame.path("event").asText("");
            return switch (event) {
                case "item" -> {
                    SpringCanonicalPayload payload = validateCanonicalOutput(frame, targetUri);
                    awaitDemand();
                    if (!cancelled.get() && !terminated.get()) {
                        downstream.onNext(payload);
                    }
                    yield false;
                }
                case "complete" -> true;
                case "error" -> throw decodePortableError(frame, targetUri);
                default -> throw new SpringForeignStepException.OutcomeUnknown(
                    spec.stepId(), spec.stepIndex(), targetUri.toString(),
                    "Unknown NDJSON event '" + event + "' in ONE_TO_MANY step " + spec.stepId(),
                    Optional.empty());
            };
        }

        private void awaitDemand() throws InterruptedException {
            synchronized (demandLock) {
                while (!cancelled.get() && demand.get() <= 0) {
                    demandLock.wait(50L);
                }
                if (!cancelled.get()) {
                    demand.updateAndGet(cur -> cur == Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0L, cur - 1L));
                }
            }
        }

        private void terminateError(Throwable failure) {
            if (terminated.compareAndSet(false, true)) {
                settleDispatchIntent(reserved, settlementForFailure(failure));
                downstream.onError(failure);
            }
        }
    }

    /** Full-duplex chunked HTTP/1.1 NDJSON subscription for MANY_TO_MANY steps. */
    private final class DuplexSubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super SpringCanonicalPayload> downstream;
        private final Flow.Publisher<SpringCanonicalPayload> upstream;
        private final URI targetUri;
        private final String path;
        private final Optional<SpringForeignDispatchIntent> reserved;
        private final AtomicReference<Socket> socketRef = new AtomicReference<>();
        private final AtomicReference<Flow.Subscription> upstreamSubRef = new AtomicReference<>();
        private final AtomicLong demand = new AtomicLong();
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean terminated = new AtomicBoolean();
        private final AtomicBoolean readerStarted = new AtomicBoolean();
        private final AtomicBoolean headersSent = new AtomicBoolean();
        private final ConcurrentLinkedQueue<Runnable> writeQueue = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean writeDraining = new AtomicBoolean();
        private final Object demandLock = new Object();
        private volatile OutputStream rawOut;
        private volatile BufferedInputStream rawIn;

        private DuplexSubscription(
            Flow.Subscriber<? super SpringCanonicalPayload> downstream,
            Flow.Publisher<SpringCanonicalPayload> upstream,
            URI targetUri,
            String path,
            Optional<SpringForeignDispatchIntent> reserved
        ) {
            this.downstream = downstream;
            this.upstream = upstream;
            this.targetUri = targetUri;
            this.path = path;
            this.reserved = reserved;
        }

        void start() {
            try {
                Socket socket = openSocket();
                socketRef.set(socket);
                rawOut = socket.getOutputStream();
                rawIn = new BufferedInputStream(socket.getInputStream());
                sendRequestHeaders();
            } catch (IOException | RuntimeException failure) {
                closeSocket();
                downstream.onSubscribe(new EmptySubscription());
                terminateError(new SpringForeignStepException.OutcomeUnknown(
                    spec.stepId(), spec.stepIndex(), targetUri.toString(),
                    "Failed opening full-duplex connection for step " + spec.stepId() + ": " + failure.getMessage(),
                    Optional.of(failure)));
                return;
            }
            downstream.onSubscribe(this);
            upstream.subscribe(new UpstreamSubscriber());
        }

        private void sendRequestHeaders() throws IOException {
            if (!headersSent.compareAndSet(false, true)) {
                return;
            }
            String ts = Instant.now().toString();
            String nonce = UUID.randomUUID().toString();
            String sig = TransitionWorkerSignature.sign(
                spec.sharedSecret(), "POST", path, ts, nonce,
                STREAMING_BODY_DIGEST_SENTINEL.getBytes(StandardCharsets.UTF_8));
            StringBuilder hdr = new StringBuilder(256);
            hdr.append("POST ").append(path).append(" HTTP/1.1\r\n");
            hdr.append("Host: ").append(normalizedOrigin.getHost()).append(':').append(normalizedOrigin.getPort()).append("\r\n");
            hdr.append("Content-Type: application/x-ndjson\r\n");
            hdr.append("Accept: application/x-ndjson\r\n");
            hdr.append("Transfer-Encoding: chunked\r\n");
            hdr.append("Connection: close\r\n");
            hdr.append(TransitionWorkerSignature.TIMESTAMP_HEADER).append(": ").append(ts).append("\r\n");
            hdr.append(TransitionWorkerSignature.NONCE_HEADER).append(": ").append(nonce).append("\r\n");
            hdr.append(TransitionWorkerSignature.SIGNATURE_HEADER).append(": ").append(sig).append("\r\n");
            hdr.append(STEP_ID_HEADER).append(": ").append(spec.stepId()).append("\r\n");
            hdr.append(STEP_INDEX_HEADER).append(": ").append(spec.stepIndex()).append("\r\n");
            hdr.append("\r\n");
            rawOut.write(hdr.toString().getBytes(StandardCharsets.US_ASCII));
            rawOut.flush();
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                cancel();
                terminateError(new IllegalArgumentException("Demand must be positive: " + n));
                return;
            }
            demand.accumulateAndGet(n, (cur, add) -> {
                long sum = cur + add;
                return sum < 0 ? Long.MAX_VALUE : sum;
            });
            synchronized (demandLock) {
                demandLock.notifyAll();
            }
            if (readerStarted.compareAndSet(false, true)) {
                Thread.startVirtualThread(this::runDownstreamReader);
            }
        }

        @Override
        public void cancel() {
            if (cancelled.compareAndSet(false, true)) {
                synchronized (demandLock) {
                    demandLock.notifyAll();
                }
                Flow.Subscription up = upstreamSubRef.getAndSet(null);
                if (up != null) {
                    up.cancel();
                }
                closeSocket();
                if (terminated.compareAndSet(false, true)) {
                    settleDispatchIntent(reserved, SpringForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
                }
            }
        }

        private void enqueueWrite(Runnable writeTask) {
            writeQueue.add(writeTask);
            if (writeDraining.compareAndSet(false, true)) {
                Thread.startVirtualThread(this::drainWrites);
            }
        }

        private void drainWrites() {
            try {
                while (true) {
                    Runnable task;
                    while ((task = writeQueue.poll()) != null) {
                        if (cancelled.get() || terminated.get()) {
                            return;
                        }
                        task.run();
                    }
                    writeDraining.set(false);
                    if (writeQueue.isEmpty() || !writeDraining.compareAndSet(false, true)) {
                        return;
                    }
                }
            } finally {
                writeDraining.set(false);
            }
        }

        private void runDownstreamReader() {
            try {
                ResponseHeaders headers = readResponseHeaders(rawIn);
                if (headers.statusCode() == 401 || headers.statusCode() == 403) {
                    terminateError(new SpringForeignStepException.AuthenticationFailure(
                        spec.stepId(), spec.stepIndex(), headers.statusCode(),
                        "Foreign function host rejected authentication for step " + spec.stepId()
                            + " (HTTP " + headers.statusCode() + ")"));
                    return;
                }
                if (headers.statusCode() != 200) {
                    terminateError(new SpringForeignStepException.OutcomeUnknown(
                        spec.stepId(), spec.stepIndex(), targetUri.toString(),
                        "Uncertain HTTP status " + headers.statusCode() + " from MANY_TO_MANY step " + spec.stepId(),
                        Optional.empty()));
                    return;
                }
                boolean sawComplete = headers.chunked()
                    ? readChunkedNdjson(rawIn)
                    : readPlainNdjson(rawIn);
                if (!cancelled.get()) {
                    if (!sawComplete) {
                        terminateError(new SpringForeignStepException.OutcomeUnknown(
                            spec.stepId(), spec.stepIndex(), targetUri.toString(),
                            "Truncated MANY_TO_MANY stream without complete sentinel for step " + spec.stepId(),
                            Optional.empty()));
                    } else if (terminated.compareAndSet(false, true)) {
                        settleDispatchIntent(reserved, SpringForeignDispatchIntentStore.SettlementStatus.VALIDATED_SUCCEEDED);
                        closeSocket();
                        downstream.onComplete();
                    }
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                terminateError(new SpringForeignStepException.OutcomeUnknown(
                    spec.stepId(), spec.stepIndex(), targetUri.toString(),
                    "Interrupted reading MANY_TO_MANY stream", Optional.of(interrupted)));
            } catch (IOException ioError) {
                if (!cancelled.get()) {
                    terminateError(new SpringForeignStepException.OutcomeUnknown(
                        spec.stepId(), spec.stepIndex(), targetUri.toString(),
                        "Uncertain transport failure reading MANY_TO_MANY stream: " + ioError.getMessage(),
                        Optional.of(ioError)));
                }
            } catch (RuntimeException failure) {
                terminateError(failure);
            }
        }

        private boolean readChunkedNdjson(BufferedInputStream in) throws IOException, InterruptedException {
            ByteArrayOutputStream lineBuf = new ByteArrayOutputStream();
            while (!cancelled.get()) {
                Optional<String> sizeLineOpt = readAsciiLine(in);
                if (sizeLineOpt.isEmpty()) {
                    return false;
                }
                String hex = sizeLineOpt.orElseThrow().split(";", 2)[0].trim();
                if (hex.isEmpty()) {
                    continue;
                }
                int chunkSize;
                try {
                    chunkSize = Integer.parseInt(hex, 16);
                } catch (NumberFormatException badChunk) {
                    throw new IOException("Malformed HTTP chunk size: " + hex, badChunk);
                }
                if (chunkSize == 0) {
                    readAsciiLine(in);
                    if (lineBuf.size() > 0 && handleFrame(lineBuf.toByteArray())) {
                        return true;
                    }
                    return false;
                }
                int remaining = chunkSize;
                while (remaining > 0) {
                    int b = in.read();
                    if (b == -1) {
                        throw new IOException("Unexpected EOF inside HTTP chunk");
                    }
                    remaining--;
                    if (b == '\n') {
                        byte[] line = lineBuf.toByteArray();
                        lineBuf.reset();
                        if (handleFrame(line)) {
                            return true;
                        }
                    } else if (b != '\r') {
                        if (lineBuf.size() >= spec.maxResponseBytes()) {
                            throw new SpringForeignStepException.OutcomeUnknown(
                                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                                "MANY_TO_MANY NDJSON frame exceeded " + spec.maxResponseBytes() + " bytes",
                                Optional.empty());
                        }
                        lineBuf.write(b);
                    }
                }
                readAsciiLine(in); // trailing CRLF after chunk data
            }
            return false;
        }

        private boolean readPlainNdjson(BufferedInputStream in) throws IOException, InterruptedException {
            ByteArrayOutputStream lineBuf = new ByteArrayOutputStream();
            while (!cancelled.get()) {
                int b = in.read();
                if (b == -1) {
                    return lineBuf.size() > 0 && handleFrame(lineBuf.toByteArray());
                }
                if (b == '\n') {
                    byte[] line = lineBuf.toByteArray();
                    lineBuf.reset();
                    if (handleFrame(line)) {
                        return true;
                    }
                } else if (b != '\r') {
                    if (lineBuf.size() >= spec.maxResponseBytes()) {
                        throw new SpringForeignStepException.OutcomeUnknown(
                            spec.stepId(), spec.stepIndex(), targetUri.toString(),
                            "MANY_TO_MANY NDJSON frame exceeded " + spec.maxResponseBytes() + " bytes",
                            Optional.empty());
                    }
                    lineBuf.write(b);
                }
            }
            return false;
        }

        private Optional<String> readAsciiLine(BufferedInputStream in) throws IOException {
            ByteArrayOutputStream buf = new ByteArrayOutputStream(32);
            while (true) {
                int b = in.read();
                if (b == -1) {
                    return buf.size() == 0
                        ? Optional.empty()
                        : Optional.of(buf.toString(StandardCharsets.ISO_8859_1));
                }
                if (b == '\n') {
                    byte[] raw = buf.toByteArray();
                    int len = (raw.length > 0 && raw[raw.length - 1] == '\r') ? raw.length - 1 : raw.length;
                    return Optional.of(new String(raw, 0, len, StandardCharsets.ISO_8859_1));
                }
                buf.write(b);
            }
        }

        private boolean handleFrame(byte[] lineBytes) throws InterruptedException {
            String trimmed = new String(lineBytes, StandardCharsets.UTF_8).trim();
            if (trimmed.isEmpty()) {
                return false;
            }
            JsonNode frame = readStrictObject(trimmed.getBytes(StandardCharsets.UTF_8), targetUri);
            String event = frame.path("event").asText("");
            return switch (event) {
                case "item" -> {
                    SpringCanonicalPayload payload = validateCanonicalOutput(frame, targetUri);
                    awaitDemand();
                    if (!cancelled.get() && !terminated.get()) {
                        downstream.onNext(payload);
                    }
                    yield false;
                }
                case "complete" -> true;
                case "error" -> throw decodePortableError(frame, targetUri);
                default -> throw new SpringForeignStepException.OutcomeUnknown(
                    spec.stepId(), spec.stepIndex(), targetUri.toString(),
                    "Unknown NDJSON event '" + event + "' in MANY_TO_MANY step " + spec.stepId(),
                    Optional.empty());
            };
        }

        private void awaitDemand() throws InterruptedException {
            synchronized (demandLock) {
                while (!cancelled.get() && demand.get() <= 0) {
                    demandLock.wait(50L);
                }
                if (!cancelled.get()) {
                    demand.updateAndGet(cur -> cur == Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0L, cur - 1L));
                }
            }
        }

        private void closeSocket() {
            closeQuietly(socketRef.getAndSet(null));
        }

        private void terminateError(Throwable failure) {
            if (terminated.compareAndSet(false, true)) {
                Flow.Subscription up = upstreamSubRef.getAndSet(null);
                if (up != null) {
                    up.cancel();
                }
                closeSocket();
                settleDispatchIntent(reserved, settlementForFailure(failure));
                downstream.onError(failure);
            }
        }

        private final class UpstreamSubscriber implements Flow.Subscriber<SpringCanonicalPayload> {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                upstreamSubRef.set(subscription);
                if (cancelled.get() || terminated.get()) {
                    subscription.cancel();
                } else {
                    subscription.request(1L);
                }
            }

            @Override
            public void onNext(SpringCanonicalPayload item) {
                enqueueWrite(() -> {
                    try {
                        String canonicalJson = validateCanonicalInput(item);
                        Map<String, Object> frame = new LinkedHashMap<>();
                        frame.put("event", "item");
                        frame.put("inputTypeId", spec.inputCanonicalTypeId());
                        frame.put("payload", SpringPolyglotJson.exactMapper().readTree(canonicalJson));
                        byte[] line = (SpringPolyglotJson.exactMapper().writeValueAsString(frame) + "\n")
                            .getBytes(StandardCharsets.UTF_8);
                        writeHttpChunk(rawOut, line);
                        Flow.Subscription up = upstreamSubRef.get();
                        if (up != null && !cancelled.get() && !terminated.get()) {
                            up.request(1L);
                        }
                    } catch (IOException writeFailure) {
                        if (!cancelled.get()) {
                            terminateError(new SpringForeignStepException.OutcomeUnknown(
                                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                                "Uncertain write failure streaming MANY_TO_MANY input: " + writeFailure.getMessage(),
                                Optional.of(writeFailure)));
                        }
                    } catch (RuntimeException validationFailure) {
                        terminateError(validationFailure);
                    }
                });
            }

            @Override
            public void onError(Throwable throwable) {
                enqueueWrite(() -> {
                    try {
                        Map<String, Object> frame = new LinkedHashMap<>();
                        frame.put("event", "error");
                        frame.put("message", Optional.ofNullable(throwable.getMessage()).orElse("Upstream stream failed"));
                        byte[] line = (SpringPolyglotJson.exactMapper().writeValueAsString(frame) + "\n")
                            .getBytes(StandardCharsets.UTF_8);
                        writeHttpChunk(rawOut, line);
                        writeFinalHttpChunk(rawOut);
                    } catch (IOException ignored) {
                        // Best-effort error propagation.
                    }
                    terminateError(throwable);
                });
            }

            @Override
            public void onComplete() {
                enqueueWrite(() -> {
                    try {
                        writeHttpChunk(rawOut, "{\"event\":\"complete\"}\n".getBytes(StandardCharsets.UTF_8));
                        writeFinalHttpChunk(rawOut);
                    } catch (IOException writeFailure) {
                        if (!cancelled.get()) {
                            terminateError(new SpringForeignStepException.OutcomeUnknown(
                                spec.stepId(), spec.stepIndex(), targetUri.toString(),
                                "Failed finishing MANY_TO_MANY request stream: " + writeFailure.getMessage(),
                                Optional.of(writeFailure)));
                        }
                    }
                });
            }
        }
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static void requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}

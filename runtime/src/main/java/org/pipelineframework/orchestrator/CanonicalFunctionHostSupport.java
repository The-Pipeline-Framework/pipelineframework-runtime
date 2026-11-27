package org.pipelineframework.orchestrator;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Shared protocol, canonical validation, HMAC signing, context propagation, and pre-send
 * dispatch-intent helpers used across {@link CanonicalFunctionHostStep},
 * {@link CanonicalFunctionHostOneToManyStep}, and {@link CanonicalFunctionHostManyToOneStep}.
 */
final class CanonicalFunctionHostSupport {

    static final int DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024;
    private static final ObjectMapper JSON = PipelineJson.mapper().copy()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private CanonicalFunctionHostSupport() {
    }

    static ObjectMapper json() {
        return JSON;
    }

    static ObjectReader strictReader() {
        return JSON.reader()
            .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    }

    static HttpClient newHttpClient(Duration connectTimeout) {
        return HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(connectTimeout)
            .build();
    }

    static boolean isNonRetryableOrUnknown(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof RemoteTransitionOutcomeUnknownException
                || current instanceof NonRetryableException) {
                return true;
            }
        }
        return false;
    }

    static String validateCanonicalInput(
        TypedDurablePayload input,
        String stepId,
        int stepIndex,
        Optional<String> commandId,
        String expectedTypeId,
        String expectedDefinitionFingerprint,
        String expectedCatalogFingerprint,
        CanonicalTypeCatalogue catalogue
    ) {
        if (input == null) {
            throw nonRetryable("Foreign function step " + stepId + " received null input", stepIndex, commandId);
        }
        if (!expectedTypeId.equals(input.canonicalTypeId())) {
            throw nonRetryable(
                "Input canonical type mismatch for step " + stepId
                    + ": expected " + expectedTypeId + " but got " + input.canonicalTypeId(),
                stepIndex, commandId);
        }
        if (!expectedDefinitionFingerprint.equals(input.typeExpressionFingerprint())) {
            throw nonRetryable("Input definition fingerprint mismatch for step " + stepId, stepIndex, commandId);
        }
        if (!expectedCatalogFingerprint.equals(input.catalogFingerprint())) {
            throw nonRetryable("Input catalog fingerprint mismatch for step " + stepId, stepIndex, commandId);
        }
        if (!JsonDurablePayloadCodec.ENCODING.equals(input.encoding())
            || input.encodingVersion() != JsonDurablePayloadCodec.ENCODING_VERSION) {
            throw nonRetryable("Unsupported canonical durable encoding for step " + stepId, stepIndex, commandId);
        }
        try {
            JsonNode tree = strictReader().readTree(input.payload());
            if (tree == null || tree.isNull()) {
                throw new IllegalArgumentException("canonical payload JSON must not be empty or null");
            }
            return catalogue.validateAndCanonicalize(expectedTypeId, tree.toString());
        } catch (RuntimeException | IOException invalid) {
            throw nonRetryable(
                "Invalid canonical input payload for step " + stepId + ": " + invalid.getMessage(),
                stepIndex, commandId);
        }
    }

    static TypedDurablePayload validateCanonicalOutput(
        JsonNode root,
        String stepId,
        String expectedOutputTypeId,
        String expectedOutputFingerprint,
        String catalogFingerprint,
        CanonicalTypeCatalogue catalogue,
        String protocol,
        URI targetUri,
        long startedNanos,
        long deadlineMillis
    ) {
        String outputTypeId = root.path("outputTypeId").asText("");
        if (!expectedOutputTypeId.equals(outputTypeId)) {
            throw unknownOutcome(protocol, targetUri, startedNanos, deadlineMillis,
                "Foreign function returned unexpected outputTypeId '" + outputTypeId
                    + "' (expected '" + expectedOutputTypeId + "') for step " + stepId,
                Optional.empty());
        }
        JsonNode payloadNode = root.get("payload");
        if (payloadNode == null || payloadNode.isNull()) {
            throw unknownOutcome(protocol, targetUri, startedNanos, deadlineMillis,
                "Foreign function returned missing payload for step " + stepId,
                Optional.empty());
        }
        try {
            String canonicalOutputJson = catalogue.validateAndCanonicalize(
                expectedOutputTypeId, JSON.writeValueAsString(payloadNode));
            return new TypedDurablePayload(
                expectedOutputTypeId,
                expectedOutputFingerprint,
                catalogFingerprint,
                JsonDurablePayloadCodec.ENCODING,
                JsonDurablePayloadCodec.ENCODING_VERSION,
                canonicalOutputJson.getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException | IOException schemaViolation) {
            throw unknownOutcome(protocol, targetUri, startedNanos, deadlineMillis,
                "Foreign function output violated canonical schema for " + expectedOutputTypeId
                    + ": " + schemaViolation.getMessage(),
                Optional.of(schemaViolation));
        }
    }

    static RuntimeException decodePortableError(
        JsonNode root,
        String stepId,
        int stepIndex,
        Optional<String> commandId,
        String protocol,
        URI targetUri,
        long startedNanos,
        long deadlineMillis
    ) {
        String rawDisposition = root.path("disposition").asText("");
        CanonicalFunctionHostStep.PortableFailureDisposition disposition;
        try {
            disposition = CanonicalFunctionHostStep.PortableFailureDisposition.valueOf(rawDisposition);
        } catch (IllegalArgumentException unknown) {
            return unknownOutcome(protocol, targetUri, startedNanos, deadlineMillis,
                "Unknown portable failure disposition '" + rawDisposition + "' for step " + stepId,
                Optional.of(unknown));
        }
        String code = root.path("code").asText(disposition.name());
        String detail = root.path("message").asText("");
        String message = detail.isBlank() ? code : code + ": " + detail;
        return switch (disposition) {
            case TRANSIENT -> new TransitionWorkerFailureException(message, stepIndex, commandId);
            case BUSINESS_REJECTION, UNCLASSIFIED -> nonRetryable(message, stepIndex, commandId);
        };
    }

    static void signHeaders(
        HttpRequest.Builder builder,
        String sharedSecret,
        String path,
        byte[] bodyForSignature,
        String stepId,
        int stepIndex
    ) {
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            sharedSecret, "POST", path, timestamp, nonce, bodyForSignature);
        builder
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature)
            .header(CanonicalFunctionHostStep.STEP_ID_HEADER, stepId)
            .header(CanonicalFunctionHostStep.STEP_INDEX_HEADER, Integer.toString(stepIndex));
    }

    static Optional<ForeignDispatchIntent> reserveDispatchIntent(
        ForeignDispatchIntentStore intentStore,
        Optional<TransitionDispatchScope> dispatchScope,
        int stepIndex,
        String stepId,
        URI targetUri,
        byte[] requestDigestInput,
        String protocol,
        long startedNanos,
        long deadlineMillis
    ) {
        if (dispatchScope.isEmpty()) {
            return Optional.empty();
        }
        TransitionDispatchScope scope = dispatchScope.orElseThrow();
        ForeignDispatchIntent intent = new ForeignDispatchIntent(
            scope.tenantId(),
            scope.executionId(),
            scope.executionVersion(),
            scope.attempt(),
            scope.transitionKey(),
            stepIndex,
            stepId,
            targetUri.toString(),
            ForeignDispatchIntent.sha256Hex(requestDigestInput),
            System.currentTimeMillis());
        ForeignDispatchIntentStore.ReservationOutcome outcome = intentStore.reserve(intent);
        if (outcome != ForeignDispatchIntentStore.ReservationOutcome.RESERVED) {
            throw unknownOutcome(protocol, targetUri, startedNanos, deadlineMillis,
                "Unresolved pre-send dispatch intent already exists for step " + stepId
                    + " (transitionKey=" + scope.transitionKey() + "); automatic resend suppressed",
                Optional.empty());
        }
        return Optional.of(intent);
    }

    static void settleDispatchIntent(
        ForeignDispatchIntentStore intentStore,
        Optional<ForeignDispatchIntent> reservedIntent,
        ForeignDispatchIntentStore.SettlementStatus status
    ) {
        reservedIntent.ifPresent(intent -> intentStore.recordSettlement(intent, status, System.currentTimeMillis()));
    }

    static ForeignDispatchIntentStore.SettlementStatus settlementForFailure(Throwable failure) {
        if (failure instanceof RemoteTransitionOutcomeUnknownException) {
            return ForeignDispatchIntentStore.SettlementStatus.UNCERTAIN;
        }
        for (Throwable cur = failure; cur != null; cur = cur.getCause()) {
            if (cur instanceof NonRetryableException) {
                return ForeignDispatchIntentStore.SettlementStatus.VALIDATED_TERMINAL;
            }
        }
        if (failure instanceof TransitionWorkerFailureException) {
            return ForeignDispatchIntentStore.SettlementStatus.VALIDATED_RETRYABLE;
        }
        return ForeignDispatchIntentStore.SettlementStatus.UNCERTAIN;
    }

    static TransitionWorkerFailureException nonRetryable(String message, int stepIndex, Optional<String> commandId) {
        return new TransitionWorkerFailureException(
            message, new NonRetryableException(message), stepIndex, commandId);
    }

    static RemoteTransitionOutcomeUnknownException unknownOutcome(
        String protocol,
        URI targetUri,
        long startedNanos,
        long deadlineMillis,
        String message,
        Optional<Throwable> cause
    ) {
        long elapsedMs = Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNanos)).toMillis();
        IllegalStateException detail = cause
            .map(err -> new IllegalStateException(message, err))
            .orElseGet(() -> new IllegalStateException(message));
        return new RemoteTransitionOutcomeUnknownException(
            protocol,
            targetUri.toString(),
            elapsedMs,
            deadlineMillis,
            detail);
    }

    static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    static void requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    static JsonNode readStrictObject(byte[] bytes, int maxBytes, String protocol, URI targetUri,
        long startedNanos, long deadlineMillis, String stepId
    ) {
        if (bytes.length > maxBytes) {
            throw unknownOutcome(protocol, targetUri, startedNanos, deadlineMillis,
                "Foreign function host response exceeded " + maxBytes + " byte limit for step " + stepId,
                Optional.empty());
        }
        try {
            JsonNode root = strictReader().readTree(bytes);
            if (root == null || !root.isObject()) {
                throw unknownOutcome(protocol, targetUri, startedNanos, deadlineMillis,
                    "Non-object JSON response from foreign function host for step " + stepId,
                    Optional.empty());
            }
            return root;
        } catch (IOException malformed) {
            throw unknownOutcome(protocol, targetUri, startedNanos, deadlineMillis,
                "Malformed JSON response from foreign function host for step " + stepId,
                Optional.of(malformed));
        }
    }

    /** Immutable snapshot of TPF thread-local execution contexts and TCCL for propagation across worker pools. */
    record CapturedContext(
        Optional<PipelineExecutionContext> execution,
        Optional<PipelineContext> pipeline,
        Optional<AwaitExecutionContext> await,
        Optional<TransitionDispatchScope> dispatchScope,
        ClassLoader contextClassLoader
    ) {
        static CapturedContext capture() {
            return new CapturedContext(
                PipelineExecutionContextHolder.get(),
                Optional.ofNullable(PipelineContextHolder.get()),
                Optional.ofNullable(AwaitExecutionContextHolder.get()),
                TransitionDispatchScopeHolder.get(),
                Thread.currentThread().getContextClassLoader());
        }

        CapturedContext install() {
            CapturedContext previous = capture();
            execution.ifPresentOrElse(PipelineExecutionContextHolder::set, PipelineExecutionContextHolder::clear);
            pipeline.ifPresentOrElse(PipelineContextHolder::set, PipelineContextHolder::clear);
            await.ifPresentOrElse(AwaitExecutionContextHolder::set, AwaitExecutionContextHolder::clear);
            dispatchScope.ifPresentOrElse(TransitionDispatchScopeHolder::set, TransitionDispatchScopeHolder::clear);
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
            return previous;
        }
    }
}

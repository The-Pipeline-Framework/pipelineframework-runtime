package org.pipelineframework.orchestrator;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Standard {@link StepOneToOne} adapter that invokes a single foreign business function over
 * authenticated HTTP using class-free {@link TypedDurablePayload} canonical JSON validated
 * against {@link CanonicalTypeCatalogue}.
 *
 * <p>Pipeline execution, retry scheduling, Await suspension/resumption, pre-send dispatch-intent
 * durability, and {@link RemoteTransitionOutcomeUnknownException} handling remain owned by the
 * standard Java executor and coordinator.</p>
 */
public final class CanonicalFunctionHostStep extends ConfigurableStep
    implements StepOneToOne<TypedDurablePayload, TypedDurablePayload> {

    public static final String FUNCTION_PATH_PREFIX = "/tpf/functions/";
    public static final String STEP_ID_HEADER = "X-TPF-Step-Id";
    public static final String STEP_INDEX_HEADER = "X-TPF-Step-Index";

    private static final String PROTOCOL = "http-function-host";

    /** Portable failure disposition reported by a foreign function host. */
    public enum PortableFailureDisposition {
        TRANSIENT,
        BUSINESS_REJECTION,
        UNCLASSIFIED
    }

    /** Immutable binding for one unary foreign business-function step. */
    public record Spec(
        String stepId,
        int stepIndex,
        String inputCanonicalTypeId,
        String inputDefinitionFingerprint,
        String outputCanonicalTypeId,
        String outputDefinitionFingerprint,
        String catalogFingerprint,
        CanonicalTypeCatalogue catalogue,
        URI baseUri,
        String sharedSecret,
        Duration requestTimeout,
        Optional<String> commandId,
        ForeignDispatchIntentStore dispatchIntentStore,
        int maxResponseBytes
    ) {
        public Spec {
            CanonicalFunctionHostSupport.requireNonBlank(stepId, "stepId");
            if (stepIndex < 0) {
                throw new IllegalArgumentException("stepIndex must be non-negative");
            }
            CanonicalFunctionHostSupport.requireNonBlank(inputCanonicalTypeId, "inputCanonicalTypeId");
            CanonicalFunctionHostSupport.requireNonBlank(inputDefinitionFingerprint, "inputDefinitionFingerprint");
            CanonicalFunctionHostSupport.requireNonBlank(outputCanonicalTypeId, "outputCanonicalTypeId");
            CanonicalFunctionHostSupport.requireNonBlank(outputDefinitionFingerprint, "outputDefinitionFingerprint");
            CanonicalFunctionHostSupport.requireNonBlank(catalogFingerprint, "catalogFingerprint");
            Objects.requireNonNull(catalogue, "catalogue");
            Objects.requireNonNull(baseUri, "baseUri");
            CanonicalFunctionHostSupport.requireNonBlank(sharedSecret, "sharedSecret");
            CanonicalFunctionHostSupport.requirePositive(requestTimeout, "requestTimeout");
            commandId = Objects.requireNonNull(commandId, "commandId").filter(v -> !v.isBlank());
            Objects.requireNonNull(dispatchIntentStore, "dispatchIntentStore");
            if (maxResponseBytes <= 0) {
                throw new IllegalArgumentException("maxResponseBytes must be positive");
            }
        }

        public Spec(
            String stepId,
            int stepIndex,
            String inputCanonicalTypeId,
            String inputDefinitionFingerprint,
            String outputCanonicalTypeId,
            String outputDefinitionFingerprint,
            String catalogFingerprint,
            CanonicalTypeCatalogue catalogue,
            URI baseUri,
            String sharedSecret,
            Duration requestTimeout,
            Optional<String> commandId
        ) {
            this(stepId, stepIndex, inputCanonicalTypeId, inputDefinitionFingerprint,
                outputCanonicalTypeId, outputDefinitionFingerprint, catalogFingerprint,
                catalogue, baseUri, sharedSecret, requestTimeout, commandId,
                ForeignDispatchIntentStore.noop(), CanonicalFunctionHostSupport.DEFAULT_MAX_RESPONSE_BYTES);
        }

        public Spec withDispatchIntentStore(ForeignDispatchIntentStore store) {
            return new Spec(stepId, stepIndex, inputCanonicalTypeId, inputDefinitionFingerprint,
                outputCanonicalTypeId, outputDefinitionFingerprint, catalogFingerprint,
                catalogue, baseUri, sharedSecret, requestTimeout, commandId,
                store, maxResponseBytes);
        }
    }

    private final Spec spec;
    private final HttpClient httpClient;

    public CanonicalFunctionHostStep(Spec spec, boolean allowLoopbackHttp) {
        this(spec, allowLoopbackHttp, CanonicalFunctionHostSupport.newHttpClient(
            Objects.requireNonNull(spec, "spec").requestTimeout()));
    }

    CanonicalFunctionHostStep(Spec spec, boolean allowLoopbackHttp, HttpClient httpClient) {
        this.spec = Objects.requireNonNull(spec, "spec");
        RegisteredRestWorkerTargets.normalizeOrigin(spec.baseUri().toString(), allowLoopbackHttp);
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
    }

    public Spec spec() {
        return spec;
    }

    @Override
    public boolean shouldRetry(Throwable failure) {
        if (failure == null || CanonicalFunctionHostSupport.isNonRetryableOrUnknown(failure)) {
            return false;
        }
        return super.shouldRetry(failure);
    }

    @Override
    public Uni<TypedDurablePayload> applyOneToOne(TypedDurablePayload input) {
        CanonicalFunctionHostSupport.CapturedContext captured = CanonicalFunctionHostSupport.CapturedContext.capture();
        return Uni.createFrom().emitter(emitter ->
            Infrastructure.getDefaultWorkerPool().execute(() -> {
                CanonicalFunctionHostSupport.CapturedContext previous = captured.install();
                try {
                    emitter.complete(invokeSynchronously(input, captured.pipeline(), captured.dispatchScope()));
                } catch (Throwable failure) {
                    emitter.fail(failure);
                } finally {
                    previous.restore();
                }
            }));
    }

    private TypedDurablePayload invokeSynchronously(
        TypedDurablePayload input,
        Optional<PipelineContext> contextSnapshot,
        Optional<TransitionDispatchScope> dispatchScope
    ) {
        String normalizedInputJson = CanonicalFunctionHostSupport.validateCanonicalInput(
            input, spec.stepId(), spec.stepIndex(), spec.commandId(),
            spec.inputCanonicalTypeId(), spec.inputDefinitionFingerprint(),
            spec.catalogFingerprint(), spec.catalogue());

        String path = FUNCTION_PATH_PREFIX + spec.stepId();
        URI targetUri = spec.baseUri().resolve(path);
        byte[] bodyBytes = buildRequestBody(normalizedInputJson, contextSnapshot);
        long startedNanos = System.nanoTime();
        long deadlineMs = spec.requestTimeout().toMillis();

        Optional<ForeignDispatchIntent> reservedIntent = CanonicalFunctionHostSupport.reserveDispatchIntent(
            spec.dispatchIntentStore(), dispatchScope, spec.stepIndex(), spec.stepId(),
            targetUri, bodyBytes, PROTOCOL, startedNanos, deadlineMs);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(targetUri)
            .timeout(spec.requestTimeout())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes));
        CanonicalFunctionHostSupport.signHeaders(
            requestBuilder, spec.sharedSecret(), path, bodyBytes, spec.stepId(), spec.stepIndex());

        HttpResponse<byte[]> response;
        try {
            response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            CanonicalFunctionHostSupport.settleDispatchIntent(
                spec.dispatchIntentStore(), reservedIntent, ForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
            throw CanonicalFunctionHostSupport.unknownOutcome(PROTOCOL, targetUri, startedNanos, deadlineMs,
                "Interrupted awaiting foreign function response", Optional.of(interrupted));
        } catch (IOException transportFailure) {
            CanonicalFunctionHostSupport.settleDispatchIntent(
                spec.dispatchIntentStore(), reservedIntent, ForeignDispatchIntentStore.SettlementStatus.UNCERTAIN);
            throw CanonicalFunctionHostSupport.unknownOutcome(PROTOCOL, targetUri, startedNanos, deadlineMs,
                "Uncertain transport outcome invoking foreign function " + spec.stepId() + ": "
                    + transportFailure.getMessage(),
                Optional.of(transportFailure));
        }

        try {
            TypedDurablePayload result = decodeResponse(response, targetUri, startedNanos, deadlineMs);
            CanonicalFunctionHostSupport.settleDispatchIntent(
                spec.dispatchIntentStore(), reservedIntent,
                ForeignDispatchIntentStore.SettlementStatus.VALIDATED_SUCCEEDED);
            return result;
        } catch (RuntimeException failure) {
            CanonicalFunctionHostSupport.settleDispatchIntent(
                spec.dispatchIntentStore(), reservedIntent,
                CanonicalFunctionHostSupport.settlementForFailure(failure));
            throw failure;
        }
    }

    private byte[] buildRequestBody(String normalizedInputJson, Optional<PipelineContext> contextSnapshot) {
        try {
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("stepId", spec.stepId());
            envelope.put("stepIndex", spec.stepIndex());
            envelope.put("inputTypeId", spec.inputCanonicalTypeId());
            envelope.put("outputTypeId", spec.outputCanonicalTypeId());
            envelope.put("payload", CanonicalFunctionHostSupport.json().readTree(normalizedInputJson));
            Map<String, String> context = new LinkedHashMap<>();
            contextSnapshot.ifPresent(ctx -> {
                if (ctx.versionTag() != null && !ctx.versionTag().isBlank()) {
                    context.put("versionTag", ctx.versionTag());
                }
            });
            envelope.put("context", context);
            return CanonicalFunctionHostSupport.json().writeValueAsBytes(envelope);
        } catch (IOException failure) {
            throw new IllegalStateException("Failed serializing canonical function request for " + spec.stepId(), failure);
        }
    }

    private TypedDurablePayload decodeResponse(
        HttpResponse<byte[]> response, URI targetUri, long startedNanos, long deadlineMs
    ) {
        int status = response.statusCode();
        if (status == 401 || status == 403) {
            throw CanonicalFunctionHostSupport.nonRetryable(
                "Foreign function host rejected authentication for step " + spec.stepId() + " (HTTP " + status + ")",
                spec.stepIndex(), spec.commandId());
        }
        if (status != 200) {
            throw CanonicalFunctionHostSupport.unknownOutcome(PROTOCOL, targetUri, startedNanos, deadlineMs,
                "Uncertain HTTP status " + status + " from foreign function host for step " + spec.stepId(),
                Optional.empty());
        }
        JsonNode root = CanonicalFunctionHostSupport.readStrictObject(
            response.body(), spec.maxResponseBytes(), PROTOCOL, targetUri, startedNanos, deadlineMs, spec.stepId());
        String outcomeStatus = root.path("status").asText("");
        return switch (outcomeStatus) {
            case "OK" -> CanonicalFunctionHostSupport.validateCanonicalOutput(
                root, spec.stepId(), spec.outputCanonicalTypeId(), spec.outputDefinitionFingerprint(),
                spec.catalogFingerprint(), spec.catalogue(), PROTOCOL, targetUri, startedNanos, deadlineMs);
            case "ERROR" -> throw CanonicalFunctionHostSupport.decodePortableError(
                root, spec.stepId(), spec.stepIndex(), spec.commandId(),
                PROTOCOL, targetUri, startedNanos, deadlineMs);
            default -> throw CanonicalFunctionHostSupport.unknownOutcome(PROTOCOL, targetUri, startedNanos, deadlineMs,
                "Unknown function host status '" + outcomeStatus + "' for step " + spec.stepId(),
                Optional.empty());
        };
    }
}

package org.pipelineframework.orchestrator;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Signed, non-retrying worker client for narrowly scoped Coordinator-owned Await operations. */
@ApplicationScoped
public class RestAwaitLifecycleClient {
    @Inject PipelineOrchestratorConfig config;
    @Inject ControlPlaneSecretResolver secrets;
    @Inject org.eclipse.microprofile.context.ThreadContext requestContext;
    private volatile HttpClient http;

    public Uni<AwaitWorkerRegistrationResult> register(URI coordinator, String credentialRef, String workerId,
        String entryId, AwaitRegistrationRequest request) {
        return send(coordinator, credentialRef, RestAwaitLifecycleProtocol.REGISTRATION_PATH,
            new AwaitWorkerRegistrationRequest(workerId, entryId, request), AwaitWorkerRegistrationResult.class).invoke(result -> {
                try {
                    var record = result.creation().record();
                    requireScope(request.scope(), record);
                    if (!request.unitId().equals(record.unitId()) || !request.stepId().equals(record.stepId())
                        || request.stepIndex() != record.stepIndex()
                        || !request.itemIndex().equals(java.util.Optional.ofNullable(record.itemIndex()))
                        || !request.causationId().equals(record.causationId())
                        || !request.invocationPath().equals(record.invocationPath())) {
                        throw new IllegalStateException("Await registration response differs from the requested occurrence");
                    }
                } catch (RuntimeException invalid) { throw unknown(coordinator, System.nanoTime(), invalid); }
            });
    }

    public Uni<AwaitInteractionRecord> claimDispatch(URI coordinator, String credentialRef, String workerId,
        String entryId, AwaitTransitionScope scope, String interactionId, long expectedVersion) {
        return send(coordinator, credentialRef, RestAwaitLifecycleProtocol.DISPATCH_CLAIM_PATH,
            new AwaitDispatchClaimRequest(workerId, entryId, scope, interactionId, expectedVersion), AwaitInteractionRecord.class)
            .invoke(record -> {
                try {
                    requireScope(scope, record);
                    if (!interactionId.equals(record.interactionId()) || record.version() != expectedVersion + 1
                        || record.status() != AwaitInteractionStatus.DISPATCHING) {
                        throw new IllegalStateException("Await dispatch response does not grant the expected first dispatch");
                    }
                } catch (RuntimeException invalid) { throw unknown(coordinator, System.nanoTime(), invalid); }
            });
    }

    public Uni<AwaitDispatchReportResult> report(URI coordinator, String credentialRef, AwaitDispatchReportRequest request) {
        return send(coordinator, credentialRef, RestAwaitLifecycleProtocol.REPORT_PATH, request, AwaitDispatchReportResult.class)
            .invoke(result -> validateResponse(coordinator, () -> {
                requireScope(request.scope(), result.record());
                if (!request.interactionId().equals(result.record().interactionId())
                    || result.record().version() < request.grantedInteractionVersion()) {
                    throw new IllegalStateException("Await observation returned a different grant");
                }
            }));
    }

    public Uni<AwaitUnitRecord> prepareUnit(URI coordinator, String credentialRef, AwaitWorkerUnitRequest request) {
        return send(coordinator, credentialRef, RestAwaitLifecycleProtocol.UNIT_PREPARE_PATH, request, AwaitUnitRecord.class)
            .invoke(unit -> validateResponse(coordinator, () -> requireUnit(request, unit)));
    }

    public Uni<AwaitUnitRecord> sealUnit(URI coordinator, String credentialRef, AwaitUnitSealRequest request) {
        return send(coordinator, credentialRef, RestAwaitLifecycleProtocol.UNIT_SEAL_PATH, request, AwaitUnitRecord.class)
            .invoke(unit -> validateResponse(coordinator, () -> requireUnit(request.unit(), unit)));
    }

    public Uni<TransitionAwaitSuspension> suspension(URI coordinator, String credentialRef, AwaitWorkerUnitRequest request) {
        return send(coordinator, credentialRef, RestAwaitLifecycleProtocol.UNIT_SUSPENSION_PATH, request, TransitionAwaitSuspension.class)
            .invoke(value -> validateResponse(coordinator, () -> {
                if (!request.scope().tenantId().equals(value.tenantId())
                    || !request.scope().executionId().equals(value.executionId())
                    || !request.unitId().equals(value.unitId()) || request.stepIndex() != value.stepIndex()) {
                    throw new IllegalStateException("Await suspension returned a different occurrence");
                }
                requireUnit(request, Objects.requireNonNull(value.unit(), "Await suspension unit"));
            }));
    }

    public Uni<SerializedTransitionPayload> resume(URI coordinator, String credentialRef, AwaitWorkerUnitRequest request) {
        return send(coordinator, credentialRef, RestAwaitLifecycleProtocol.UNIT_RESUME_PATH, request, SerializedTransitionPayload.class);
    }

    private void validateResponse(URI coordinator, Runnable validation) {
        try { validation.run(); }
        catch (RuntimeException invalid) { throw unknown(coordinator, System.nanoTime(), invalid); }
    }

    private static void requireUnit(AwaitWorkerUnitRequest request, AwaitUnitRecord unit) {
        if (!request.scope().tenantId().equals(unit.tenantId()) || !request.scope().executionId().equals(unit.executionId())
            || !request.unitId().equals(unit.unitId()) || !request.stepId().equals(unit.stepId())
            || request.stepIndex() != unit.stepIndex()) {
            throw new IllegalStateException("Await unit response differs from the requested occurrence");
        }
    }

    private <T> Uni<T> send(URI coordinator, String credentialRef, String path, Object request, Class<T> resultType) {
        return Uni.createFrom().deferred(() -> {
            URI origin = requireOrigin(coordinator);
            String secret = WorkerSecretSupport.resolve(null, credentialRef, secrets, "unused", "worker credential reference");
            byte[] body;
            try { body = PipelineJson.mapper().writeValueAsBytes(request); }
            catch (java.io.IOException failure) { return Uni.createFrom().failure(failure); }
            String timestamp = Instant.now().toString();
            String nonce = UUID.randomUUID().toString();
            String signature = TransitionWorkerSignature.sign(secret, RestAwaitLifecycleProtocol.METHOD, path, timestamp, nonce, body);
            HttpRequest httpRequest = HttpRequest.newBuilder(origin.resolve(path)).timeout(config.workerRest().requestTimeout())
                .header("Content-Type", "application/json")
                .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
                .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
                .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
            long start = System.nanoTime();
            var signal = org.pipelineframework.runtime.RuntimeAdapterBootstrap.captureRequestExecutor(
                requestContext == null ? Runnable::run : requestContext.currentContextExecutor());
            return Uni.createFrom().completionStage(() -> client().sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray()))
                .onFailure().transform(failure -> unknown(origin, start, failure))
                .map(response -> {
                    if (response.statusCode() >= 500) throw unknown(origin, start,
                        new IllegalStateException("Await lifecycle response was unavailable"));
                    if (response.statusCode() < 200 || response.statusCode() >= 300) {
                        throw new Denied("Await lifecycle request denied with HTTP " + response.statusCode());
                    }
                    try {
                        T value = PipelineJson.mapper().readerFor(resultType)
                            .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                            .with(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                            .readValue(response.body());
                        if (value == null) throw new IllegalArgumentException("Missing Await lifecycle result");
                        return value;
                    } catch (java.io.IOException | IllegalArgumentException malformed) {
                        throw unknown(origin, start, malformed);
                    }
                }).emitOn(signal);
        });
    }

    private RemoteTransitionOutcomeUnknownException unknown(URI coordinator, long start, Throwable failure) {
        while (failure instanceof CompletionException && failure.getCause() != null) failure = failure.getCause();
        return new RemoteTransitionOutcomeUnknownException("await-rest", coordinator.toString(),
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), config.workerRest().requestTimeout().toMillis(), failure);
    }

    private URI requireOrigin(URI uri) {
        Objects.requireNonNull(uri, "Coordinator origin");
        boolean loopback = Set.of("127.0.0.1", "::1", "[::1]").contains(java.util.Optional.ofNullable(uri.getHost()).orElse(""));
        if (!("https".equalsIgnoreCase(uri.getScheme())
            || "http".equalsIgnoreCase(uri.getScheme()) && loopback && config.worker().allowLoopbackHttp())
            || uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null
            || !uri.getPath().isEmpty() && !uri.getPath().equals("/")) {
            throw new IllegalArgumentException("Coordinator must be a trusted HTTPS origin or enabled loopback HTTP origin");
        }
        return uri;
    }

    private HttpClient client() {
        if (http == null) synchronized (this) {
            if (http == null) http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(config.workerRest().connectTimeout()).build();
        }
        return http;
    }

    @PreDestroy void close() { if (http != null) http.close(); }

    private static void requireScope(AwaitTransitionScope scope, AwaitInteractionRecord record) {
        if (!scope.tenantId().equals(record.tenantId()) || !scope.executionId().equals(record.executionId())) {
            throw new IllegalStateException("Await lifecycle response differs from the authorized execution");
        }
    }

    private static final class Denied extends IllegalStateException {
        Denied(String message) { super(message); }
    }
}

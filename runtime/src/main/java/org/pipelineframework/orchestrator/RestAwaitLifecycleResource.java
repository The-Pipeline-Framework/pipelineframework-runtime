package org.pipelineframework.orchestrator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pipelineframework.AwaitTransitionAuthority;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Default-disabled, worker-authenticated requests for Coordinator-owned Await lifecycle actions. */
@ApplicationScoped
@Path(RestAwaitLifecycleProtocol.ROOT)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class RestAwaitLifecycleResource {
    @Inject PipelineOrchestratorConfig config;
    @Inject RegisteredRestWorkerTargets workers;
    @Inject ControlPlaneSecretResolver secrets;
    @Inject AwaitTransitionAuthority authority;
    @Inject org.eclipse.microprofile.context.ThreadContext requestContext;
    private final TransitionWorkerNonceReplayGuard nonces = new TransitionWorkerNonceReplayGuard();

    @POST @Path(RestAwaitLifecycleProtocol.REGISTRATION_RESOURCE_PATH) @Blocking
    public Uni<Response> register(@HeaderParam(TransitionWorkerSignature.TIMESTAMP_HEADER) String timestamp,
        @HeaderParam(TransitionWorkerSignature.NONCE_HEADER) String nonce,
        @HeaderParam(TransitionWorkerSignature.SIGNATURE_HEADER) String signature, byte[] body) {
        if (!enabled()) return response(Response.Status.NOT_FOUND);
        AwaitWorkerRegistrationRequest request;
        try { request = decode(body, AwaitWorkerRegistrationRequest.class); }
        catch (RuntimeException failure) { return response(Response.Status.BAD_REQUEST); }
        var signal = signalContext();
        return authenticate(request.registration().scope(), request.workerId(), request.entryId(),
            RestAwaitLifecycleProtocol.REGISTRATION_PATH, timestamp, nonce, signature, body)
            .chain(accepted -> accepted ? authority.registerForWorker(request.registration()).map(value -> Response.ok(value).build())
                : response(Response.Status.UNAUTHORIZED))
            .onFailure(IllegalArgumentException.class).recoverWithItem(() -> Response.status(Response.Status.BAD_REQUEST).build())
            .onFailure(IllegalStateException.class).recoverWithItem(() -> Response.status(Response.Status.CONFLICT).build())
            .emitOn(signal);
    }

    @POST @Path(RestAwaitLifecycleProtocol.DISPATCH_CLAIM_RESOURCE_PATH) @Blocking
    public Uni<Response> claim(@HeaderParam(TransitionWorkerSignature.TIMESTAMP_HEADER) String timestamp,
        @HeaderParam(TransitionWorkerSignature.NONCE_HEADER) String nonce,
        @HeaderParam(TransitionWorkerSignature.SIGNATURE_HEADER) String signature, byte[] body) {
        if (!enabled()) return response(Response.Status.NOT_FOUND);
        AwaitDispatchClaimRequest request;
        try { request = decode(body, AwaitDispatchClaimRequest.class); }
        catch (RuntimeException failure) { return response(Response.Status.BAD_REQUEST); }
        var signal = signalContext();
        return authenticate(request.scope(), request.workerId(), request.entryId(), RestAwaitLifecycleProtocol.DISPATCH_CLAIM_PATH,
            timestamp, nonce, signature, body)
            .chain(accepted -> accepted ? authority.claimDispatch(request.scope(), request.interactionId(), request.expectedInteractionVersion())
                .map(value -> Response.ok(value).build()) : response(Response.Status.UNAUTHORIZED))
            .onFailure(IllegalArgumentException.class).recoverWithItem(() -> Response.status(Response.Status.BAD_REQUEST).build())
            .onFailure(IllegalStateException.class).recoverWithItem(() -> Response.status(Response.Status.CONFLICT).build())
            .emitOn(signal);
    }

    @POST @Path(RestAwaitLifecycleProtocol.REPORT_RESOURCE_PATH) @Blocking
    public Uni<Response> report(@HeaderParam(TransitionWorkerSignature.TIMESTAMP_HEADER) String timestamp,
        @HeaderParam(TransitionWorkerSignature.NONCE_HEADER) String nonce,
        @HeaderParam(TransitionWorkerSignature.SIGNATURE_HEADER) String signature, byte[] body) {
        if (!enabled()) return response(Response.Status.NOT_FOUND);
        AwaitDispatchReportRequest request;
        try { request = decode(body, AwaitDispatchReportRequest.class); }
        catch (RuntimeException failure) { return response(Response.Status.BAD_REQUEST); }
        var signal = signalContext();
        return authenticate(request.scope(), request.workerId(), request.entryId(), RestAwaitLifecycleProtocol.REPORT_PATH,
            timestamp, nonce, signature, body, false)
            .chain(accepted -> accepted ? authority.report(request).map(value -> Response.ok(value).build())
                : response(Response.Status.UNAUTHORIZED))
            .onFailure(IllegalArgumentException.class).recoverWithItem(() -> Response.status(Response.Status.BAD_REQUEST).build())
            .onFailure(IllegalStateException.class).recoverWithItem(() -> Response.status(Response.Status.CONFLICT).build())
            .emitOn(signal);
    }

    @POST @Path(RestAwaitLifecycleProtocol.UNIT_PREPARE_RESOURCE_PATH) @Blocking
    public Uni<Response> prepareUnit(@HeaderParam(TransitionWorkerSignature.TIMESTAMP_HEADER) String timestamp,
        @HeaderParam(TransitionWorkerSignature.NONCE_HEADER) String nonce,
        @HeaderParam(TransitionWorkerSignature.SIGNATURE_HEADER) String signature, byte[] body) {
        if (!enabled()) return response(Response.Status.NOT_FOUND);
        AwaitWorkerUnitRequest request;
        try { request = decode(body, AwaitWorkerUnitRequest.class); }
        catch (RuntimeException failure) { return response(Response.Status.BAD_REQUEST); }
        var signal = signalContext();
        return authenticate(request.scope(), request.workerId(), request.entryId(), RestAwaitLifecycleProtocol.UNIT_PREPARE_PATH,
            timestamp, nonce, signature, body, true)
            .chain(accepted -> accepted ? authority.prepareUnit(request).map(value -> Response.ok(value).build())
                : response(Response.Status.UNAUTHORIZED))
            .onFailure(IllegalArgumentException.class).recoverWithItem(() -> Response.status(Response.Status.BAD_REQUEST).build())
            .onFailure(IllegalStateException.class).recoverWithItem(() -> Response.status(Response.Status.CONFLICT).build())
            .emitOn(signal);
    }

    @POST @Path(RestAwaitLifecycleProtocol.UNIT_SEAL_RESOURCE_PATH) @Blocking
    public Uni<Response> sealUnit(@HeaderParam(TransitionWorkerSignature.TIMESTAMP_HEADER) String timestamp,
        @HeaderParam(TransitionWorkerSignature.NONCE_HEADER) String nonce,
        @HeaderParam(TransitionWorkerSignature.SIGNATURE_HEADER) String signature, byte[] body) {
        if (!enabled()) return response(Response.Status.NOT_FOUND);
        AwaitUnitSealRequest request;
        try { request = decode(body, AwaitUnitSealRequest.class); }
        catch (RuntimeException failure) { return response(Response.Status.BAD_REQUEST); }
        var signal = signalContext();
        return authenticate(request.unit().scope(), request.unit().workerId(), request.unit().entryId(), RestAwaitLifecycleProtocol.UNIT_SEAL_PATH,
            timestamp, nonce, signature, body, false)
            .chain(accepted -> accepted ? authority.sealUnit(request).map(value -> Response.ok(value).build())
                : response(Response.Status.UNAUTHORIZED))
            .onFailure(IllegalArgumentException.class).recoverWithItem(() -> Response.status(Response.Status.BAD_REQUEST).build())
            .onFailure(IllegalStateException.class).recoverWithItem(() -> Response.status(Response.Status.CONFLICT).build())
            .emitOn(signal);
    }

    @POST @Path(RestAwaitLifecycleProtocol.UNIT_SUSPENSION_RESOURCE_PATH) @Blocking
    public Uni<Response> suspension(@HeaderParam(TransitionWorkerSignature.TIMESTAMP_HEADER) String timestamp,
        @HeaderParam(TransitionWorkerSignature.NONCE_HEADER) String nonce,
        @HeaderParam(TransitionWorkerSignature.SIGNATURE_HEADER) String signature, byte[] body) {
        if (!enabled()) return response(Response.Status.NOT_FOUND);
        AwaitWorkerUnitRequest request;
        try { request = decode(body, AwaitWorkerUnitRequest.class); }
        catch (RuntimeException failure) { return response(Response.Status.BAD_REQUEST); }
        var signal = signalContext();
        return authenticate(request.scope(), request.workerId(), request.entryId(), RestAwaitLifecycleProtocol.UNIT_SUSPENSION_PATH,
            timestamp, nonce, signature, body, false)
            .chain(accepted -> accepted ? authority.suspension(request).map(value -> Response.ok(value).build())
                : response(Response.Status.UNAUTHORIZED))
            .onFailure(IllegalArgumentException.class).recoverWithItem(() -> Response.status(Response.Status.BAD_REQUEST).build())
            .onFailure(IllegalStateException.class).recoverWithItem(() -> Response.status(Response.Status.CONFLICT).build())
            .emitOn(signal);
    }

    @POST @Path(RestAwaitLifecycleProtocol.UNIT_RESUME_RESOURCE_PATH) @Blocking
    public Uni<Response> resume(@HeaderParam(TransitionWorkerSignature.TIMESTAMP_HEADER) String timestamp,
        @HeaderParam(TransitionWorkerSignature.NONCE_HEADER) String nonce,
        @HeaderParam(TransitionWorkerSignature.SIGNATURE_HEADER) String signature, byte[] body) {
        if (!enabled()) return response(Response.Status.NOT_FOUND);
        AwaitWorkerUnitRequest request;
        try { request = decode(body, AwaitWorkerUnitRequest.class); }
        catch (RuntimeException failure) { return response(Response.Status.BAD_REQUEST); }
        var signal = signalContext();
        return authenticate(request.scope(), request.workerId(), request.entryId(), RestAwaitLifecycleProtocol.UNIT_RESUME_PATH,
            timestamp, nonce, signature, body, false)
            .chain(accepted -> accepted ? authority.resume(request).map(value -> Response.ok(value).build())
                : response(Response.Status.UNAUTHORIZED))
            .onFailure(IllegalArgumentException.class).recoverWithItem(() -> Response.status(Response.Status.BAD_REQUEST).build())
            .onFailure(IllegalStateException.class).recoverWithItem(() -> Response.status(Response.Status.CONFLICT).build())
            .emitOn(signal);
    }

    private boolean enabled() {
        return config.mode() == OrchestratorMode.QUEUE_ASYNC && RegisteredRestWorkerTargets.enabled(config);
    }

    private Uni<Boolean> authenticate(AwaitTransitionScope scope, String workerId, String entryId, String path,
        String timestamp, String nonce, String signature, byte[] body) {
        return authenticate(scope, workerId, entryId, path, timestamp, nonce, signature, body, true);
    }

    private Uni<Boolean> authenticate(AwaitTransitionScope scope, String workerId, String entryId, String path,
        String timestamp, String nonce, String signature, byte[] body, boolean live) {
        if (timestamp == null || nonce == null || nonce.isBlank() || signature == null || signature.isBlank()) {
            return Uni.createFrom().item(false);
        }
        long received;
        try { received = TransitionWorkerSignature.parseTimestamp(timestamp); }
        catch (RuntimeException malformed) { return Uni.createFrom().item(false); }
        long now = System.currentTimeMillis();
        long tolerance = Math.max(0L, config.workerRest().signatureTolerance().toMillis());
        if (received < now - tolerance || received > now + tolerance) return Uni.createFrom().item(false);
        return Uni.createFrom().deferred(() -> live ? workers.lifecycleCredentialRef(scope, workerId, entryId)
            : workers.lifecycleReportCredentialRef(scope, workerId, entryId)).map(reference -> {
            String secret = WorkerSecretSupport.resolve(null, reference, secrets, "unused", "retained worker credential reference");
            String expected = TransitionWorkerSignature.sign(secret, RestAwaitLifecycleProtocol.METHOD, path, timestamp, nonce, body);
            if (!TransitionWorkerSignature.matches(expected, signature)) return false;
            String scopedNonce = scope.tenantId().length() + ":" + scope.tenantId() + workerId.length() + ":" + workerId
                + entryId.length() + ":" + entryId + nonce;
            return nonces.accept(scopedNonce, received, now, tolerance);
        }).onFailure().recoverWithItem(false);
    }

    private java.util.concurrent.Executor signalContext() {
        return org.pipelineframework.runtime.RuntimeAdapterBootstrap.captureRequestExecutor(
            requestContext == null ? Runnable::run : requestContext.currentContextExecutor());
    }

    private static <T> T decode(byte[] body, Class<T> type) {
        try {
            T value = PipelineJson.mapper().readerFor(type).with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .with(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .readValue(body == null ? new byte[0] : body);
            if (value == null) throw new IllegalArgumentException("Request is required");
            return value;
        } catch (java.io.IOException failure) { throw new IllegalArgumentException("Malformed Await lifecycle request", failure); }
    }

    private static Uni<Response> response(Response.Status status) { return Uni.createFrom().item(Response.status(status).build()); }
}

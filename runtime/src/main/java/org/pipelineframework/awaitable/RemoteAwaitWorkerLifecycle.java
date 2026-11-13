package org.pipelineframework.awaitable;

import java.net.URI;
import java.util.Optional;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.*;

/** Typed worker adapter for operation-scoped Coordinator authority; it owns no durable store. */
@ApplicationScoped
public class RemoteAwaitWorkerLifecycle {
    @Inject PipelineOrchestratorConfig config;
    @Inject PipelineReleaseIdentityResolver identity;
    @Inject TransitionPayloadCodec framing;
    @Inject RestAwaitLifecycleClient client;

    public boolean enabled() {
        return Optional.ofNullable(config).map(PipelineOrchestratorConfig::worker)
            .map(PipelineOrchestratorConfig.WorkerConfig::awaitAuthority)
            .flatMap(PipelineOrchestratorConfig.WorkerAwaitAuthorityConfig::baseUrl).isPresent();
    }

    public boolean supported() {
        if (!enabled()) return false;
        try {
            var settings = config.worker().awaitAuthority();
            if (settings.workerId().isEmpty() || settings.entryId().isEmpty()
                || config.workerRest().sharedSecretRef().isEmpty()) return false;
            var contract = identity.compiledContract().orElseThrow();
            for (var step : contract.steps()) {
                if (step.deferredCompletion().isEmpty()) continue;
                Object raw = step.deferredCompletion().get("runtimeDescriptor");
                if (!(raw instanceof Map<?,?> metadata) || !(metadata.get("stepId") instanceof String stepId)) return false;
                PinnedAwaitDescriptor.resolve(contract, stepId);
            }
            return true;
        } catch (RuntimeException unavailable) { return false; }
    }

    public Uni<AwaitWorkerRegistrationResult> register(AwaitCompletionDescriptor descriptor,
        AwaitExecutionContext context, String unitId, String causationId, Optional<Integer> itemIndex,
        Object request, String assignee, String group) {
        return Uni.createFrom().deferred(() -> {
            var scope = requireScope(context);
            var contract = identity.compiledContract().orElseThrow();
            var pinned = PinnedAwaitDescriptor.resolve(contract, descriptor.stepId());
            var codec = new CompiledCanonicalTransitionPayloadCodec(framing, contract,
                pinned.canonicalInputTypeId(), pinned.canonicalInputTypeId());
            var registration = new AwaitRegistrationRequest(scope, descriptor.stepId(), context.currentStepIndex(),
                context.identityScope(), unitId, causationId, itemIndex, codec.encode(request),
                Optional.ofNullable(assignee).filter(value -> !value.isBlank()),
                Optional.ofNullable(group).filter(value -> !value.isBlank()), context.invocationPathForAwait());
            var settings = config.worker().awaitAuthority();
            return client.register(URI.create(settings.baseUrl().orElseThrow()), secretRef(),
                settings.workerId().orElseThrow(), settings.entryId().orElseThrow(), registration);
        });
    }

    public Uni<AwaitInteractionRecord> claim(AwaitExecutionContext context, AwaitInteractionRecord record) {
        var settings = config.worker().awaitAuthority();
        return client.claimDispatch(URI.create(settings.baseUrl().orElseThrow()), secretRef(),
            settings.workerId().orElseThrow(), settings.entryId().orElseThrow(), requireScope(context),
            record.interactionId(), record.version());
    }

    public Object canonicalRequest(AwaitCompletionDescriptor descriptor, AwaitInteractionRecord record) {
        var contract = identity.compiledContract().orElseThrow();
        var pinned = PinnedAwaitDescriptor.resolve(contract, descriptor.stepId());
        return new CompiledCanonicalTransitionPayloadCodec(framing, contract,
            pinned.canonicalInputTypeId(), pinned.canonicalInputTypeId()).decode(framing.encode(record.requestPayload()));
    }

    public Uni<AwaitDispatchReportResult> report(AwaitExecutionContext context, AwaitInteractionRecord grant,
        AwaitDispatchReportKind kind, Map<String,Object> metadata, Optional<String> failure,
        Optional<CommandDispatchSettlement> settlement, Optional<Object> acknowledgement) {
        var settings = config.worker().awaitAuthority();
        var request = new AwaitDispatchReportRequest(settings.workerId().orElseThrow(), settings.entryId().orElseThrow(),
            requireScope(context), grant.interactionId(), grant.version(), kind, metadata, failure, settlement,
            acknowledgement.map(framing::encode));
        return client.report(URI.create(settings.baseUrl().orElseThrow()), secretRef(), request);
    }

    public Uni<AwaitUnitRecord> seal(AwaitExecutionContext context, String unitId, int expectedItems) {
        return client.sealUnit(endpoint(), secretRef(), new AwaitUnitSealRequest(unitRequest(context, unitId), expectedItems));
    }

    public Uni<Object> resume(AwaitExecutionContext context, String unitId) {
        var request = unitRequest(context, unitId);
        var contract = identity.compiledContract().orElseThrow();
        var pinned = PinnedAwaitDescriptor.resolve(contract, request.stepId());
        var codec = new CompiledCanonicalTransitionPayloadCodec(framing, contract,
            pinned.canonicalOutputTypeId(), pinned.canonicalOutputTypeId());
        return client.resume(endpoint(), secretRef(), request).map(codec::decode);
    }

    public Uni<TransitionAwaitSuspension> suspension(AwaitSuspendedException suspended, AwaitTransitionScope scope) {
        var occurrence = AwaitUnitOccurrence.fromUnitId(suspended.unitId()).orElseThrow();
        var settings = config.worker().awaitAuthority();
        var request = new AwaitWorkerUnitRequest(settings.workerId().orElseThrow(), settings.entryId().orElseThrow(), scope,
            occurrence.stepId(), occurrence.stepIndex(), occurrence.identityScope(), suspended.unitId(), Optional.empty());
        return client.suspension(endpoint(), secretRef(), request);
    }

    public Object canonicalCompletion(AwaitInteractionRecord record) {
        var contract = identity.compiledContract().orElseThrow();
        var pinned = PinnedAwaitDescriptor.resolve(contract, record.stepId());
        return new CompiledCanonicalTransitionPayloadCodec(framing, contract,
            pinned.canonicalOutputTypeId(), pinned.canonicalOutputTypeId()).decode(framing.encode(record.responsePayload()));
    }

    private AwaitWorkerUnitRequest unitRequest(AwaitExecutionContext context, String unitId) {
        var occurrence = AwaitUnitOccurrence.fromUnitId(unitId).orElseThrow();
        var settings = config.worker().awaitAuthority();
        return new AwaitWorkerUnitRequest(settings.workerId().orElseThrow(), settings.entryId().orElseThrow(),
            requireScope(context), occurrence.stepId(), occurrence.stepIndex(), occurrence.identityScope(), unitId,
            context.invocationPathForAwait());
    }

    private URI endpoint() { return URI.create(config.worker().awaitAuthority().baseUrl().orElseThrow()); }

    private String secretRef() {
        return config.workerRest().sharedSecretRef().orElseThrow(() ->
            new IllegalStateException("Remote Await authority requires a worker credential reference"));
    }

    private AwaitTransitionScope requireScope(AwaitExecutionContext context) {
        var scope = context.transitionScope().orElseThrow(() -> new IllegalStateException("Await transition scope is absent"));
        if (!identity.pipelineId(config).equals(scope.pipelineId()) || !identity.contractVersion().equals(scope.contractVersion())
            || !identity.releaseVersion(config).equals(scope.releaseVersion())) {
            throw new IllegalStateException("Await transition differs from the typed worker Release");
        }
        return scope;
    }
}

package org.pipelineframework.orchestrator;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;
import org.pipelineframework.orchestrator.worker.*;

/** Environment-owned REST bindings shared by admission checks and command-time routing. */
@ApplicationScoped
public class RegisteredRestWorkerTargets implements PipelineTransitionWorker {
    private static final org.jboss.logging.Logger LOG = org.jboss.logging.Logger.getLogger(RegisteredRestWorkerTargets.class);
    @Inject PipelineOrchestratorConfig config;
    @Inject PipelineReleaseRegistry releases;
    @Inject PipelineWorkerRegistry workers;
    @Inject RestPipelineTransitionWorker rest;
    @Inject PipelineContractDescriptorLoader contractLoader;

    public static boolean enabled(PipelineOrchestratorConfig config) {
        return Optional.ofNullable(config).map(PipelineOrchestratorConfig::worker)
            .map(PipelineOrchestratorConfig.WorkerConfig::targetingMode)
            .filter("registered-rest"::equals).isPresent();
    }

    public void validateConfiguration() {
        String mode = Optional.ofNullable(config.worker().targetingMode()).orElse("legacy");
        if (!Set.of("legacy", "registered-rest").contains(mode)) {
            throw new IllegalArgumentException("Unsupported worker targeting mode");
        }
        if (!enabled(config)) return;
        if (config.workerRest().isEnabled() || config.workerGrpc().isEnabled() || config.workerSqs().isEnabled()) {
            throw new IllegalArgumentException("Registered REST targeting cannot be combined with fixed remote targets");
        }
        if (!RestTransitionWorkerProtocol.EXECUTE_PATH.equals(config.workerRest().path())
            || !RestTransitionWorkerProtocol.CAPABILITIES_PATH.equals(config.workerRest().capabilitiesPath())) {
            throw new IllegalArgumentException("Registered REST targeting requires native worker routes");
        }
        if (config.worker().targets().isEmpty()) throw new IllegalArgumentException("Registered REST targets are required");
        Set<List<String>> keys = new HashSet<>();
        for (var target : config.worker().targets().values()) {
            List<String> key = List.of(text(target.tenantId()), text(target.pipelineId()), text(target.workerId()));
            if (!keys.add(key)) throw new IllegalArgumentException("Duplicate scoped registered worker binding");
            text(target.contractVersion()); text(target.releaseVersion()); text(target.artifactId());
            text(target.artifactDigest()); text(target.sharedSecretRef());
            if (!"rest".equals(target.protocol())) throw new IllegalArgumentException("Registered targeting supports REST only");
            endpoint(target.endpoint());
        }
    }

    private static String text(String value) {
        if (value == null || value.isBlank() || !value.equals(value.trim())) {
            throw new IllegalArgumentException("Registered worker binding fields must be nonblank canonical values");
        }
        return value;
    }

    private URI endpoint(String value) {
        URI uri;
        try { uri = URI.create(value).normalize(); }
        catch (IllegalArgumentException failure) { throw new IllegalArgumentException("Invalid registered worker endpoint"); }
        boolean loopback = Set.of("127.0.0.1", "[::1]", "::1").contains(Optional.ofNullable(uri.getHost()).orElse(""));
        boolean secure = "https".equalsIgnoreCase(uri.getScheme());
        boolean local = "http".equalsIgnoreCase(uri.getScheme()) && loopback && config.worker().allowLoopbackHttp();
        if ((!secure && !local) || uri.getHost() == null || uri.getUserInfo() != null
            || uri.getQuery() != null || uri.getFragment() != null
            || (!uri.getPath().isEmpty() && !"/".equals(uri.getPath()))) {
            throw new IllegalArgumentException("Registered worker endpoint must be a trusted HTTPS origin or explicit loopback HTTP origin");
        }
        String host = uri.getHost().toLowerCase(java.util.Locale.ROOT);
        int port = uri.getPort() < 0 ? (secure ? 443 : 80) : uri.getPort();
        if (port < 1 || port > 65535) throw new IllegalArgumentException("Invalid registered worker endpoint port");
        return URI.create(uri.getScheme().toLowerCase(java.util.Locale.ROOT) + "://" + host + ":" + port);
    }

    public void validateRegistration(PipelineWorkerRegistration registration) {
        if (!enabled(config)) return;
        validateConfiguration();
        boolean bound = config.worker().targets().values().stream().anyMatch(target ->
            target.tenantId().equals(registration.tenantId()) && target.pipelineId().equals(registration.pipelineId())
                && target.workerId().equals(registration.workerId()) && target.contractVersion().equals(registration.contractVersion())
                && target.releaseVersion().equals(registration.releaseVersion()) && target.protocol().equals(registration.protocol())
                && target.artifactId().equals(registration.artifactId()) && target.artifactDigest().equals(registration.artifactDigest())
                && endpoint(target.endpoint()).equals(endpoint(registration.endpoint())));
        if (!bound) throw new IllegalArgumentException("Worker registration does not match a server-owned target binding");
    }

    public Uni<PipelineWorkerAvailabilityResult> check(PipelineWorkerAvailabilityRequest request) {
        return resolve(request).map(selected -> PipelineWorkerAvailabilityResult.available("rest", selected.capability()))
            .onFailure().recoverWithItem(failure -> PipelineWorkerAvailabilityResult.unavailable("rest", "No verified registered REST worker available"));
    }

    @Override public String providerName() { return "rest"; }

    @Override public Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command) {
        var request = new PipelineWorkerAvailabilityRequest(command.tenantId(), command.pipelineId(),
            command.contractVersion(), command.releaseVersion(), "", "");
        // Never recover an execution failure by dispatching to another replica: the remote outcome may be unknown.
        // Capability encodings describe the wire envelope, not the command's encoded application payload.
        return resolve(request).chain(selected -> rest.executeTransition(command,
            endpoint(selected.target().endpoint()), selected.target().sharedSecretRef()));
    }

    private Uni<Selection> resolve(PipelineWorkerAvailabilityRequest request) {
        return Uni.createFrom().deferred(() -> {
            validateConfiguration();
            return releases.get(request.tenantId(), request.pipelineId(), request.releaseVersion()).chain(optional -> {
                PipelineReleaseRecord release = optional.orElseThrow(() -> new IllegalArgumentException("Pinned Release is unavailable"));
                if (!release.contractVersion().equals(request.contractVersion())) throw new IllegalArgumentException("Pinned Contract does not match Release");
                validateCompiledContract(release);
                PipelineReleaseArtifactDescriptor runnable = wholeBundle(release);
                // Admission callers may identify the Compiled Truth carrier, which need not be the runnable artifact.
                if ((!request.artifactId().isBlank() || !request.artifactDigest().isBlank())
                    && release.descriptor().artifacts().stream().noneMatch(artifact ->
                    artifact.artifactId().equals(request.artifactId()) && artifact.digest().equals(request.artifactDigest()))) {
                    throw new IllegalArgumentException("Requested artifact is not part of pinned Release");
                }
                return workers.list(request.tenantId(), request.pipelineId(), System.currentTimeMillis(),
                    PipelineReleaseRuntimeBeans.workerStaleAfter(config)).chain(records -> {
                        var eligible = config.worker().targets().entrySet().stream().sorted(Map.Entry.comparingByKey())
                            .map(Map.Entry::getValue).filter(target -> target.tenantId().equals(request.tenantId())
                                && target.pipelineId().equals(request.pipelineId()) && target.contractVersion().equals(request.contractVersion())
                                && target.releaseVersion().equals(request.releaseVersion()) && target.artifactId().equals(runnable.artifactId())
                                && target.artifactDigest().equals(runnable.digest()))
                            .filter(target -> records.stream().anyMatch(record -> eligibleRecord(record, target)))
                            .toList();
                        return probe(eligible, 0);
                    });
            });
        });
    }

    /** Guard coordination paths before any local terminal materialization shortcut. */
    public Uni<Void> validateExecutionPin(ExecutionRecord<?, ?> execution) {
        return releases.get(execution.tenantId(), execution.pipelineId(), execution.releaseVersion()).invoke(optional -> {
            var release = optional.orElseThrow(() -> new IllegalStateException("Pinned Release is unavailable"));
            if (!release.contractVersion().equals(execution.contractVersion())) throw new IllegalStateException("Pinned Contract is unavailable");
            validateCompiledContract(release);
            wholeBundle(release);
        }).replaceWithVoid();
    }

    private void validateCompiledContract(PipelineReleaseRecord release) {
        var compiled = contractLoader.load().orElseThrow(() -> new IllegalStateException("Generated Coordinator Contract is unavailable"));
        if (!compiled.equals(release.contract())) {
            throw new IllegalStateException("Registered REST targeting requires the Coordinator's generated Contract and layout; differing Contracts are unsupported");
        }
    }

    private PipelineReleaseArtifactDescriptor wholeBundle(PipelineReleaseRecord release) {
        Set<String> steps = release.contract().steps().stream().map(PipelineBundleStepDescriptor::authoredName).collect(Collectors.toSet());
        Set<String> capabilities = new HashSet<>(release.contract().capabilities().transitionWorkerProtocols());
        if (release.contract().capabilities().localTransitionExecution()) capabilities.add("local");
        List<PipelineReleaseArtifactDescriptor> candidates = release.descriptor().artifacts().stream()
            .filter(artifact -> "application-archive".equals(artifact.kind()) && !steps.isEmpty()
                && artifact.stepIds().containsAll(steps) && artifact.capabilities().containsAll(capabilities)).toList();
        if (candidates.size() != 1) throw new IllegalArgumentException("Registered REST targeting requires one whole-bundle application archive; distributed placement is unsupported");
        return candidates.getFirst();
    }

    private boolean eligibleRecord(PipelineWorkerRecord record, PipelineOrchestratorConfig.RegisteredRestTargetConfig target) {
        return record.state() == PipelineWorkerState.HEALTHY && record.tenantId().equals(target.tenantId())
            && record.pipelineId().equals(target.pipelineId()) && record.contractVersion().equals(target.contractVersion())
            && record.releaseVersion().equals(target.releaseVersion()) && record.workerId().equals(target.workerId())
            && record.protocol().equals(target.protocol()) && record.artifactId().equals(target.artifactId())
            && record.artifactDigest().equals(target.artifactDigest()) && normalizedRecordEndpoint(record.endpoint()).filter(endpoint(target.endpoint())::equals).isPresent();
    }

    private Optional<URI> normalizedRecordEndpoint(String value) {
        try { return Optional.of(endpoint(value)); }
        catch (IllegalArgumentException failure) { return Optional.empty(); }
    }

    private Uni<Selection> probe(List<PipelineOrchestratorConfig.RegisteredRestTargetConfig> targets, int index) {
        if (index >= targets.size()) return Uni.createFrom().failure(new IllegalStateException("No verified registered REST worker available"));
        var target = targets.get(index);
        return rest.capabilities(endpoint(target.endpoint()), target.sharedSecretRef()).chain(capability -> {
            if (!PipelineWorkerCapability.PROTOCOL_VERSION.equals(capability.protocolVersion())
                || !"rest".equals(capability.providerName()) || !capability.workerProtocols().contains("rest")
                || !capability.payloadEncodings().contains(TransitionPayloadEncoding.JSON)
                || !target.pipelineId().equals(capability.pipelineId()) || !target.contractVersion().equals(capability.contractVersion())
                || !target.releaseVersion().equals(capability.releaseVersion()) || !target.artifactId().equals(capability.artifactId())
                || !target.artifactDigest().equals(capability.artifactDigest())) {
                return Uni.createFrom().<Selection>failure(new IllegalStateException("Registered REST worker capability is unverified"));
            }
            return workers.list(target.tenantId(), target.pipelineId(), System.currentTimeMillis(),
                PipelineReleaseRuntimeBeans.workerStaleAfter(config)).map(records -> {
                    if (records.stream().noneMatch(record -> eligibleRecord(record, target))) throw new IllegalStateException("Registered worker became unavailable");
                    return new Selection(target, capability);
                });
        }).onFailure().invoke(failure -> LOG.warnf("Registered REST capability probe failed for tenant=%s worker=%s (%s)",
            target.tenantId(), target.workerId(), failure.getClass().getName()))
            .onFailure().recoverWithUni(failure -> probe(targets, index + 1));
    }

    private record Selection(PipelineOrchestratorConfig.RegisteredRestTargetConfig target, PipelineWorkerCapability capability) {}
}

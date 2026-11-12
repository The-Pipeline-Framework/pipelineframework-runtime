package org.pipelineframework.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Objects;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding.Attempt;

/** Immutable identity of an explicitly paired, possibly dispatched transition. */
public final class PairedDispatchIntent {
    static final String UNKNOWN_CODE = "REMOTE_OUTCOME_UNKNOWN";
    static final String UNKNOWN_DIAGNOSTIC = "The paired worker may have executed without a validated result";

    private final String owner;
    private final TransitionCommandEnvelope command;
    private final String evidence;
    private final String digest;

    private PairedDispatchIntent(Attempt attempt, String owner) {
        Objects.requireNonNull(attempt, "attempt");
        if (owner == null || owner.isBlank()) {
            throw new IllegalArgumentException("Paired dispatch requires a claimed lease owner");
        }
        this.owner = owner;
        this.command = attempt.command();
        if (!command.transitionKey().equals(command.executionId() + ":" + command.currentStepIndex()
                + ":" + command.attempt())) {
            throw new IllegalArgumentException("Paired dispatch requires the claimed whole-bundle transition key");
        }
        var target = attempt.target();
        var truth = attempt.compiledTruth();
        try {
            var json = PipelineJson.mapper();
            String commandDigest = NativeWorkerArchive.sha256(json.writeValueAsBytes(command));
            var snapshot = new Evidence(1, command.tenantId(), command.executionId(), command.executionVersion(),
                owner, command.transitionKey(), command.pipelineId(), command.contractVersion(), command.releaseVersion(),
                command.attempt(), command.currentStepIndex(), command.stopBeforeStepIndex(), command.resultShape(),
                commandDigest, target.workerId(), target.endpoint().toASCIIString(), target.sharedSecretRef(),
                target.language(), target.descriptorDigest(), target.runnableArtifactId(), target.runnableArtifactDigest(),
                truth.artifactId(), truth.digest(), attempt.projectionDigest(), target.profiles(),
                attempt.expectedInputTypeId(), attempt.expectedOutputTypeId());
            byte[] bytes = json.writeValueAsBytes(snapshot);
            this.evidence = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            this.digest = NativeWorkerArchive.sha256(bytes);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Cannot encode paired dispatch intent", failure);
        }
    }

    public static PairedDispatchIntent from(Attempt attempt, String owner) {
        return new PairedDispatchIntent(attempt, owner);
    }

    TransitionCommandEnvelope command() {
        return command;
    }

    public String transitionKey() {
        return command.transitionKey();
    }

    String owner() {
        return owner;
    }

    String evidence() {
        return evidence;
    }

    String digest() {
        return digest;
    }

    String storageKey() {
        try {
            byte[] scope = PipelineJson.mapper().writeValueAsBytes(
                List.of(command.tenantId(), command.executionId(), command.executionVersion()));
            // Distinct from the length-prefixed execution-key and native admission namespaces.
            return "paired-dispatch:v1:" + NativeWorkerArchive.sha256(scope);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Cannot encode paired dispatch identity", failure);
        }
    }

    boolean matchesLease(ExecutionRecord<?, ?> execution, long now) {
        return execution != null && execution.status() == ExecutionStatus.RUNNING
            && execution.version() == command.executionVersion() && owner.equals(execution.leaseOwner())
            && execution.leaseExpiresEpochMs() > now;
    }

    private record Evidence(int schemaVersion, String tenantId, String executionId, long version, String leaseOwner,
        String transitionKey, String pipelineId, String contractVersion, String releaseVersion, int attempt,
        int currentStepIndex, int stopBeforeStepIndex, ExecutionResultShape resultShape, String commandDigest,
        String workerId, String endpoint, String sharedSecretRef, NativeWorkerReleaseBinding.Language language,
        String descriptorDigest, String runnableArtifactId, String runnableArtifactDigest, String compiledTruthArtifactId,
        String compiledTruthArtifactDigest, String projectionDigest, NativeWorkerReleaseBinding.Profiles profiles,
        String inputTypeId, String outputTypeId) {}
}

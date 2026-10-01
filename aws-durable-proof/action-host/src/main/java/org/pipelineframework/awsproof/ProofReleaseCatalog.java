package org.pipelineframework.awsproof;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptorValidator;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineReleaseStatus;

/** Registers the proof's generated release in the shared durable release catalogue before admission. */
@ApplicationScoped
final class ProofReleaseCatalog {
    private static final String ARTIFACT_ID = "proof-action-host";
    private static final String ARTIFACT_URI =
        "maven:org.pipelineframework:pipelineframework-aws-durable-proof-action-host:26.9.4-SNAPSHOT";

    @Inject
    PipelineReleaseRegistry releases;

    @Inject
    PipelineReleaseIdentityResolver identity;

    Uni<Void> ensureRegistered(
        String tenantId,
        String pipelineId,
        String contractVersion,
        String releaseVersion
    ) {
        var contract = identity.contract();
        if (!contract.pipelineId().equals(pipelineId) || !contract.contractVersion().equals(contractVersion)) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                "proof release coordinates do not match the generated contract"));
        }
        long now = System.currentTimeMillis();
        String artifactDigest = requiredEnvironment("TPF_PROOF_ACTION_ARTIFACT_DIGEST");
        var descriptor = releaseDescriptor(pipelineId, contractVersion, releaseVersion, artifactDigest);
        new PipelineReleaseDescriptorValidator().validate(descriptor, contract);
        var record = new PipelineReleaseRecord(
            tenantId,
            pipelineId,
            contractVersion,
            releaseVersion,
            PipelineReleaseStatus.ACTIVE,
            descriptor,
            ARTIFACT_ID,
            artifactDigest,
            ARTIFACT_URI,
            0L,
            artifactDigest,
            contract,
            now,
            now,
            now);
        return releases.register(record)
            .onItem().transformToUni(registered -> releases.activate(
                registered.tenantId(), registered.pipelineId(), registered.releaseVersion(), now))
            .onItem().transformToUni(activated -> activated.isPresent()
                ? Uni.createFrom().voidItem()
                : Uni.createFrom().failure(new IllegalStateException("proof release activation was not retained")));
    }

    static PipelineReleaseDescriptor releaseDescriptor(
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        String artifactDigest
    ) {
        var artifact = new PipelineReleaseArtifactDescriptor(
            ARTIFACT_ID,
            "lambda-zip",
            ARTIFACT_URI,
            artifactDigest,
            List.of("ProofAwaitApproval"),
            List.of("local", "SQS"));
        return new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            pipelineId,
            contractVersion,
            releaseVersion,
            ARTIFACT_ID,
            List.of(artifact));
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required");
        }
        return value;
    }
}

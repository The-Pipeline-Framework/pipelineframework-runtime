package org.pipelineframework.orchestrator.release;

import java.security.MessageDigest;
import java.util.HexFormat;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Existing immutable Release/Compiled Truth evidence, not a new semantic Release identity. */
public record PipelineReleaseEvidence(
    PipelineReleaseDescriptor descriptor,
    PipelineContractDescriptor contract,
    String primaryArtifactId,
    String primaryArtifactDigest,
    String primaryArtifactUri,
    long primaryArtifactSizeBytes,
    String primaryArtifactChecksum,
    String metadataFingerprint) {

    public PipelineReleaseEvidence {
        java.util.Objects.requireNonNull(descriptor, "descriptor");
        Snapshot snapshot = new Snapshot(descriptor, contract, primaryArtifactId, primaryArtifactDigest,
            primaryArtifactUri, primaryArtifactSizeBytes, primaryArtifactChecksum);
        if (metadataFingerprint == null || !metadataFingerprint.equals(fingerprint(snapshot))) {
            throw new IllegalArgumentException("Immutable Release evidence fingerprint mismatch");
        }
    }

    static PipelineReleaseEvidence from(PipelineReleaseRecord release) {
        Snapshot snapshot = new Snapshot(release.descriptor(), release.contract(), release.primaryArtifactId(),
            release.primaryArtifactDigest(), release.primaryArtifactUri(), release.primaryArtifactSizeBytes(),
            release.primaryArtifactChecksum());
        return new PipelineReleaseEvidence(snapshot.descriptor(), snapshot.contract(), snapshot.primaryArtifactId(),
            snapshot.primaryArtifactDigest(), snapshot.primaryArtifactUri(), snapshot.primaryArtifactSizeBytes(),
            snapshot.primaryArtifactChecksum(), fingerprint(snapshot));
    }

    private static String fingerprint(Snapshot snapshot) {
        try {
            byte[] bytes = PipelineJson.mapper().writer()
                .with(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .writeValueAsBytes(snapshot);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception failure) {
            throw new IllegalStateException("Unable to fingerprint immutable registered Release metadata", failure);
        }
    }

    private record Snapshot(PipelineReleaseDescriptor descriptor, PipelineContractDescriptor contract,
        String primaryArtifactId, String primaryArtifactDigest, String primaryArtifactUri,
        long primaryArtifactSizeBytes, String primaryArtifactChecksum) { }
}

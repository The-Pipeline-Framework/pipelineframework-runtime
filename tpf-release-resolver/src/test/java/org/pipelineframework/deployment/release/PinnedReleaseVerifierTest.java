package org.pipelineframework.deployment.release;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;

class PinnedReleaseVerifierTest {
    @TempDir Path temp;

    @Test
    void rejectsReplacementWithSameModelBeforeOpeningArtifacts() throws Exception {
        Path descriptor = temp.resolve("pipeline-release.json");
        Path missing = temp.resolve("missing.zip");
        var artifact = new PipelineReleaseArtifactDescriptor("truth", "compiled-truth",
            missing.toUri().toASCIIString(), "sha256:" + "0".repeat(64), List.of(), List.of());
        var release = new PipelineReleaseDescriptor(1, "pipeline", "contract", "release", "truth", List.of(artifact));
        var mapper = PipelineJson.mapper();
        byte[] original = mapper.writeValueAsBytes(release);
        byte[] replacement = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(release);
        Files.write(descriptor, replacement);
        Path destination = temp.resolve("resolved");
        var verifier = new ReleaseVerifier();
        var profile = ResolverProfile.localOnly();

        var failure = assertThrows(ReleaseVerificationException.class, () ->
            verifier.verify(descriptor, digest(original), destination, profile, CredentialResolver.NONE));
        assertEquals(ReleaseVerificationException.Category.DESCRIPTOR, failure.category());
        assertEquals("Release descriptor digest mismatch", failure.getMessage());
        assertFalse(Files.exists(destination));

        var artifactFailure = assertThrows(ReleaseVerificationException.class, () ->
            verifier.verify(descriptor, digest(replacement), destination, profile, CredentialResolver.NONE));
        assertEquals(ReleaseVerificationException.Category.RESOLUTION, artifactFailure.category());
        var legacyFailure = assertThrows(ReleaseVerificationException.class, () ->
            verifier.verify(descriptor, temp.resolve("legacy"), profile, CredentialResolver.NONE));
        assertEquals(ReleaseVerificationException.Category.RESOLUTION, legacyFailure.category());
    }

    private static String digest(byte[] bytes) throws Exception {
        return "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}

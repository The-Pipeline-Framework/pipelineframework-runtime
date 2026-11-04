package org.pipelineframework.deployment.release;

import java.util.Arrays;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;

/** One in-memory view retaining the exact input bytes and the authoritative shared model. */
public record LoadedRelease(byte[] originalBytes, String descriptorDigest, PipelineReleaseDescriptor descriptor) {
    public LoadedRelease {
        if (originalBytes == null || descriptorDigest == null || descriptor == null) {
            throw new IllegalArgumentException("Loaded Release fields are required");
        }
        originalBytes = Arrays.copyOf(originalBytes, originalBytes.length);
    }

    @Override
    public byte[] originalBytes() {
        return Arrays.copyOf(originalBytes, originalBytes.length);
    }
}

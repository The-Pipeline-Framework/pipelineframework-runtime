package org.pipelineframework.deployment.release;

import java.util.Arrays;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;

/** One in-memory view retaining the exact input bytes and the authoritative shared model. */
public record LoadedRelease(byte[] originalBytes, String descriptorDigest, PipelineReleaseDescriptor descriptor) {
    public LoadedRelease {
        originalBytes = Arrays.copyOf(originalBytes, originalBytes.length);
        if (descriptorDigest == null || descriptor == null) {
            throw new IllegalArgumentException("Loaded Release fields are required");
        }
    }

    @Override
    public byte[] originalBytes() {
        return Arrays.copyOf(originalBytes, originalBytes.length);
    }
}

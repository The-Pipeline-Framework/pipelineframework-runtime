package org.pipelineframework.deployment.release;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptorValidator;

public final class ReleaseLoader {
    public LoadedRelease load(Path source) throws IOException {
        byte[] bytes = Files.readAllBytes(source.toAbsolutePath().normalize());
        PipelineReleaseDescriptor descriptor = PipelineJson.mapper().readValue(bytes, PipelineReleaseDescriptor.class);
        new PipelineReleaseDescriptorValidator().validate(descriptor);
        return new LoadedRelease(bytes, "sha256:" + sha256(bytes), descriptor);
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest algorithm is unavailable", e);
        }
    }
}

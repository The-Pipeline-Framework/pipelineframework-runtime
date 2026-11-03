package org.pipelineframework.deployment.release;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactResolver;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactUri;
import org.pipelineframework.orchestrator.release.PipelineReleaseClosureResolver;

public final class ReleaseVerifier {
    private final ReleaseLoader loader = new ReleaseLoader();

    public VerifiedRelease verify(
        Path descriptorFile,
        Path destination,
        ResolverProfile profile,
        CredentialResolver credentials
    ) throws IOException {
        LoadedRelease loaded;
        try {
            loaded = loader.load(descriptorFile);
        } catch (IOException | IllegalArgumentException e) {
            throw new ReleaseVerificationException(
                ReleaseVerificationException.Category.DESCRIPTOR, "Invalid Pipeline Release Descriptor", e);
        }
        Map<PipelineReleaseArtifactUri.Scheme, PipelineReleaseArtifactResolver> resolvers =
            new EnumMap<>(PipelineReleaseArtifactUri.Scheme.class);
        if (profile.fileEnabled()) {
            resolvers.put(PipelineReleaseArtifactUri.Scheme.FILE,
                new FileArtifactResolverProvider().create(true, credentials));
        }
        profile.maven().ifPresent(configuration -> resolvers.put(
            PipelineReleaseArtifactUri.Scheme.MAVEN,
            new MavenArtifactResolverProvider().create(configuration, credentials)));
        profile.oci().ifPresent(configuration -> resolvers.put(
            PipelineReleaseArtifactUri.Scheme.OCI,
            new OciArtifactResolverProvider().create(configuration, credentials)));
        var closure = new PipelineReleaseClosureResolver(
            resolvers,
            path -> PipelineJson.mapper().readValue(path.toFile(), PipelineContractDescriptor.class));
        try {
            return new VerifiedRelease(loaded, closure.resolve(loaded.descriptor(), destination));
        } catch (IOException e) {
            throw new ReleaseVerificationException(
                ReleaseVerificationException.Category.RESOLUTION, "Failed to resolve a Release artifact", e);
        } catch (IllegalArgumentException e) {
            ReleaseVerificationException.Category category =
                e.getMessage() != null && (e.getMessage().contains("digest mismatch")
                    || e.getMessage().contains("resolver configured"))
                    ? ReleaseVerificationException.Category.RESOLUTION
                    : ReleaseVerificationException.Category.CLOSURE;
            throw new ReleaseVerificationException(category, "Invalid Release closure: " + e.getMessage(), e);
        }
    }
}

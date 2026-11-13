package org.pipelineframework.deployment.release;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactResolver;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactUri;
import org.pipelineframework.orchestrator.release.PipelineReleaseClosureResolver;

public final class ReleaseVerifier {
    private static final Pattern DESCRIPTOR_DIGEST = Pattern.compile("sha256:[0-9a-f]{64}");

    private final ReleaseLoader loader = new ReleaseLoader();

    public VerifiedRelease verify(
        Path descriptorFile,
        Path destination,
        ResolverProfile profile,
        CredentialResolver credentials
    ) throws IOException {
        return verify(descriptorFile, Optional.empty(), destination, profile, credentials);
    }

    /** Verifies the deployment's exact descriptor bytes before opening any artifact. */
    public VerifiedRelease verify(
        Path descriptorFile,
        String expectedDescriptorDigest,
        Path destination,
        ResolverProfile profile,
        CredentialResolver credentials
    ) throws IOException {
        Objects.requireNonNull(expectedDescriptorDigest, "expectedDescriptorDigest");
        if (!DESCRIPTOR_DIGEST.matcher(expectedDescriptorDigest).matches()) {
            throw new IllegalArgumentException("Expected Release descriptor digest must use sha256:<64 lowercase hex>");
        }
        return verify(descriptorFile, Optional.of(expectedDescriptorDigest), destination, profile, credentials);
    }

    private VerifiedRelease verify(
        Path descriptorFile,
        Optional<String> expectedDescriptorDigest,
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
        if (expectedDescriptorDigest.filter(digest -> !digest.equals(loaded.descriptorDigest())).isPresent()) {
            var mismatch = new IllegalArgumentException("Release descriptor digest mismatch");
            throw new ReleaseVerificationException(
                ReleaseVerificationException.Category.DESCRIPTOR, mismatch.getMessage(), mismatch);
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

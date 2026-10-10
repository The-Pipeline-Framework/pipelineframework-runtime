package org.pipelineframework.orchestrator.release;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarFile;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.deployment.release.CredentialResolver;
import org.pipelineframework.deployment.release.MavenResolverConfiguration;
import org.pipelineframework.deployment.release.ReleaseVerifier;
import org.pipelineframework.deployment.release.ResolverProfile;
import org.pipelineframework.orchestrator.LocalPipelineReleaseArtifactStore;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseArtifactStore;
import org.pipelineframework.orchestrator.PipelineReleaseRuntimeBeans;
import org.pipelineframework.orchestrator.PipelineReleaseStoredArtifact;

/**
 * Validates build-produced release descriptors before Coordinator registration.
 */
@ApplicationScoped
public class PipelineReleaseRegistrar {

    private static final Set<String> SUPPORTED_KINDS = Set.of(
        "jar",
        "application-archive",
        "native-binary",
        "compiled-truth",
        "container-image",
        "lambda-zip",
        "lambda-image");

    private final PipelineReleaseDescriptorValidator releaseValidator = new PipelineReleaseDescriptorValidator();

    @Inject
    PipelineReleaseDescriptorLoader descriptorLoader;

    @Inject
    PipelineContractDescriptorLoader contractLoader;

    @Inject
    PipelineReleaseArtifactStore artifactStore;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    public PipelineReleaseRecord validate(
        String tenantId,
        String pipelineId,
        String releaseDescriptorPath,
        long nowEpochMs) {
        validateRequired("tenantId", tenantId);
        validateRequired("pipelineId", pipelineId);
        validateRequired("releaseDescriptorPath", releaseDescriptorPath);
        Path descriptorPath = Path.of(releaseDescriptorPath);
        if (!descriptorPath.isAbsolute()) {
            throw new IllegalArgumentException("releaseDescriptorPath must be absolute");
        }
        PipelineReleaseDescriptor descriptor = loader().load(descriptorPath);
        validateDescriptorIdentity(pipelineId, descriptor);
        ValidatedArtifact primary = descriptor.artifacts().stream().anyMatch(artifact -> artifact.uri().startsWith("maven:"))
            ? validateResolvedArtifacts(descriptorPath, descriptor)
            : validateArtifacts(descriptor);
        PipelineContractDescriptor contract = primary.contract()
            .orElseThrow(() -> new IllegalArgumentException(
                "Compiled Truth artifact is missing " + PipelineContractDescriptor.RESOURCE_PATH));
        releaseValidator.validate(descriptor, contract);
        return new PipelineReleaseRecord(
            tenantId.trim(),
            pipelineId.trim(),
            descriptor.contractVersion(),
            descriptor.releaseVersion(),
            PipelineReleaseStatus.REGISTERED,
            descriptor,
            primary.artifactId(),
            primary.artifactDigest(),
            primary.artifactUri(),
            primary.artifactSizeBytes(),
            primary.artifactChecksum(),
            contract,
            nowEpochMs,
            nowEpochMs,
            0L);
    }

    public Uni<PipelineReleaseRecord> validateAsync(
        String tenantId,
        String pipelineId,
        String releaseDescriptorPath,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> validate(tenantId, pipelineId, releaseDescriptorPath, nowEpochMs))
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    public void verify(PipelineReleaseRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("Release record is required");
        }
        if (record.primaryArtifactUri() != null && !record.primaryArtifactUri().isBlank()) {
            artifactStore().verify(record);
        }
    }

    private void validateDescriptorIdentity(String expectedPipelineId, PipelineReleaseDescriptor descriptor) {
        releaseValidator.validate(descriptor);
        if (!expectedPipelineId.trim().equals(descriptor.pipelineId().trim())) {
            throw new IllegalArgumentException("Release descriptor pipelineId does not match request path");
        }
    }

    private ValidatedArtifact validateResolvedArtifacts(Path descriptorPath, PipelineReleaseDescriptor descriptor) {
        // Keep legacy file/JAR admission intact. Canonical Maven closures use the shared resolver,
        // including validation of every artifact and the embedded contract before any storage writes.
        descriptor.artifacts().forEach(this::validateArtifactDescriptor);
        Path workspace;
        try {
            workspace = Files.createTempDirectory("tpf-release-admission-");
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create Release admission workspace", e);
        }
        try (var admission = new AdmissionWorkspace(workspace)) {
            var verified = new ReleaseVerifier().verify(
                descriptorPath, admission.root().resolve("closure"), resolverProfile(), CredentialResolver.NONE);
            var resolved = verified.resolved();
            if (!descriptor.equals(resolved.descriptor())) {
                throw new IllegalArgumentException("Release descriptor changed during admission");
            }
            Optional<ValidatedArtifact> primary = Optional.empty();
            for (var artifact : resolved.artifacts()) {
                PipelineReleaseStoredArtifact stored = artifactStore().store(artifact.file());
                if (artifact.descriptor().artifactId().equals(descriptor.compiledTruthArtifactId())) {
                    primary = Optional.of(new ValidatedArtifact(
                        artifact.descriptor().artifactId(), artifact.descriptor().digest(), stored.artifactUri(),
                        stored.artifactSizeBytes(), stored.artifactChecksum(), Optional.of(resolved.contract())));
                }
            }
            return primary.orElseGet(ValidatedArtifact::empty);
        } catch (IOException e) {
            // Resolver causes may contain repository URLs/settings details; keep the admin response generic.
            throw new IllegalArgumentException("Release closure could not be resolved or verified", e);
        }
    }

    private ResolverProfile resolverProfile() {
        var config = Optional.ofNullable(orchestratorConfig)
            .map(PipelineOrchestratorConfig::releases)
            .map(PipelineOrchestratorConfig.ReleasesConfig::resolver);
        Optional<Path> settings = config.flatMap(PipelineOrchestratorConfig.ReleaseResolverConfig::mavenSettings).map(Path::of);
        settings.filter(path -> !Files.isRegularFile(path) || !Files.isReadable(path)).ifPresent(path -> {
            throw new IllegalArgumentException("Configured Maven settings file must be readable");
        });
        Path localRepository = config.flatMap(PipelineOrchestratorConfig.ReleaseResolverConfig::mavenLocalRepository)
            .map(Path::of).orElseGet(() -> PipelineReleaseRuntimeBeans.storageRoot(orchestratorConfig).resolve("maven"));
        List<String> repositories = config.map(PipelineOrchestratorConfig.ReleaseResolverConfig::mavenRepositories)
            .orElseGet(() -> List.of("https://repo.maven.apache.org/maven2"));
        List<URI> locations;
        try {
            locations = repositories.stream().map(URI::create).toList();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Configured Maven repository URI is invalid", e);
        }
        if (locations.stream().anyMatch(uri -> uri.getUserInfo() != null)) {
            throw new IllegalArgumentException("Maven repository credentials must be configured through settings servers");
        }
        // The public resolver uses a nullable settings Path to represent explicitly absent settings.
        return new ResolverProfile(true,
            Optional.of(new MavenResolverConfiguration(settings.orElse(null), localRepository, locations)), Optional.empty());
    }

    private record AdmissionWorkspace(Path root) implements AutoCloseable {
        @Override
        public void close() {
            removeWorkspace(root);
        }
    }

    private static void removeWorkspace(Path workspace) {
        try (var paths = Files.walk(workspace)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot remove Release admission workspace", e);
        }
    }

    private ValidatedArtifact validateArtifacts(PipelineReleaseDescriptor descriptor) {
        ValidatedArtifact primary = null;
        for (PipelineReleaseArtifactDescriptor artifact : descriptor.artifacts()) {
            validateArtifactDescriptor(artifact);
            ValidatedArtifact validated = switch (artifact.kind().toLowerCase(Locale.ROOT)) {
                case "jar" -> validateJarArtifact(descriptor, artifact);
                case "application-archive", "native-binary", "lambda-zip", "compiled-truth" ->
                    validateLocalFileArtifact(descriptor, artifact);
                case "container-image", "lambda-image" -> ValidatedArtifact.remote(artifact);
                default -> throw new IllegalArgumentException("Unsupported release artifact kind " + artifact.kind());
            };
            if (artifact.artifactId().equals(descriptor.compiledTruthArtifactId())) {
                primary = validated;
            }
        }
        return primary == null ? ValidatedArtifact.empty() : primary;
    }

    private void validateArtifactDescriptor(PipelineReleaseArtifactDescriptor artifact) {
        validateRequired("artifactId", artifact.artifactId());
        validateRequired("kind", artifact.kind());
        validateRequired("uri", artifact.uri());
        validateRequired("digest", artifact.digest());
        if (!SUPPORTED_KINDS.contains(artifact.kind().toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Unsupported release artifact kind " + artifact.kind());
        }
        if (!artifact.digest().startsWith("sha256:")) {
            throw new IllegalArgumentException("Release artifact digest must use sha256:<hex>");
        }
    }

    private ValidatedArtifact validateJarArtifact(
        PipelineReleaseDescriptor descriptor,
        PipelineReleaseArtifactDescriptor artifact) {
        Path artifactPath = readableLocalPath(artifact, "Release JAR artifact must point to a readable local file");
        String checksum = validateDigest(artifact, artifactPath);
        PipelineContractDescriptor contract = loadContractFromJar(artifactPath)
            .orElseThrow(() -> new IllegalArgumentException(
                "Release JAR artifact is missing " + PipelineContractDescriptor.RESOURCE_PATH));
        validateContractCompatibility(descriptor, contract);
        PipelineReleaseStoredArtifact stored = artifactStore().store(artifactPath);
        return new ValidatedArtifact(
            artifact.artifactId(),
            artifact.digest(),
            stored.artifactUri(),
            stored.artifactSizeBytes(),
            stored.artifactChecksum(),
            Optional.of(contract));
    }

    private ValidatedArtifact validateLocalFileArtifact(
        PipelineReleaseDescriptor descriptor,
        PipelineReleaseArtifactDescriptor artifact) {
        Path artifactPath = readableLocalPath(artifact, "Release local artifact must point to a readable file");
        validateDigest(artifact, artifactPath);
        Optional<PipelineContractDescriptor> contract = loadContractFromJar(artifactPath);
        contract.ifPresent(value -> validateContractCompatibility(descriptor, value));
        PipelineReleaseStoredArtifact stored = artifactStore().store(artifactPath);
        return new ValidatedArtifact(
            artifact.artifactId(),
            artifact.digest(),
            stored.artifactUri(),
            stored.artifactSizeBytes(),
            stored.artifactChecksum(),
            contract);
    }

    private Optional<PipelineContractDescriptor> loadContractFromJar(Path path) {
        try (JarFile jar = new JarFile(path.toFile())) {
            var entry = jar.getJarEntry(PipelineContractDescriptor.RESOURCE_PATH);
            if (entry == null) {
                return Optional.empty();
            }
            try (InputStream stream = jar.getInputStream(entry)) {
                return Optional.of(contractLoader().load(stream));
            }
        } catch (java.util.zip.ZipException e) {
            return Optional.empty();
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to inspect release artifact: " + e.getMessage(), e);
        }
    }

    private void validateContractCompatibility(
        PipelineReleaseDescriptor descriptor,
        PipelineContractDescriptor contract) {
        if (!descriptor.pipelineId().equals(contract.pipelineId())) {
            throw new IllegalArgumentException("Release artifact contract pipelineId does not match release descriptor");
        }
        if (!descriptor.contractVersion().equals(contract.contractVersion())) {
            throw new IllegalArgumentException("Release artifact contractVersion does not match release descriptor");
        }
    }

    private String validateDigest(PipelineReleaseArtifactDescriptor artifact, Path path) {
        String checksum = sha256(path);
        if (!artifact.digest().equals("sha256:" + checksum)) {
            throw new IllegalArgumentException("Release artifact digest does not match local file checksum");
        }
        return checksum;
    }

    private static Path readableLocalPath(PipelineReleaseArtifactDescriptor artifact, String message) {
        Path artifactPath = localPath(artifact.uri());
        if (!Files.isRegularFile(artifactPath) || !Files.isReadable(artifactPath)) {
            throw new IllegalArgumentException(message);
        }
        return artifactPath;
    }

    private static Path localPath(String uri) {
        if (uri.startsWith("file:")) {
            return Path.of(java.net.URI.create(uri)).toAbsolutePath().normalize();
        }
        return Path.of(uri).toAbsolutePath().normalize();
    }

    private static String sha256(Path path) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream stream = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = stream.read(buffer)) >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest algorithm is unavailable", e);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to compute artifact digest: " + e.getMessage(), e);
        }
    }

    private PipelineReleaseDescriptorLoader loader() {
        return descriptorLoader == null ? new PipelineReleaseDescriptorLoader() : descriptorLoader;
    }

    private PipelineContractDescriptorLoader contractLoader() {
        return contractLoader == null ? new PipelineContractDescriptorLoader() : contractLoader;
    }

    private PipelineReleaseArtifactStore artifactStore() {
        return artifactStore == null
            ? new LocalPipelineReleaseArtifactStore(PipelineReleaseRuntimeBeans.storageRoot(orchestratorConfig))
            : artifactStore;
    }

    private static void validateRequired(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
    }

    private record ValidatedArtifact(
        String artifactId,
        String artifactDigest,
        String artifactUri,
        long artifactSizeBytes,
        String artifactChecksum,
        Optional<PipelineContractDescriptor> contract
    ) {
        static ValidatedArtifact remote(PipelineReleaseArtifactDescriptor artifact) {
            return new ValidatedArtifact(
                artifact.artifactId(),
                artifact.digest(),
                "",
                0L,
                "",
                Optional.empty());
        }

        static ValidatedArtifact empty() {
            return new ValidatedArtifact("", "", "", 0L, "", Optional.empty());
        }
    }
}

package org.pipelineframework.release.maven;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarFile;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactKind;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptorValidator;

final class ReleaseDescriptorGenerator {
    private static final long LOCK_RETRY_MILLIS = 10L;
    private static final Set<PipelineReleaseArtifactKind> SUPPORTED_KINDS = EnumSet.of(
        PipelineReleaseArtifactKind.JAR,
        PipelineReleaseArtifactKind.APPLICATION_ARCHIVE,
        PipelineReleaseArtifactKind.NATIVE_BINARY,
        PipelineReleaseArtifactKind.LAMBDA_ZIP,
        PipelineReleaseArtifactKind.COMPILED_TRUTH);

    private final ObjectMapper mapper;

    ReleaseDescriptorGenerator(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    PipelineContractDescriptor loadContract(Path contractFile) {
        requireReadableFile(contractFile, "Pipeline Contract");
        try (InputStream input = Files.newInputStream(contractFile)) {
            return mapper.readValue(input, PipelineContractDescriptor.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read Pipeline Contract " + contractFile + ": " + e.getMessage(), e);
        }
    }

    PipelineReleaseDescriptor generate(
        PipelineContractDescriptor contract,
        String releaseVersion,
        String compiledTruthArtifactId,
        Path metadataDirectory,
        List<ReleaseArtifactInput> artifacts,
        boolean allowLocalUris
    ) {
        if (contract == null) {
            throw new IllegalArgumentException("Pipeline Contract is required");
        }
        String pipelineId = requireText(contract.pipelineId(), "pipelineId");
        String contractVersion = requireText(contract.contractVersion(), "contractVersion");
        if (contract.steps() == null) {
            throw new IllegalArgumentException("Pipeline Contract steps are required");
        }
        String version = requireText(releaseVersion, "releaseVersion");
        String carrierId = requireText(compiledTruthArtifactId, "compiledTruthArtifactId");
        if (artifacts == null || artifacts.isEmpty()) {
            throw new IllegalArgumentException("At least one release artifact is required");
        }

        Set<String> knownStepIds = new HashSet<>();
        for (PipelineBundleStepDescriptor step : contract.steps()) {
            knownStepIds.add(step.authoredName());
        }

        Set<String> artifactIds = new HashSet<>();
        List<PipelineReleaseArtifactDescriptor> descriptors = new ArrayList<>();
        for (ReleaseArtifactInput artifact : artifacts) {
            String artifactId = requireText(artifact.artifactId(), "artifactId");
            if (!artifactIds.add(artifactId)) {
                throw new IllegalArgumentException("Duplicate release artifactId " + artifactId);
            }
            String kindValue = requireText(artifact.kind(), "kind").toLowerCase(Locale.ROOT);
            PipelineReleaseArtifactKind kind = PipelineReleaseArtifactKind.fromWireValue(kindValue);
            if (!SUPPORTED_KINDS.contains(kind)) {
                throw new IllegalArgumentException(
                    "Unsupported build-produced release artifact kind " + kindValue + "; supported kinds are " + SUPPORTED_KINDS);
            }
            requireReadableFile(artifact.file(), "Release artifact " + artifactId);
            String uri = requireText(artifact.uri(), "uri");
            if (kind == PipelineReleaseArtifactKind.JAR) {
                validateEmbeddedContract(artifact.file(), contract);
            }
            List<String> stepIds = validatedStepIds(artifact.stepIds(), knownStepIds, artifactId);
            List<String> capabilities = validatedValues(artifact.capabilities(), "capability", artifactId);
            descriptors.add(new PipelineReleaseArtifactDescriptor(
                artifactId,
                kindValue,
                uri,
                "sha256:" + sha256(artifact.file()),
                stepIds,
                capabilities));
        }
        PipelineReleaseDescriptor descriptor = new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            pipelineId,
            contractVersion,
            version,
            carrierId,
            descriptors);
        PipelineReleaseDescriptorValidator validator = new PipelineReleaseDescriptorValidator();
        validator.validate(descriptor, contract);
        if (!allowLocalUris) {
            validator.validatePromotable(descriptor);
        }
        ReleaseArtifactInput carrier = artifacts.stream()
            .filter(artifact -> carrierId.equals(artifact.artifactId()))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(
                "compiledTruthArtifactId does not reference a configured artifact: " + carrierId));
        validateCompiledTruthCarrier(carrier.file(), metadataDirectory);
        return descriptor;
    }

    byte[] serialize(PipelineReleaseDescriptor descriptor) {
        try {
            byte[] json = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(descriptor);
            byte[] output = new byte[json.length + 1];
            System.arraycopy(json, 0, output, 0, json.length);
            output[json.length] = '\n';
            return output;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Pipeline Release Descriptor", e);
        }
    }

    void write(Path outputFile, PipelineReleaseDescriptor descriptor) {
        byte[] content = serialize(descriptor);
        Path target = outputFile.toAbsolutePath().normalize();
        Path parent = target.getParent();
        if (parent == null) {
            throw new IllegalArgumentException("Release descriptor output must have a parent directory");
        }
        try {
            Files.createDirectories(parent);
            writeLocked(target, parent, descriptor, content);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write Pipeline Release Descriptor " + outputFile + ": " + e.getMessage(), e);
        }
    }

    private void writeLocked(
        Path target,
        Path parent,
        PipelineReleaseDescriptor descriptor,
        byte[] content
    ) throws IOException {
        Path lockFile = target.resolveSibling("." + target.getFileName() + ".lock");
        try (FileChannel lockChannel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = acquireFileLock(lockChannel)) {
            if (Files.exists(target)) {
                validateExistingDescriptor(target, descriptor, content);
            }
            Path temporary = Files.createTempFile(parent, target.getFileName().toString(), ".tmp");
            try {
                Files.write(temporary, content);
                moveIntoPlace(temporary, target);
            } finally {
                Files.deleteIfExists(temporary);
            }
        }
    }

    private static FileLock acquireFileLock(FileChannel lockChannel) throws IOException {
        while (true) {
            try {
                return lockChannel.lock();
            } catch (OverlappingFileLockException ignored) {
                try {
                    Thread.sleep(LOCK_RETRY_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for release descriptor lock", e);
                }
            }
        }
    }

    static List<String> defaultStepIds(PipelineContractDescriptor contract) {
        return contract.steps().stream().map(PipelineBundleStepDescriptor::authoredName).toList();
    }

    static List<String> defaultCapabilities(PipelineContractDescriptor contract) {
        LinkedHashSet<String> capabilities = new LinkedHashSet<>();
        if (contract.capabilities().localTransitionExecution()) {
            capabilities.add("local");
        }
        capabilities.addAll(contract.capabilities().transitionWorkerProtocols());
        return List.copyOf(capabilities);
    }

    private void validateEmbeddedContract(Path artifact, PipelineContractDescriptor expected) {
        try (JarFile jar = new JarFile(artifact.toFile())) {
            var entry = jar.getJarEntry(PipelineContractDescriptor.RESOURCE_PATH);
            if (entry == null) {
                throw new IllegalArgumentException(
                    "Release JAR artifact " + artifact + " is missing " + PipelineContractDescriptor.RESOURCE_PATH);
            }
            PipelineContractDescriptor embedded;
            try (InputStream input = jar.getInputStream(entry)) {
                embedded = mapper.readValue(input, PipelineContractDescriptor.class);
            }
            if (!expected.equals(embedded)) {
                throw new IllegalArgumentException(
                    "Release JAR artifact " + artifact + " embeds different Pipeline Contract content");
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to inspect release JAR artifact " + artifact + ": " + e.getMessage(), e);
        }
    }

    private static void validateCompiledTruthCarrier(Path artifact, Path metadataDirectory) {
        if (metadataDirectory == null || !Files.isDirectory(metadataDirectory)) {
            throw new IllegalArgumentException("Compiled Truth directory must point to META-INF/pipeline: " + metadataDirectory);
        }
        Map<String, byte[]> expected = new TreeMap<>();
        try (var paths = Files.walk(metadataDirectory)) {
            for (Path path : paths.sorted().toList()) {
                if (Files.isSymbolicLink(path)) {
                    throw new IllegalArgumentException("Compiled Truth directory must not contain symbolic links: " + path);
                }
                if (Files.isRegularFile(path)) {
                    String entryName = "META-INF/pipeline/"
                        + metadataDirectory.relativize(path).toString().replace(path.getFileSystem().getSeparator(), "/");
                    expected.put(entryName, Files.readAllBytes(path));
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to inspect compiler-produced Compiled Truth", e);
        }
        if (expected.isEmpty() || !expected.containsKey(PipelineContractDescriptor.RESOURCE_PATH)) {
            throw new IllegalArgumentException(
                "Compiled Truth directory is missing " + PipelineContractDescriptor.RESOURCE_PATH);
        }

        Map<String, byte[]> actual = new TreeMap<>();
        try (JarFile archive = new JarFile(artifact.toFile())) {
            var entries = archive.entries();
            while (entries.hasMoreElements()) {
                var entry = entries.nextElement();
                if (!entry.isDirectory() && entry.getName().startsWith("META-INF/pipeline/")) {
                    if (actual.put(entry.getName(), archive.getInputStream(entry).readAllBytes()) != null) {
                        throw new IllegalArgumentException("Duplicate Compiled Truth archive entry " + entry.getName());
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to inspect Compiled Truth carrier " + artifact, e);
        }
        if (!actual.keySet().equals(expected.keySet())) {
            throw new IllegalArgumentException(
                "Compiled Truth carrier does not contain the complete META-INF/pipeline resource set");
        }
        expected.forEach((name, content) -> {
            if (!MessageDigest.isEqual(content, actual.get(name))) {
                throw new IllegalArgumentException("Compiled Truth carrier resource differs from compiler output: " + name);
            }
        });
    }

    private void validateExistingDescriptor(Path outputFile, PipelineReleaseDescriptor descriptor, byte[] content) {
        try {
            byte[] existingContent = Files.readAllBytes(outputFile);
            PipelineReleaseDescriptor existing = mapper.readValue(existingContent, PipelineReleaseDescriptor.class);
            boolean sameIdentity = existing.pipelineId().equals(descriptor.pipelineId())
                && existing.contractVersion().equals(descriptor.contractVersion())
                && existing.releaseVersion().equals(descriptor.releaseVersion());
            if (sameIdentity && !MessageDigest.isEqual(existingContent, content)) {
                throw new IllegalStateException(
                    "Refusing to replace immutable release identity " + descriptor.pipelineId() + "/"
                        + descriptor.contractVersion() + "/" + descriptor.releaseVersion() + " with different content");
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to inspect existing release descriptor " + outputFile, e);
        }
    }

    private static List<String> validatedStepIds(List<String> values, Set<String> knownStepIds, String artifactId) {
        List<String> stepIds = validatedValues(values, "stepId", artifactId);
        for (String stepId : stepIds) {
            if (!knownStepIds.contains(stepId)) {
                throw new IllegalArgumentException("Unknown stepId " + stepId + " for release artifact " + artifactId);
            }
        }
        return stepIds;
    }

    private static List<String> validatedValues(List<String> values, String name, String artifactId) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        LinkedHashSet<String> result = new LinkedHashSet<>();
        for (String value : values) {
            String validated = requireText(value, name + " for release artifact " + artifactId);
            if (!result.add(validated)) {
                throw new IllegalArgumentException("Duplicate " + name + " " + validated + " for release artifact " + artifactId);
            }
        }
        return List.copyOf(result);
    }

    private static String sha256(Path file) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(file)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = input.read(buffer)) != -1) {
                    digest.update(buffer, 0, read);
                }
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest algorithm is unavailable", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to digest release artifact " + file + ": " + e.getMessage(), e);
        }
    }

    private static void moveIntoPlace(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void requireReadableFile(Path file, String label) {
        if (file == null || !Files.isRegularFile(file) || !Files.isReadable(file)) {
            throw new IllegalArgumentException(label + " must point to a readable file: " + file);
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }
}

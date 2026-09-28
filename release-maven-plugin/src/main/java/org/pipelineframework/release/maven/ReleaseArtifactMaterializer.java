package org.pipelineframework.release.maven;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactKind;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactUri;

/** Turns directory-shaped build outputs into deterministic byte-addressable release artifacts. */
final class ReleaseArtifactMaterializer {
    private static final long ZIP_TIMESTAMP_MILLIS = 315_532_800_000L;
    private static final String COMPILED_TRUTH_PREFIX = "META-INF/pipeline/";

    List<ReleaseArtifactInput> materialize(
        List<ReleaseArtifactInput> inputs,
        String compiledTruthArtifactId,
        Path metadataDirectory,
        Path buildDirectory
    ) {
        Path outputDirectory = buildDirectory.resolve("pipeline-release-artifacts");
        List<ReleaseArtifactInput> materialized = new ArrayList<>();
        for (ReleaseArtifactInput input : inputs) {
            Path source = input.file();
            if (source == null || !Files.isDirectory(source)) {
                materialized.add(input);
                continue;
            }
            PipelineReleaseArtifactKind kind = PipelineReleaseArtifactKind.fromWireValue(input.kind());
            if (kind != PipelineReleaseArtifactKind.APPLICATION_ARCHIVE
                && kind != PipelineReleaseArtifactKind.COMPILED_TRUTH) {
                throw new IllegalArgumentException(
                    "Directory release artifacts require kind application-archive or compiled-truth: " + input.artifactId());
            }
            Path archive = outputDirectory.resolve(safeFileName(input.artifactId()) + ".zip");
            boolean carrier = input.artifactId().equals(compiledTruthArtifactId);
            createArchive(source, kind, carrier, metadataDirectory, archive);
            String uri = PipelineReleaseArtifactUri.parse(input.uri()).scheme() == PipelineReleaseArtifactUri.Scheme.FILE
                ? archive.toAbsolutePath().normalize().toUri().toASCIIString()
                : input.uri();
            materialized.add(new ReleaseArtifactInput(
                input.artifactId(), input.kind(), archive, uri, input.stepIds(), input.capabilities()));
        }
        return List.copyOf(materialized);
    }

    private static void createArchive(
        Path source,
        PipelineReleaseArtifactKind kind,
        boolean carrier,
        Path metadataDirectory,
        Path archive
    ) {
        try {
            Files.createDirectories(archive.getParent());
            Map<String, Path> entries = new LinkedHashMap<>();
            if (kind == PipelineReleaseArtifactKind.APPLICATION_ARCHIVE) {
                collectFiles(source, "", entries);
            }
            if (kind == PipelineReleaseArtifactKind.COMPILED_TRUTH) {
                collectFiles(source, COMPILED_TRUTH_PREFIX, entries);
            } else if (carrier) {
                collectFiles(metadataDirectory, COMPILED_TRUTH_PREFIX, entries);
            }
            if (entries.isEmpty()) {
                throw new IllegalArgumentException("Release artifact directory is empty: " + source);
            }
            try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(archive))) {
                for (Map.Entry<String, Path> entry : entries.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .toList()) {
                    ZipEntry zipEntry = new ZipEntry(entry.getKey());
                    zipEntry.setTime(ZIP_TIMESTAMP_MILLIS);
                    zip.putNextEntry(zipEntry);
                    Files.copy(entry.getValue(), zip);
                    zip.closeEntry();
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to materialize release artifact " + source, e);
        }
    }

    private static void collectFiles(Path root, String prefix, Map<String, Path> entries) throws IOException {
        if (root == null || !Files.isDirectory(root)) {
            throw new IllegalArgumentException("Compiled Truth directory must exist: " + root);
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.naturalOrder()).toList()) {
                if (Files.isSymbolicLink(path)) {
                    throw new IllegalArgumentException("Release artifact directories must not contain symbolic links: " + path);
                }
                if (!Files.isRegularFile(path)) {
                    continue;
                }
                String name = prefix + root.relativize(path).toString().replace(path.getFileSystem().getSeparator(), "/");
                Path previous = entries.putIfAbsent(name, path);
                if (previous != null && Files.mismatch(previous, path) != -1L) {
                    throw new IllegalArgumentException("Conflicting release archive entry " + name);
                }
            }
        }
    }

    private static String safeFileName(String artifactId) {
        if (artifactId == null || artifactId.isBlank()) {
            throw new IllegalArgumentException("artifactId is required");
        }
        return artifactId.replaceAll("[^A-Za-z0-9_.-]", "-");
    }
}

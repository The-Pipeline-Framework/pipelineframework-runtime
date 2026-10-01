package org.pipelineframework.release.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReleaseArtifactMaterializerTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void rejectsDistinctArtifactIdsThatResolveToTheSameArchiveBeforeOverwritingIt() throws Exception {
        Path first = Files.createDirectory(temporaryDirectory.resolve("first"));
        Path second = Files.createDirectory(temporaryDirectory.resolve("second"));
        Files.writeString(first.resolve("payload.txt"), "first");
        Files.writeString(second.resolve("payload.txt"), "second");
        Path buildDirectory = temporaryDirectory.resolve("build");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () ->
            new ReleaseArtifactMaterializer().materialize(List.of(
                input("app/a", first), input("app?a", second)), "other", null, buildDirectory));

        assertTrue(failure.getMessage().contains("app/a"));
        assertTrue(failure.getMessage().contains("app?a"));
        Path archive = buildDirectory.resolve("pipeline-release-artifacts/app-a.zip");
        try (ZipFile zip = new ZipFile(archive.toFile())) {
            assertEquals("first", new String(zip.getInputStream(zip.getEntry("payload.txt")).readAllBytes(),
                StandardCharsets.UTF_8));
        }
    }

    private static ReleaseArtifactInput input(String artifactId, Path directory) {
        return new ReleaseArtifactInput(
            artifactId, "application-archive", directory, directory.toUri().toASCIIString(), List.of(), List.of());
    }
}

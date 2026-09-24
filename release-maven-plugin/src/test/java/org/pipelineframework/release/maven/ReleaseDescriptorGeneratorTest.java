package org.pipelineframework.release.maven;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptorLoader;

class ReleaseDescriptorGeneratorTest {
    @TempDir
    Path temporaryDirectory;

    private ReleaseDescriptorGenerator generator;
    private PipelineContractDescriptor contract;

    @BeforeEach
    void setUp() {
        generator = new ReleaseDescriptorGenerator(PipelineJson.mapper());
        contract = contract("orders", "sha256:contract");
    }

    @Test
    void producesSchemaOneDescriptorFromExactJarBytes() throws Exception {
        Path jar = jar("orders.jar", contract, "payload");
        var descriptor = generator.generate(contract, "2026.09.23.1", List.of(new ReleaseArtifactInput(
            "orders",
            "jar",
            jar,
            "file:///releases/orders.jar",
            ReleaseDescriptorGenerator.defaultStepIds(contract),
            ReleaseDescriptorGenerator.defaultCapabilities(contract))));

        assertEquals(PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION, descriptor.schemaVersion());
        assertEquals(contract.pipelineId(), descriptor.pipelineId());
        assertEquals(contract.contractVersion(), descriptor.contractVersion());
        assertEquals("2026.09.23.1", descriptor.releaseVersion());
        assertEquals(List.of("Validate", "Store"), descriptor.artifacts().getFirst().stepIds());
        assertEquals(List.of("local", "rest", "grpc"), descriptor.artifacts().getFirst().capabilities());
        assertEquals("sha256:" + sha256(jar), descriptor.artifacts().getFirst().digest());
    }

    @Test
    void preservesConfiguredArtifactOrderAndExplicitAbsence() throws Exception {
        Path worker = Files.writeString(temporaryDirectory.resolve("worker.bin"), "worker");
        Path archive = Files.writeString(temporaryDirectory.resolve("function.zip"), "archive");

        var descriptor = generator.generate(contract, "release-1", List.of(
            new ReleaseArtifactInput("worker", "native-binary", worker, "s3://releases/worker", List.of("Store"), List.of("grpc")),
            new ReleaseArtifactInput("function", "lambda-zip", archive, "s3://releases/function.zip", List.of(), List.of())));

        assertEquals(List.of("worker", "function"), descriptor.artifacts().stream().map(value -> value.artifactId()).toList());
        assertEquals(List.of("Store"), descriptor.artifacts().getFirst().stepIds());
        assertTrue(descriptor.artifacts().get(1).stepIds().isEmpty());
        assertTrue(descriptor.artifacts().get(1).capabilities().isEmpty());
    }

    @Test
    void writesDeterministicContentAndAllowsIdenticalRegeneration() throws Exception {
        Path jar = jar("orders.jar", contract, "payload");
        var descriptor = descriptor(jar, "release-1");
        Path output = temporaryDirectory.resolve("pipeline-release.json");

        generator.write(output, descriptor);
        byte[] first = Files.readAllBytes(output);
        generator.write(output, descriptor);

        assertArrayEquals(first, Files.readAllBytes(output));
        assertEquals('\n', first[first.length - 1]);
        assertEquals(descriptor, PipelineJson.mapper().readValue(first, PipelineReleaseDescriptor.class));
        assertEquals(descriptor, new PipelineReleaseDescriptorLoader().load(output));
    }

    @Test
    void refusesDifferentContentForExistingImmutableIdentity() throws Exception {
        Path jar = jar("orders.jar", contract, "one");
        Path output = temporaryDirectory.resolve("pipeline-release.json");
        generator.write(output, descriptor(jar, "release-1"));
        jar("orders.jar", contract, "two");

        IllegalStateException failure = assertThrows(
            IllegalStateException.class,
            () -> generator.write(output, descriptor(jar, "release-1")));

        assertTrue(failure.getMessage().contains("Refusing to replace immutable release identity"));
    }

    @Test
    void permitsChangedArtifactUnderNewReleaseIdentity() throws Exception {
        Path jar = jar("orders.jar", contract, "one");
        Path output = temporaryDirectory.resolve("pipeline-release.json");
        generator.write(output, descriptor(jar, "release-1"));
        jar("orders.jar", contract, "two");

        var changed = descriptor(jar, "release-2");
        generator.write(output, changed);

        assertEquals(changed, PipelineJson.mapper().readValue(output.toFile(), PipelineReleaseDescriptor.class));
    }

    @Test
    void rejectsUnknownStepUnsupportedKindAndDuplicateArtifactIdentity() throws Exception {
        Path artifact = Files.writeString(temporaryDirectory.resolve("artifact.bin"), "artifact");
        assertThrows(IllegalArgumentException.class, () -> generator.generate(contract, "release-1", List.of(
            new ReleaseArtifactInput("worker", "native-binary", artifact, "file:///worker", List.of("Missing"), List.of()))));
        assertThrows(IllegalArgumentException.class, () -> generator.generate(contract, "release-1", List.of(
            new ReleaseArtifactInput("image", "container-image", artifact, "oci://image", List.of(), List.of()))));
        assertThrows(IllegalArgumentException.class, () -> generator.generate(contract, "release-1", List.of(
            new ReleaseArtifactInput("worker", "local-file", artifact, "file:///one", List.of(), List.of()),
            new ReleaseArtifactInput("worker", "local-file", artifact, "file:///two", List.of(), List.of()))));
    }

    @Test
    void rejectsMissingAndMismatchedEmbeddedContracts() throws Exception {
        Path noContract = temporaryDirectory.resolve("empty.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(noContract))) {
            output.putNextEntry(new JarEntry("content.txt"));
            output.write("content".getBytes());
            output.closeEntry();
        }
        assertThrows(IllegalArgumentException.class, () -> descriptor(noContract, "release-1"));

        Path wrongContract = jar("wrong.jar", contract("other", "sha256:other"), "payload");
        assertThrows(IllegalArgumentException.class, () -> descriptor(wrongContract, "release-1"));
    }

    private PipelineReleaseDescriptor descriptor(Path jar, String releaseVersion) {
        return generator.generate(contract, releaseVersion, List.of(new ReleaseArtifactInput(
            "orders",
            "jar",
            jar,
            jar.toUri().toASCIIString(),
            ReleaseDescriptorGenerator.defaultStepIds(contract),
            ReleaseDescriptorGenerator.defaultCapabilities(contract))));
    }

    private Path jar(String name, PipelineContractDescriptor embeddedContract, String content) throws IOException {
        Path jar = temporaryDirectory.resolve(name);
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            JarEntry contractEntry = new JarEntry(PipelineContractDescriptor.RESOURCE_PATH);
            contractEntry.setTime(0L);
            output.putNextEntry(contractEntry);
            output.write(PipelineJson.mapper().writeValueAsBytes(embeddedContract));
            output.closeEntry();
            JarEntry contentEntry = new JarEntry("content.txt");
            contentEntry.setTime(0L);
            output.putNextEntry(contentEntry);
            output.write(content.getBytes());
            output.closeEntry();
        }
        return jar;
    }

    private static PipelineContractDescriptor contract(String pipelineId, String contractVersion) {
        return new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            pipelineId,
            contractVersion,
            contractVersion.substring("sha256:".length()),
            "COMPUTE",
            "REST",
            "orders-app",
            false,
            "monolith",
            List.of(
                step(0, "Validate"),
                step(1, "Store")),
            new PipelineBundleCapabilities(true, List.of("rest", "grpc", "rest")));
    }

    private static PipelineBundleStepDescriptor step(int index, String name) {
        return new PipelineBundleStepDescriptor(
            index,
            name,
            "service",
            "ONE_TO_ONE",
            "input",
            "output",
            "example." + name,
            "",
            Map.of(),
            Map.of());
    }

    private static String sha256(Path file) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
    }
}

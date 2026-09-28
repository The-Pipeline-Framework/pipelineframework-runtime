package org.pipelineframework.release.maven;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
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
    private Path metadataDirectory;

    @BeforeEach
    void setUp() {
        generator = new ReleaseDescriptorGenerator(PipelineJson.mapper());
        contract = contract("orders", "sha256:contract");
        metadataDirectory = temporaryDirectory.resolve("classes/META-INF/pipeline");
        writeMetadata(contract);
    }

    @Test
    void producesSchemaOneDescriptorFromExactJarBytes() throws Exception {
        Path jar = jar("orders.jar", contract, "payload");
        var descriptor = generator.generate(contract, "2026.09.23.1", "orders", metadataDirectory, List.of(new ReleaseArtifactInput(
            "orders",
            "jar",
            jar,
            "file:///releases/orders.jar",
            ReleaseDescriptorGenerator.defaultStepIds(contract),
            ReleaseDescriptorGenerator.defaultCapabilities(contract))), true);

        assertEquals(PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION, descriptor.schemaVersion());
        assertEquals(contract.pipelineId(), descriptor.pipelineId());
        assertEquals(contract.contractVersion(), descriptor.contractVersion());
        assertEquals("2026.09.23.1", descriptor.releaseVersion());
        assertEquals(List.of("Validate", "Store"), descriptor.artifacts().getFirst().stepIds());
        assertEquals(List.of("local", "rest", "grpc"), descriptor.artifacts().getFirst().capabilities());
        assertEquals("sha256:" + sha256(jar), descriptor.artifacts().getFirst().digest());
    }

    @Test
    void preservesConfiguredArtifactOrderAndRequiresCompleteCoverage() throws Exception {
        Path worker = Files.writeString(temporaryDirectory.resolve("worker.bin"), "worker");
        Path archive = jar("function.zip", contract, "archive");

        var descriptor = generator.generate(contract, "release-1", "function", metadataDirectory, List.of(
            new ReleaseArtifactInput(
                "worker", "native-binary", worker, "maven:example:worker:bin:1", List.of("Store"), List.of("grpc")),
            new ReleaseArtifactInput(
                "function", "lambda-zip", archive, "maven:example:function:zip:1", List.of("Validate"), List.of("local", "rest"))),
            false);

        assertEquals(List.of("worker", "function"), descriptor.artifacts().stream().map(value -> value.artifactId()).toList());
        assertEquals(List.of("Store"), descriptor.artifacts().getFirst().stepIds());
        assertEquals(List.of("Validate"), descriptor.artifacts().get(1).stepIds());
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
    void serializesCompetingWritesForTheSameImmutableIdentity() throws Exception {
        Path firstJar = jar("first.jar", contract, "one");
        Path secondJar = jar("second.jar", contract, "two");
        PipelineReleaseDescriptor first = descriptor(firstJar, "release-1");
        PipelineReleaseDescriptor second = descriptor(secondJar, "release-1");
        Path output = temporaryDirectory.resolve("pipeline-release.json");
        CountDownLatch start = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(2)) {
            var firstWrite = executor.submit(() -> writeAfter(start, generator, output, first));
            var secondWrite = executor.submit(() -> writeAfter(
                start,
                new ReleaseDescriptorGenerator(PipelineJson.mapper()),
                output,
                second));
            start.countDown();
            List<Optional<RuntimeException>> outcomes = List.of(firstWrite.get(), secondWrite.get());

            assertEquals(1, outcomes.stream().filter(Optional::isEmpty).count());
            assertEquals(1, outcomes.stream().flatMap(Optional::stream)
                .filter(IllegalStateException.class::isInstance).count());
            PipelineReleaseDescriptor written = PipelineJson.mapper().readValue(output.toFile(), PipelineReleaseDescriptor.class);
            assertTrue(written.equals(first) || written.equals(second));
        }
    }

    @Test
    void rejectsUnknownStepUnsupportedKindAndDuplicateArtifactIdentity() throws Exception {
        Path artifact = Files.writeString(temporaryDirectory.resolve("artifact.bin"), "artifact");
        Path jar = jar("artifact.jar", contract, "artifact");
        assertThrows(IllegalArgumentException.class, () -> generator.generate(
            contract, "release-1", "worker", metadataDirectory, List.of(
                new ReleaseArtifactInput(
                    "worker", "jar", jar, jar.toUri().toASCIIString(), List.of("Missing"), List.of("local", "rest", "grpc"))), true));
        assertThrows(IllegalArgumentException.class, () -> generator.generate(
            contract, "release-1", "image", metadataDirectory, List.of(
                new ReleaseArtifactInput(
                    "image", "container-image", artifact, "oci://registry/image@sha256:" + "a".repeat(64),
                    List.of("Validate", "Store"), List.of("local", "rest", "grpc"))), false));
        assertThrows(IllegalArgumentException.class, () -> generator.generate(
            contract, "release-1", "worker", metadataDirectory, List.of(
                new ReleaseArtifactInput("worker", "local-file", artifact, artifact.toUri().toASCIIString(), List.of(), List.of()),
                new ReleaseArtifactInput("worker", "local-file", artifact, artifact.toUri().toASCIIString(), List.of(), List.of())), true));
    }

    @Test
    void rejectsNullAssociationsThroughDescriptorValidation() throws Exception {
        Path artifact = Files.writeString(temporaryDirectory.resolve("artifact.bin"), "artifact");
        ReleaseArtifactInput input = new ReleaseArtifactInput(
            "worker",
            "jar",
            jar("null.jar", contract, "payload"),
            temporaryDirectory.resolve("null.jar").toUri().toASCIIString(),
            Collections.singletonList(null),
            null);

        assertTrue(input.capabilities().isEmpty());
        assertThrows(IllegalArgumentException.class, () -> generator.generate(
            contract, "release-1", "worker", metadataDirectory, List.of(input), true));
    }

    @Test
    void rejectsBlankContractIdentityBeforeArtifactProcessing() {
        PipelineContractDescriptor invalid = contract(" ", "sha256:contract");

        assertThrows(IllegalArgumentException.class, () -> generator.generate(
            invalid, "release-1", "orders", metadataDirectory, List.of(), true));
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

        PipelineContractDescriptor alteredContent = contract(
            contract.pipelineId(),
            contract.contractVersion(),
            List.of(step(0, "Altered")));
        Path sameIdentity = jar("same-identity.jar", alteredContent, "payload");
        assertThrows(IllegalArgumentException.class, () -> descriptor(sameIdentity, "release-1"));
    }

    @Test
    void rejectsCarrierMissingAnyCompilerProducedMetadata() throws Exception {
        Path incomplete = temporaryDirectory.resolve("incomplete.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(incomplete))) {
            JarEntry contractEntry = new JarEntry(PipelineContractDescriptor.RESOURCE_PATH);
            output.putNextEntry(contractEntry);
            output.write(PipelineJson.mapper().writeValueAsBytes(contract));
            output.closeEntry();
        }

        assertThrows(IllegalArgumentException.class, () -> descriptor(incomplete, "release-1"));
    }

    private static Optional<RuntimeException> writeAfter(
        CountDownLatch start,
        ReleaseDescriptorGenerator writer,
        Path output,
        PipelineReleaseDescriptor descriptor
    ) throws InterruptedException {
        start.await();
        try {
            writer.write(output, descriptor);
            return Optional.empty();
        } catch (RuntimeException failure) {
            return Optional.of(failure);
        }
    }

    private PipelineReleaseDescriptor descriptor(Path jar, String releaseVersion) {
        return generator.generate(contract, releaseVersion, "orders", metadataDirectory, List.of(new ReleaseArtifactInput(
            "orders",
            "jar",
            jar,
            jar.toUri().toASCIIString(),
            ReleaseDescriptorGenerator.defaultStepIds(contract),
            ReleaseDescriptorGenerator.defaultCapabilities(contract))), true);
    }

    private Path jar(String name, PipelineContractDescriptor embeddedContract, String content) throws IOException {
        Path jar = temporaryDirectory.resolve(name);
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            JarEntry contractEntry = new JarEntry(PipelineContractDescriptor.RESOURCE_PATH);
            contractEntry.setTime(0L);
            output.putNextEntry(contractEntry);
            output.write(PipelineJson.mapper().writeValueAsBytes(embeddedContract));
            output.closeEntry();
            for (String metadata : List.of("order.json", "telemetry.json")) {
                JarEntry metadataEntry = new JarEntry("META-INF/pipeline/" + metadata);
                metadataEntry.setTime(0L);
                output.putNextEntry(metadataEntry);
                output.write(Files.readAllBytes(metadataDirectory.resolve(metadata)));
                output.closeEntry();
            }
            JarEntry contentEntry = new JarEntry("content.txt");
            contentEntry.setTime(0L);
            output.putNextEntry(contentEntry);
            output.write(content.getBytes());
            output.closeEntry();
        }
        return jar;
    }

    private void writeMetadata(PipelineContractDescriptor sourceContract) {
        try {
            Files.createDirectories(metadataDirectory);
            Files.write(metadataDirectory.resolve("pipeline-contract.json"),
                PipelineJson.mapper().writeValueAsBytes(sourceContract));
            Files.writeString(metadataDirectory.resolve("order.json"), "[\"Validate\",\"Store\"]\n");
            Files.writeString(metadataDirectory.resolve("telemetry.json"), "{\"pipeline\":\"orders\"}\n");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static PipelineContractDescriptor contract(String pipelineId, String contractVersion) {
        return contract(pipelineId, contractVersion, List.of(step(0, "Validate"), step(1, "Store")));
    }

    private static PipelineContractDescriptor contract(
        String pipelineId,
        String contractVersion,
        List<PipelineBundleStepDescriptor> steps
    ) {
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
            steps,
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
            Map.of());
    }

    private static String sha256(Path file) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
    }
}

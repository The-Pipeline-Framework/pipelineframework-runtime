package org.pipelineframework.release.maven;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.MavenArtifactCoordinates;
import org.pipelineframework.orchestrator.release.MavenRepositoryPipelineReleaseArtifactResolver;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactUri;
import org.pipelineframework.orchestrator.release.PipelineReleaseClosureResolver;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.ResolvedPipelineRelease;

class IndependentReleaseConsumerConformanceTest {
    private static final long ZIP_TIMESTAMP_MILLIS = 315_532_800_000L;

    @TempDir
    Path temporaryDirectory;

    @Test
    void reconstructsEveryReleaseShapeFromOnlyTheDescriptorInTwoResolverEnvironments() throws Exception {
        Path metadata = metadataDirectory();
        PipelineContractDescriptor contract = PipelineJson.mapper().readValue(
            metadata.resolve("pipeline-contract.json").toFile(), PipelineContractDescriptor.class);

        for (Scenario scenario : List.of(
            singleJar(contract, metadata),
            fastJar(contract, metadata),
            modular(contract, metadata),
            nativeRelease(contract, metadata))) {
            Path firstRepository = temporaryDirectory.resolve("repositories/first/" + scenario.name());
            Path secondRepository = temporaryDirectory.resolve("repositories/second/" + scenario.name());
            publish(scenario, firstRepository);
            publish(scenario, secondRepository);

            verifyFromDescriptorOnly(scenario, firstRepository, metadata, "first");
            verifyFromDescriptorOnly(scenario, secondRepository, metadata, "second");
        }
    }

    private Scenario singleJar(PipelineContractDescriptor contract, Path metadata) throws Exception {
        Path jar = jar("single.jar", metadata, Map.of("app.txt", "single"));
        return generate("single", contract, metadata, "single", List.of(new ReleaseArtifactInput(
            "single",
            "jar",
            jar,
            "maven:example:single:1",
            List.of("Validate", "Store"),
            List.of("local", "rest", "grpc"))));
    }

    private Scenario fastJar(PipelineContractDescriptor contract, Path metadata) throws Exception {
        Path fastJarDirectory = temporaryDirectory.resolve("inputs/fast/quarkus-app");
        Files.createDirectories(fastJarDirectory.resolve("lib/main"));
        Files.writeString(fastJarDirectory.resolve("quarkus-run.jar"), "runner");
        Files.writeString(fastJarDirectory.resolve("lib/main/application.jar"), "application");
        return generate("fast", contract, metadata, "fast", List.of(new ReleaseArtifactInput(
            "fast",
            "application-archive",
            fastJarDirectory,
            "maven:example:fast:zip:1",
            List.of("Validate", "Store"),
            List.of("local", "rest", "grpc"))));
    }

    private Scenario modular(PipelineContractDescriptor contract, Path metadata) throws Exception {
        Path application = jar("modular-app.jar", metadata, Map.of("app.txt", "application"));
        Path worker = jar("modular-worker.zip", null, Map.of("worker.txt", "worker"));
        return generate("modular", contract, metadata, "modular-app", List.of(
            new ReleaseArtifactInput(
                "modular-app",
                "jar",
                application,
                "maven:example:modular-app:1",
                List.of("Validate"),
                List.of("local", "rest")),
            new ReleaseArtifactInput(
                "modular-worker",
                "lambda-zip",
                worker,
                "maven:example:modular-worker:zip:1",
                List.of("Store"),
                List.of("grpc"))));
    }

    private Scenario nativeRelease(PipelineContractDescriptor contract, Path metadata) throws Exception {
        Path binary = temporaryDirectory.resolve("inputs/native/orders-runner");
        Files.createDirectories(binary.getParent());
        Files.writeString(binary, "native executable bytes");
        return generate("native", contract, metadata, "native-truth", List.of(
            new ReleaseArtifactInput(
                "native-runner",
                "native-binary",
                binary,
                "maven:example:native-runner:bin:1",
                List.of("Validate", "Store"),
                List.of("local", "rest", "grpc")),
            new ReleaseArtifactInput(
                "native-truth",
                "compiled-truth",
                metadata,
                "maven:example:native-truth:zip:1",
                List.of(),
                List.of())));
    }

    private Scenario generate(
        String name,
        PipelineContractDescriptor contract,
        Path metadata,
        String carrierId,
        List<ReleaseArtifactInput> inputs
    ) throws Exception {
        Path buildDirectory = temporaryDirectory.resolve("build/" + name);
        List<ReleaseArtifactInput> materialized = new ReleaseArtifactMaterializer()
            .materialize(inputs, carrierId, metadata, buildDirectory);
        ReleaseDescriptorGenerator generator = new ReleaseDescriptorGenerator(PipelineJson.mapper());
        PipelineReleaseDescriptor descriptor = generator.generate(
            contract, "release-" + name, carrierId, metadata, materialized, false);
        Path descriptorFile = buildDirectory.resolve("pipeline-release.json");
        generator.write(descriptorFile, descriptor);
        return new Scenario(name, descriptorFile, descriptor, materialized);
    }

    private void publish(Scenario scenario, Path repository) throws IOException {
        for (int index = 0; index < scenario.inputs().size(); index++) {
            PipelineReleaseArtifactUri uri = PipelineReleaseArtifactUri.parse(
                scenario.descriptor().artifacts().get(index).uri());
            MavenArtifactCoordinates coordinates = uri.maven().orElseThrow();
            Path destination = repository.resolve(coordinates.repositoryPath());
            Files.createDirectories(destination.getParent());
            Files.copy(scenario.inputs().get(index).file(), destination);
        }
    }

    private void verifyFromDescriptorOnly(
        Scenario scenario,
        Path repository,
        Path expectedMetadata,
        String environment
    ) throws Exception {
        PipelineReleaseDescriptor descriptor = PipelineJson.mapper().readValue(
            scenario.descriptorFile().toFile(), PipelineReleaseDescriptor.class);
        Map<PipelineReleaseArtifactUri.Scheme, org.pipelineframework.orchestrator.release.PipelineReleaseArtifactResolver>
            resolvers = new EnumMap<>(PipelineReleaseArtifactUri.Scheme.class);
        resolvers.put(
            PipelineReleaseArtifactUri.Scheme.MAVEN,
            new MavenRepositoryPipelineReleaseArtifactResolver(repository));
        PipelineReleaseClosureResolver consumer = new PipelineReleaseClosureResolver(
            resolvers,
            path -> PipelineJson.mapper().readValue(path.toFile(), PipelineContractDescriptor.class));
        Path freshDestination = temporaryDirectory.resolve(
            "resolved/" + environment + "/" + scenario.name());

        ResolvedPipelineRelease resolved = consumer.resolve(descriptor, freshDestination);

        assertEquals(scenario.descriptor(), resolved.descriptor());
        assertEquals(descriptor.artifacts().size(), resolved.artifacts().size());
        assertEquals(descriptor.pipelineId(), resolved.contract().pipelineId());
        assertEquals(descriptor.contractVersion(), resolved.contract().contractVersion());
        assertMetadataEquals(expectedMetadata, resolved.compiledTruthDirectory().resolve("META-INF/pipeline"));
    }

    private Path metadataDirectory() throws Exception {
        Path metadata = temporaryDirectory.resolve("inputs/classes/META-INF/pipeline");
        Files.createDirectories(metadata.resolve("types"));
        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            "orders",
            "sha256:contract",
            "contract",
            "COMPUTE",
            "REST",
            "orders-app",
            false,
            "monolith",
            List.of(step(0, "Validate"), step(1, "Store")),
            new PipelineBundleCapabilities(true, List.of("rest", "grpc")));
        Files.write(metadata.resolve("pipeline-contract.json"), PipelineJson.mapper().writeValueAsBytes(contract));
        Files.writeString(metadata.resolve("order.json"), "[\"Validate\",\"Store\"]\n");
        Files.writeString(metadata.resolve("telemetry.json"), "{\"pipeline\":\"orders\"}\n");
        Files.writeString(metadata.resolve("branching.json"), "{}\n");
        Files.writeString(metadata.resolve("platform.json"), "{\"layout\":\"monolith\"}\n");
        Files.writeString(metadata.resolve("types/order.json"), "{\"type\":\"Order\"}\n");
        return metadata;
    }

    private Path jar(String name, Path metadata, Map<String, String> entries) throws IOException {
        Path jar = temporaryDirectory.resolve("inputs/" + name);
        Files.createDirectories(jar.getParent());
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            if (metadata != null) {
                try (var paths = Files.walk(metadata)) {
                    for (Path file : paths.filter(Files::isRegularFile).sorted().toList()) {
                        addEntry(
                            output,
                            "META-INF/pipeline/" + metadata.relativize(file).toString().replace('\\', '/'),
                            Files.readAllBytes(file));
                    }
                }
            }
            for (Map.Entry<String, String> entry : entries.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
                addEntry(output, entry.getKey(), entry.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        return jar;
    }

    private static void addEntry(JarOutputStream output, String name, byte[] bytes) throws IOException {
        JarEntry entry = new JarEntry(name);
        entry.setTime(ZIP_TIMESTAMP_MILLIS);
        output.putNextEntry(entry);
        output.write(bytes);
        output.closeEntry();
    }

    private static void assertMetadataEquals(Path expected, Path actual) throws IOException {
        List<Path> expectedFiles;
        try (var paths = Files.walk(expected)) {
            expectedFiles = paths.filter(Files::isRegularFile).map(expected::relativize).sorted().toList();
        }
        List<Path> actualFiles;
        try (var paths = Files.walk(actual)) {
            actualFiles = paths.filter(Files::isRegularFile).map(actual::relativize).sorted().toList();
        }
        assertEquals(expectedFiles, actualFiles);
        for (Path relative : expectedFiles) {
            assertArrayEquals(Files.readAllBytes(expected.resolve(relative)), Files.readAllBytes(actual.resolve(relative)));
        }
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

    private record Scenario(
        String name,
        Path descriptorFile,
        PipelineReleaseDescriptor descriptor,
        List<ReleaseArtifactInput> inputs
    ) {
        private Scenario {
            inputs = List.copyOf(inputs);
        }
    }
}

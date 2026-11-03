package org.pipelineframework.deployment.release;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.MavenArtifactCoordinates;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactUri;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;

class IndependentConsumerConformanceTest {
    @TempDir Path temporaryDirectory;

    @Test
    void reconstructsAllReleaseShapesFromDescriptorOnlyAcrossResolverEnvironments() throws Exception {
        Path metadata = metadata();
        PipelineContractDescriptor contract = PipelineJson.mapper().readValue(
            metadata.resolve("pipeline-contract.json").toFile(), PipelineContractDescriptor.class);

        for (Scenario scenario : List.of(
            singleJar(contract, metadata),
            fastJar(contract, metadata),
            modular(contract, metadata),
            nativeRelease(contract, metadata))) {
            byte[] immutableDescriptor = Files.readAllBytes(scenario.descriptor());
            for (String environment : List.of("staging", "production")) {
                Path repository = temporaryDirectory.resolve("repositories/" + environment + "/" + scenario.name());
                publish(scenario, repository);
                Path fresh = temporaryDirectory.resolve("fresh/" + environment + "/" + scenario.name());
                Files.createDirectories(fresh.getParent());
                var verified = new ReleaseVerifier().verify(
                    scenario.descriptor(), fresh,
                    new ResolverProfile(false, Optional.of(new MavenResolverConfiguration(null, repository, List.of())), Optional.empty()),
                    CredentialResolver.NONE);

                assertArrayEquals(immutableDescriptor, verified.loaded().originalBytes());
                assertEquals(scenario.descriptorModel(), verified.resolved().descriptor());
                assertEquals(contract, verified.resolved().contract());
                assertEquals(scenario.artifacts().size(), verified.resolved().artifacts().size());
                assertCompiledTruthEquals(metadata, verified.resolved().compiledTruthDirectory());
                for (int index = 0; index < scenario.artifacts().size(); index++) {
                    assertEquals(
                        scenario.descriptorModel().artifacts().get(index).digest(),
                        "sha256:" + sha256(verified.resolved().artifacts().get(index).file()));
                }
            }
            assertArrayEquals(immutableDescriptor, Files.readAllBytes(scenario.descriptor()));
        }
    }

    @Test
    void resolvesDigestQualifiedOciManifestWithoutChangingReleaseSemantics() throws Exception {
        byte[] manifest = "{\"schemaVersion\":2,\"mediaType\":\"application/vnd.oci.image.manifest.v1+json\"}".getBytes();
        String digest = "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(manifest));
        HttpServer registry = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        registry.createContext("/v2/example/orders/manifests/" + digest, exchange -> {
            exchange.sendResponseHeaders(200, manifest.length);
            exchange.getResponseBody().write(manifest);
            exchange.close();
        });
        registry.start();
        try {
            Path metadata = metadata();
            PipelineContractDescriptor contract = PipelineJson.mapper().readValue(
                metadata.resolve("pipeline-contract.json").toFile(), PipelineContractDescriptor.class);
            Path carrier = archive("oci-carrier.jar", metadata, Map.of());
            String registryName = "127.0.0.1:" + registry.getAddress().getPort();
            PipelineReleaseDescriptor descriptor = new PipelineReleaseDescriptor(
                PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
                contract.pipelineId(), contract.contractVersion(), "release-oci", "truth", List.of(
                    new PipelineReleaseArtifactDescriptor(
                        "truth", "jar", carrier.toAbsolutePath().normalize().toUri().toASCIIString(),
                        "sha256:" + sha256(carrier), List.of(), List.of()),
                    new PipelineReleaseArtifactDescriptor(
                        "image", "container-image", "oci://" + registryName + "/example/orders@" + digest,
                        digest, List.of("Validate", "Store"), List.of("local", "rest", "grpc"))));
            Path descriptorFile = temporaryDirectory.resolve("descriptors/oci/pipeline-release.json");
            Files.createDirectories(descriptorFile.getParent());
            Files.write(descriptorFile, PipelineJson.mapper().writeValueAsBytes(descriptor));

            var verified = new ReleaseVerifier().verify(
                descriptorFile, temporaryDirectory.resolve("fresh/oci"),
                new ResolverProfile(true, Optional.empty(), Optional.of(
                    new OciResolverConfiguration("", Map.of(), Set.of(registryName)))),
                CredentialResolver.NONE);

            assertEquals(digest, verified.resolved().artifacts().get(1).descriptor().digest());
            assertArrayEquals(manifest, Files.readAllBytes(verified.resolved().artifacts().get(1).file()));
        } finally {
            registry.stop(0);
        }
    }

    private Scenario singleJar(PipelineContractDescriptor contract, Path metadata) throws Exception {
        Path artifact = archive("single.jar", metadata, Map.of("application.txt", "single"));
        return scenario("single", contract, "single", List.of(input(
            "single", "jar", artifact, "maven:example:single:1",
            List.of("Validate", "Store"), List.of("local", "rest", "grpc"))));
    }

    private Scenario fastJar(PipelineContractDescriptor contract, Path metadata) throws Exception {
        Path layout = temporaryDirectory.resolve("input/fast");
        Files.createDirectories(layout.resolve("lib/main"));
        Files.writeString(layout.resolve("quarkus-run.jar"), "runner");
        Files.writeString(layout.resolve("lib/main/application.jar"), "application");
        Path artifact = archiveDirectory("fast.zip", layout, metadata);
        return scenario("fast", contract, "fast", List.of(input(
            "fast", "application-archive", artifact, "maven:example:fast:zip:1",
            List.of("Validate", "Store"), List.of("local", "rest", "grpc"))));
    }

    private Scenario modular(PipelineContractDescriptor contract, Path metadata) throws Exception {
        Path application = archive("modular-app.jar", metadata, Map.of("application.txt", "app"));
        Path worker = archive("modular-worker.zip", null, Map.of("worker.txt", "worker"));
        return scenario("modular", contract, "application", List.of(
            input("application", "jar", application, "maven:example:modular-app:1",
                List.of("Validate"), List.of("local", "rest")),
            input("worker", "lambda-zip", worker, "maven:example:modular-worker:zip:1",
                List.of("Store"), List.of("grpc"))));
    }

    private Scenario nativeRelease(PipelineContractDescriptor contract, Path metadata) throws Exception {
        Path binary = temporaryDirectory.resolve("input/native/orders");
        Files.createDirectories(binary.getParent());
        Files.writeString(binary, "native executable bytes");
        Path truth = archive("native-truth.zip", metadata, Map.of());
        return scenario("native", contract, "truth", List.of(
            input("native", "native-binary", binary, "maven:example:native:bin:1",
                List.of("Validate", "Store"), List.of("local", "rest", "grpc")),
            input("truth", "compiled-truth", truth, "maven:example:native-truth:zip:1", List.of(), List.of())));
    }

    private Scenario scenario(
        String name,
        PipelineContractDescriptor contract,
        String carrier,
        List<Input> inputs
    ) throws Exception {
        List<PipelineReleaseArtifactDescriptor> artifacts = new ArrayList<>();
        for (Input input : inputs) {
            artifacts.add(new PipelineReleaseArtifactDescriptor(
                input.id(), input.kind(), input.uri(), "sha256:" + sha256(input.file()), input.steps(), input.capabilities()));
        }
        PipelineReleaseDescriptor descriptor = new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            contract.pipelineId(), contract.contractVersion(), "release-" + name, carrier, artifacts);
        Path file = temporaryDirectory.resolve("descriptors/" + name + "/pipeline-release.json");
        Files.createDirectories(file.getParent());
        byte[] json = PipelineJson.mapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(descriptor);
        byte[] output = java.util.Arrays.copyOf(json, json.length + 1);
        output[json.length] = '\n';
        Files.write(file, output);
        return new Scenario(name, file, descriptor, inputs);
    }

    private void publish(Scenario scenario, Path repository) throws IOException {
        for (Input input : scenario.artifacts()) {
            MavenArtifactCoordinates coordinates = PipelineReleaseArtifactUri.parse(input.uri()).maven().orElseThrow();
            Path target = repository.resolve(coordinates.repositoryPath());
            Files.createDirectories(target.getParent());
            Files.copy(input.file(), target);
        }
    }

    private Path metadata() throws Exception {
        Path directory = temporaryDirectory.resolve("compiler/META-INF/pipeline");
        Files.createDirectories(directory);
        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            "orders", "sha256:" + "a".repeat(64), "a".repeat(64), "COMPUTE", "REST", "orders-app", false, "monolith",
            List.of(step(0, "Validate"), step(1, "Store")),
            new PipelineBundleCapabilities(true, List.of("rest", "grpc")));
        Files.write(directory.resolve("pipeline-contract.json"), PipelineJson.mapper().writeValueAsBytes(contract));
        Files.writeString(directory.resolve("order.json"), "[\"Validate\",\"Store\"]\n");
        Files.writeString(directory.resolve("telemetry.json"), "{\"pipeline\":\"orders\"}\n");
        return directory;
    }

    private static PipelineBundleStepDescriptor step(int index, String name) {
        return new PipelineBundleStepDescriptor(
            index, name, "service", "ONE_TO_ONE", "input", "output", "example." + name, "", Map.of());
    }

    private Path archive(String name, Path metadata, Map<String, String> content) throws IOException {
        Path target = temporaryDirectory.resolve("input/" + name);
        Files.createDirectories(target.getParent());
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(target))) {
            if (metadata != null) addDirectory(zip, metadata, "META-INF/pipeline/");
            for (Map.Entry<String, String> entry : content.entrySet()) {
                put(zip, entry.getKey(), entry.getValue().getBytes());
            }
        }
        return target;
    }

    private Path archiveDirectory(String name, Path source, Path metadata) throws IOException {
        Path target = temporaryDirectory.resolve("input/" + name);
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(target))) {
            addDirectory(zip, source, "");
            addDirectory(zip, metadata, "META-INF/pipeline/");
        }
        return target;
    }

    private static void addDirectory(ZipOutputStream zip, Path root, String prefix) throws IOException {
        try (var paths = Files.walk(root)) {
            for (Path file : paths.filter(Files::isRegularFile).sorted().toList()) {
                put(zip, prefix + root.relativize(file).toString().replace(file.getFileSystem().getSeparator(), "/"),
                    Files.readAllBytes(file));
            }
        }
    }

    private static void put(ZipOutputStream zip, String name, byte[] content) throws IOException {
        ZipEntry entry = new ZipEntry(name);
        entry.setTime(0L);
        zip.putNextEntry(entry);
        zip.write(content);
        zip.closeEntry();
    }

    private static void assertCompiledTruthEquals(Path expected, Path actualRoot) throws IOException {
        Path actual = actualRoot.resolve("META-INF/pipeline");
        try (var expectedFiles = Files.walk(expected)) {
            for (Path file : expectedFiles.filter(Files::isRegularFile).toList()) {
                assertArrayEquals(Files.readAllBytes(file), Files.readAllBytes(actual.resolve(expected.relativize(file))));
            }
        }
        assertTrue(Files.isRegularFile(actual.resolve("pipeline-contract.json")));
    }

    private static String sha256(Path file) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
    }

    private static Input input(
        String id, String kind, Path file, String uri, List<String> steps, List<String> capabilities
    ) {
        return new Input(id, kind, file, uri, steps, capabilities);
    }

    private record Input(String id, String kind, Path file, String uri, List<String> steps, List<String> capabilities) {}
    private record Scenario(String name, Path descriptor, PipelineReleaseDescriptor descriptorModel, List<Input> artifacts) {}
}

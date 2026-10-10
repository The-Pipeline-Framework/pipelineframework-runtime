package org.pipelineframework.extension;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import io.quarkus.test.QuarkusExtensionTest;
import jakarta.inject.Inject;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistrar;

/** Exercises the extension's normal isolated application classloader, not a manually wired registrar. */
@Isolated
class MavenReleaseAdmissionCdiTest {
    private static final Path DIRECTORY = temporaryDirectory();
    private static final String PIPELINE = "org.example.admission";
    private static final String CONTRACT = "sha256:contract";
    private static final String CARRIER = "maven:org.example:admission:zip:application:1.0.0";

    @RegisterExtension
    static final QuarkusExtensionTest APPLICATION = new QuarkusExtensionTest()
        .withApplicationRoot(archive -> archive.addClass(Handler.class))
        .overrideConfigKey("quarkus.devservices.enabled", "false")
        .overrideConfigKey("quarkus.http.test-port", "0")
        .overrideConfigKey("pipeline.orchestrator.releases.resolver.maven-repositories",
            DIRECTORY.resolve("repository").toUri().toString())
        .overrideConfigKey("pipeline.orchestrator.releases.resolver.maven-local-repository",
            DIRECTORY.resolve("cache").toString())
        .overrideConfigKey("pipeline.orchestrator.releases.storage.root", DIRECTORY.resolve("store").toString())
        .setAfterAllCustomizer(() -> deleteDirectory(DIRECTORY));

    @Inject
    PipelineReleaseRegistrar registrar;

    @Inject
    PipelineOrchestratorConfig config;

    @Test
    void connectorRoleUsesTheServiceLocatorsClassIdentity() throws Exception {
        assertSame(RepositoryConnectorFactory.class,
            Class.forName(RepositoryConnectorFactory.class.getName(), false, DefaultServiceLocator.class.getClassLoader()));
    }

    @Test
    void nativeRegistrarResolvesAndVerifiesAnUnchangedCanonicalMavenClosure() throws Exception {
        Path directory = Path.of(config.releases().storage().root()).getParent();
        Fixture fixture = prepare(directory);
        byte[] original = Files.readAllBytes(fixture.descriptorPath());

        var record = registrar.validate("tenant", PIPELINE, fixture.descriptorPath().toString(), 1000L);

        assertEquals(fixture.descriptor(), record.descriptor());
        assertArrayEquals(original, Files.readAllBytes(fixture.descriptorPath()));
        assertEquals(CARRIER, record.descriptor().artifacts().getFirst().uri());
        assertArrayEquals(Files.readAllBytes(fixture.carrier()),
            Files.readAllBytes(Path.of(URI.create(record.primaryArtifactUri()))));
        var expectedDigests = new java.util.HashSet<>(List.of(digest(fixture.carrier()), digest(fixture.worker())));
        try (var stored = Files.walk(directory.resolve("store"))) {
            var files = stored.filter(Files::isRegularFile).toList();
            assertEquals(2, files.size());
            for (Path file : files) {
                expectedDigests.remove(digest(file));
            }
        }
        assertEquals(java.util.Set.of(), expectedDigests);
        Files.delete(fixture.carrier());
        Files.delete(fixture.worker());
        registrar.verify(record);
    }

    static Fixture prepare(Path directory) throws Exception {
        Path repository = Files.createDirectories(directory.resolve("repository/org/example/admission/1.0.0"));
        Path carrier = repository.resolve("admission-1.0.0-application.zip");
        var contract = new PipelineContractDescriptor(1, PIPELINE, CONTRACT, "contract", "COMPUTE", "REST",
            "monolith-svc", false, "monolith", List.of(new PipelineBundleStepDescriptor(0, "Validate", "service",
                "ONE_TO_ONE", String.class.getName(), "Output", "Runtime", "Client", null)),
            PipelineBundleCapabilities.defaults());
        try (var zip = new JarOutputStream(Files.newOutputStream(carrier))) {
            zip.putNextEntry(new JarEntry(PipelineContractDescriptor.RESOURCE_PATH));
            zip.write(PipelineJson.mapper().writeValueAsBytes(contract));
            zip.closeEntry();
            zip.putNextEntry(new JarEntry("META-INF/pipeline/order.json"));
            zip.write("[\"Validate\"]".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        Path worker = Files.writeString(repository.resolve("admission-1.0.0-worker.jar"), "immutable worker bytes");
        var descriptor = new PipelineReleaseDescriptor(1, PIPELINE, CONTRACT, CONTRACT, "application", List.of(
            new PipelineReleaseArtifactDescriptor("application", "application-archive", CARRIER, digest(carrier),
                List.of("Validate"), List.of("local", "rest", "grpc", "sqs")),
            new PipelineReleaseArtifactDescriptor("worker", "jar", "maven:org.example:admission:jar:worker:1.0.0",
                digest(worker), List.of(), List.of("local", "rest", "grpc", "sqs"))));
        Path descriptorPath = directory.resolve("pipeline-release.json");
        PipelineJson.mapper().writeValue(descriptorPath.toFile(), descriptor);
        return new Fixture(descriptorPath, descriptor, carrier, worker);
    }

    record Fixture(Path descriptorPath, PipelineReleaseDescriptor descriptor, Path carrier, Path worker) {}

    private static Path temporaryDirectory() {
        try {
            Path directory = Files.createTempDirectory("tpf-maven-admission-");
            directory.toFile().deleteOnExit();
            return directory;
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static String digest(Path path) throws Exception {
        return "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
    }

    static void deleteDirectory(Path directory) {
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    public static class Handler implements com.amazonaws.services.lambda.runtime.RequestHandler<String, String> {
        @Override
        public String handleRequest(String input, com.amazonaws.services.lambda.runtime.Context context) {
            return input;
        }
    }
}

package org.pipelineframework.extension;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import io.quarkus.test.QuarkusProdModeTest;
import io.quarkus.test.ProdBuildResults;
import io.quarkus.test.ProdModeTestResults;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.dto.HostedReleaseRegisterRequest;

/** The packaged runner must admit the same closure without runner-parent-first configuration. */
@Isolated
class MavenReleaseAdmissionProdModeTest {
    private static final Path DIRECTORY = temporaryDirectory();

    @ProdBuildResults
    ProdModeTestResults build;

    @RegisterExtension
    static final QuarkusProdModeTest APPLICATION = new QuarkusProdModeTest()
        .withApplicationRoot(archive -> archive.addClass(MavenReleaseAdmissionCdiTest.Handler.class))
        .setRun(true)
        .setRuntimeProperties(java.util.Map.of("quarkus.http.port", "0"))
        .setJVMArgs(java.util.List.of("--enable-preview"))
        .overrideConfigKey("quarkus.devservices.enabled", "false")
        .overrideConfigKey("pipeline.orchestrator.admin.enabled", "true")
        .overrideConfigKey("pipeline.orchestrator.admin.admin-token", "test-token")
        .overrideConfigKey("pipeline.orchestrator.releases.resolver.maven-repositories",
            DIRECTORY.resolve("repository").toUri().toString())
        .overrideConfigKey("pipeline.orchestrator.releases.resolver.maven-local-repository",
            DIRECTORY.resolve("cache").toString())
        .overrideConfigKey("pipeline.orchestrator.releases.storage.root", DIRECTORY.resolve("store").toString());

    @Test
    void packagedNativeAdminResolvesAndVerifiesCanonicalMavenClosure() throws Exception {
        Path runner = build.getBuiltArtifactPath();
        Path runtime;
        try (var libraries = Files.list(runner.getParent().resolve("lib/main"))) {
            runtime = libraries.filter(path -> path.getFileName().toString()
                .matches("org\\.pipelineframework\\.pipelineframework-[0-9].*\\.jar"))
                .findFirst().orElseThrow();
        }
        try (var jar = new java.util.jar.JarFile(runtime.toFile())) {
            var metadata = new java.util.Properties();
            try (var input = jar.getInputStream(jar.getJarEntry("META-INF/quarkus-extension.properties"))) {
                metadata.load(input);
            }
            assertEquals("org.apache.maven.resolver:maven-resolver-spi", metadata.getProperty("parent-first-artifacts"));
            assertNull(metadata.getProperty("runner-parent-first-artifacts"));
            System.out.println("Packaged release admission extension metadata: " + metadata);
        }
        System.out.println("Packaged release admission runner SHA-256: " + sha256(runner));
        System.out.println("Packaged release admission runtime SHA-256: " + sha256(runtime));
        var fixture = MavenReleaseAdmissionCdiTest.prepare(DIRECTORY);
        byte[] original = Files.readAllBytes(fixture.descriptorPath());
        var address = java.util.regex.Pattern.compile("Listening on: (http://[^\\s]+)")
            .matcher(APPLICATION.getStartupConsoleOutput());
        assertTrue(address.find(), APPLICATION.getStartupConsoleOutput());
        String base = address.group(1) + "/tpf/admin/tenants/tenant/pipelines/org.example.admission/releases";
        try (var client = HttpClient.newHttpClient()) {
            var register = HttpRequest.newBuilder(URI.create(base + "/register"))
                .header("Authorization", "Bearer test-token").header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(PipelineJson.mapper().writeValueAsString(
                    new HostedReleaseRegisterRequest(fixture.descriptorPath().toString())))).build();
            var registered = client.send(register, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, registered.statusCode(), registered.body());
            var record = PipelineJson.mapper().readTree(registered.body());
            assertEquals(fixture.descriptor(), PipelineJson.mapper().treeToValue(
                record.path("descriptor"), PipelineReleaseDescriptor.class));
            assertEquals("maven:org.example:admission:zip:application:1.0.0",
                record.path("descriptor").path("artifacts").get(0).path("uri").asText());
            assertArrayEquals(original, Files.readAllBytes(fixture.descriptorPath()));
            assertArrayEquals(Files.readAllBytes(fixture.carrier()),
                Files.readAllBytes(Path.of(URI.create(record.path("primaryArtifactUri").asText()))));
            Files.delete(fixture.carrier());
            Files.delete(fixture.worker());
            var activate = HttpRequest.newBuilder(URI.create(base + "/sha256:contract/activate"))
                .header("Authorization", "Bearer test-token").POST(HttpRequest.BodyPublishers.noBody()).build();
            var activated = client.send(activate, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, activated.statusCode(), activated.body());
        }
    }

    @AfterAll
    static void cleanUp() {
        MavenReleaseAdmissionCdiTest.deleteDirectory(DIRECTORY);
    }

    private static Path temporaryDirectory() {
        try {
            return Files.createTempDirectory("tpf-packaged-maven-admission-");
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static String sha256(Path path) throws Exception {
        return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
            .digest(Files.readAllBytes(path)));
    }
}

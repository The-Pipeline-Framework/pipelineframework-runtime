package org.pipelineframework.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import io.quarkus.test.QuarkusExtensionTest;
import io.quarkus.test.common.http.TestHTTPResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;
import org.pipelineframework.config.pipeline.PipelineJson;
import jakarta.inject.Inject;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;

/** Normal CDI, authentication, JSON and HTTP routes using a verified canonical Maven release closure. */
@Isolated
class ActivationOperationHttpTest {
    private static final Path DIRECTORY = directory();
    @RegisterExtension
    static final QuarkusExtensionTest APPLICATION = new QuarkusExtensionTest()
        .withApplicationRoot(archive -> archive.addClass(Handler.class))
        .overrideConfigKey("quarkus.devservices.enabled", "false")
        .overrideConfigKey("quarkus.http.test-port", "0")
        .overrideConfigKey("pipeline.orchestrator.admin.enabled", "true")
        .overrideConfigKey("pipeline.orchestrator.admin.admin-token", "test-token")
        .overrideConfigKey("pipeline.orchestrator.releases.registry.provider", "memory")
        .overrideConfigKey("pipeline.orchestrator.releases.resolver.maven-repositories", DIRECTORY.resolve("repository").toUri().toString())
        .overrideConfigKey("pipeline.orchestrator.releases.resolver.maven-local-repository", DIRECTORY.resolve("cache").toString())
        .overrideConfigKey("pipeline.orchestrator.releases.storage.root", DIRECTORY.resolve("store").toString())
        .setAfterAllCustomizer(() -> deleteDirectory(DIRECTORY));

    @TestHTTPResource
    URI base;

    @Inject
    PipelineOrchestratorConfig config;

    @Test
    void normalHttpRegistrationActivationReplayInquiryAndAuthorization() throws Exception {
        var fixture = prepare(Path.of(config.releases().storage().root()).getParent());
        String releases = "/tpf/admin/tenants/tenant/pipelines/org.example.admission/releases";
        String operations = "/tpf/admin/tenants/tenant/pipelines/org.example.admission/activation-operations";
        try (HttpClient client = HttpClient.newHttpClient()) {
            assertEquals(401, post(client, operations, "{}", null).statusCode());
            assertEquals(401, get(client, operations + "/key", "wrong").statusCode());
            assertEquals(400, post(client, operations, "{}", "test-token").statusCode());
            assertEquals(404, get(client, operations + "/unknown", "test-token").statusCode());
            String register = PipelineJson.mapper().writeValueAsString(java.util.Map.of("releaseDescriptorPath", fixture.descriptorPath().toString()));
            assertEquals(200, post(client, releases + "/register", register, "test-token").statusCode());
            String request = "{\"operationKey\":\"key\",\"contractVersion\":\"sha256:contract\",\"releaseVersion\":\"sha256:contract\"}";
            var created = post(client, operations, request, "test-token");
            assertEquals(200, created.statusCode(), created.body());
            var receipt = PipelineJson.mapper().readTree(created.body());
            assertTrue(receipt.get("activationId").asText().length() > 0);
            assertEquals("sha256:contract", receipt.get("releaseVersion").asText());
            assertEquals(fixture.descriptor().artifacts().size(), receipt.get("immutableReleaseIdentity").get("descriptor").get("artifacts").size());
            assertEquals(64, receipt.get("immutableReleaseIdentity").get("metadataFingerprint").asText().length());
            assertEquals(receipt, PipelineJson.mapper().readTree(get(client, operations + "/key", "test-token").body()));
            assertEquals(receipt, PipelineJson.mapper().readTree(post(client, operations, request, "test-token").body()));
            assertEquals(409, post(client, operations, request.replace("sha256:contract\"}", "different\"}"), "test-token").statusCode());
            assertEquals(404, get(client, operations.replace("/tenant/", "/other/") + "/key", "test-token").statusCode());
            assertEquals(200, get(client, releases + "/active", "test-token").statusCode());
        }
    }

    private HttpResponse<String> post(HttpClient client, String path, String body, String token) throws Exception {
        var builder = HttpRequest.newBuilder(base.resolve(path)).header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body));
        if (token != null) builder.header("Authorization", "Bearer " + token);
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(HttpClient client, String path, String token) throws Exception {
        return client.send(HttpRequest.newBuilder(base.resolve(path)).header("Authorization", "Bearer " + token).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private static Path directory() {
        try { return Files.createTempDirectory("activation-operation-http-"); }
        catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
    }

    private static Fixture prepare(Path directory) throws Exception {
        Path repository = Files.createDirectories(directory.resolve("repository/org/example/admission/1.0.0"));
        Path carrier = repository.resolve("admission-1.0.0-application.zip");
        var contract = new PipelineContractDescriptor(1, "org.example.admission", "sha256:contract", "contract", "COMPUTE", "REST",
            "monolith-svc", false, "monolith", java.util.List.of(new PipelineBundleStepDescriptor(0, "Validate", "service",
                "ONE_TO_ONE", String.class.getName(), "Output", "Runtime", "Client", null)), PipelineBundleCapabilities.defaults());
        try (var zip = new java.util.jar.JarOutputStream(Files.newOutputStream(carrier))) {
            zip.putNextEntry(new java.util.jar.JarEntry(PipelineContractDescriptor.RESOURCE_PATH));
            zip.write(PipelineJson.mapper().writeValueAsBytes(contract));
            zip.closeEntry();
            zip.putNextEntry(new java.util.jar.JarEntry("META-INF/pipeline/order.json"));
            zip.write("[\"Validate\"]".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        var descriptor = new PipelineReleaseDescriptor(1, "org.example.admission", "sha256:contract", "sha256:contract", "application",
            java.util.List.of(new PipelineReleaseArtifactDescriptor("application", "application-archive",
                "maven:org.example:admission:zip:application:1.0.0",
                "sha256:" + java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(carrier))),
                java.util.List.of("Validate"), java.util.List.of("local", "rest", "grpc", "sqs"))));
        Path descriptorPath = directory.resolve("pipeline-release.json");
        PipelineJson.mapper().writeValue(descriptorPath.toFile(), descriptor);
        return new Fixture(descriptorPath, descriptor);
    }

    private record Fixture(Path descriptorPath, PipelineReleaseDescriptor descriptor) { }

    private static void deleteDirectory(Path directory) {
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
    }

    public static class Handler implements com.amazonaws.services.lambda.runtime.RequestHandler<String, String> {
        @Override public String handleRequest(String input, com.amazonaws.services.lambda.runtime.Context context) { return input; }
    }
}

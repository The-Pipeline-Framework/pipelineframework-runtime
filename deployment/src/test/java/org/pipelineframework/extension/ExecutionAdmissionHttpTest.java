package org.pipelineframework.extension;

import static org.junit.jupiter.api.Assertions.*;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import io.quarkus.test.QuarkusExtensionTest;
import io.quarkus.test.common.http.TestHTTPResource;
import io.smallrye.mutiny.Uni;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.*;
import org.pipelineframework.orchestrator.worker.*;

/** Actual HTTP/CDI ingress with native memory authority and a verified canonical Maven closure. */
@Isolated
class ExecutionAdmissionHttpTest {
    private static final Path DIRECTORY = directory();
    private static final String PIPELINE = "org.example.admission";
    private static final PipelineContractDescriptor CONTRACT = new PipelineContractDescriptor(1, PIPELINE, "contract", "contract", "COMPUTE", "REST",
        "monolith-svc", false, "monolith", List.of(new PipelineBundleStepDescriptor(0, "Validate", "service", "ONE_TO_ONE",
            String.class.getName(), String.class.getName(), "Runtime", "Client", null)), PipelineBundleCapabilities.defaults());
    @RegisterExtension
    static final QuarkusExtensionTest APPLICATION = new QuarkusExtensionTest()
        .withApplicationRoot(archive -> archive.addClasses(Handler.class, Dispatcher.class, Availability.class)
            .addAsResource(new StringAsset(json(CONTRACT)), PipelineContractDescriptor.RESOURCE_PATH))
        .overrideConfigKey("quarkus.devservices.enabled", "false")
        .overrideConfigKey("quarkus.http.test-port", "0")
        .overrideConfigKey("pipeline.orchestrator.mode", "QUEUE_ASYNC")
        .overrideConfigKey("pipeline.orchestrator.idempotency-policy", "CLIENT_KEY_REQUIRED")
        .overrideConfigKey("pipeline.orchestrator.process-loops-disabled", "true")
        .overrideConfigKey("pipeline.orchestrator.dispatcher-provider", "admission-test")
        .overrideConfigKey("pipeline.orchestrator.control-plane.enabled", "true")
        .overrideConfigKey("pipeline.orchestrator.control-plane.admin-token", "test-token")
        .overrideConfigKey("pipeline.orchestrator.admin.enabled", "true")
        .overrideConfigKey("pipeline.orchestrator.admin.admin-token", "test-token")
        .overrideConfigKey("pipeline.orchestrator.releases.registry.provider", "memory")
        .overrideConfigKey("pipeline.orchestrator.releases.resolver.maven-repositories", DIRECTORY.resolve("repository").toUri().toString())
        .overrideConfigKey("pipeline.orchestrator.releases.resolver.maven-local-repository", DIRECTORY.resolve("cache").toString())
        .overrideConfigKey("pipeline.orchestrator.releases.storage.root", DIRECTORY.resolve("store").toString())
        .setAfterAllCustomizer(() -> deleteDirectory(DIRECTORY));

    @TestHTTPResource URI base;
    @Inject Dispatcher dispatcher;
    @Inject Availability availability;
    @Inject PipelineReleaseRegistry registry;
    @Inject PipelineOrchestratorConfig config;

    @Test
    void expectedPinAdmissionAndExactReplayBypassRenewedArtifactAndWorkerChecks() throws Exception {
        Path descriptor = prepare(Path.of(config.releases().storage().root()).getParent());
        String releases = "/tpf/admin/tenants/tenant/pipelines/" + PIPELINE + "/releases";
        String admissions = "/tpf/control-plane/tenants/tenant/pipelines/" + PIPELINE + "/execution-admissions";
        try (HttpClient client = HttpClient.newHttpClient()) {
            assertEquals(401, post(client, admissions, "{}", null).statusCode());
            assertEquals(401, get(client, admissions + "/key", "wrong").statusCode());
            assertEquals(400, post(client, admissions, "{}", "test-token").statusCode());
            assertEquals(404, get(client, admissions + "/unknown", "test-token").statusCode());
            String registration = json(Map.of("releaseDescriptorPath", descriptor.toString()));
            var registered = post(client, releases + "/register", registration, "test-token");
            assertEquals(200, registered.statusCode(), registered.body());
            var original = registry.get("tenant", PIPELINE, "release-a").await().indefinitely().orElseThrow();
            var b = new PipelineReleaseRecord(original.tenantId(), original.pipelineId(), original.contractVersion(), "release-b",
                PipelineReleaseStatus.REGISTERED, new PipelineReleaseDescriptor(1, PIPELINE, "contract", "release-b",
                    original.descriptor().compiledTruthArtifactId(), original.descriptor().artifacts()), original.primaryArtifactId(),
                original.primaryArtifactDigest(), original.primaryArtifactUri(), original.primaryArtifactSizeBytes(), original.primaryArtifactChecksum(),
                original.contract(), 1, 1, 0);
            registry.register(b).await().indefinitely();
            registry.activate("tenant", PIPELINE, "release-b", System.currentTimeMillis()).await().indefinitely();
            String request = request("key", "release-a", "\"original\"");
            var created = post(client, admissions, request, "test-token");
            assertEquals(200, created.statusCode(), created.body());
            var receipt = PipelineJson.mapper().readTree(created.body());
            assertEquals("release-a", receipt.get("releaseVersion").asText());
            assertFalse(receipt.get("executionId").asText().isBlank());
            assertFalse(receipt.has("inputBytes"));
            assertEquals("release-b", registry.active("tenant", PIPELINE).await().indefinitely().orElseThrow().releaseVersion());
            assertEquals(1, dispatcher.entries());
            assertEquals(1, availability.entries());
            String malformed = request("malformed", "release-a", "\"?\"");
            for (String surrogate : List.of("\\uD800", "\\uDC00")) {
                assertEquals(400, post(client, admissions, malformed.replace("?", surrogate), "test-token").statusCode());
                assertEquals(404, get(client, admissions + "/malformed", "test-token").statusCode());
            }
            assertEquals(1, dispatcher.entries());
            assertEquals(1, availability.entries());
            assertEquals(200, post(client, admissions, malformed, "test-token").statusCode());
            String unicode = request("unicode", "release-a", "\"é😀\"");
            var unicodeReceipt = post(client, admissions, unicode, "test-token");
            assertEquals(200, unicodeReceipt.statusCode(), unicodeReceipt.body());
            assertEquals(PipelineJson.mapper().readTree(unicodeReceipt.body()),
                PipelineJson.mapper().readTree(post(client, admissions, unicode, "test-token").body()));
            assertEquals(3, dispatcher.entries());
            assertEquals(3, availability.entries());
            availability.setAvailable(false);
            Files.delete(Path.of(URI.create(original.primaryArtifactUri())));
            assertThrows(IllegalStateException.class, () -> new PipelineReleaseRegistrar().verify(original));
            assertEquals(receipt, PipelineJson.mapper().readTree(get(client, admissions + "/key", "test-token").body()));
            assertEquals(receipt, PipelineJson.mapper().readTree(post(client, admissions, request, "test-token").body()));
            for (String changed : List.of(request("key", "release-a", "\"changed\""), request("key", "release-b", "\"original\""),
                request.replace("UNI", "MULTI"), request.replace("java.lang.String", "java.util.Map"), request.replace("json", "other"),
                request.replace("false", "true"))) {
                assertEquals(409, post(client, admissions, changed, "test-token").statusCode());
            }
            assertEquals(404, get(client, admissions.replace("/tenant/", "/other/") + "/key", "test-token").statusCode());
            assertEquals(404, get(client, admissions.replace(PIPELINE, "org.example.other") + "/key", "test-token").statusCode());
            assertEquals(3, dispatcher.entries());
            assertEquals(3, availability.entries());
        }
    }

    private static String request(String key, String release, String payload) {
        return json(Map.of("clientKey", key, "contractVersion", "contract", "releaseVersion", release, "inputShape", "UNI",
            "inputPayload", Map.of("payloadTypeId", "java.lang.String", "payloadEncoding", JsonTransitionPayloadCodec.ENCODING, "payload", payload), "outputStreaming", false));
    }
    private static String json(Object value) {
        try { return PipelineJson.mapper().writeValueAsString(value); }
        catch (Exception error) { throw new IllegalStateException(error); }
    }
    private HttpResponse<String> post(HttpClient client, String path, String body, String token) throws Exception {
        var request = HttpRequest.newBuilder(base.resolve(path)).header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body));
        if (token != null) request.header("Authorization", "Bearer " + token);
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }
    private HttpResponse<String> get(HttpClient client, String path, String token) throws Exception {
        return client.send(HttpRequest.newBuilder(base.resolve(path)).header("Authorization", "Bearer " + token).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    }
    private static Path prepare(Path configuredDirectory) throws Exception {
        Path directory = Files.createDirectories(configuredDirectory.resolve("repository/org/example/admission/1.0.0"));
        Path carrier = directory.resolve("admission-1.0.0-application.zip");
        try (var zip = new java.util.jar.JarOutputStream(Files.newOutputStream(carrier))) {
            zip.putNextEntry(new java.util.jar.JarEntry(PipelineContractDescriptor.RESOURCE_PATH));
            zip.write(PipelineJson.mapper().writeValueAsBytes(CONTRACT));
            zip.closeEntry();
            zip.putNextEntry(new java.util.jar.JarEntry("META-INF/pipeline/order.json"));
            zip.write("[\"Validate\"]".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        var descriptor = new PipelineReleaseDescriptor(1, PIPELINE, "contract", "release-a", "application", List.of(
            new PipelineReleaseArtifactDescriptor("application", "application-archive", "maven:org.example:admission:zip:application:1.0.0",
                "sha256:" + java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(carrier))),
                List.of("Validate"), List.of("local", "rest", "grpc", "sqs"))));
        Path descriptorPath = configuredDirectory.resolve("pipeline-release.json");
        PipelineJson.mapper().writeValue(descriptorPath.toFile(), descriptor);
        return descriptorPath;
    }
    private static Path directory() {
        try { return Files.createTempDirectory("execution-admission-http-"); }
        catch (java.io.IOException error) { throw new java.io.UncheckedIOException(error); }
    }
    private static void deleteDirectory(Path directory) {
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        } catch (java.io.IOException error) { throw new java.io.UncheckedIOException(error); }
    }
    @ApplicationScoped
    public static class Dispatcher implements WorkDispatcher {
        final AtomicInteger entries = new AtomicInteger();
        public int entries() { return entries.get(); }
        public String providerName() { return "admission-test"; }
        public Optional<String> startupValidationError() { return Optional.empty(); }
        public Uni<Void> enqueueNow(ExecutionWorkItem item) { return Uni.createFrom().item(() -> { entries.incrementAndGet(); return null; }); }
        public Uni<Void> enqueueDelayed(ExecutionWorkItem item, java.time.Duration delay) { return enqueueNow(item); }
    }
    @Alternative
    @Priority(1)
    @ApplicationScoped
    public static class Availability implements PipelineWorkerAvailability {
        final AtomicInteger entries = new AtomicInteger();
        volatile boolean available = true;
        public int entries() { return entries.get(); }
        public void setAvailable(boolean value) { available = value; }
        public Uni<PipelineWorkerAvailabilityResult> check(PipelineWorkerAvailabilityRequest request) {
            entries.incrementAndGet();
            return Uni.createFrom().item(new PipelineWorkerAvailabilityResult(available, "test-boundary", null, "test provider boundary"));
        }
    }
    public static class Handler implements com.amazonaws.services.lambda.runtime.RequestHandler<String, String> {
        public String handleRequest(String input, com.amazonaws.services.lambda.runtime.Context context) { return input; }
    }
}

package org.pipelineframework.extension;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import io.quarkus.test.QuarkusExtensionTest;
import io.quarkus.test.common.http.TestHTTPResource;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.*;

/** Normal CDI/HTTP current-event serialization; native registration below is a fixture, not artifact verification. */
@Isolated
class CurrentActivationHttpTest {
    @RegisterExtension static final QuarkusExtensionTest APPLICATION = new QuarkusExtensionTest()
        .withApplicationRoot(archive -> archive.addClass(Handler.class))
        .overrideConfigKey("quarkus.devservices.enabled", "false")
        .overrideConfigKey("quarkus.http.test-port", "0")
        .overrideConfigKey("pipeline.orchestrator.admin.enabled", "true")
        .overrideConfigKey("pipeline.orchestrator.admin.admin-token", "token")
        .overrideConfigKey("pipeline.orchestrator.releases.registry.provider", "memory");

    @TestHTTPResource URI base;
    @Inject PipelineReleaseRegistry registry;

    @Test void actualHttpCurrentIdentityDistinguishesSameReleaseTimeAndPreservesHistoricalReceipt() throws Exception {
        String path = "/tpf/admin/tenants/tenant/pipelines/pipeline/current-activation";
        try (var client = HttpClient.newHttpClient()) {
            assertEquals(401, get(client, path, "wrong").statusCode());
            assertEquals("NONE", PipelineJson.mapper().readTree(get(client, path, "token").body()).path("state").asText());
            var release = new PipelineReleaseRecord("tenant", "pipeline", "contract", "release", PipelineReleaseStatus.REGISTERED,
                new PipelineReleaseDescriptor(1, "pipeline", "contract", "release", "artifact", List.of()),
                "artifact", "sha256:artifact", "file:/artifact.jar", 1, "artifact", null, 1, 1, 0);
            registry.register(release).await().indefinitely();
            var first = registry.activateOnce(new ActivationOperationCommand("first", release, 3000)).await().indefinitely();
            var second = registry.activateOnce(new ActivationOperationCommand("second", release, 3000)).await().indefinitely();
            var before = registry.list("tenant", "pipeline").await().indefinitely();
            var response = get(client, path, "token");
            assertEquals(200, response.statusCode());
            var json = PipelineJson.mapper().readTree(response.body());
            assertEquals("IDENTIFIED", json.path("state").asText());
            var event = json.path("identifiedEvent");
            assertEquals(second.activationId(), event.path("activationId").asText());
            assertNotEquals(first.activationId(), event.path("activationId").asText());
            assertEquals("contract", event.path("contractVersion").asText());
            assertEquals("release", event.path("releaseVersion").asText());
            assertEquals("sha256:artifact", event.path("immutableReleaseIdentity").path("primaryArtifactDigest").asText());
            assertEquals(3000, event.path("activatedAtEpochMs").asLong());
            assertEquals(before, registry.list("tenant", "pipeline").await().indefinitely());
            assertEquals(first, registry.getActivationOperation("tenant", "pipeline", "first").await().indefinitely().orElseThrow());
            assertEquals("NONE", PipelineJson.mapper().readTree(get(client,
                "/tpf/admin/tenants/other/pipelines/pipeline/current-activation", "token").body()).path("state").asText());
        }
    }

    private HttpResponse<String> get(HttpClient client, String path, String token) throws Exception {
        return client.send(HttpRequest.newBuilder(base.resolve(path)).header("Authorization", "Bearer " + token).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    }

    public static class Handler implements com.amazonaws.services.lambda.runtime.RequestHandler<String, String> {
        @Override public String handleRequest(String input, com.amazonaws.services.lambda.runtime.Context context) { return input; }
    }
}

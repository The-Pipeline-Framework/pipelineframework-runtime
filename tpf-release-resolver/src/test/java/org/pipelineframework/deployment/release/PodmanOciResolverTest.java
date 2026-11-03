package org.pipelineframework.deployment.release;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;

@EnabledIfEnvironmentVariable(named = "TPF_RUN_PODMAN_OCI_TEST", matches = "true")
class PodmanOciResolverTest {
    @Test
    void resolvesDigestQualifiedManifestFromPodmanRegistry() throws Exception {
        String container = "tpf-oci-test-" + UUID.randomUUID();
        command("podman", "run", "--detach", "--rm", "--name", container, "--publish", "127.0.0.1::5000", "registry:2");
        try {
            String address = awaitAddress(container);
            byte[] configuration = "{}".getBytes(StandardCharsets.UTF_8);
            String configurationDigest = digest(configuration);
            uploadBlob(address, configurationDigest, configuration);
            byte[] manifest = ("{\"schemaVersion\":2,\"mediaType\":\"application/vnd.oci.image.manifest.v1+json\","
                + "\"config\":{\"mediaType\":\"application/vnd.oci.empty.v1+json\",\"digest\":\""
                + configurationDigest + "\",\"size\":" + configuration.length + "},\"layers\":[]}")
                .getBytes(StandardCharsets.UTF_8);
            String manifestDigest = digest(manifest);
            put(address + "/v2/example/orders/manifests/latest", "application/vnd.oci.image.manifest.v1+json", manifest);

            var artifact = new PipelineReleaseArtifactDescriptor(
                "image", "container-image", "oci://" + address.substring("http://".length())
                    + "/example/orders@" + manifestDigest,
                manifestDigest, List.of(), List.of());
            byte[] resolved = new OciArtifactResolverProvider()
                .create(new OciResolverConfiguration("", Map.of(), Set.of(address.substring("http://".length()))),
                    CredentialResolver.NONE)
                .open(artifact).readAllBytes();

            assertArrayEquals(manifest, resolved);
            assertEquals(manifestDigest, digest(resolved));
        } finally {
            command("podman", "rm", "--force", container);
        }
    }

    private static String awaitAddress(String container) throws Exception {
        for (int attempt = 0; attempt < 50; attempt++) {
            String port = command("podman", "port", container, "5000/tcp").trim();
            if (!port.isBlank()) {
                String address = "http://" + port;
                try {
                    HttpResponse<Void> response = HttpClient.newHttpClient().send(
                        HttpRequest.newBuilder(URI.create(address + "/v2/")).GET().build(),
                        HttpResponse.BodyHandlers.discarding());
                    if (response.statusCode() == 200) return address;
                } catch (IOException ignored) {
                    // Registry process is still starting.
                }
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("Podman registry did not become ready");
    }

    private static void uploadBlob(String address, String digest, byte[] content) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<Void> start = client.send(
            HttpRequest.newBuilder(URI.create(address + "/v2/example/orders/blobs/uploads/"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(),
            HttpResponse.BodyHandlers.discarding());
        assertEquals(202, start.statusCode());
        String location = start.headers().firstValue("Location").orElseThrow();
        URI upload = URI.create(address).resolve(location + (location.contains("?") ? "&" : "?") + "digest=" + digest);
        HttpResponse<Void> finish = client.send(
            HttpRequest.newBuilder(upload).PUT(HttpRequest.BodyPublishers.ofByteArray(content)).build(),
            HttpResponse.BodyHandlers.discarding());
        assertEquals(201, finish.statusCode());
    }

    private static void put(String location, String contentType, byte[] content) throws Exception {
        HttpResponse<Void> response = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(URI.create(location)).header("Content-Type", contentType)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(content)).build(),
            HttpResponse.BodyHandlers.discarding());
        assertEquals(201, response.statusCode());
    }

    private static String digest(byte[] value) throws Exception {
        return "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    }

    private static String command(String... command) throws Exception {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (process.waitFor() != 0) throw new IllegalStateException(String.join(" ", command) + " failed: " + output);
        return output;
    }
}

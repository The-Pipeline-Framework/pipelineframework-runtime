package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.net.URL;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DisklessMultipartHttpTest.NoFileUploadProfile.class)
class DisklessMultipartHttpTest {
    @TestHTTPResource("/tpf-test/diskless-upload")
    URL endpoint;

    @Test
    void streamsMultipartWhenQuarkusFileUploadHandlingIsDisabled() throws Exception {
        byte[] content = new byte[1024 * 1024];
        java.util.Arrays.fill(content, (byte) 'x');
        byte[] start = ("--boundary\r\nContent-Disposition: form-data; name=\"file\"; filename=\"one.bin\"\r\n"
            + "Content-Type: application/octet-stream\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
        byte[] end = "\r\n--boundary--\r\n".getBytes(StandardCharsets.US_ASCII);
        byte[] request = new byte[start.length + content.length + end.length];
        System.arraycopy(start, 0, request, 0, start.length);
        System.arraycopy(content, 0, request, start.length, content.length);
        System.arraycopy(end, 0, request, start.length + content.length, end.length);

        HttpRequest upload = HttpRequest.newBuilder(URI.create(endpoint.toString()))
            .header("Content-Type", "multipart/form-data; boundary=boundary")
            .POST(HttpRequest.BodyPublishers.ofByteArray(request)).build();
        try (HttpClient client = HttpClient.newHttpClient()) {
            var response = client.send(upload, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode(), response.body());
            assertEquals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content)),
                response.body());
        }
    }

    public static class NoFileUploadProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.devservices.enabled", "false",
                "quarkus.http.body.handle-file-uploads", "false",
                "quarkus.http.body.uploads-directory", "/proc/tpf-upload-files-not-allowed");
        }
    }
}

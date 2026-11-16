package org.pipelineframework.aws.durable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class AwsDurableRegisteredRestTargetSourceTest {
    private static final String PREFIX = "arn:aws:secretsmanager:us-east-2:123456789012:secret:tpf/worker-signing/example/";
    private static final String CREDENTIAL = "config:tpf.aws.durable.rest-credential." + PREFIX
        + "worker@active-ABC123@11111111-1111-1111-1111-111111111111";
    private static final String PROPERTY_PREFIX = "pipeline.orchestrator.worker.targets.release1.";

    @Test
    void pinsImmutableVersionAndReadsScopedProperties() throws Exception {
        var data = data();
        var targets = AwsDurableRegisteredRestTargetSource.parse(data, "S3Version", digest(data), PREFIX);
        assertEquals("tenant", targets.get("release1").tenantId());
        assertEquals("entry", targets.get("release1").entryId().orElseThrow());
        assertEquals(CREDENTIAL, targets.get("release1").sharedSecretRef());
        assertEquals("rest", targets.get("release1").protocol());
        assertEquals(PREFIX + "worker@active-ABC123",
            AwsDurableRegisteredRestTargetSource.credentialRequest(CREDENTIAL, PREFIX).secretId());
    }

    @Test
    void rejectsUntrustedOrAmbiguousPublications() throws Exception {
        var data = data();
        assertThrows(IllegalArgumentException.class,
            () -> AwsDurableRegisteredRestTargetSource.parse(data, "null", digest(data), PREFIX));
        assertThrows(IllegalArgumentException.class,
            () -> AwsDurableRegisteredRestTargetSource.parse(data, "S3Version", "wrong", PREFIX));
        assertThrows(IllegalArgumentException.class,
            () -> AwsDurableRegisteredRestTargetSource.parse(data, "S3Version", digest(data), PREFIX + "foreign/"));
        byte[] duplicate = (new String(data, StandardCharsets.UTF_8) + PROPERTY_PREFIX + "tenant-id=foreign\n")
            .getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class,
            () -> AwsDurableRegisteredRestTargetSource.parse(duplicate, "S3Version", digest(duplicate), PREFIX));
    }

    @Test
    void coalescesColdReadsAndFailsClosedOnAnInvalidRefresh() throws Exception {
        var clock = new AtomicLong();
        var s3 = mock(S3Client.class);
        var source = new AwsDurableRegisteredRestTargetSource(s3, clock::get);
        source.bucket = Optional.of("routing-bucket");
        source.key = Optional.of("tpf/rest-targets/regional.properties");
        source.secretArnPrefix = Optional.of(PREFIX);
        var request = GetObjectRequest.builder().bucket("routing-bucket").key("tpf/rest-targets/regional.properties").build();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        byte[] data = data();
        when(s3.getObject(request)).thenAnswer(ignored -> {
            entered.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return object(data, digest(data));
        }).thenAnswer(ignored -> object(data, "incorrect-digest"));
        try {
            var first = source.snapshot().subscribeAsCompletionStage();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var second = source.snapshot().subscribeAsCompletionStage();
            release.countDown();
            assertEquals(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS));
            source.snapshot().await().atMost(Duration.ofSeconds(5));
            verify(s3).getObject(request);

            clock.addAndGet(Duration.ofSeconds(6).toNanos());
            assertThrows(IllegalStateException.class, () -> source.snapshot().await().atMost(Duration.ofSeconds(5)));
            assertThrows(IllegalStateException.class, () -> source.snapshot().await().atMost(Duration.ofSeconds(5)));
            verify(s3, times(2)).getObject(request);
        } finally {
            release.countDown();
            source.close();
        }
    }

    private static ResponseInputStream<GetObjectResponse> object(byte[] data, String digest) {
        var response = GetObjectResponse.builder().versionId("S3Version").metadata(Map.of("digest", digest)).build();
        return new ResponseInputStream<>(response, AbortableInputStream.create(new ByteArrayInputStream(data)));
    }

    private static byte[] data() {
        return ("# generation 2 attempt publication\n"
            + PROPERTY_PREFIX + "entry-id=entry\n"
            + PROPERTY_PREFIX + "tenant-id=tenant\n"
            + PROPERTY_PREFIX + "pipeline-id=pipeline\n"
            + PROPERTY_PREFIX + "contract-version=1\n"
            + PROPERTY_PREFIX + "release-version=2\n"
            + PROPERTY_PREFIX + "worker-id=worker\n"
            + PROPERTY_PREFIX + "endpoint=https://worker.example\n"
            + PROPERTY_PREFIX + "shared-secret-ref=" + CREDENTIAL + "\n"
            + PROPERTY_PREFIX + "artifact-id=app\n"
            + PROPERTY_PREFIX + "artifact-digest=sha256:identity\n")
            .getBytes(StandardCharsets.UTF_8);
    }

    private static String digest(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }
}

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
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;
import software.amazon.awssdk.services.ssm.model.Parameter;
import software.amazon.awssdk.services.ssm.model.ParameterType;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class AwsDurableRegisteredRestTargetSourceTest {
    private static final String PREFIX = "arn:aws:ssm:us-east-2:123456789012:parameter/tpf/worker-signing/example/";
    private static final String CREDENTIAL = "config:tpf.aws.durable.rest-parameter." + PREFIX + "worker@7";
    private static final String PROPERTY_PREFIX = "pipeline.orchestrator.worker.targets.release1.";

    @Test
    void pinsImmutableVersionAndReadsScopedProperties() throws Exception {
        var data = data();
        var targets = AwsDurableRegisteredRestTargetSource.parse(data, "S3Version", digest(data), PREFIX);
        assertEquals("tenant", targets.get("release1").tenantId());
        assertEquals("entry", targets.get("release1").entryId().orElseThrow());
        assertEquals(CREDENTIAL, targets.get("release1").sharedSecretRef());
        assertEquals("rest", targets.get("release1").protocol());
        var request = AwsDurableRegisteredRestTargetSource.credentialRequest(CREDENTIAL, PREFIX);
        assertEquals(PREFIX + "worker:7", request.read().name());
        assertTrue(request.read().withDecryption());
        assertThrows(IllegalArgumentException.class, () ->
            AwsDurableRegisteredRestTargetSource.credentialRequest(CREDENTIAL.replace("@7", "@latest"), PREFIX));
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
        var source = new AwsDurableRegisteredRestTargetSource(s3, null, clock::get);
        source.bucket = Optional.of("routing-bucket");
        source.key = Optional.of("tpf/rest-targets/regional.properties");
        source.parameterArnPrefix = Optional.of(PREFIX);
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

    @Test
    void onlyCachesTheExactSecureStringVersion() {
        var ssm = mock(SsmClient.class);
        var source = new AwsDurableRegisteredRestTargetSource(null, ssm, System::nanoTime);
        source.parameterArnPrefix = Optional.of(PREFIX);
        var request = GetParameterRequest.builder().name(PREFIX + "worker:7").withDecryption(true).build();
        var parameter = Parameter.builder().arn(PREFIX + "worker").name("/tpf/worker-signing/example/worker")
            .type(ParameterType.SECURE_STRING).value("a".repeat(32));
        when(ssm.getParameter(request))
            .thenReturn(GetParameterResponse.builder().parameter(parameter.version(8L).build()).build())
            .thenReturn(GetParameterResponse.builder().parameter(parameter.version(7L).build()).build());
        try {
            assertThrows(IllegalStateException.class, () -> source.prepareCredential(CREDENTIAL).await().atMost(Duration.ofSeconds(5)));
            assertThrows(IllegalStateException.class, () -> source.preparedCredential(CREDENTIAL));
            source.prepareCredential(CREDENTIAL).await().atMost(Duration.ofSeconds(5));
            source.prepareCredential(CREDENTIAL).await().atMost(Duration.ofSeconds(5));
            assertEquals(Optional.of("a".repeat(32)), source.preparedCredential(CREDENTIAL));
            verify(ssm, times(2)).getParameter(request);
        } finally {
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

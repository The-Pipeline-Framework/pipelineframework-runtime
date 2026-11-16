package org.pipelineframework.aws.durable;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AwsDurableRestRequestSignerTest {
    @Test
    void signsTheExistingTpfRequestForBothFunctionUrlDirections() {
        var signer = new AwsDurableRestRequestSigner(() ->
            AwsSessionCredentials.create("TESTACCESSKEY", "test-secret", "test-session"));
        signer.region = Optional.of("us-east-1");
        for (String path : new String[] {
            "/pipeline/worker/transitions/execute", "/pipeline/coordinator/await/registrations"
        }) {
            byte[] body = "{\"test\":true}".getBytes(StandardCharsets.UTF_8);
            var builder = HttpRequest.newBuilder(URI.create("https://test.lambda-url.us-east-1.on.aws" + path))
                .header(TransitionWorkerSignature.SIGNATURE_HEADER, "tpf-signature")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));

            signer.sign(builder, body);

            var headers = builder.build().headers();
            assertThat(headers.firstValue(TransitionWorkerSignature.SIGNATURE_HEADER)).contains("tpf-signature");
            assertThat(headers.firstValue("Authorization").orElseThrow())
                .startsWith("AWS4-HMAC-SHA256 Credential=TESTACCESSKEY/")
                .contains("/us-east-1/lambda/aws4_request");
            assertThat(headers.firstValue("x-amz-security-token")).contains("test-session");
        }
    }

    @Test
    void rejectsUnexpectedOriginsBeforeResolvingAwsCredentials() {
        var signer = new AwsDurableRestRequestSigner(() -> {
            throw new AssertionError("Unexpected credential lookup");
        });
        signer.region = Optional.of("us-east-1");

        assertThatThrownBy(() -> signer.sign(
            HttpRequest.newBuilder(URI.create("https://test.lambda-url.us-west-2.on.aws")), new byte[0]))
            .isInstanceOf(IllegalArgumentException.class);
    }
}

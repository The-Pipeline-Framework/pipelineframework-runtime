package org.pipelineframework.aws.durable;

import java.io.ByteArrayInputStream;
import java.net.http.HttpRequest;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.orchestrator.WorkerRestRequestSigner;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;

/** Adds IAM authentication to configured Lambda Function URL calls. */
@ApplicationScoped
public class AwsDurableRestRequestSigner implements WorkerRestRequestSigner {
    @Inject
    @ConfigProperty(name = "tpf.aws.durable.rest-signing-region")
    Optional<String> region;

    private final DefaultCredentialsProvider credentials;
    private final Supplier<AwsCredentialsIdentity> identity;
    private final AwsV4HttpSigner signer = AwsV4HttpSigner.create();

    public AwsDurableRestRequestSigner() {
        credentials = DefaultCredentialsProvider.builder().build();
        identity = credentials::resolveCredentials;
    }

    AwsDurableRestRequestSigner(Supplier<AwsCredentialsIdentity> identity) {
        credentials = null;
        this.identity = identity;
    }

    @Override
    public void sign(HttpRequest.Builder builder, byte[] body) {
        if (region.isEmpty()) {
            return;
        }
        HttpRequest request = builder.build();
        String host = request.uri().getHost();
        if (!"https".equals(request.uri().getScheme()) || host == null
            || !host.endsWith(".lambda-url." + region.orElseThrow() + ".on.aws")) {
            throw new IllegalArgumentException("AWS Durable REST signing requires a Function URL in the configured Region");
        }

        var unsigned = SdkHttpRequest.builder().uri(request.uri())
            .method(SdkHttpMethod.fromValue(request.method())).headers(request.headers().map()).build();
        var signed = signer.sign(SignRequest.builder(identity.get())
            .request(unsigned)
            .payload(() -> new ByteArrayInputStream(body))
            .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "lambda")
            .putProperty(AwsV4HttpSigner.REGION_NAME, region.orElseThrow())
            .build());
        signed.request().headers().forEach((name, values) -> {
            if (!Set.of("host", "content-length").contains(name.toLowerCase(java.util.Locale.ROOT))) {
                builder.setHeader(name, values.getFirst());
                values.stream().skip(1).forEach(value -> builder.header(name, value));
            }
        });
    }

    @PreDestroy
    void close() {
        if (credentials != null) credentials.close();
    }
}

package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.QuarkusUnitTest;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;

@Isolated
class AwsDurableLambdaClientApplicationTest {
    @RegisterExtension
    static final QuarkusUnitTest application = LambdaClientAugmentation.sdkDefault()
        .withApplicationRoot(archive -> archive.addClasses(AwsDurableLambdaClientProducer.class,
            LambdaClientAugmentation.class,
            ApplicationClientProducer.class));

    @Inject
    LambdaClient client;

    @AfterEach
    void closeFixtureClient() { client.close(); }

    @Test
    void applicationClientOverridesBothDefaults() {
        assertThat(client.serviceClientConfiguration().region()).isEqualTo(Region.EU_WEST_1);
    }

    @Singleton
    public static class ApplicationClientProducer {
        @Produces
        @Singleton
        LambdaClient applicationClient() {
            return LambdaClient.builder().region(Region.EU_WEST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create()).build();
        }
    }
}

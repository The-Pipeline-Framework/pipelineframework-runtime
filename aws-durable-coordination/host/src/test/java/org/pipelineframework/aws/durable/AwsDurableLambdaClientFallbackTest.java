package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.QuarkusUnitTest;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;

@Isolated
class AwsDurableLambdaClientFallbackTest {
    @RegisterExtension
    static final QuarkusUnitTest application = LambdaClientAugmentation.fallback();

    @Inject
    Instance<LambdaClient> clients;

    @Test
    void suppliesClientWithoutSdkExtension() {
        String previousRegion = System.getProperty("aws.region");
        System.setProperty("aws.region", "us-east-1");
        try {
            assertThat(clients.isResolvable()).isTrue();
            assertThat(clients.get().serviceClientConfiguration().region()).isEqualTo(Region.US_EAST_1);
        } finally {
            if (previousRegion == null) {
                System.clearProperty("aws.region");
            } else {
                System.setProperty("aws.region", previousRegion);
            }
        }
    }
}

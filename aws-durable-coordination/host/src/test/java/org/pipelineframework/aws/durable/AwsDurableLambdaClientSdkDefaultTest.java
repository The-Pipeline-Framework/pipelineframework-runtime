package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.QuarkusUnitTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;

@Isolated
class AwsDurableLambdaClientSdkDefaultTest {
    @RegisterExtension
    static final QuarkusUnitTest application = LambdaClientAugmentation.sdkDefault();

    @Inject
    LambdaClient client;

    @AfterEach
    void closeFixtureClient() { client.close(); }

    @Test
    void yieldsToSdkExtensionDefaultClient() {
        assertThat(client.serviceClientConfiguration().region()).isEqualTo(Region.US_WEST_2);
    }
}

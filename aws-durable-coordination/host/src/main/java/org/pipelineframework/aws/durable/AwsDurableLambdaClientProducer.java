package org.pipelineframework.aws.durable;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import software.amazon.awssdk.services.lambda.LambdaClient;

/** Existing host transport uses the standard AWS execution-role and region configuration. */
@ApplicationScoped
public class AwsDurableLambdaClientProducer {
    @Produces
    @Singleton
    @DefaultBean
    LambdaClient client() { return LambdaClient.builder().build(); }

    void close(@Disposes LambdaClient client) { client.close(); }
}

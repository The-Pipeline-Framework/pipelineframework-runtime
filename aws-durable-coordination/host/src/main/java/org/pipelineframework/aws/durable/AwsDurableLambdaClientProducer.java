package org.pipelineframework.aws.durable;

import io.quarkus.arc.DefaultBean;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import software.amazon.awssdk.services.lambda.LambdaClient;

/** Existing host transport uses the standard AWS execution-role and region configuration. */
@ApplicationScoped
public class AwsDurableLambdaClientProducer {
    /** Supplies a fallback below the SDK extension's default client and application-owned clients. */
    @Produces
    @Singleton
    @DefaultBean
    @Priority(-1)
    LambdaClient client() { return LambdaClient.builder().build(); }

    /** Releases only the client owned by this producer. */
    void close(@Disposes LambdaClient client) { client.close(); }
}

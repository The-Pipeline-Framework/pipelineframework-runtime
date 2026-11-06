package org.pipelineframework.aws.durable;

import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.gizmo2.Const;
import io.quarkus.gizmo2.desc.MethodDesc;
import io.quarkus.test.QuarkusUnitTest;
import jakarta.inject.Singleton;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.LambdaClientBuilder;
import software.amazon.awssdk.utils.builder.SdkBuilder;

final class LambdaClientAugmentation {
    private LambdaClientAugmentation() {}

    static QuarkusUnitTest fallback() {
        return new QuarkusUnitTest().overrideConfigKey("quarkus.devservices.enabled", "false")
            .overrideConfigKey("quarkus.http.test-port", "0")
            .overrideConfigKey("quarkus.grpc.server.test-port", "0")
            .withApplicationRoot(archive -> archive.addClasses(
                AwsDurableLambdaClientProducer.class, LambdaClientAugmentation.class));
    }

    static QuarkusUnitTest sdkDefault() {
        return fallback().addBuildChainCustomizer(chain -> chain.addBuildStep(context ->
            // Quarkiverse AmazonClientExtensionsProcessor registers an unprioritized default bean
            // (priority 0), rather than a regular bean. Preserve that competing-default behavior.
            context.produce(SyntheticBeanBuildItem.configure(LambdaClient.class)
                .defaultBean().unremovable().scope(Singleton.class)
                .creator(generation -> {
                    var method = generation.createMethod();
                    var builder = method.localVar("builder", method.invokeStatic(MethodDesc.of(
                        LambdaClient.class, "builder", LambdaClientBuilder.class)));
                    var region = method.localVar("region", method.invokeStatic(MethodDesc.of(
                        Region.class, "of", Region.class, String.class), Const.of("us-west-2")));
                    method.invokeInterface(MethodDesc.of(
                        AwsClientBuilder.class, "region", AwsClientBuilder.class, Region.class), builder, region);
                    method.return_(method.invokeInterface(MethodDesc.of(
                        SdkBuilder.class, "build", Object.class), builder));
                }).done()))
            .produces(SyntheticBeanBuildItem.class).build());
    }
}

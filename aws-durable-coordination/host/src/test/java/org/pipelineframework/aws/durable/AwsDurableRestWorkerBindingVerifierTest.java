package org.pipelineframework.aws.durable;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.release.PipelineReleaseDispatchBinding;
import org.pipelineframework.orchestrator.release.PipelineReleaseEntryBinding;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AwsDurableRestWorkerBindingVerifierTest {
    private static final String FUNCTION = "arn:aws:lambda:us-east-1:123456789012:function:worker";
    private static final String ORIGIN = "https://worker.lambda-url.us-east-1.on.aws";
    private static final PipelineReleaseEntryBinding BINDING = new PipelineReleaseEntryBinding(
        "tenant", "pipeline", "contract", "release", "entry", "artifact", "artifact-digest", "aws",
        FUNCTION + ":3", "code-digest", "deployment",
        Optional.of(new PipelineReleaseDispatchBinding("rest", ORIGIN, "config:worker", "worker")), Optional.of("configuration"));

    @Test
    void reusesTrustedObservationAcrossCalls() {
        var verifier = new AwsDurableRestWorkerBindingVerifier();
        verifier.lambda = lambda(GetAliasResponse.builder().functionVersion("3").build(), "configuration");
        verifier.region = Optional.of("us-east-1");

        verifier.verify(BINDING).await().indefinitely();
        verifier.verify(BINDING).await().indefinitely();

        verify(verifier.lambda, times(1)).getAlias(any(Consumer.class));
        verify(verifier.lambda, times(1)).getFunctionConfiguration(any(Consumer.class));
    }

    @Test
    void publicationRejectsRepointingWeightedRoutingAndConfigurationDrift() {
        var repointed = GetAliasResponse.builder().functionVersion("4").build();
        var weighted = GetAliasResponse.builder().functionVersion("3")
            .routingConfig(builder -> builder.additionalVersionWeights(Map.of("4", 0.1))).build();
        var expected = GetAliasResponse.builder().functionVersion("3").build();

        assertThrows(IllegalStateException.class, () -> check(lambda(repointed, "configuration")));
        assertThrows(IllegalStateException.class, () -> check(lambda(weighted, "configuration")));
        assertThrows(IllegalStateException.class, () -> check(lambda(expected, "changed")));
        check(lambda(expected, "configuration"));
    }

    @Test
    void refusesToPublishAnUnverifiedBinding() {
        var verifier = new AwsDurableRestWorkerBindingVerifier();
        verifier.region = Optional.of("us-east-1");
        verifier.lambda = lambda(GetAliasResponse.builder().functionVersion("4").build(), "configuration");
        var registry = mock(PipelineReleaseRegistry.class);

        assertThrows(IllegalStateException.class, () -> verifier.publish(registry, BINDING).await().indefinitely());
        verifyNoInteractions(registry);
    }

    private static void check(LambdaClient lambda) {
        AwsDurableRestWorkerBindingVerifier.verifyPublication(lambda, "us-east-1", BINDING);
    }

    private static LambdaClient lambda(GetAliasResponse alias, String configDigest) {
        var client = mock(LambdaClient.class);
        when(client.getAlias(org.mockito.ArgumentMatchers.<Consumer<GetAliasRequest.Builder>>any()))
            .thenAnswer(invocation -> {
                var builder = GetAliasRequest.builder();
                invocation.<Consumer<GetAliasRequest.Builder>>getArgument(0).accept(builder);
                var request = builder.build();
                assertEquals(FUNCTION, request.functionName());
                assertEquals("release", request.name());
                return alias;
            });
        when(client.getFunctionUrlConfig(org.mockito.ArgumentMatchers.<Consumer<GetFunctionUrlConfigRequest.Builder>>any()))
            .thenAnswer(invocation -> {
                var builder = GetFunctionUrlConfigRequest.builder();
                invocation.<Consumer<GetFunctionUrlConfigRequest.Builder>>getArgument(0).accept(builder);
                var request = builder.build();
                assertEquals(FUNCTION, request.functionName());
                assertEquals("release", request.qualifier());
                return GetFunctionUrlConfigResponse.builder()
                    .functionUrl("HTTPS://WORKER.lambda-url.us-east-1.on.aws:443/")
                    .authType(FunctionUrlAuthType.AWS_IAM).build();
            });
        when(client.getFunctionConfiguration(org.mockito.ArgumentMatchers.<Consumer<GetFunctionConfigurationRequest.Builder>>any()))
            .thenAnswer(invocation -> {
                var builder = GetFunctionConfigurationRequest.builder();
                invocation.<Consumer<GetFunctionConfigurationRequest.Builder>>getArgument(0).accept(builder);
                var request = builder.build();
                assertEquals(FUNCTION, request.functionName());
                assertEquals("3", request.qualifier());
                return GetFunctionConfigurationResponse.builder()
                    .version("3").codeSha256("code-digest").configSha256(configDigest).build();
            });
        return client;
    }
}

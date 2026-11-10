package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.pipelineframework.aws.durable.AwsDurableRetainedReleaseRoutingIT.*;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.release.*;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;

class AwsDurableReleaseRouterTest {
    @Test
    void workUsesPinnedReleaseAfterActivationAndRollback() throws Exception {
        var f = new Fixture(); f.bind(arn("1"), codeDigest("release-a"));
        f.registry.register(release("release-b")).await().indefinitely();
        f.registry.activate("tenant", "pipeline", "release-b", 2).await().indefinitely();
        f.registry.activate("tenant", "pipeline", "release-a", 3).await().indefinitely();
        when(f.lambda.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class))).thenReturn(
            GetFunctionConfigurationResponse.builder().codeSha256(codeDigest("release-a")).build());
        when(f.lambda.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().statusCode(200)
            .executedVersion("1").payload(SdkBytes.fromUtf8String("{\"batchItemFailures\":[]}")).build());
        assertThat(f.handle()).isEqualTo(SqsMessageDisposition.ACKNOWLEDGE);
        verify(f.lambda).invoke(argThat((InvokeRequest request) -> request.functionName().equals(arn("1"))
            && request.payload().asUtf8String().contains("execution-a")));
        assertThat(f.localCalls).hasValue(0);
    }

    @Test
    void absentBindingNeverFallsBackToCurrentPackage() {
        var f = new Fixture();
        assertThatThrownBy(f::handle).hasMessageContaining("binding is unavailable");
        verifyNoInteractions(f.lambda); assertThat(f.localCalls).hasValue(0);
    }

    @Test
    void changedProviderCodeCannotExecuteOrFallBack() {
        var f = new Fixture(); f.bind(arn("1"), codeDigest("release-a"));
        when(f.lambda.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class))).thenReturn(
            GetFunctionConfigurationResponse.builder().codeSha256(codeDigest("release-b")).build());
        assertThatThrownBy(f::handle).hasMessageContaining("digest is unverified");
        verify(f.lambda, never()).invoke(any(InvokeRequest.class)); assertThat(f.localCalls).hasValue(0);
    }

    @Test
    void codeObservationMustMatchOriginalReleaseZip() {
        var f = new Fixture(); f.bind(arn("1"), codeDigest("release-b"));
        assertThatThrownBy(f::handle).hasMessageContaining("original Release ZIP");
        verifyNoInteractions(f.lambda); assertThat(f.localCalls).hasValue(0);
    }

    @Test
    void aliasAndDifferentFunctionCannotAcquireRoutingAuthority() {
        for (String destination : java.util.List.of(arn("active"), arn("1").replace("transition:", "other:"))) {
            var f = new Fixture(); f.bind(destination, codeDigest("release-a"));
            assertThatThrownBy(f::handle).hasMessageContaining("exact function");
            verifyNoInteractions(f.lambda); assertThat(f.localCalls).hasValue(0);
        }
    }

    @Test
    void ambiguousInvocationAcknowledgementNeverAcknowledgesWork() {
        var f = new Fixture(); f.bind(arn("1"), codeDigest("release-a"));
        when(f.lambda.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class))).thenReturn(
            GetFunctionConfigurationResponse.builder().codeSha256(codeDigest("release-a")).build());
        when(f.lambda.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().statusCode(200)
            .executedVersion("2").payload(SdkBytes.fromUtf8String("{\"batchItemFailures\":[]}")).build());
        assertThatThrownBy(f::handle).hasMessageContaining("verified response"); assertThat(f.localCalls).hasValue(0);
    }

    @Test
    void destinationPartialFailurePreservesRedelivery() throws Exception {
        var f = new Fixture(); f.bind(arn("1"), codeDigest("release-a"));
        when(f.lambda.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class))).thenReturn(
            GetFunctionConfigurationResponse.builder().codeSha256(codeDigest("release-a")).build());
        when(f.lambda.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().statusCode(200)
            .executedVersion("1").payload(SdkBytes.fromUtf8String("{\"batchItemFailures\":[{\"itemIdentifier\":\"message\"}]}" )).build());
        assertThat(f.handle()).isEqualTo(SqsMessageDisposition.RETRY); assertThat(f.localCalls).hasValue(0);
    }

    @Test
    void awaitCompletionPreservesOriginalOpaqueTokenAndPayloadForSelectedRuntimeAdmission() throws Exception {
        var f = new Fixture();
        String original = PipelineJson.mapper().writeValueAsString(new org.pipelineframework.awaitable.sqs.SqsAwaitCompletionEnvelope(
            "tenant", "interaction-a", "correlation-a", "opaque-original-token", "stable-key", java.util.Map.of("approved", true), "actor-a"));
        when(f.controlPlane.getAwaitSemanticCheckpoint("tenant", "interaction-a")).thenReturn(Uni.createFrom().item(Optional.of(
            new AwaitSemanticCheckpoint("tenant", "execution-a", "interaction-a", "correlation-a", "unit-a", "await-step",
                org.pipelineframework.awaitable.AwaitInteractionStatus.COMPLETED, "pipeline", contract("release-a").contractVersion(), "release-a"))));
        String destination = arn("1").replace(":transition:", ":completion:");
        f.registry.registerEntryBinding(new PipelineReleaseEntryBinding("tenant", "pipeline", contract("release-a").contractVersion(),
            "release-a", "completion", "archive", digest("release-a"), "aws-lambda", destination, codeDigest("release-a"), "op-a"))
            .await().indefinitely();
        f.router.entryId = Optional.of("completion");
        var context = mock(com.amazonaws.services.lambda.runtime.Context.class);
        when(context.getInvokedFunctionArn()).thenReturn(arn("2").replace(":transition:", ":completion:"));
        when(f.lambda.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class))).thenReturn(
            GetFunctionConfigurationResponse.builder().codeSha256(codeDigest("release-a")).build());
        when(f.lambda.invoke(any(InvokeRequest.class))).thenAnswer(call -> {
            InvokeRequest request = call.getArgument(0);
            assertThat(request.functionName()).isEqualTo(destination);
            assertThat(PipelineJson.mapper().readTree(request.payload().asUtf8String()).get("Records").get(0).get("body").asText())
                .isEqualTo(original);
            return InvokeResponse.builder().statusCode(200).executedVersion("1")
                .payload(SdkBytes.fromUtf8String("{\"batchItemFailures\":[]}")).build();
        });
        assertThat(f.router.completion(new SqsInboundMessage(Optional.of("completion"), Optional.of(original)), context, message -> {
            f.localCalls.incrementAndGet(); return Uni.createFrom().item(SqsMessageDisposition.ACKNOWLEDGE);
        }).await().atMost(Duration.ofSeconds(2))).isEqualTo(SqsMessageDisposition.ACKNOWLEDGE);
        assertThat(f.localCalls).hasValue(0);
    }

    @Test
    void correlationOnlyAwaitCompletionCannotFallBackToCurrentPackage() throws Exception {
        var f = new Fixture();
        String original = PipelineJson.mapper().writeValueAsString(new org.pipelineframework.awaitable.sqs.SqsAwaitCompletionEnvelope(
            "tenant", null, "correlation-a", "token", "stable-key", "approved", "actor-a"));
        assertThatThrownBy(() -> f.router.completion(new SqsInboundMessage(Optional.of("completion"), Optional.of(original)), context("2"), message -> {
            f.localCalls.incrementAndGet(); return Uni.createFrom().item(SqsMessageDisposition.ACKNOWLEDGE);
        }).await().atMost(Duration.ofSeconds(2))).hasMessageContaining("correlation-only");
        assertThat(f.localCalls).hasValue(0); verifyNoInteractions(f.lambda, f.controlPlane);
    }

    private static final class Fixture {
        final InMemoryPipelineReleaseRegistry registry = new InMemoryPipelineReleaseRegistry();
        final PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        final LambdaClient lambda = mock(LambdaClient.class);
        final AtomicInteger localCalls = new AtomicInteger();
        final AwsDurableReleaseRouter router;
        Fixture() {
            registry.register(release("release-a")).await().indefinitely();
            when(controlPlane.getExecutionSemanticCheckpoint("tenant", "execution-a")).thenReturn(Uni.createFrom().item(
                new ExecutionSemanticCheckpoint("tenant", "execution-a", "pipeline", contract("release-a").contractVersion(), "release-a", ExecutionStatus.RUNNING)));
            router = router(registry, controlPlane, lambda);
        }
        void bind(String resource, String code) {
            registry.registerEntryBinding(new PipelineReleaseEntryBinding("tenant", "pipeline", contract("release-a").contractVersion(),
                "release-a", "transition", "archive", digest("release-a"), "aws-lambda", resource, code, "operation")).await().indefinitely();
        }
        SqsMessageDisposition handle() throws Exception {
            String json = PipelineJson.mapper().writeValueAsString(new ExecutionWorkItem("tenant", "execution-a"));
            return router.work(new SqsInboundMessage(Optional.of("message"), Optional.of(json)), context("2"), message -> {
                localCalls.incrementAndGet(); return Uni.createFrom().item(SqsMessageDisposition.ACKNOWLEDGE);
            }).await().atMost(Duration.ofSeconds(2));
        }
    }
}

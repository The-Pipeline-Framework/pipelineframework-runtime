package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableHostedProfile;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.CoordinationHost;
import org.pipelineframework.orchestrator.ExecutionClientAdmission;
import org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDeployment;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseEntryBinding;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineReleaseStatus;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.DurableExecutionAlreadyStartedException;
import software.amazon.awssdk.services.lambda.model.GetFunctionConfigurationRequest;
import software.amazon.awssdk.services.lambda.model.GetFunctionConfigurationResponse;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

class AwsDurableHttpSubmissionTest {
    private static final String FAMILY = "arn:aws:lambda:eu-west-1:123456789012:function:coordinator";
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(3);

    @Test
    void providerAcceptanceWaitsForNativeAdmissionAndKeepsItsOriginalTimestamp() throws Exception {
        var fixture = new Fixture();
        var readingNative = new CountDownLatch(1);
        var visible = new CompletableFuture<Optional<ExecutionClientAdmission>>();
        when(fixture.read()).thenReturn(Uni.createFrom().item(Optional.empty()), Uni.createFrom().deferred(() -> {
            readingNative.countDown();
            return Uni.createFrom().completionStage(visible);
        }));
        CompletableFuture<org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto> response;
        org.pipelineframework.context.PipelineContextHolder.set(
            org.pipelineframework.context.PipelineContext.fromHeaders(null, null, null));
        try {
            response = fixture.submission.submit(Uni.createFrom().item("input"), " tenant ", " key ", false)
                .subscribeAsCompletionStage();
        } finally {
            org.pipelineframework.context.PipelineContextHolder.clear();
        }
        assertThat(readingNative.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
        assertThat(response).isNotCompleted();
        visible.complete(Optional.of(fixture.admission("input")));
        assertThat(response.get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).satisfies(value -> {
            assertThat(value.executionId()).isEqualTo("execution");
            assertThat(value.submittedAtEpochMs()).isEqualTo(43);
            assertThat(value.duplicate()).isFalse();
            assertThat(value.statusUrl()).isEqualTo("/pipeline/executions/execution");
        });
        var request = ArgumentCaptor.forClass(InvokeRequest.class);
        verify(fixture.submission.lambda).invoke(request.capture());
        assertThat(request.getValue().invocationType()).isEqualTo(InvocationType.EVENT);
        assertThat(request.getValue().functionName()).isEqualTo(FAMILY);
        assertThat(request.getValue().qualifier()).isEqualTo("7");
        var durable = fixture.submission.mapper.readValue(request.getValue().payload().asUtf8String(), AwsDurableExecutionInput.class);
        assertThat(durable.idempotencyKey()).isEqualTo("key");
        assertThat(durable.submissionAttemptId()).isPresent();
        assertThat(request.getValue().durableExecutionName()).isEqualTo(
            org.pipelineframework.aws.durable.model.AwsDurableExecutionNames.durableExecutionName(durable));
    }

    @Test
    void nativeReplayReturnsDuplicateWithoutStartingProvider() {
        var fixture = new Fixture();
        when(fixture.read()).thenReturn(Uni.createFrom().item(Optional.of(fixture.admission("input"))));
        assertThat(fixture.submit("input").duplicate()).isTrue();
        verifyNoInteractions(fixture.submission.lambda, fixture.submission.releases);
    }

    @Test
    void providerNameConflictStillRequiresMatchingNativeInput() {
        var fixture = new Fixture();
        when(fixture.read()).thenReturn(Uni.createFrom().item(Optional.empty()),
            Uni.createFrom().item(Optional.of(fixture.admission("different"))));
        when(fixture.submission.lambda.invoke(any(InvokeRequest.class)))
            .thenThrow(DurableExecutionAlreadyStartedException.builder().message("named execution exists").build());
        assertThatThrownBy(() -> fixture.submit("input")).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("same initial input");
    }

    @Test
    void existingNativeAdmissionWithDifferentInputFailsBeforeProviderInvocation() {
        var fixture = new Fixture();
        when(fixture.read()).thenReturn(Uni.createFrom().item(Optional.of(fixture.admission("different"))));
        assertThatThrownBy(() -> fixture.submit("input")).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("same initial input");
        verifyNoInteractions(fixture.submission.lambda, fixture.submission.releases);
    }

    @Test
    void providerNameConflictWithMatchingNativeInputReturnsDuplicate() {
        var fixture = new Fixture();
        when(fixture.read()).thenReturn(Uni.createFrom().item(Optional.empty()),
            Uni.createFrom().item(Optional.of(fixture.admission("input"))));
        when(fixture.submission.lambda.invoke(any(InvokeRequest.class)))
            .thenThrow(DurableExecutionAlreadyStartedException.builder().message("named execution exists").build());
        assertThat(fixture.submit("input").duplicate()).isTrue();
    }

    @Test
    void providerAcceptanceWithoutNativeAdmissionTimesOut() {
        var fixture = new Fixture();
        when(fixture.read()).thenReturn(Uni.createFrom().item(Optional.empty()));
        assertThatThrownBy(() -> fixture.submission.submit("input", "tenant", "key", false)
            .await().atMost(Duration.ofSeconds(10)))
            .hasMessageContaining("Native admission was not observed");
        verify(fixture.submission.lambda).invoke(any(InvokeRequest.class));
    }

    @Test
    void changedNumberedCoordinatorCannotStartExecution() {
        var fixture = new Fixture();
        when(fixture.read()).thenReturn(Uni.createFrom().item(Optional.empty()));
        when(fixture.submission.lambda.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class)))
            .thenReturn(GetFunctionConfigurationResponse.builder().version("7").codeSha256("wrong").build());
        assertThatThrownBy(() -> fixture.submit("input")).hasMessageContaining("could not be verified");
        verify(fixture.submission.lambda, never()).invoke(any(InvokeRequest.class));
    }

    private static final class Fixture {
        final AwsDurableHttpSubmission submission = new AwsDurableHttpSubmission();

        Fixture() {
            var contract = AwsDurableRetainedReleaseRoutingIT.contract("release");
            submission.controlPlane = mock(PipelineControlPlane.class);
            submission.config = mock(PipelineOrchestratorConfig.class);
            submission.identity = mock(PipelineReleaseIdentityResolver.class);
            submission.admissionPolicy = mock(ControlPlaneAdmissionPolicy.class);
            submission.releases = mock(PipelineReleaseRegistry.class);
            submission.lambda = mock(LambdaClient.class);
            submission.mapper = new ObjectMapper().registerModule(new Jdk8Module());
            submission.codec = new JsonTransitionPayloadCodec();
            submission.coordinatorFunctionFamily = Optional.of(FAMILY);
            when(submission.config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
            when(submission.config.coordinationHost()).thenReturn(CoordinationHost.AWS_DURABLE);
            when(submission.config.strictStartup()).thenReturn(true);
            when(submission.config.processLoopsDisabled()).thenReturn(true);
            when(submission.admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
            when(submission.identity.contract()).thenReturn(contract);
            when(submission.identity.pipelineId(submission.config)).thenReturn("pipeline");
            when(submission.identity.contractVersion()).thenReturn("contract-release");
            when(submission.identity.releaseVersion(submission.config)).thenReturn("release");
            String digest = AwsDurableRetainedReleaseRoutingIT.digest("release");
            String codeDigest = AwsDurableRetainedReleaseRoutingIT.codeDigest("release");
            var artifact = new PipelineReleaseArtifactDescriptor("archive", "lambda-zip", "s3://release/archive", digest, List.of(), List.of());
            var entry = new PipelineReleaseDeployment.Entry("coordinator", "archive", contract.module(), ".",
                AwsDurableCoordinatorHandler.class.getName(), "AWS_DURABLE_COORDINATOR", List.of());
            var deployment = new PipelineReleaseDeployment(AwsDurableHostedProfile.PROFILE_ID,
                List.of(entry), List.of(), null);
            var descriptor = new PipelineReleaseDescriptor(2, "pipeline", "contract-release", "release",
                "archive", List.of(artifact), deployment);
            var release = new PipelineReleaseRecord("tenant", "pipeline", "contract-release", "release", PipelineReleaseStatus.ACTIVE,
                descriptor, "archive", digest, artifact.uri(), 0, "", contract, 1, 1, 1);
            when(submission.releases.active("tenant", "pipeline")).thenReturn(Uni.createFrom().item(Optional.of(release)));
            var binding = new PipelineReleaseEntryBinding("tenant", "pipeline", "contract-release", "release",
                "coordinator", "archive", digest, "aws-lambda", FAMILY + ":7", codeDigest, "deployment");
            when(submission.releases.getEntryBinding("tenant", "pipeline", "contract-release", "release", "coordinator"))
                .thenReturn(Uni.createFrom().item(Optional.of(binding)));
            when(submission.lambda.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class)))
                .thenReturn(GetFunctionConfigurationResponse.builder().version("7").codeSha256(codeDigest).build());
            when(submission.lambda.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().statusCode(202).build());
        }

        Uni<Optional<ExecutionClientAdmission>> read() {
            return submission.controlPlane.getExecutionAdmissionByClientKey("tenant", "key", "pipeline", "release");
        }

        ExecutionClientAdmission admission(String input) {
            return new ExecutionClientAdmission(new ExecutionSemanticCheckpoint("tenant", "execution", "pipeline",
                "contract-release", "release", ExecutionStatus.QUEUED), 43,
                Optional.of(ExecutionClientAdmission.fingerprint(submission.codec.encode(input))));
        }

        org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto submit(String input) {
            return submission.submit(input, "tenant", "key", false).await().atMost(TEST_TIMEOUT);
        }
    }
}

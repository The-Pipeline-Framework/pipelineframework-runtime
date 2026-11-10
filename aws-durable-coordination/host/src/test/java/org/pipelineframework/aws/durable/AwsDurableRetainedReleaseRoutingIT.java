package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.release.*;
import com.amazonaws.services.lambda.runtime.Context;
import io.smallrye.mutiny.Uni;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Open Hosted acceptance gate: a queue mapping moved to B must still execute a retained A command on A.
 * Uses the production handler, authentication, execution identity guard and response serialization.
 * Compiler metadata and semantic checkpoint are deterministic fixtures; AWS transports are substituted.
 * A real authored transformation runs in the retained application through the production runner,
 * identity guard, authentication and portable result serialization.
 */
class AwsDurableRetainedReleaseRoutingIT {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"release-a,1,release-b,2", "release-b,2,release-a,1"})
    void retainedReleaseATransitionMustNotBeFailedByActiveBWorker(String retained, String retainedVersion,
        String active, String activeVersion) throws Exception {
        var outbound = new java.util.concurrent.atomic.AtomicReference<String>();
        var sqs = mock(SqsClient.class);
        when(sqs.sendMessage(any(SendMessageRequest.class))).thenAnswer(call -> {
            SendMessageRequest response = call.getArgument(0);
            outbound.set(response.messageBody());
            return null;
        });

        // The unchanged SQS event-source mapping now selects the retained numbered B worker.
        // No routing is simulated here: invoke the real handler selected by that mapping.
        var registry = new InMemoryPipelineReleaseRegistry();
        for (String release : List.of("release-a", "release-b")) {
            registry.register(release(release)).await().indefinitely();
            String version = release.equals("release-a") ? "1" : "2";
            registry.registerEntryBinding(new PipelineReleaseEntryBinding("tenant", "pipeline", contract(release).contractVersion(), release,
                "transition", "archive", digest(release), "aws-lambda", arn(version), codeDigest(release), "op-" + release))
                .await().indefinitely();
        }
        registry.activate("tenant", "pipeline", active, 2).await().indefinitely();
        var controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getExecutionSemanticCheckpoint("tenant", "execution-a")).thenReturn(Uni.createFrom().item(
            new ExecutionSemanticCheckpoint("tenant", "execution-a", "pipeline", contract(retained).contractVersion(), retained, ExecutionStatus.RUNNING)));
        var lambda = mock(LambdaClient.class);
        var destination = new java.util.ArrayList<String>();
        when(lambda.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class))).thenReturn(
            GetFunctionConfigurationResponse.builder().codeSha256(codeDigest(retained)).build());
        when(lambda.invoke(any(InvokeRequest.class))).thenAnswer(call -> {
            InvokeRequest invocation = call.getArgument(0);
            destination.add(invocation.functionName());
            var json = PipelineJson.mapper().readTree(invocation.payload().asUtf8String()).get("Records").get(0);
            var message = new SQSEvent.SQSMessage();
            message.setMessageId(json.get("messageId").asText());
            message.setBody(json.get("body").asText());
            var forwarded = new SQSEvent(); forwarded.setRecords(List.of(message));
            var original = handler(retained, sqs);
            original.releases = router(registry, controlPlane, lambda);
            var outcome = original.handleRequest(forwarded, context(retainedVersion));
            return InvokeResponse.builder().statusCode(200).executedVersion(retainedVersion)
                .payload(SdkBytes.fromUtf8String(PipelineJson.mapper().writeValueAsString(outcome))).build();
        });
        var activeB = handler(active, sqs);
        activeB.releases = router(registry, controlPlane, lambda);
        var response = activeB.handleRequest(event(command(retained), true), context(activeVersion));
        assertThat(destination).containsExactly(arn(retainedVersion));
        assertThat(response.getBatchItemFailures()).isEmpty();
        assertThat(outbound.get()).as("production worker must publish a response").isNotNull();
        var result = PipelineJson.mapper().readValue(outbound.get(), SqsTransitionWorkerResponse.class);

        // Real authored transformation, real runner and real portable result encoding at the retained version.
        assertThat(result.resultEnvelope())
            .as("retained A must reach A's numbered entry after B activation; B must not fail it locally")
            .doesNotContain("but local worker is pipelineId=pipeline, contractVersion="
                + PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION + ", releaseVersion=" + active);
        var wire = PipelineJson.mapper().readValue(result.resultEnvelope(), TransitionWireResult.class);
        assertThat(wire.outcome()).as(result.resultEnvelope()).isEqualTo(TransitionWorkerOutcome.COMPLETED);
        assertThat(TransitionResultEnvelope.fromWireResult(wire).decodeOutputItems(new JsonTransitionPayloadCodec()))
            .isEqualTo(List.of(retained + ":INPUT"));
    }

    @Test
    void unauthenticatedRetainedReleaseCommandHasNoEffect() throws Exception {
        var sqs = mock(SqsClient.class);
        var response = handler("release-b", sqs).handleRequest(event(command("release-a"), false), null);
        assertThat(response.getBatchItemFailures()).isEmpty();
        verifyNoInteractions(sqs);
    }

    private static AwsDurableTransitionWorkerHandler handler(String release, SqsClient sqs) throws Exception {
        var config = mock(PipelineOrchestratorConfig.class);
        var worker = mock(PipelineOrchestratorConfig.SqsWorkerConfig.class);
        when(config.pipelineId()).thenReturn("pipeline");
        when(config.releaseVersion()).thenReturn(release);
        when(config.workerSqs()).thenReturn(worker);
        when(worker.responseQueueUrl()).thenReturn(Optional.of("https://sqs.example/response"));
        when(worker.requestTimeout()).thenReturn(Duration.ofSeconds(1));
        when(worker.signatureTolerance()).thenReturn(Duration.ofMinutes(2));
        when(worker.sharedSecret()).thenReturn(Optional.of(release + "-worker-secret"));
        when(worker.sharedSecretRef()).thenReturn(Optional.empty());

        var execution = org.pipelineframework.RetainedReleaseApplicationFixture.application(config, contract(release), release);
        var action = new SqsTransitionWorkerAction();
        inject(action, "orchestratorConfig", config);
        inject(action, "executionService", execution);
        inject(action, "client", sqs);
        var handler = new AwsDurableTransitionWorkerHandler();
        handler.action = action;
        return handler;
    }

    private static TransitionCommandEnvelope command(String release) {
        return new TransitionCommandEnvelope("tenant", "execution-a", "pipeline",
            contract(release).contractVersion(), release, 0, -1, 0,
            ExecutionResultShape.SINGLE, 1, "execution-a:0:0", "trace", "java.lang.String",
            JsonTransitionPayloadCodec.ENCODING, "\"input\"");
    }

    private static SQSEvent event(TransitionCommandEnvelope command, boolean authenticated) throws Exception {
        String body = PipelineJson.mapper().writeValueAsString(command);
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = authenticated ? TransitionWorkerSignature.sign(command.releaseVersion() + "-worker-secret",
            SqsTransitionWorkerProtocol.SIGNATURE_METHOD, SqsTransitionWorkerProtocol.REQUEST_SIGNATURE_PATH,
            timestamp, nonce, SqsTransitionWorkerProtocol.signedBytes("request-a", body)) : "invalid";
        var request = new SqsTransitionWorkerRequest("request-a", SqsTransitionWorkerProtocol.PROTOCOL_VERSION,
            SqsTransitionWorkerProtocol.PAYLOAD_ENCODING, body, timestamp, nonce, signature);
        var message = new SQSEvent.SQSMessage();
        message.setMessageId("retained-a");
        message.setBody(PipelineJson.mapper().writeValueAsString(request));
        var event = new SQSEvent();
        event.setRecords(List.of(message));
        return event;
    }


    static String digest(String release) {
        return "sha256:" + java.util.HexFormat.of().formatHex(hash(release));
    }
    static String codeDigest(String release) { return java.util.Base64.getEncoder().encodeToString(hash(release)); }
    private static byte[] hash(String release) {
        try { return java.security.MessageDigest.getInstance("SHA-256").digest(release.getBytes(java.nio.charset.StandardCharsets.UTF_8)); }
        catch (java.security.NoSuchAlgorithmException failure) { throw new IllegalStateException(failure); }
    }
    static String arn(String version) { return "arn:aws:lambda:eu-west-1:123456789012:function:transition:" + version; }
    static Context context(String version) {
        var context = mock(Context.class); when(context.getInvokedFunctionArn()).thenReturn(arn(version)); return context;
    }
    static AwsDurableReleaseRouter router(PipelineReleaseRegistry registry, PipelineControlPlane controlPlane, LambdaClient lambda) {
        var router = new AwsDurableReleaseRouter(); router.releases = registry; router.controlPlane = controlPlane;
        router.lambda = lambda; router.entryId = Optional.of("transition"); return router;
    }
    static PipelineContractDescriptor contract(String release) {
        return new PipelineContractDescriptor(1, "pipeline", "contract-" + release, "hash-" + release,
            "COMPUTE", "SQS", "archive", false, "monolith", List.of(new PipelineBundleStepDescriptor(0, "Transform",
                org.pipelineframework.RetainedReleaseApplicationFixture.ReleaseTransform.class.getName(), "ONE_TO_ONE",
                "java.lang.String", "java.lang.String", "monolith", "Client", null)), PipelineBundleCapabilities.defaults());
    }
    static PipelineReleaseRecord release(String version) {
        var contract = contract(version);
        var artifact = new PipelineReleaseArtifactDescriptor("archive", "lambda-zip", "s3://releases/" + version,
            digest(version), List.of(), List.of());
        var deployment = new PipelineReleaseDeployment("aws-durable", List.of(
            new PipelineReleaseDeployment.Entry("transition", "archive", contract.module(), ".",
                "org.pipelineframework.aws.durable.AwsDurableTransitionWorkerHandler", "sqs", List.of()),
            new PipelineReleaseDeployment.Entry("work", "archive", contract.module(), ".",
                "org.pipelineframework.aws.durable.AwsDurableWorkItemHandler", "sqs", List.of()),
            new PipelineReleaseDeployment.Entry("action", "archive", contract.module(), ".",
                "org.pipelineframework.aws.durable.AwsDurableActionGatewayHandler", "internal", List.of()),
            new PipelineReleaseDeployment.Entry("completion", "archive", contract.module(), ".",
                "org.pipelineframework.aws.durable.AwsDurableAwaitCompletionHandler", "sqs", List.of())), List.of(),
            new PipelineReleaseDeployment.Check("transition", Map.of("payloadTypeId", "java.lang.String", "payloadEncoding", "json", "payload", "\"input\""),
                Map.of("payloadTypeId", "java.lang.String", "payloadEncoding", "json", "payload", "\"output\""), 1000));
        var descriptor = new PipelineReleaseDescriptor(2, "pipeline", contract.contractVersion(), version, "archive", List.of(artifact), deployment);
        return new PipelineReleaseRecord("tenant", "pipeline", contract.contractVersion(), version, PipelineReleaseStatus.REGISTERED,
            descriptor, "archive", artifact.digest(), artifact.uri(), 0, "", contract, 1, 1, 0);
    }

    private static void inject(Object instance, String name, Object value) throws Exception {
        Field field = instance.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(instance, value);
    }
}

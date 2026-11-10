package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.pipelineframework.CompatibleScalarAwaitHttpFixture.set;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import jakarta.enterprise.inject.Instance;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.RetainedReleaseApplicationFixture;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.*;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

/** Exercises the existing Work handler and runner with both placements.
 * The coordinator supplies a pinned transition; AWS transports are local substitutes.
 */
class AwsDurableTransitionPlacementIT {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void executesInWorkOrThroughTheSignedTransitionHandler(boolean separateTransition) throws Exception {
        String version = "release-a";
        var contract = AwsDurableRetainedReleaseRoutingIT.contract(version);
        var config = config(version, separateTransition);
        var workRuntime = RetainedReleaseApplicationFixture.application(config, contract, "work");
        var remoteRuntime = RetainedReleaseApplicationFixture.application(config, contract, "remote");
        var sqs = mock(SqsClient.class);
        var remote = new AwsDurableTransitionWorkerHandler();
        remote.action = new SqsTransitionWorkerAction();
        set(remote.action, "orchestratorConfig", config);
        set(remote.action, "executionService", remoteRuntime);
        set(remote.action, "client", sqs);

        var reply = new AtomicReference<String>();
        when(sqs.sendMessage(any(SendMessageRequest.class))).thenAnswer(call -> {
            SendMessageRequest message = call.getArgument(0);
            if (message.queueUrl().endsWith("/request")) {
                assertThat(remote.handleRequest(event(message.messageBody()), null).getBatchItemFailures()).isEmpty();
            } else {
                assertThat(message.queueUrl()).endsWith("/response");
                assertThat(reply.compareAndSet(null, message.messageBody())).isTrue();
            }
            return SendMessageResponse.builder().messageId("fixture").build();
        });
        when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(call -> {
            String body = reply.get();
            assertThat(body).isNotNull();
            return ReceiveMessageResponse.builder().messages(
                Message.builder().messageId("reply").receiptHandle("receipt").body(body).build()).build();
        });
        var sqsWorker = new SqsPipelineTransitionWorker();
        set(sqsWorker, "client", sqs);
        set(sqsWorker, "orchestratorConfig", config);
        set(sqsWorker, "invocationRuntime", new PipelineInvocationRuntime());
        var selector = new PipelineTransitionWorkerSelector();
        set(selector, "orchestratorConfig", config);
        set(selector, "sqsWorker", sqsWorker);
        set(workRuntime, "transitionWorkerSelector", selector);

        var registry = new InMemoryPipelineReleaseRegistry();
        registry.register(AwsDurableRetainedReleaseRoutingIT.release(version)).await().indefinitely();
        String arn = "arn:aws:lambda:us-east-2:123456789012:function:work:1";
        registry.registerEntryBinding(new PipelineReleaseEntryBinding("tenant", "pipeline", contract.contractVersion(),
            version, "work", "archive", AwsDurableRetainedReleaseRoutingIT.digest(version), "aws-lambda", arn,
            AwsDurableRetainedReleaseRoutingIT.codeDigest(version), "op")).await().indefinitely();
        var coordinator = mock(PipelineControlPlane.class);
        when(coordinator.getExecutionSemanticCheckpoint("tenant", "exec")).thenReturn(Uni.createFrom().item(
            new ExecutionSemanticCheckpoint("tenant", "exec", "pipeline", contract.contractVersion(), version, ExecutionStatus.RUNNING)));
        var item = new ExecutionWorkItem("tenant", "exec");
        var command = new TransitionCommandEnvelope("tenant", "exec", "pipeline", contract.contractVersion(), version,
            0, -1, 0, ExecutionResultShape.SINGLE, 1, "exec:0:0", "trace", String.class.getName(),
            JsonTransitionPayloadCodec.ENCODING, "\"hello\"");
        var result = new AtomicReference<TransitionResultEnvelope>();
        when(coordinator.processExecutionWorkItem(eq(item), any(), any())).thenAnswer(call -> {
            PipelineTransitionWorker worker = call.getArgument(1);
            return worker.executeTransition(command).invoke(result::set).replaceWithVoid();
        });
        set(workRuntime, "controlPlane", coordinator);
        var lambda = mock(LambdaClient.class);
        var router = AwsDurableRetainedReleaseRoutingIT.router(registry, coordinator, lambda);
        router.entryId = Optional.of("work");
        @SuppressWarnings("unchecked") Instance<AwsDurableReleaseRouter> instance = mock(Instance.class);
        when(instance.isResolvable()).thenReturn(true);
        when(instance.get()).thenReturn(router);
        var work = new AwsDurableWorkItemHandler();
        work.config = config;
        work.releases = instance;
        work.action = new SqsWorkItemAction();
        set(work.action, "pipelineExecutionService", workRuntime);
        var context = mock(Context.class);
        when(context.getInvokedFunctionArn()).thenReturn(arn);

        assertThat(work.handleRequest(event(PipelineJson.mapper().writeValueAsString(item)), context)
            .getBatchItemFailures()).isEmpty();
        assertThat(result.get().outcome()).isEqualTo(TransitionWorkerOutcome.COMPLETED);
        assertThat(result.get().decodeOutputItems(new JsonTransitionPayloadCodec()))
            .isEqualTo(List.of((separateTransition ? "remote" : "work") + ":HELLO"));
        if (separateTransition) {
            verify(sqs, times(2)).sendMessage(any(SendMessageRequest.class));
            verify(sqs).deleteMessage(any(DeleteMessageRequest.class));
        } else {
            verifyNoInteractions(sqs);
        }
        verifyNoInteractions(lambda);
    }

    private static PipelineOrchestratorConfig config(String version, boolean separate) {
        var values = new java.util.HashMap<>(Map.of(
            "pipeline.orchestrator.mode", "QUEUE_ASYNC", "pipeline.orchestrator.coordination-host", "AWS_DURABLE",
            "pipeline.orchestrator.pipeline-id", "pipeline", "pipeline.orchestrator.release-version", version));
        if (separate) values.putAll(Map.of(
            "pipeline.orchestrator.worker.sqs.request-queue-url", "https://sqs.example/request",
            "pipeline.orchestrator.worker.sqs.response-queue-url", "https://sqs.example/response",
            "pipeline.orchestrator.worker.sqs.request-timeout", Duration.ofSeconds(3).toString(),
            "pipeline.orchestrator.worker.sqs.shared-secret", "fixture-secret"));
        return new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class)
            .withSources(new PropertiesConfigSource(values, "placement-test", 300))
            .build().getConfigMapping(PipelineOrchestratorConfig.class);
    }

    private static SQSEvent event(String body) {
        var message = new SQSEvent.SQSMessage();
        message.setMessageId("message");
        message.setBody(body);
        var event = new SQSEvent();
        event.setRecords(List.of(message));
        return event;
    }
}

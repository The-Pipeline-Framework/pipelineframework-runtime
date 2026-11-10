package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.pipelineframework.aws.durable.AwsDurableRetainedReleaseRoutingIT.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.*;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.release.*;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;

class AwsDurableDueReleaseDispatchTest {
    private static final String FAMILY = "arn:aws:lambda:eu-west-1:123456789012:function:action";
    private static final AwaitItemContinuationCommand COMMAND = new AwaitItemContinuationCommand(
        "tenant", "execution-a", "unit-a", "interaction-a", 0, 1, 1000);

    @Test
    void activeBSweeperInvokesRetainedAActionWithoutSelectingBsLocalApplication() throws Exception {
        var f = new Fixture(); f.bind(FAMILY + ":1");
        f.registry.register(release("release-b")).await().indefinitely();
        f.registry.activate("tenant", "pipeline", "release-b", 2000).await().indefinitely();
        var original = mock(PipelineControlPlane.class);
        when(original.getExecutionSemanticCheckpoint("tenant", "execution-a")).thenReturn(Uni.createFrom().item(checkpoint()));
        when(original.processAwaitItemContinuation(any())).thenAnswer(call ->
            Uni.createFrom().item(AwaitItemContinuationResult.completed(call.getArgument(0))));
        var actions = new AwsDurableControlPlaneActions(original, mock(AwsDurableCallbackBindingRepository.class), value -> value,
            new ObjectMapper(), Duration.ofSeconds(1), Duration.ofDays(1));
        when(f.lambda.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class))).thenReturn(
            GetFunctionConfigurationResponse.builder().codeSha256(codeDigest("release-a")).build());
        var targets = new java.util.ArrayList<String>();
        when(f.lambda.invoke(any(InvokeRequest.class))).thenAnswer(call -> {
            InvokeRequest invocation = call.getArgument(0); targets.add(invocation.functionName());
            var request = PipelineJson.mapper().readValue(invocation.payload().asUtf8String(), AwsDurableActionRequest.class);
            var response = actions.invoke(request);
            return InvokeResponse.builder().statusCode(200).executedVersion("1")
                .payload(SdkBytes.fromUtf8String(PipelineJson.mapper().writeValueAsString(response))).build();
        });
        var tick = event();
        assertThat(f.handler.handleRequest(tick, null).getBatchItemFailures()).isEmpty();
        assertThat(targets).containsExactly(FAMILY + ":1");
        // A pinned Durable driver's Action SWEEP must use the same dispatcher, never a global local-app sweep.
        when(f.current.sweepOnce(anyLong(), any(AwaitItemContinuationDispatcher.class))).thenAnswer(call -> {
            AwaitItemContinuationDispatcher dispatcher = call.getArgument(1);
            return dispatcher.dispatch(COMMAND).replaceWith(new CoordinatorSweepResult(call.getArgument(0), 10, 0, 0));
        });
        var driverActions = new AwsDurableControlPlaneActions(f.current, mock(AwsDurableCallbackBindingRepository.class), value -> value,
            new ObjectMapper(), Duration.ofSeconds(1), Duration.ofDays(1), f.router::dispatchDue);
        var driver = new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint("tenant", "execution-a", "pipeline",
            contract("release-a").contractVersion(), "release-a"), 1);
        assertThat(driverActions.invoke(AwsDurableActionRequest.sweep(driver)).executionStatus()).contains("SWEEPED");
        assertThat(targets).containsExactly(FAMILY + ":1", FAMILY + ":1");
        verify(original, times(2)).processAwaitItemContinuation(argThat(command -> command.tenantId().equals("tenant")
            && command.executionId().equals("execution-a") && command.interactionId().equals("interaction-a")
            && command.nowEpochMs() > 1000 && command.attempt() == 1));
        verify(f.current, never()).sweepOnce(anyLong());
        verify(f.current, never()).processAwaitItemContinuation(any());
    }

    @Test
    void absentOrForeignActionBindingLeavesTickRetryableWithoutLocalFallback() {
        for (String destination : List.of("absent", FAMILY.replace("action", "foreign") + ":1")) {
            var f = new Fixture(); if (!destination.equals("absent")) f.bind(destination);
            assertThat(f.handler.handleRequest(event(), null).getBatchItemFailures())
                .extracting(item -> item.getItemIdentifier()).containsExactly("tick");
            verifyNoInteractions(f.lambda);
            verify(f.current, never()).sweepOnce(anyLong());
            verify(f.current, never()).processAwaitItemContinuation(any());
        }
    }

    @Test
    void partialBlankAndInvalidConfiguredRoutingNeverFallsBackToCurrentApplication() {
        record Route(Optional<String> entry, Optional<String> family) {}
        var routes = List.of(
            new Route(Optional.of("action"), Optional.empty()),
            new Route(Optional.empty(), Optional.of(FAMILY)),
            new Route(Optional.of(""), Optional.of(FAMILY)),
            new Route(Optional.of("action"), Optional.of("")),
            new Route(Optional.of("action"), Optional.of("arn:aws:lambda:*:*:function:*")),
            new Route(Optional.of(" action "), Optional.of(FAMILY)),
            new Route(Optional.of(""), Optional.of("")));
        for (Route route : routes) {
            var f = new Fixture(); f.bind(FAMILY + ":1");
            f.router.continuationEntryId = route.entry(); f.router.actionFunctionFamily = route.family();
            assertThat(f.router.dueEnabled()).as("any configured route requires fail-closed dispatch").isTrue();
            assertThat(f.handler.handleRequest(event(), null).getBatchItemFailures())
                .extracting(item -> item.getItemIdentifier()).containsExactly("tick");
            verify(f.current, never()).sweepOnce(anyLong());
            verify(f.current, never()).processAwaitItemContinuation(any());
            verifyNoInteractions(f.lambda);
        }
    }

    private static ExecutionSemanticCheckpoint checkpoint() {
        return new ExecutionSemanticCheckpoint("tenant", "execution-a", "pipeline", contract("release-a").contractVersion(),
            "release-a", ExecutionStatus.WAITING_EXTERNAL);
    }
    private static SQSEvent event() {
        var message = new SQSEvent.SQSMessage(); message.setMessageId("tick");
        message.setBody("{\"tenantId\":\"foreign\",\"nowEpochMs\":0}");
        var event = new SQSEvent(); event.setRecords(List.of(message)); return event;
    }
    private static final class Fixture {
        final InMemoryPipelineReleaseRegistry registry = new InMemoryPipelineReleaseRegistry();
        final PipelineControlPlane current = mock(PipelineControlPlane.class);
        final LambdaClient lambda = mock(LambdaClient.class);
        final AwsDurableReleaseRouter router;
        final AwsDurableDueWorkHandler handler;
        Fixture() {
            registry.register(release("release-a")).await().indefinitely();
            when(current.getExecutionSemanticCheckpoint("tenant", "execution-a")).thenReturn(Uni.createFrom().item(checkpoint()));
            when(current.sweepOnce(eq(1000L), any(AwaitItemContinuationDispatcher.class))).thenAnswer(call -> {
                AwaitItemContinuationDispatcher dispatcher = call.getArgument(1);
                return dispatcher.dispatch(COMMAND).replaceWith(new CoordinatorSweepResult(1000, 10, 0, 0));
            });
            router = router(registry, current, lambda); router.continuationEntryId = Optional.of("action");
            router.actionFunctionFamily = Optional.of(FAMILY);
            handler = new AwsDurableDueWorkHandler(current, Clock.fixed(Instant.ofEpochMilli(1000), ZoneOffset.UTC));
            handler.releases = mock(jakarta.enterprise.inject.Instance.class);
            when(handler.releases.isResolvable()).thenReturn(true);
            when(handler.releases.get()).thenReturn(router);
            handler.config=mock(org.pipelineframework.orchestrator.PipelineOrchestratorConfig.class);
            when(handler.config.coordinationHost()).thenReturn(org.pipelineframework.orchestrator.CoordinationHost.AWS_DURABLE);
        }
        void bind(String resource) {
            registry.registerEntryBinding(new PipelineReleaseEntryBinding("tenant", "pipeline", contract("release-a").contractVersion(),
                "release-a", "action", "archive", digest("release-a"), "aws-lambda", resource, codeDigest("release-a"), "operation-a"))
                .await().indefinitely();
        }
    }
}

package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.*;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;
import org.pipelineframework.CompatibleScalarAwaitHttpFixture;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.release.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;

/** Joins real HTTP resource completion and semantic state to queued Work and retained host routing.
 * It does not claim HTTP wire transport, HTTP initial admission or completed continuation execution.
 */
class AwsDurableCompatibleScalarHttpCompletionIT {
  @Test void compatibleHttpBCompletionQueuesOriginalAAndWorkMappingBInvokesNumberedA() throws Exception {
    var fixture=fixture("shared-retained-key","compatible-projector-v1");
    assertThat(fixture.execution().status()).isEqualTo(ExecutionStatus.WAITING_EXTERNAL);
    var registry=new InMemoryPipelineReleaseRegistry();
    for(String release:List.of("release-a","release-b"))registry.register(AwsDurableRetainedReleaseRoutingIT.release(release)).await().indefinitely();
    registry.registerEntryBinding(new PipelineReleaseEntryBinding("tenant","pipeline",
        AwsDurableRetainedReleaseRoutingIT.contract("release-a").contractVersion(),"release-a","work","archive",
        AwsDurableRetainedReleaseRoutingIT.digest("release-a"),"aws-lambda",arn("1"),
        AwsDurableRetainedReleaseRoutingIT.codeDigest("release-a"),"op-a")).await().indefinitely();
    registry.activate("tenant","pipeline","release-b",2).await().indefinitely();
    assertThat(fixture.completeAtHttpB().getStatus()).isEqualTo(200);
    assertThat(fixture.interaction().status()).isEqualTo(AwaitInteractionStatus.COMPLETED);
    assertThat(fixture.execution().status()).isEqualTo(ExecutionStatus.QUEUED);
    assertThat(fixture.execution().releaseVersion()).isEqualTo("release-a");
    assertThat(fixture.queue).containsExactly(new ExecutionWorkItem("tenant",fixture.waiting.executionId()));
    var lambda=mock(LambdaClient.class);
    when(lambda.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class))).thenReturn(
        GetFunctionConfigurationResponse.builder().codeSha256(AwsDurableRetainedReleaseRoutingIT.codeDigest("release-a")).build());
    var destinations=new ArrayList<String>();
    when(lambda.invoke(any(InvokeRequest.class))).thenAnswer(call->{
      InvokeRequest request=call.getArgument(0);destinations.add(request.functionName());
      var delivered=PipelineJson.mapper().readTree(request.payload().asUtf8String()).get("Records").get(0).get("body").asText();
      assertThat(PipelineJson.mapper().readValue(delivered,ExecutionWorkItem.class)).isEqualTo(fixture.queue.getFirst());
      // AWS invocation acknowledgement only: no fabricated application completion/result.
      return InvokeResponse.builder().statusCode(200).executedVersion("1")
          .payload(SdkBytes.fromUtf8String("{\"batchItemFailures\":[]}")).build();
    });
    var router=AwsDurableRetainedReleaseRoutingIT.router(registry,fixture.controlPlane,lambda);router.entryId=Optional.of("work");
    @SuppressWarnings("unchecked") Instance<AwsDurableReleaseRouter> selected=mock(Instance.class);
    when(selected.isResolvable()).thenReturn(true);when(selected.get()).thenReturn(router);
    var handler=new AwsDurableWorkItemHandler();handler.releases=selected;
    handler.config=mock(PipelineOrchestratorConfig.class);when(handler.config.coordinationHost()).thenReturn(CoordinationHost.AWS_DURABLE);
    handler.action=spy(new SqsWorkItemAction());
    var message=new SQSEvent.SQSMessage();message.setMessageId("actual-resume-work");
    message.setBody(PipelineJson.mapper().writeValueAsString(fixture.queue.getFirst()));
    var event=new SQSEvent();event.setRecords(List.of(message));
    var context=mock(Context.class);
    when(context.getInvokedFunctionArn()).thenReturn(arn("2"));
    when(context.getRemainingTimeInMillis()).thenReturn(120_000);
    assertThat(handler.handleRequest(event,context).getBatchItemFailures()).isEmpty();
    assertThat(destinations).containsExactly(arn("1"));verifyNoInteractions(handler.action);
    assertThat(fixture.execution().status()).isEqualTo(ExecutionStatus.QUEUED);
  }
  @Test void changedBKeyRejectsOldTokenWithoutChangingWaitingExecution() throws Exception {
    var fixture=fixture("changed-key","compatible-projector-v1");var execution=fixture.execution();var interaction=fixture.interaction();
    assertThatThrownBy(fixture::completeAtHttpB).hasStackTraceContaining("Invalid await resume token signature");
    assertThat(fixture.execution()).isEqualTo(execution);assertThat(fixture.interaction()).isEqualTo(interaction);
    assertThat(fixture.queue).isEmpty();
  }
  @Test void changedBProjectorRejectsOldCompletionWithoutChangingWaitingExecution() throws Exception {
    var fixture=fixture("shared-retained-key","changed-projector-v2");var execution=fixture.execution();var interaction=fixture.interaction();
    assertThatThrownBy(fixture::completeAtHttpB).hasStackTraceContaining("pinned completion projector");
    assertThat(fixture.execution()).isEqualTo(execution);assertThat(fixture.interaction()).isEqualTo(interaction);
    assertThat(fixture.queue).isEmpty();
  }
  private static CompatibleScalarAwaitHttpFixture fixture(String key,String projector) throws Exception {
    return new CompatibleScalarAwaitHttpFixture(AwsDurableRetainedReleaseRoutingIT.contract("release-a"),
        AwsDurableRetainedReleaseRoutingIT.contract("release-b"),key,projector);
  }
  private static String arn(String version){return "arn:aws:lambda:eu-west-1:123456789012:function:work:"+version;}
}

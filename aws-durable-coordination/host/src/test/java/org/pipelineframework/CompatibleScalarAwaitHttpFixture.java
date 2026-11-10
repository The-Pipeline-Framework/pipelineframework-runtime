package org.pipelineframework;

import static org.mockito.Mockito.*;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.awaitable.store.*;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.controlplane.*;
import org.pipelineframework.orchestrator.dto.HostedAwaitCompletionRequest;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** Real scalar completion/state/continuation wiring, with in-memory durable-store and SQS substitutes.
 * Starts at a genuinely WAITING interaction: HTTP submission and provider suspension are separate gates.
 */
public final class CompatibleScalarAwaitHttpFixture {
  public final InMemoryExecutionStateStore executions = new InMemoryExecutionStateStore();
  public final InMemoryAwaitInteractionStore interactions = new InMemoryAwaitInteractionStore();
  public final InMemoryAwaitUnitStore units = new InMemoryAwaitUnitStore();
  public final List<ExecutionWorkItem> queue = new ArrayList<>();
  public final LocalPipelineControlPlane controlPlane = new LocalPipelineControlPlane();
  public final ExecutionRecord<Object,Object> waiting;
  public final AwaitInteractionRecord interaction;
  public final String token;
  private final HostedPipelineControlPlaneResource http;

  public CompatibleScalarAwaitHttpFixture(PipelineContractDescriptor a, PipelineContractDescriptor b,
      String bSecret, String bProjector) throws Exception {
    long now=System.currentTimeMillis();
    var created=executions.createOrGetExecution(new ExecutionCreateCommand("tenant", "held-a", a.pipelineId(),
        a.contractVersion(), "release-a", new ExecutionInputSnapshot(ExecutionInputShape.UNI,"request"),
        ExecutionResultShape.SINGLE, Optional.of(String.class.getName()), 0, now, now/1000+3600))
        .await().atMost(Duration.ofSeconds(3)).record();
    var descriptorA=descriptor("compatible-projector-v1");
    var aAwait=awaits("shared-retained-key", descriptorA);
    AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", created.executionId(), 0,
        AwaitContinuationMode.DURABLE_HANDOFF, TerminalOutputOwnership.TRANSITION_WORKER,
        Map.of(), Optional.empty(), created.awaitOccurrenceCause()));
    try {
      interaction=aAwait.createOrGet(descriptorA,"tenant",created.executionId(),0,"cause-a","request",null,null)
          .await().atMost(Duration.ofSeconds(3)).record();
    } finally { AwaitExecutionContextHolder.clear(); }
    aAwait.markDispatchComplete("tenant",interaction.unitId(),1,now).await().atMost(Duration.ofSeconds(3));
    waiting=executions.markWaitingExternal("tenant",created.executionId(),created.version(),"await-a",
        interaction.unitId(),0,now).await().atMost(Duration.ofSeconds(3)).orElseThrow();
    var signer=new AwaitResumeTokenService(); set(signer,"configuredSecret",Optional.of("shared-retained-key"));
    token=signer.sign(interaction,now);
    var config=mock(PipelineOrchestratorConfig.class,RETURNS_DEEP_STUBS);
    when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(config.coordinationHost()).thenReturn(CoordinationHost.AWS_DURABLE);
    when(config.pipelineId()).thenReturn("pipeline"); when(config.releaseVersion()).thenReturn("release-b");
    when(config.controlPlane().enabled()).thenReturn(true);
    when(config.controlPlane().adminToken()).thenReturn(Optional.of("test-admin"));
    when(config.controlPlane().adminTokenRef()).thenReturn(Optional.empty());
    when(config.tenancy().allowedTenants()).thenReturn(List.of("tenant"));
    var coordinator=new QueueAsyncCoordinator();
    coordinator.orchestratorConfig=config; coordinator.executionStateStore=executions;
    coordinator.awaitCoordinator=awaits(bSecret,descriptor(bProjector));
    coordinator.executionInputPolicy=new ExecutionInputPolicy(); coordinator.executionInputPolicy.orchestratorConfig=config;
    coordinator.transitionPayloadCodec=new JsonTransitionPayloadCodec();
    coordinator.workDispatcher=new WorkDispatcher() {
      public String providerName(){return "test-sqs-boundary";}
      public Uni<Void> enqueueNow(ExecutionWorkItem item){return Uni.createFrom().item(()->{queue.add(item);return null;});}
      public Uni<Void> enqueueDelayed(ExecutionWorkItem item,Duration delay){return enqueueNow(item);}
    };
    coordinator.segmentBoundaryLedger=new SegmentBoundaryLedger(new InMemoryControlPlaneJournal());
    coordinator.releaseIdentityResolver=new PipelineReleaseIdentityResolver();
    set(coordinator.releaseIdentityResolver,"cachedContract",Optional.of(b));
    // Inject already selected real in-memory providers; do not run production startup qualification here.
    set(coordinator,"queueProvidersInitialized",true);
    controlPlane.queueAsyncCoordinator=coordinator;
    var service=RetainedReleaseApplicationFixture.application(config,b,"release-b");
    service.controlPlane=controlPlane;
    service.transitionWorkerSelector=new PipelineTransitionWorkerSelector();
    set(service.transitionWorkerSelector,"orchestratorConfig",config);
    controlPlane.pipelineExecutionService=service;
    http=new HostedPipelineControlPlaneResource();
    set(http,"orchestratorConfig",config);set(http,"executionService",service);
    set(http,"payloadCodec",new JsonTransitionPayloadCodec());
  }
  public Response completeAtHttpB(){
    var request=new HostedAwaitCompletionRequest(interaction.interactionId(), interaction.correlationId(), token,
        "callback-a",new JsonTransitionPayloadCodec().encode("approved"),"provider");
    return http.completeInteraction("tenant","Bearer test-admin",request).await().atMost(Duration.ofSeconds(3));
  }
  public ExecutionRecord<Object,Object> execution(){return executions.getExecution("tenant",waiting.executionId()).await().indefinitely().orElseThrow();}
  public AwaitInteractionRecord interaction(){return interactions.get("tenant",interaction.interactionId()).await().indefinitely().orElseThrow();}
  private AwaitCoordinator awaits(String secret, AwaitCompletionDescriptor descriptor) throws Exception {
    var value=new AwaitCoordinator();set(value,"resolvedInteractionStore",interactions);set(value,"resolvedUnitStore",units);
    var tokens=new AwaitResumeTokenService();set(tokens,"configuredSecret",Optional.of(secret));set(value,"resumeTokenService",tokens);
    var registry=new AwaitCompletionDescriptorRegistry();registry.register(descriptor);set(value,"descriptorFactory",registry);
    return value;
  }
  private static AwaitCompletionDescriptor descriptor(String projector){
    return new AwaitCompletionDescriptor("HeldApproval",String.class.getName(),String.class.getName(),"ONE_TO_ONE",
        Duration.ofMinutes(10),"interactionId","interaction-api",Map.of(),List.of(),String.class.getName(),String.class.getName(),
        Function.identity(),Function.identity(),projector,(request,completion,metadata)->completion,true);
  }
  public static void set(Object target,String name,Object value) throws Exception {
    Class<?> type=target.getClass();Field field=null;
    while(type!=null){try{field=type.getDeclaredField(name);break;}catch(NoSuchFieldException absent){type=type.getSuperclass();}}
    if(field==null)throw new NoSuchFieldException(name);field.setAccessible(true);field.set(target,value);
  }
}

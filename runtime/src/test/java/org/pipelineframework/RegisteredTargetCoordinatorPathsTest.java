package org.pipelineframework;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import jakarta.enterprise.inject.Instance;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.*;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Native coordination and persisted pins; remote execution/classpath metadata are explicit fixtures. */
class RegisteredTargetCoordinatorPathsTest {
    private RegisteredRestWorkerTargetsTest fixture;
    private InMemoryExecutionStateStore executions;
    private InMemoryAwaitInteractionStore interactions;
    private InMemoryAwaitUnitStore units;
    private AwaitCoordinator await;
    private PipelineExecutionService service;
    private QueueAsyncCoordinator coordinator;

    @BeforeEach void setup() throws Exception {
        fixture = new RegisteredRestWorkerTargetsTest(); fixture.setup();
        executions = new InMemoryExecutionStateStore();
        interactions = new InMemoryAwaitInteractionStore(); units = new InMemoryAwaitUnitStore();
        await = new AwaitCoordinator();
        inject(await, "resolvedInteractionStore", interactions); inject(await, "resolvedUnitStore", units);
        var descriptors = new AwaitCompletionDescriptorRegistry();
        descriptors.register(new AwaitCompletionDescriptor("AwaitApproval", String.class.getName(), String.class.getName(),
            java.time.Duration.ofMinutes(1), "interactionId", "interaction-api", java.util.Map.of(), List.of()));
        inject(await, "descriptorFactory", descriptors);
        rebuildCoordinator();
    }

    @AfterEach void close() { fixture.close(); }

    private void rebuildCoordinator() {
        var config = fixture.configForTest();
        coordinator = new QueueAsyncCoordinator();
        coordinator.orchestratorConfig = config;
        coordinator.executionStateStore = executions;
        coordinator.workDispatcher = mock(WorkDispatcher.class);
        when(coordinator.workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
        when(coordinator.workDispatcher.enqueueDelayed(any(), any())).thenReturn(Uni.createFrom().voidItem());
        coordinator.deadLetterPublisher = mock(DeadLetterPublisher.class);
        when(coordinator.deadLetterPublisher.publish(any())).thenReturn(Uni.createFrom().voidItem());
        when(coordinator.workDispatcher.providerName()).thenReturn("event");
        when(coordinator.deadLetterPublisher.providerName()).thenReturn("log");
        when(coordinator.workDispatcher.startupValidationError()).thenReturn(Optional.empty());
        when(coordinator.deadLetterPublisher.startupValidationError()).thenReturn(Optional.empty());
        coordinator.executionStateStores = providers(executions);
        coordinator.workDispatchers = providers(coordinator.workDispatcher);
        coordinator.deadLetterPublishers = providers(coordinator.deadLetterPublisher);
        coordinator.executionInputPolicy = new ExecutionInputPolicy();
        coordinator.executionFailureHandler = new ExecutionFailureHandler();
        coordinator.executionFailureHandler.orchestratorConfig = config;
        coordinator.awaitCoordinator = await;
        coordinator.awaitLiveCompletionRegistry = new AwaitLiveCompletionRegistry();
        coordinator.transitionWorkerExecutor = new TransitionWorkerExecutor(config, new PipelineInvocationRuntime());
        coordinator.transitionPayloadCodec = new JsonTransitionPayloadCodec();
        coordinator.segmentBoundaryLedger = new SegmentBoundaryLedger(new InMemoryControlPlaneJournal());
        coordinator.releaseRegistry = fixture.releasesForTest();
        coordinator.registeredTargets = fixture.targetsForTest();
        var controlPlane = new LocalPipelineControlPlane();
        controlPlane.queueAsyncCoordinator = coordinator;
        service = new PipelineExecutionService();
        service.controlPlane = controlPlane;
        service.queueAsyncCoordinator = coordinator;
        service.transitionWorkerSelector = fixture.selectorForTest();
        service.transitionWorkerExecutor = coordinator.transitionWorkerExecutor;
        service.transitionPayloadCodec = coordinator.transitionPayloadCodec;
        service.awaitCoordinator = await;
        service.pipelineStepResolver = mock(PipelineStepResolver.class);
        when(service.pipelineStepResolver.loadPipelineSteps()).thenReturn(List.of(new Object(), new Object(), new Object()));
        service.stepOrderer = mock(PipelineStepOrderer.class);
        when(service.stepOrderer.orderSteps(any())).thenAnswer(invocation -> invocation.getArgument(0));
        controlPlane.pipelineExecutionService = service;
    }

    private ExecutionRecord<Object,Object> create(String key, ExecutionResultShape shape) {
        long now = System.currentTimeMillis();
        return executions.createOrGetExecution(new ExecutionCreateCommand("tenant", key, "pipeline", "contract", "A",
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"), shape, now, now / 1000 + 3600))
            .await().indefinitely().record();
    }

    private ExecutionRecord<Object,Object> current(ExecutionRecord<?,?> original) {
        return executions.getExecution("tenant", original.executionId()).await().indefinitely().orElseThrow();
    }

    private void process(ExecutionRecord<?,?> record) {
        service.processExecutionWorkItem(new ExecutionWorkItem("tenant", record.executionId())).await().indefinitely();
    }

    @Test void initialAndTerminalRedriveDispatchThroughNativeSelectorWithOriginalPin() {
        var record = create("initial", ExecutionResultShape.SINGLE);
        fixture.activateBForTest(); process(record);
        assertEquals(ExecutionStatus.SUCCEEDED, current(record).status());
        assertEquals("A", current(record).releaseVersion());
        assertEquals(1, fixture.aCallsForTest()); assertEquals(0, fixture.bCallsForTest());
        var failed = create("failed-redrive", ExecutionResultShape.SINGLE);
        executions.markTerminalFailure("tenant", failed.executionId(), failed.version(), ExecutionStatus.FAILED,
            "failed-transition", "fixture-failure", "external failure fixture", 0, System.currentTimeMillis())
            .await().indefinitely().orElseThrow();
        rebuildCoordinator();
        coordinator.redriveExecution("tenant", failed.executionId(), current(failed).version(), true, "replay")
            .await().indefinitely();
        process(failed);
        assertEquals(ExecutionStatus.SUCCEEDED, current(failed).status());
        assertEquals("A", current(failed).releaseVersion());
        assertEquals(2, fixture.aCallsForTest()); assertEquals(0, fixture.bCallsForTest());
    }

    @Test void scalarAwaitCompletionAndFreshCoordinatorKeepOriginalPin() throws Exception {
        var parent = create("scalar", ExecutionResultShape.SINGLE);
        var interaction = seedAwait(parent, false);
        fixture.activateBForTest(); rebuildCoordinator();
        service.completeAwaitInteraction(new AwaitCompletionCommand("tenant", interaction.interactionId(), "correlation",
            "scalar-completion", "approved", "actor", System.currentTimeMillis())).await().indefinitely();
        assertEquals(ExecutionStatus.QUEUED, current(parent).status());
        process(parent);
        assertEquals(ExecutionStatus.SUCCEEDED, current(parent).status());
        assertEquals("A", current(parent).releaseVersion());
        assertEquals(1, fixture.aCallsForTest()); assertEquals(0, fixture.bCallsForTest());
    }

    @Test void itemizedAwaitCompletionInvokesNativeCommandAwareSelector() throws Exception {
        var parent = create("itemized", ExecutionResultShape.MATERIALIZED_MULTI);
        var interaction = seedAwait(parent, true);
        fixture.activateBForTest(); rebuildCoordinator();
        service.completeAwaitInteraction(new AwaitCompletionCommand("tenant", interaction.interactionId(), "correlation",
            "item-completion", "approved", "actor", System.currentTimeMillis())).await().indefinitely();
        assertEquals(1, fixture.aCallsForTest()); assertEquals(0, fixture.bCallsForTest());
        assertEquals("A", current(parent).releaseVersion());
        assertEquals(ExecutionStatus.QUEUED, current(parent).status());
        process(parent);
        assertEquals(ExecutionStatus.SUCCEEDED, current(parent).status());
    }

    @Test void unknownCompiledContractFailsBeforeTerminalShortcutOrWorkerInvocation() {
        var record = create("terminal-guard", ExecutionResultShape.SINGLE);
        long now = System.currentTimeMillis();
        executions.markWaitingExternal("tenant", record.executionId(), record.version(), "await", "unit", 99, now)
            .await().indefinitely();
        executions.markAwaitCompleted("tenant", record.executionId(), "unit", 99, now).await().indefinitely().orElseThrow();
        fixture.forgetCompiledContractForTest();
        assertThrows(IllegalStateException.class, () -> process(record));
        assertEquals(ExecutionStatus.QUEUED, current(record).status());
        assertEquals(0, fixture.aCallsForTest()); assertEquals(0, fixture.bCallsForTest());
    }

    private AwaitInteractionRecord seedAwait(ExecutionRecord<Object,Object> parent, boolean itemized) {
        long now = System.currentTimeMillis(); String unit = "unit-" + parent.executionId();
        executions.markWaitingExternal("tenant", parent.executionId(), parent.version(), "await", unit, 0, now)
            .await().indefinitely();
        units.createOrGet(new AwaitUnitCreateCommand("tenant", unit, parent.executionId(), "AwaitApproval", 0,
            "ONE_TO_ONE", now, now / 1000 + 3600)).await().indefinitely();
        units.markDispatchComplete("tenant", unit, 1, now).await().indefinitely();
        var interaction = interactions.createOrGet(new AwaitCreateCommand("tenant", parent.executionId(), "AwaitApproval", 0,
            String.class.getName(), "cause", "request", "correlation", "request", "reviewer", "reviewers", "test", unit,
            itemized ? 0 : null, now, now + 60000, now / 1000 + 3600)).await().indefinitely().record();
        return interaction;
    }

    private static void inject(AwaitCoordinator target, String name, Object value) throws Exception {
        Field field = AwaitCoordinator.class.getDeclaredField(name); field.setAccessible(true); field.set(target, value);
    }

    @SuppressWarnings("unchecked")
    private static <T> Instance<T> providers(T provider) {
        Instance<T> candidates = mock(Instance.class);
        when(candidates.stream()).thenAnswer(ignored -> Stream.of(provider));
        return candidates;
    }
}

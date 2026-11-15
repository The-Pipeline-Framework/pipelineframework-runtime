package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.orchestrator.DeadLetterEnvelope;
import org.pipelineframework.orchestrator.DynamoAwaitLifecycleTestStores;
import org.pipelineframework.orchestrator.DynamoExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionDurablePayloadResolver;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBindingTest;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBindingTest.TransportFixture;
import org.pipelineframework.orchestrator.PairedTransitionSettlementStore;
import org.pipelineframework.orchestrator.PairedTransitionSettlementStore.Lease;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.RemoteTransitionOutcomeUnknownException;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope.PortableFailure.Code;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.release.InMemoryPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseStatus;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/** Exercises the existing execution state owner with explicitly admitted, paired outcomes. */
@Testcontainers(disabledWithoutDocker = true)
class PairedTransitionSettlementIT {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final long LEASE_MS = Duration.ofMinutes(2).toMillis();
  private static final String TABLE_PREFIX = "paired_transition_settlement";

  @Container
  static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
      DockerImageName.parse("localstack/localstack:3.8")).withServices("dynamodb");

  @TempDir static Path fixtureDirectory;
  private static DynamoDbClient dynamo;
  private static TransportFixture fixture;
  private static ExecutionDurablePayloadResolver durablePayloads;

  private final List<DeadLetterEnvelope> deadLetters = new ArrayList<>();
  private final WorkDispatcher dispatcher = mock(WorkDispatcher.class);
  private final AwaitItemContinuationHandler continuation = mock(AwaitItemContinuationHandler.class);
  private final SegmentBoundaryLedger ledger = spy(new SegmentBoundaryLedger());
  private final CheckpointPublicationService checkpointPublication = mock(CheckpointPublicationService.class);
  private final ObjectPublishCompletionService objectPublication = mock(ObjectPublishCompletionService.class);
  private ExecutionStateStore store;
  private SegmentCommitEffects effects;

  @BeforeAll
  static void createStoreTablesAndPrepareRelease() throws Exception {
    dynamo = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
        .region(Region.of(LOCALSTACK.getRegion()))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
        .build();
    createTable("_execution", "tenant_id", "execution_id");
    createTable("_execution_key", "tenant_execution_key");
    fixture = NativeWorkerReleaseBindingTest.transportFixture(fixtureDirectory);
    durablePayloads = registeredPayloadResolver(fixture);
  }

  @AfterAll
  static void closeClient() {
    if (dynamo != null) {
      dynamo.close();
    }
  }

  @BeforeEach
  void prepareSettlement() throws ReflectiveOperationException {
    store = storeWithResolver();
    var config = mock(PipelineOrchestratorConfig.class);
    when(config.maxRetries()).thenReturn(3);
    var failureHandler = new ExecutionFailureHandler();
    failureHandler.orchestratorConfig = config;
    failureHandler.segmentBoundaryLedger = ledger;
    effects = new SegmentCommitEffects(store, dispatcher, envelope -> {
      deadLetters.add(envelope);
      return Uni.createFrom().voidItem();
    }, mock(AwaitCoordinator.class), failureHandler, () -> ledger, mock(AwaitContinuations.class),
        new TerminalPublicationBoundary(checkpointPublication, objectPublication, JsonTransitionPayloadCodec::new,
            () -> ledger), ignored -> {});
  }

  @Test
  void persistsTheValidatedNativePayloadAndRejectsADifferentDuplicate() throws Exception {
    var claim = claim("completed", System.currentTimeMillis(), LEASE_MS);
    var retained = attempt(claim);
    settle(claim, completion(retained, 42));

    var completed = read(claim);
    assertEquals(ExecutionStatus.SUCCEEDED, completed.status());
    assertEquals(claim.record().version() + 1, completed.version());
    assertEquals(claim.transitionKey(), completed.lastTransitionKey());
    assertEquals(List.of(output(retained, 42)), completed.resultPayload());

    settle(claim, completion(retained, -7));
    assertEquals(completed, read(claim));
    assertTrue(deadLetters.isEmpty());
    verifyNoInteractions(dispatcher, continuation);
  }

  @ParameterizedTest
  @EnumSource(TerminalPublication.class)
  void rejectsPairedCompletionWithConfiguredTerminalPublication(TerminalPublication publication) throws Exception {
    when(checkpointPublication.enabled()).thenReturn(publication == TerminalPublication.CHECKPOINT);
    when(objectPublication.enabled()).thenReturn(publication == TerminalPublication.OBJECT);

    var expired = claim(publication + "-expired", System.currentTimeMillis() - LEASE_MS, 1);
    var staleResult = completion(attempt(expired), -1);
    settle(expired, staleResult);
    assertEquals(expired.record(), read(expired));

    var replacement = ClaimedSegment.from(store.claimLease(expired.record().tenantId(), expired.record().executionId(),
        "replacement", System.currentTimeMillis(), LEASE_MS).await().atMost(TIMEOUT).orElseThrow());
    settle(expired, staleResult);
    settle(replacement, staleResult);
    assertEquals(replacement.record(), read(replacement));
    assertTrue(deadLetters.isEmpty());

    var currentResult = completion(attempt(replacement), 42);
    settle(replacement, currentResult);
    var rejected = read(replacement);
    assertEquals(ExecutionStatus.FAILED, rejected.status());
    assertEquals(replacement.record().version() + 1, rejected.version());
    assertEquals(replacement.record().attempt(), rejected.attempt());
    assertEquals(replacement.transitionKey(), rejected.lastTransitionKey());
    assertEquals("PAIRED_TERMINAL_PUBLICATION_UNSUPPORTED", rejected.errorCode());
    assertNull(rejected.resultPayload());

    settle(replacement, currentResult);
    assertEquals(rejected, read(replacement));
    assertEquals(1, deadLetters.size());
    assertEquals(rejected.errorCode(), deadLetters.getFirst().errorCode());
    assertFalse(deadLetters.getFirst().retryable());
    verify(ledger).recordRunFailed(eq(rejected), eq(rejected.errorCode()), eq(rejected.errorMessage()), anyLong());
    verify(ledger, never()).recordSegmentCompleted(any(), any(), any(), anyLong());
    verify(ledger, never()).recordRunSucceeded(any(), any(), anyLong());
    verify(ledger, never()).claimTerminalPublication(any(), any(), any(), anyLong());
    verify(ledger, never()).completeTerminalPublication(any(), any(), any(), anyLong());
    verifyNoMoreInteractions(ignoreStubs(checkpointPublication, objectPublication));
    verifyNoInteractions(dispatcher, continuation);
  }

  @ParameterizedTest
  @EnumSource(Code.class)
  void persistsEachPortableFailureAsTerminalDespiteCoordinatorRetryBudget(Code code) throws Exception {
    var claim = claim(code.name(), System.currentTimeMillis(), LEASE_MS);
    var retained = attempt(claim);
    var failed = NativeWorkerReleaseBindingTest.decodeResult(retained, PipelineJson.mapper().writeValueAsString(Map.of(
        "outcome", "FAILED", "resultProfile", retained.target().profiles().resultProfile(),
        "failure", Map.of("code", code.name(), "stepId", "B", "diagnostic", code.diagnostic()))));
    settle(claim, failed);
    settle(claim, failed);

    var persisted = read(claim);
    assertEquals(ExecutionStatus.FAILED, persisted.status());
    assertEquals(claim.record().version() + 1, persisted.version());
    assertEquals(claim.record().attempt(), persisted.attempt());
    assertEquals(code.name(), persisted.errorCode());
    assertEquals(code.diagnostic(), persisted.errorMessage());
    assertEquals(1, persisted.failedStepIndex());
    assertTrue(persisted.failedCommandId().isEmpty());
    assertEquals(1, deadLetters.size());
    assertEquals(code.name(), deadLetters.getFirst().errorCode());
    assertEquals(code.diagnostic(), deadLetters.getFirst().errorMessage());
    assertFalse(deadLetters.getFirst().retryable());
    verifyNoInteractions(dispatcher, continuation);
  }

  @Test
  void rejectsAnExpiredLeaseAndAnOldResultAgainstEitherClaimAfterReplacement() throws Exception {
    var expired = claim("expired", System.currentTimeMillis() - LEASE_MS, 1);
    var expiredAttempt = attempt(expired);
    var oldResponse = completion(expiredAttempt, -1);
    settle(expired, oldResponse);
    var pairedStore = (PairedTransitionSettlementStore) store;
    var oldLease = new Lease(expired.record().tenantId(), expired.record().executionId(), expired.record().version(),
        expired.record().leaseOwner(), expired.transitionKey());
    assertTrue(pairedStore.settlePairedSuccess(oldLease, output(expiredAttempt, -1)).await().atMost(TIMEOUT).isEmpty());
    assertEquals(expired.record(), read(expired));

    var replacement = ClaimedSegment.from(store.claimLease(expired.record().tenantId(), expired.record().executionId(),
        "replacement", System.currentTimeMillis(), LEASE_MS).await().atMost(TIMEOUT).orElseThrow());
    settle(expired, oldResponse);
    settle(replacement, oldResponse);
    effects.fail(expired, unknownOutcome()).await().atMost(TIMEOUT);
    var wrongOwner = new Lease(oldLease.tenantId(), oldLease.executionId(), replacement.record().version(),
        oldLease.owner(), replacement.transitionKey());
    assertTrue(pairedStore.settlePairedSuccess(wrongOwner, output(expiredAttempt, -1)).await().atMost(TIMEOUT).isEmpty());
    assertEquals(replacement.record(), read(replacement));

    var current = attempt(replacement);
    settle(replacement, completion(current, 7));
    var completed = read(replacement);
    settle(expired, oldResponse);
    assertEquals(List.of(output(current, 7)), completed.resultPayload());
    assertEquals(completed, read(replacement));
    assertTrue(deadLetters.isEmpty());
    verifyNoInteractions(dispatcher, continuation);
  }

  @Test
  void preservesCurrentUnknownOutcomeWithoutAllowingAnAutomaticClaim() throws Exception {
    var claim = claim("unknown", System.currentTimeMillis(), LEASE_MS);
    effects.fail(claim, unknownOutcome()).await().atMost(TIMEOUT);
    settle(claim, completion(attempt(claim), 99));

    var persisted = read(claim);
    assertEquals(ExecutionStatus.REMOTE_OUTCOME_UNKNOWN, persisted.status());
    assertEquals(claim.record().version() + 1, persisted.version());
    assertEquals(claim.record().attempt(), persisted.attempt());
    assertEquals("REMOTE_OUTCOME_UNKNOWN", persisted.errorCode());
    long afterLease = claim.record().leaseExpiresEpochMs() + 1;
    assertTrue(store.findDueExecutions(afterLease, 20).await().atMost(TIMEOUT).stream()
        .noneMatch(due -> due.executionId().equals(persisted.executionId())));
    assertTrue(store.claimLease(persisted.tenantId(), persisted.executionId(), "automatic", afterLease, LEASE_MS)
        .await().atMost(TIMEOUT).isEmpty());
    assertTrue(deadLetters.isEmpty());
    verifyNoInteractions(dispatcher, continuation);
  }

  private static RemoteTransitionOutcomeUnknownException unknownOutcome() {
    return new RemoteTransitionOutcomeUnknownException("rest", "worker.example", 30, 30,
        new IOException("response interrupted"));
  }

  private enum TerminalPublication {
    CHECKPOINT,
    OBJECT
  }

  private ClaimedSegment claim(String key, long now, long leaseMs) {
    var target = fixture.target();
    // Seed the retained state without introducing hosted native payload admission in this test.
    var seedStore = DynamoAwaitLifecycleTestStores.executionStore(dynamo, TABLE_PREFIX);
    var created = seedStore.createOrGetExecution(new ExecutionCreateCommand(target.tenantId(), key, target.pipelineId(),
        target.contractVersion(), target.releaseVersion(), "seed", ExecutionResultShape.SINGLE, now,
        now / 1000 + 3600)).await().atMost(TIMEOUT).record();
    return ClaimedSegment.from(store.claimLease(created.tenantId(), created.executionId(), "coordinator", now, leaseMs)
        .await().atMost(TIMEOUT).orElseThrow());
  }

  private static NativeWorkerReleaseBinding.Attempt attempt(ClaimedSegment claim) {
    var record = claim.record();
    var target = fixture.target();
    return fixture.binding().pin(new TransitionCommandEnvelope(record.tenantId(), record.executionId(), record.pipelineId(),
        record.contractVersion(), record.releaseVersion(), record.currentStepIndex(), -1, record.attempt(),
        record.resultShape(), record.version(), claim.transitionKey(), "test-trace",
        target.coordinatorContract().steps().getFirst().inputTypeId(), target.profiles().payloadEncoding(),
        "{\"value\":5}"));
  }

  private static TransitionResultEnvelope completion(NativeWorkerReleaseBinding.Attempt attempt, int value)
      throws IOException {
    return NativeWorkerReleaseBindingTest.decodeResult(attempt, PipelineJson.mapper().writeValueAsString(Map.of(
        "outcome", "COMPLETED", "outputPayloads", List.of(output(attempt, value)),
        "terminalOutputPublished", false, "terminalInputPassthrough", false)));
  }

  private static SerializedTransitionPayload output(NativeWorkerReleaseBinding.Attempt attempt, int value) {
    return new SerializedTransitionPayload(attempt.expectedOutputTypeId(), attempt.target().profiles().payloadEncoding(),
        "{\"value\":" + value + "}");
  }

  private void settle(ClaimedSegment claim, TransitionResultEnvelope response) {
    effects.commit(SegmentCommitPlan.from(claim, response), continuation).await().atMost(TIMEOUT);
  }

  private ExecutionRecord<Object, Object> read(ClaimedSegment claim) throws ReflectiveOperationException {
    return storeWithResolver().getExecution(claim.record().tenantId(), claim.record().executionId())
        .await().atMost(TIMEOUT).orElseThrow();
  }

  private static ExecutionStateStore storeWithResolver() throws ReflectiveOperationException {
    var store = DynamoAwaitLifecycleTestStores.executionStore(dynamo, TABLE_PREFIX);
    inject(DynamoExecutionStateStore.class, store, "durablePayloadResolver", durablePayloads);
    return store;
  }

  private static ExecutionDurablePayloadResolver registeredPayloadResolver(TransportFixture fixture)
      throws ReflectiveOperationException {
    var target = fixture.target();
    var contract = target.coordinatorContract();
    var descriptor = fixture.descriptor();
    var artifact = descriptor.artifacts().stream().filter(item -> item.artifactId().equals(target.runnableArtifactId()))
        .findFirst().orElseThrow();
    var registry = new InMemoryPipelineReleaseRegistry();
    long now = System.currentTimeMillis();
    registry.register(new PipelineReleaseRecord(target.tenantId(), target.pipelineId(), target.contractVersion(),
        target.releaseVersion(), PipelineReleaseStatus.REGISTERED, descriptor, artifact.artifactId(), artifact.digest(),
        artifact.uri(), 1, artifact.digest(), contract, now, now, 0)).await().atMost(TIMEOUT);
    var resolver = new ExecutionDurablePayloadResolver();
    inject(ExecutionDurablePayloadResolver.class, resolver, "releaseRegistry", registry);
    inject(ExecutionDurablePayloadResolver.class, resolver, "codec", new JsonDurablePayloadCodec());
    return resolver;
  }

  private static void inject(Class<?> owner, Object instance, String name, Object value)
      throws ReflectiveOperationException {
    var field = owner.getDeclaredField(name);
    field.setAccessible(true);
    field.set(instance, value);
  }

  private static void createTable(String suffix, String... keys) {
    var attributes = new ArrayList<AttributeDefinition>();
    var schema = new ArrayList<KeySchemaElement>();
    for (String key : keys) {
      attributes.add(AttributeDefinition.builder().attributeName(key).attributeType(ScalarAttributeType.S).build());
      schema.add(KeySchemaElement.builder().attributeName(key)
          .keyType(schema.isEmpty() ? KeyType.HASH : KeyType.RANGE).build());
    }
    dynamo.createTable(request -> request.tableName(TABLE_PREFIX + suffix).billingMode(BillingMode.PAY_PER_REQUEST)
        .attributeDefinitions(attributes).keySchema(schema));
  }
}

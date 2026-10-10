package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitLiveCompletionRegistry;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.sqs.AwsShapedAwaitTestComponents;
import org.pipelineframework.awaitable.sqs.SqsAwaitCompletionAction;
import org.pipelineframework.awaitable.sqs.SqsAwaitCompletionEnvelope;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.orchestrator.AwsShapedCoordinatorTestComponents;
import org.pipelineframework.orchestrator.CoordinatorSweepResult;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.DynamoAwaitLifecycleTestStores;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionRedriveResult;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionResultShapeResolver;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.LocalControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineTransitionWorkerSelector;
import org.pipelineframework.orchestrator.SqsInboundMessage;
import org.pipelineframework.orchestrator.SqsMessageDisposition;
import org.pipelineframework.orchestrator.SqsPipelineTransitionWorker;
import org.pipelineframework.orchestrator.SqsTransitionWorkerAction;
import org.pipelineframework.orchestrator.SqsWorkDispatcher;
import org.pipelineframework.orchestrator.SqsWorkItemAction;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutionMode;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.telemetry.PipelineRunContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.lambda.durable.DurableContext;
import software.amazon.lambda.durable.testing.LocalDurableTestRunner;

/**
 * AWS-shaped proof that coordinator actions survive replacement runtimes over DynamoDB and SQS.
 * LocalStack supplies durable substrates; each receive and scheduled wake-up remains single-shot.
 */
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwsShapedCoordinatorActionsIT {

    private static final String TABLE_PREFIX = "aws_shaped_coordinator";
    private static final String PIPELINE_ID = PipelineContractDescriptor.DEFAULT_PIPELINE_ID;
    private static final String CONTRACT_VERSION = PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION;
    private static final String RELEASE_VERSION = PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION;
    private static final String WORKER_SECRET = "aws-shaped-worker-secret";
    private static final String PROVIDER_CALLBACK_ID = "provider_callback_id";
    private static final String PROVIDER_DRIVER_GENERATION = "provider_driver_generation";

    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8"))
        .withServices("dynamodb", "dynamodbstreams", "sqs");

    private DynamoDbClient dynamoAdmin;
    private DynamoDbStreamsClient dynamoStreams;
    private SqsClient sqsAdmin;
    private String interactionStreamArn;

    @BeforeAll
    void createDurableSubstrates() {
        dynamoAdmin = dynamoClient();
        dynamoStreams = dynamoStreamsClient();
        sqsAdmin = sqsClient();
        createTable(TABLE_PREFIX + "_execution", "tenant_id", Optional.of("execution_id"));
        createTable(TABLE_PREFIX + "_execution_key", "tenant_execution_key", Optional.empty());
        createTable(TABLE_PREFIX + "_execution_payload", "payload_id", Optional.of("payload_part"));
        createTable(TABLE_PREFIX + "_unit", "tenant_id", Optional.of("unit_id"));
        interactionStreamArn = createAwaitInteractionTable(TABLE_PREFIX + "_interaction");
        createTable(TABLE_PREFIX + "_interaction_key", "lookup_key", Optional.empty());
    }

    @AfterAll
    void closeDurableSubstrates() {
        if (dynamoAdmin != null) {
            dynamoAdmin.close();
        }
        if (dynamoStreams != null) {
            dynamoStreams.close();
        }
        if (sqsAdmin != null) {
            sqsAdmin.close();
        }
    }

    @Test
    void submitAwaitCompletionResumeAndResultSurviveRestartAndReplay() throws Exception {
        try (ScenarioQueues queues = createQueues("await")) {
            String tenantId = "tenant-await-" + suffix();
            String interactionId;
            String correlationId;
            String originalWorkBody;
            String executionId;

            try (RuntimeFixture runtimeA = runtime(queues, WorkerOutcome.WAIT, 0)) {
                RunAsyncAcceptedDto accepted = runtimeA.controlPlane.executePipelineAsync(
                        "request", tenantId, "await-key", false,
                        PIPELINE_ID, CONTRACT_VERSION, RELEASE_VERSION)
                    .await().atMost(Duration.ofSeconds(10));
                executionId = accepted.executionId();
                interactionId = "interaction-" + executionId;
                correlationId = "correlation-" + executionId;
                runtimeA.seedScalarAwait(tenantId, executionId, interactionId, correlationId);

                assertEquals(ExecutionStatus.QUEUED,
                    runtimeA.controlPlane.getExecutionStatus(tenantId, executionId)
                        .await().atMost(Duration.ofSeconds(10)).status());

                HandledMessage handled = runtimeA.invokeWorkWithTransition(executionId);
                originalWorkBody = handled.message().body();
                assertEquals(SqsMessageDisposition.ACKNOWLEDGE, handled.disposition());
                assertEquals(ExecutionStatus.WAITING_EXTERNAL,
                    runtimeA.controlPlane.getExecutionStatus(tenantId, executionId)
                        .await().atMost(Duration.ofSeconds(10)).status());
                assertEquals(AwaitInteractionStatus.DISPATCHED,
                    runtimeA.interactions.get(tenantId, interactionId)
                        .await().atMost(Duration.ofSeconds(10)).orElseThrow().status());
            }

            try (RuntimeFixture runtimeB = runtime(queues, WorkerOutcome.COMPLETE, 0)) {
                runtimeB.send(queues.workQueueUrl(), originalWorkBody);
                HandledMessage replayedWork = runtimeB.invokeNext(
                    queues.workQueueUrl(), runtimeB.workAction::handle);
                assertEquals(SqsMessageDisposition.ACKNOWLEDGE, replayedWork.disposition());
                assertTrue(runtimeB.receive(queues.transitionRequestQueueUrl(), 0, 1).isEmpty(),
                    "a replayed WAITING_EXTERNAL work item must not invoke the worker again");

                String completionBody = PipelineJson.mapper().writeValueAsString(new SqsAwaitCompletionEnvelope(
                    tenantId,
                    interactionId,
                    correlationId,
                    "",
                    "completion-" + executionId,
                    Map.of("decision", "approved"),
                    "local-proof"));
                runtimeB.send(queues.awaitQueueUrl(), completionBody);
                assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
                    runtimeB.invokeNext(queues.awaitQueueUrl(), message ->
                        runtimeB.awaitAction.handle(message, Duration.ofSeconds(10))).disposition());

                runtimeB.send(queues.awaitQueueUrl(), completionBody);
                assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
                    runtimeB.invokeNext(queues.awaitQueueUrl(), message ->
                        runtimeB.awaitAction.handle(message, Duration.ofSeconds(10))).disposition());

                assertEquals(ExecutionStatus.QUEUED,
                    runtimeB.controlPlane.getExecutionStatus(tenantId, executionId)
                        .await().atMost(Duration.ofSeconds(10)).status());
                List<Message> resumeMessages = runtimeB.receive(queues.workQueueUrl(), 5, 10);
                assertFalse(resumeMessages.isEmpty(), "completion must enqueue the parent execution");
                runtimeB.invokeWorkMessageWithTransition(resumeMessages.getFirst(), executionId);
                for (Message duplicate : resumeMessages.subList(1, resumeMessages.size())) {
                    runtimeB.invokeMessage(queues.workQueueUrl(), duplicate, runtimeB.workAction::handle);
                }

                assertEquals(1, runtimeB.transitionExecutions.get(),
                    "duplicate completion delivery must not execute the resumed transition twice");
                assertEquals(ExecutionStatus.SUCCEEDED,
                    runtimeB.controlPlane.getExecutionStatus(tenantId, executionId)
                        .await().atMost(Duration.ofSeconds(10)).status());
                assertEquals("approved-result",
                    runtimeB.controlPlane.getExecutionResult(tenantId, executionId, String.class, false)
                        .await().atMost(Duration.ofSeconds(10)));
                Object rawResult = runtimeB.controlPlane.getExecutionResultPayload(tenantId, executionId)
                    .await().atMost(Duration.ofSeconds(10));
                SerializedTransitionPayload serialized = assertInstanceOf(
                    SerializedTransitionPayload.class, rawResult);
                assertEquals(String.class.getName(), serialized.payloadTypeId());
                assertEquals("approved-result", new JsonTransitionPayloadCodec().decode(serialized));
                assertEquals(AwaitInteractionStatus.COMPLETED,
                    runtimeB.interactions.get(tenantId, interactionId)
                        .await().atMost(Duration.ofSeconds(10)).orElseThrow().status());
            }
        }
    }

    @Test
    void syntheticScheduledWakeupDispatchesOneDueRetry() throws Exception {
        try (ScenarioQueues queues = createQueues("sweep");
             RuntimeFixture runtime = runtime(queues, WorkerOutcome.COMPLETE, 1)) {
            String tenantId = "tenant-sweep-" + suffix();
            RunAsyncAcceptedDto accepted = runtime.controlPlane.executePipelineAsync(
                    "retry-input", tenantId, "retry-key", false,
                    PIPELINE_ID, CONTRACT_VERSION, RELEASE_VERSION)
                .await().atMost(Duration.ofSeconds(10));
            Message initial = runtime.take(queues.workQueueUrl());
            runtime.delete(queues.workQueueUrl(), initial);

            ExecutionRecord<Object, Object> queued = runtime.executionStore.getExecution(
                    tenantId, accepted.executionId())
                .await().atMost(Duration.ofSeconds(10)).orElseThrow();
            assertEquals(tenantId, queued.tenantId());
            assertEquals(accepted.executionId(), queued.executionId());
            String persistedExecutionKey = queued.executionKey();
            assertNotNull(persistedExecutionKey);
            assertFalse(persistedExecutionKey.isBlank());
            assertEquals(PIPELINE_ID, queued.pipelineId());
            assertEquals(CONTRACT_VERSION, queued.contractVersion());
            assertEquals(RELEASE_VERSION, queued.releaseVersion());
            assertEquals(ExecutionStatus.QUEUED, queued.status());
            assertEquals(0L, queued.version());
            assertEquals(0, queued.attempt());
            assertNull(queued.leaseOwner());
            assertEquals(0L, queued.leaseExpiresEpochMs());
            assertEquals(queued.createdAtEpochMs(), queued.nextDueEpochMs());
            assertEquals(queued.createdAtEpochMs(), queued.updatedAtEpochMs());
            long observedNow = System.currentTimeMillis();
            long persistedNow = Math.max(queued.nextDueEpochMs(), queued.createdAtEpochMs());
            long fixedNow = Math.max(observedNow, persistedNow);
            assertTrue(fixedNow >= persistedNow);
            assertEquals(persistedNow, Math.max(persistedNow - 1, persistedNow),
                "A regressed observation must not precede persisted authority");
            ExecutionRecord<Object, Object> claimed = runtime.executionStore.claimLease(
                    tenantId, accepted.executionId(), "retry-arranger", fixedNow, 1_000L)
                .await().atMost(Duration.ofSeconds(10)).orElseThrow();
            assertEquals(ExecutionStatus.RUNNING, claimed.status());
            assertEquals(tenantId, claimed.tenantId());
            assertEquals(accepted.executionId(), claimed.executionId());
            assertEquals(persistedExecutionKey, claimed.executionKey());
            assertEquals(PIPELINE_ID, claimed.pipelineId());
            assertEquals(CONTRACT_VERSION, claimed.contractVersion());
            assertEquals(RELEASE_VERSION, claimed.releaseVersion());
            assertEquals(queued.version() + 1L, claimed.version());
            assertEquals("retry-arranger", claimed.leaseOwner());
            assertEquals(fixedNow + 1_000L, claimed.leaseExpiresEpochMs());
            ExecutionRecord<Object, Object> retry = runtime.executionStore.scheduleRetry(
                    tenantId,
                    accepted.executionId(),
                    claimed.version(),
                    1,
                    fixedNow,
                    accepted.executionId() + ":0:0",
                    "RetryableFailure",
                    "retry scheduled for synthetic wake-up",
                    fixedNow)
                .await().atMost(Duration.ofSeconds(10)).orElseThrow();
            assertEquals(tenantId, retry.tenantId());
            assertEquals(accepted.executionId(), retry.executionId());
            assertEquals(persistedExecutionKey, retry.executionKey());
            assertEquals(queued.pipelineId(), retry.pipelineId());
            assertEquals(queued.contractVersion(), retry.contractVersion());
            assertEquals(queued.releaseVersion(), retry.releaseVersion());
            assertEquals(ExecutionStatus.WAIT_RETRY, retry.status());
            assertEquals(claimed.version() + 1L, retry.version());
            assertEquals(1, retry.attempt());
            assertEquals(fixedNow, retry.nextDueEpochMs());
            assertEquals(fixedNow, retry.updatedAtEpochMs());
            assertEquals(accepted.executionId() + ":0:0", retry.lastTransitionKey());
            assertNull(retry.leaseOwner());
            assertEquals(0L, retry.leaseExpiresEpochMs());

            CoordinatorSweepResult result = runtime.controlPlane.sweepOnce(fixedNow)
                .await().atMost(Duration.ofSeconds(10));

            assertEquals(fixedNow, result.nowEpochMs());
            assertEquals(7, result.limit());
            assertEquals(0, result.timedOutAwaitCount());
            assertEquals(1, result.dispatchedExecutionCount());

            runtime.invokeWorkWithTransition(accepted.executionId());
            assertEquals(ExecutionStatus.SUCCEEDED,
                runtime.controlPlane.getExecutionStatus(tenantId, accepted.executionId())
                    .await().atMost(Duration.ofSeconds(10)).status());
            assertEquals(0, runtime.controlPlane.sweepOnce(fixedNow + 1L)
                .await().atMost(Duration.ofSeconds(10)).dispatchedExecutionCount());
        }
    }

    @Test
    void terminalFailurePublishesDlqAndRedrivePreservesPinnedRelease() throws Exception {
        try (ScenarioQueues queues = createQueues("redrive")) {
            String tenantId = "tenant-redrive-" + suffix();
            String executionId;
            long failedVersion;

            try (RuntimeFixture failing = runtime(queues, WorkerOutcome.FAIL, 0)) {
                RunAsyncAcceptedDto accepted = failing.controlPlane.executePipelineAsync(
                        "redrive-input", tenantId, "redrive-key", false,
                        PIPELINE_ID, CONTRACT_VERSION, RELEASE_VERSION)
                    .await().atMost(Duration.ofSeconds(10));
                executionId = accepted.executionId();
                failing.invokeWorkWithTransition(executionId);

                var failed = failing.controlPlane.getExecutionStatus(tenantId, executionId)
                    .await().atMost(Duration.ofSeconds(10));
                assertEquals(ExecutionStatus.FAILED, failed.status());
                failedVersion = failed.version();
                Message dlq = failing.take(queues.dlqQueueUrl());
                JsonNode dlqBody = PipelineJson.mapper().readTree(dlq.body());
                assertEquals(executionId, dlqBody.path("executionId").asText());
                failing.delete(queues.dlqQueueUrl(), dlq);
            }

            try (RuntimeFixture recovered = runtime(queues, WorkerOutcome.COMPLETE, 0)) {
                ExecutionRedriveResult redriven = recovered.controlPlane.redriveExecution(
                        tenantId, executionId, failedVersion, true, "operator-approved local proof")
                    .await().atMost(Duration.ofSeconds(10));

                assertEquals(ExecutionStatus.FAILED, redriven.previousStatus());
                assertEquals(ExecutionStatus.QUEUED, redriven.status());
                assertEquals(PIPELINE_ID, redriven.pipelineId());
                assertEquals(CONTRACT_VERSION, redriven.contractVersion());
                assertEquals(RELEASE_VERSION, redriven.releaseVersion());

                recovered.invokeWorkWithTransition(executionId);
                assertEquals(ExecutionStatus.SUCCEEDED,
                    recovered.controlPlane.getExecutionStatus(tenantId, executionId)
                        .await().atMost(Duration.ofSeconds(10)).status());
                assertEquals("approved-result",
                    recovered.controlPlane.getExecutionResult(tenantId, executionId, String.class, false)
                        .await().atMost(Duration.ofSeconds(10)));
            }
        }
    }

    @Test
    void lambdaDurableDriverPreservesTpfAuthorityAcrossReplayAndCallback() throws Exception {
        try (ScenarioQueues queues = createQueues("durable-driver");
             RuntimeFixture runtime = runtime(queues, WorkerOutcome.WAIT_THEN_COMPLETE, 0)) {
            String tenantId = "tenant-durable-" + suffix();
            DurableDriverInput input = new DurableDriverInput(tenantId, "durable-key-" + suffix(), 1L);
            AtomicReference<AwaitBridge> bridge = new AtomicReference<>();
            LocalDurableTestRunner<DurableDriverInput, DurableDriverResult> runner =
                durableRunner(runtime, bridge);

            var pending = runner.run(input);

            assertEquals(software.amazon.lambda.durable.model.ExecutionStatus.PENDING, pending.getStatus());
            String executionId = pending.getOperation("submit").getStepResult(String.class);
            AwaitBridge initialBridge = bridge.get();
            assertNotNull(initialBridge);
            assertEquals(executionId, initialBridge.executionId());
            assertEquals("interaction-" + executionId, initialBridge.interactionId());
            assertEquals("correlation-" + executionId, initialBridge.correlationId());
            assertEquals(runner.getCallbackId("await-completion-callback"), initialBridge.providerCallbackId());
            assertEquals(ExecutionStatus.WAITING_EXTERNAL,
                runtime.controlPlane.getExecutionStatus(tenantId, executionId)
                    .await().atMost(Duration.ofSeconds(10)).status());
            assertEquals(AwaitInteractionStatus.DISPATCHED,
                runtime.interactions.get(tenantId, initialBridge.interactionId())
                    .await().atMost(Duration.ofSeconds(10)).orElseThrow().status());
            assertEquals(1, runtime.transitionExecutions.get());

            runner.simulateFireAndForgetCheckpointLoss("dispatch-to-await");
            var replayedDispatch = runner.run(input);
            assertEquals(software.amazon.lambda.durable.model.ExecutionStatus.PENDING,
                replayedDispatch.getStatus());
            assertEquals(1, runtime.transitionExecutions.get(),
                "provider checkpoint loss after a TPF commit must not repeat the worker transition");

            runner.simulateFireAndForgetCheckpointLoss("submit");
            var replayed = runner.run(input);

            assertEquals(software.amazon.lambda.durable.model.ExecutionStatus.PENDING, replayed.getStatus());
            assertEquals(executionId, replayed.getOperation("submit").getStepResult(String.class));
            assertEquals(1, runtime.transitionExecutions.get(),
                "provider replay must not repeat a checkpointed TPF worker transition");
            assertTrue(runtime.receive(queues.workQueueUrl(), 0, 1).isEmpty(),
                "idempotent submit replay must not enqueue duplicate work");

            String completionId = "completion-" + executionId;
            runtime.send(queues.awaitQueueUrl(), PipelineJson.mapper().writeValueAsString(
                new SqsAwaitCompletionEnvelope(
                    tenantId,
                    initialBridge.interactionId(),
                    initialBridge.correlationId(),
                    "",
                    completionId,
                    Map.of("decision", "approved"),
                    "durable-driver-proof")));
            assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
                runtime.invokeNext(queues.awaitQueueUrl(), message ->
                    runtime.awaitAction.handle(message, Duration.ofSeconds(10))).disposition());
            assertEquals(ExecutionStatus.QUEUED,
                runtime.controlPlane.getExecutionStatus(tenantId, executionId)
                    .await().atMost(Duration.ofSeconds(10)).status());

            DurableAwaitChange completion = awaitCompletionChange(
                tenantId, initialBridge.interactionId());
            assertEquals(input.driverGeneration(), completion.driverGeneration());
            DurableAwaitWakeupAction wakeupAction = new DurableAwaitWakeupAction(runtime, 1);
            assertEquals(WakeupDisposition.RETRY, wakeupAction.handle(completion, runner),
                "a provider callback delivery failure must remain retryable");
            assertEquals(software.amazon.lambda.durable.model.ExecutionStatus.PENDING,
                runner.run(input).getStatus());
            assertEquals(WakeupDisposition.ACKNOWLEDGE, wakeupAction.handle(completion, runner));
            assertEquals(WakeupDisposition.ACKNOWLEDGE, wakeupAction.handle(completion, runner),
                "a duplicate stream delivery after callback completion must be acknowledged");
            var completed = runner.runUntilComplete(input);

            assertTrue(completed.isSucceeded());
            DurableDriverResult result = completed.getResult();
            assertEquals(executionId, result.executionId());
            assertEquals(PIPELINE_ID, result.pipelineId());
            assertEquals(CONTRACT_VERSION, result.contractVersion());
            assertEquals(RELEASE_VERSION, result.releaseVersion());
            assertEquals(ExecutionStatus.SUCCEEDED.name(), result.status());
            assertEquals("approved-result", result.result());
            assertEquals(String.class.getName(), result.rawPayloadTypeId());
            assertEquals(2, runtime.transitionExecutions.get());
            assertEquals(AwaitInteractionStatus.COMPLETED,
                runtime.interactions.get(tenantId, initialBridge.interactionId())
                    .await().atMost(Duration.ofSeconds(10)).orElseThrow().status());
        }
    }

    @Test
    void lambdaDurableDriverReconstructsFromTpfCheckpointAfterProviderHistoryLoss() throws Exception {
        try (ScenarioQueues queues = createQueues("durable-recovery");
             RuntimeFixture runtime = runtime(queues, WorkerOutcome.WAIT_THEN_COMPLETE, 0)) {
            String tenantId = "tenant-durable-recovery-" + suffix();
            String executionKey = "durable-recovery-key-" + suffix();
            DurableDriverInput firstInput = new DurableDriverInput(tenantId, executionKey, 1L);
            AtomicReference<AwaitBridge> firstBridge = new AtomicReference<>();
            LocalDurableTestRunner<DurableDriverInput, DurableDriverResult> firstRunner =
                durableRunner(runtime, firstBridge);

            var firstPending = firstRunner.run(firstInput);
            assertEquals(software.amazon.lambda.durable.model.ExecutionStatus.PENDING,
                firstPending.getStatus());
            String executionId = firstPending.getOperation("submit").getStepResult(String.class);
            AwaitBridge abandonedBridge = firstBridge.get();
            assertNotNull(abandonedBridge);
            assertEquals(1, runtime.transitionExecutions.get());

            DurableDriverInput replacementInput = new DurableDriverInput(tenantId, executionKey, 2L);
            AtomicReference<AwaitBridge> replacementBridge = new AtomicReference<>();
            LocalDurableTestRunner<DurableDriverInput, DurableDriverResult> replacementRunner =
                durableRunner(runtime, replacementBridge);

            var replacementPending = replacementRunner.run(replacementInput);
            assertEquals(software.amazon.lambda.durable.model.ExecutionStatus.PENDING,
                replacementPending.getStatus());
            assertEquals(executionId,
                replacementPending.getOperation("submit").getStepResult(String.class));
            AwaitBridge activeBridge = replacementBridge.get();
            assertNotNull(activeBridge);
            assertFalse(abandonedBridge.providerCallbackId().equals(activeBridge.providerCallbackId()));
            assertEquals(1, runtime.transitionExecutions.get(),
                "replacement provider history must attach without redispatching TPF work");

            runtime.completeAwait(
                tenantId,
                executionId,
                activeBridge.interactionId(),
                activeBridge.correlationId(),
                "durable-recovery-proof");
            DurableAwaitChange completion = awaitCompletionChange(tenantId, activeBridge.interactionId());
            assertEquals(2L, completion.driverGeneration());
            assertEquals(activeBridge.providerCallbackId(), completion.providerCallbackId());

            DurableAwaitWakeupAction wakeupAction = new DurableAwaitWakeupAction(runtime, 0);
            DurableAwaitChange staleChange = new DurableAwaitChange(
                tenantId,
                executionId,
                abandonedBridge.interactionId(),
                abandonedBridge.correlationId(),
                abandonedBridge.providerCallbackId(),
                1L);
            assertEquals(WakeupDisposition.ACKNOWLEDGE,
                wakeupAction.handle(staleChange, firstRunner));
            assertFalse(firstRunner.getOperation("await-completion-callback").isCompleted(),
                "a stale callback generation must not wake the abandoned provider execution");

            assertEquals(WakeupDisposition.ACKNOWLEDGE,
                wakeupAction.handle(completion, replacementRunner));
            var completed = replacementRunner.runUntilComplete(replacementInput);
            assertTrue(completed.isSucceeded());
            assertEquals(executionId, completed.getResult().executionId());
            assertEquals(2, runtime.transitionExecutions.get());

            firstRunner.completeCallback(
                abandonedBridge.providerCallbackId(),
                PipelineJson.mapper().writeValueAsString(new DurableAwaitSignal(
                    executionId,
                    abandonedBridge.interactionId(),
                    abandonedBridge.correlationId())));
            var lateAbandonedReplay = firstRunner.runUntilComplete(firstInput);
            assertTrue(lateAbandonedReplay.isSucceeded());
            assertEquals(executionId, lateAbandonedReplay.getResult().executionId());
            assertEquals(2, runtime.transitionExecutions.get(),
                "a late abandoned driver replay must observe the terminal TPF checkpoint");
        }
    }

    private DurableDriverResult driveWithLambdaDurability(
        DurableDriverInput input,
        DurableContext context,
        RuntimeFixture runtime,
        AtomicReference<AwaitBridge> bridge
    ) {
        String executionId = context.step("submit", String.class, () ->
            runtime.controlPlane.executePipelineAsync(
                    "durable-input",
                    input.tenantId(),
                    input.executionKey(),
                    false,
                    PIPELINE_ID,
                    CONTRACT_VERSION,
                    RELEASE_VERSION)
                .await().atMost(Duration.ofSeconds(10)).executionId());
        String interactionId = "interaction-" + executionId;
        String correlationId = "correlation-" + executionId;

        context.step("dispatch-to-await", Boolean.class, () -> {
            SemanticCheckpoint checkpoint = runtime.semanticCheckpoint(
                input.tenantId(), executionId, interactionId);
            if (!checkpoint.awaitPresent() && checkpoint.executionStatus() == ExecutionStatus.QUEUED) {
                runtime.seedScalarAwait(input.tenantId(), executionId, interactionId, correlationId);
                invokeWorkUnchecked(runtime, executionId);
            }
            return Boolean.TRUE;
        });

        SemanticCheckpoint checkpoint = context.step(
            "load-await-checkpoint",
            SemanticCheckpoint.class,
            () -> runtime.semanticCheckpoint(input.tenantId(), executionId, interactionId));
        if (!checkpoint.executionStatus().terminal()
            && !AwaitInteractionStatus.COMPLETED.name().equals(checkpoint.awaitStatus())) {
            DurableAwaitSignal signal = context.waitForCallback(
                "await-completion",
                DurableAwaitSignal.class,
                (providerCallbackId, ignored) -> {
                    AwaitBridge candidate = new AwaitBridge(
                        providerCallbackId, executionId, interactionId, correlationId);
                    if (!runtime.bindProviderCallback(
                        input.tenantId(), candidate, input.driverGeneration())) {
                        throw new IllegalStateException(
                            "Provider callback generation was superseded before it could be registered");
                    }
                    bridge.set(candidate);
                });
            if (!executionId.equals(signal.executionId())
                || !interactionId.equals(signal.interactionId())
                || !correlationId.equals(signal.correlationId())) {
                throw new IllegalStateException("Provider callback did not correlate to the TPF Await interaction");
            }
            context.step("observe-completed-await", Boolean.class, () -> {
                SemanticCheckpoint completed = runtime.semanticCheckpoint(
                    input.tenantId(), executionId, interactionId);
                if (!AwaitInteractionStatus.COMPLETED.name().equals(completed.awaitStatus())) {
                    throw new IllegalStateException("Provider wake-up preceded TPF Await admission");
                }
                return Boolean.TRUE;
            });
        }

        return context.step("resume-and-result", DurableDriverResult.class, () -> {
            SemanticCheckpoint current = runtime.semanticCheckpoint(
                input.tenantId(), executionId, interactionId);
            if (!current.executionStatus().terminal()) {
                if (current.executionStatus() != ExecutionStatus.QUEUED
                    || !AwaitInteractionStatus.COMPLETED.name().equals(current.awaitStatus())) {
                    throw new IllegalStateException("TPF checkpoint is not ready for resume");
                }
                invokeWorkUnchecked(runtime, executionId);
            }
            var status = runtime.controlPlane.getExecutionStatus(input.tenantId(), executionId)
                .await().atMost(Duration.ofSeconds(10));
            ExecutionRecord<Object, Object> execution = runtime.executionStore
                .getExecution(input.tenantId(), executionId)
                .await().atMost(Duration.ofSeconds(10)).orElseThrow();
            String result = runtime.controlPlane.<String>getExecutionResult(
                    input.tenantId(), executionId, String.class, false)
                .await().atMost(Duration.ofSeconds(10));
            Object rawResult = runtime.controlPlane.getExecutionResultPayload(input.tenantId(), executionId)
                .await().atMost(Duration.ofSeconds(10));
            if (!(rawResult instanceof SerializedTransitionPayload serialized)) {
                throw new IllegalStateException("Expected serialized TPF transition result");
            }
            return new DurableDriverResult(
                executionId,
                execution.pipelineId(),
                execution.contractVersion(),
                execution.releaseVersion(),
                status.status().name(),
                result,
                serialized.payloadTypeId());
        });
    }

    private LocalDurableTestRunner<DurableDriverInput, DurableDriverResult> durableRunner(
        RuntimeFixture runtime,
        AtomicReference<AwaitBridge> bridge
    ) {
        return LocalDurableTestRunner.create(
                DurableDriverInput.class,
                (driverInput, context) -> driveWithLambdaDurability(
                    driverInput, context, runtime, bridge))
            .withOutputType(DurableDriverResult.class);
    }

    private DurableAwaitChange awaitCompletionChange(String tenantId, String interactionId) {
        String shardIterator = dynamoStreams.describeStream(DescribeStreamRequest.builder()
                .streamArn(interactionStreamArn)
                .build())
            .streamDescription()
            .shards()
            .stream()
            .findFirst()
            .map(shard -> dynamoStreams.getShardIterator(GetShardIteratorRequest.builder()
                    .streamArn(interactionStreamArn)
                    .shardId(shard.shardId())
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                    .build())
                .shardIterator())
            .orElseThrow(() -> new IllegalStateException("Await interaction stream has no shard"));
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline && shardIterator != null) {
            var records = dynamoStreams.getRecords(GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .limit(100)
                .build());
            for (Record record : records.records()) {
                Map<String, AttributeValue> image = record.dynamodb().newImage();
                if (attributeEquals(image, "tenant_id", tenantId)
                    && attributeEquals(image, "interaction_id", interactionId)
                    && attributeEquals(image, "status", AwaitInteractionStatus.COMPLETED.name())
                    && image.containsKey(PROVIDER_CALLBACK_ID)
                    && image.containsKey(PROVIDER_DRIVER_GENERATION)) {
                    return new DurableAwaitChange(
                        tenantId,
                        image.get("execution_id").s(),
                        interactionId,
                        image.get("correlation_id").s(),
                        image.get(PROVIDER_CALLBACK_ID).s(),
                        Long.parseLong(image.get(PROVIDER_DRIVER_GENERATION).n()));
                }
            }
            shardIterator = records.nextShardIterator();
            try {
                Thread.sleep(25L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while reading Await completion stream", e);
            }
        }
        throw new IllegalStateException("No completed Await stream record for " + interactionId);
    }

    private static boolean attributeEquals(
        Map<String, AttributeValue> image,
        String attribute,
        String expected
    ) {
        return image.containsKey(attribute) && expected.equals(image.get(attribute).s());
    }

    private static void invokeWorkUnchecked(RuntimeFixture runtime, String executionId) {
        try {
            assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
                runtime.invokeWorkWithTransition(executionId).disposition());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to invoke a bounded TPF work action", e);
        }
    }

    private RuntimeFixture runtime(ScenarioQueues queues, WorkerOutcome outcome, int maxRetries) throws Exception {
        return new RuntimeFixture(queues, outcome, maxRetries);
    }

    private ScenarioQueues createQueues(String scenario) {
        String prefix = "tpf-" + scenario + "-" + suffix();
        return new ScenarioQueues(
            createQueue(prefix + "-work"),
            createQueue(prefix + "-await"),
            createQueue(prefix + "-transition-request"),
            createQueue(prefix + "-transition-response"),
            createQueue(prefix + "-dlq"));
    }

    private String createQueue(String name) {
        return sqsAdmin.createQueue(CreateQueueRequest.builder().queueName(name).build()).queueUrl();
    }

    private DynamoDbClient dynamoClient() {
        return DynamoDbClient.builder()
            .endpointOverride(LOCALSTACK.getEndpoint())
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
            .build();
    }

    private DynamoDbStreamsClient dynamoStreamsClient() {
        return DynamoDbStreamsClient.builder()
            .endpointOverride(LOCALSTACK.getEndpoint())
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
            .build();
    }

    private SqsClient sqsClient() {
        return SqsClient.builder()
            .endpointOverride(LOCALSTACK.getEndpoint())
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
            .build();
    }

    private void createTable(String name, String hash, Optional<String> range) {
        List<AttributeDefinition> attributes = new ArrayList<>();
        attributes.add(AttributeDefinition.builder().attributeName(hash).attributeType(ScalarAttributeType.S).build());
        List<KeySchemaElement> keys = new ArrayList<>();
        keys.add(KeySchemaElement.builder().attributeName(hash).keyType(KeyType.HASH).build());
        range.ifPresent(attribute -> {
            attributes.add(AttributeDefinition.builder().attributeName(attribute)
                .attributeType(ScalarAttributeType.S).build());
            keys.add(KeySchemaElement.builder().attributeName(attribute).keyType(KeyType.RANGE).build());
        });
        dynamoAdmin.createTable(CreateTableRequest.builder()
            .tableName(name)
            .attributeDefinitions(attributes)
            .keySchema(keys)
            .provisionedThroughput(ProvisionedThroughput.builder()
                .readCapacityUnits(10L)
                .writeCapacityUnits(10L)
                .build())
            .build());
        dynamoAdmin.waiter().waitUntilTableExists(request -> request.tableName(name));
    }

    private String createAwaitInteractionTable(String name) {
        ProvisionedThroughput throughput = ProvisionedThroughput.builder()
            .readCapacityUnits(10L)
            .writeCapacityUnits(10L)
            .build();
        dynamoAdmin.createTable(CreateTableRequest.builder()
            .tableName(name)
            .attributeDefinitions(
                AttributeDefinition.builder().attributeName("tenant_id")
                    .attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("interaction_id")
                    .attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("query_deadline_key")
                    .attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("query_deadline_sort")
                    .attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("query_continuation_key")
                    .attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("query_continuation_due_epoch_ms")
                    .attributeType(ScalarAttributeType.N).build(),
                AttributeDefinition.builder().attributeName("query_execution_key")
                    .attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("query_execution_sort")
                    .attributeType(ScalarAttributeType.S).build())
            .keySchema(
                KeySchemaElement.builder().attributeName("tenant_id").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("interaction_id").keyType(KeyType.RANGE).build())
            .globalSecondaryIndexes(
                GlobalSecondaryIndex.builder()
                    .indexName("await-interaction-pending-by-deadline")
                    .keySchema(
                        KeySchemaElement.builder().attributeName("query_deadline_key")
                            .keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("query_deadline_sort")
                            .keyType(KeyType.RANGE).build())
                    .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                    .provisionedThroughput(throughput)
                    .build(),
                GlobalSecondaryIndex.builder()
                    .indexName("await-interaction-continuation-work")
                    .keySchema(
                        KeySchemaElement.builder().attributeName("query_continuation_key")
                            .keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("query_continuation_due_epoch_ms")
                            .keyType(KeyType.RANGE).build())
                    .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                    .provisionedThroughput(throughput)
                    .build(),
                GlobalSecondaryIndex.builder()
                    .indexName("await-interaction-by-execution")
                    .keySchema(
                        KeySchemaElement.builder().attributeName("query_execution_key")
                            .keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("query_execution_sort")
                            .keyType(KeyType.RANGE).build())
                    .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                    .provisionedThroughput(throughput)
                    .build())
            .streamSpecification(StreamSpecification.builder()
                .streamEnabled(true)
                .streamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                .build())
            .provisionedThroughput(throughput)
            .build());
        dynamoAdmin.waiter().waitUntilTableExists(request -> request.tableName(name));
        return dynamoAdmin.describeTable(DescribeTableRequest.builder().tableName(name).build())
            .table().latestStreamArn();
    }

    private static String suffix() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static void markStartupHealthy(PipelineExecutionService service) throws Exception {
        Field field = PipelineExecutionService.class.getDeclaredField("startupHealthState");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<PipelineExecutionService.StartupHealthState> state =
            (AtomicReference<PipelineExecutionService.StartupHealthState>) field.get(service);
        state.set(PipelineExecutionService.StartupHealthState.HEALTHY);
    }

    private enum WorkerOutcome {
        WAIT,
        WAIT_THEN_COMPLETE,
        COMPLETE,
        FAIL
    }

    private record DurableDriverInput(String tenantId, String executionKey, long driverGeneration) {
    }

    private record AwaitBridge(
        String providerCallbackId,
        String executionId,
        String interactionId,
        String correlationId
    ) {
    }

    private record DurableAwaitSignal(
        String executionId,
        String interactionId,
        String correlationId
    ) {
    }

    private record DurableAwaitChange(
        String tenantId,
        String executionId,
        String interactionId,
        String correlationId,
        String providerCallbackId,
        long driverGeneration
    ) {
    }

    private record ProviderCallbackBinding(String providerCallbackId, long driverGeneration) {
    }

    private record SemanticCheckpoint(
        String executionId,
        ExecutionStatus executionStatus,
        int currentStepIndex,
        long executionVersion,
        boolean awaitPresent,
        String awaitStatus
    ) {
    }

    private record DurableDriverResult(
        String executionId,
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        String status,
        String result,
        String rawPayloadTypeId
    ) {
    }

    private enum WakeupDisposition {
        ACKNOWLEDGE,
        RETRY
    }

    private static final class DurableAwaitWakeupAction {
        private final RuntimeFixture runtime;
        private final AtomicInteger failuresRemaining;

        private DurableAwaitWakeupAction(RuntimeFixture runtime, int failuresBeforeSuccess) {
            this.runtime = runtime;
            this.failuresRemaining = new AtomicInteger(failuresBeforeSuccess);
        }

        private WakeupDisposition handle(
            DurableAwaitChange change,
            LocalDurableTestRunner<DurableDriverInput, DurableDriverResult> runner
        ) {
            ProviderCallbackBinding current = runtime.providerCallbackBinding(
                    change.tenantId(), change.interactionId())
                .orElseThrow(() -> new IllegalStateException("Await callback binding is missing"));
            if (!current.providerCallbackId().equals(change.providerCallbackId())
                || current.driverGeneration() != change.driverGeneration()) {
                return WakeupDisposition.ACKNOWLEDGE;
            }
            AwaitInteractionStatus awaitStatus = runtime.interactions
                .get(change.tenantId(), change.interactionId())
                .await().atMost(Duration.ofSeconds(10))
                .orElseThrow(() -> new IllegalStateException("Await interaction is missing"))
                .status();
            if (awaitStatus != AwaitInteractionStatus.COMPLETED) {
                return WakeupDisposition.RETRY;
            }
            if (runner.getOperation("await-completion-callback").isCompleted()) {
                return WakeupDisposition.ACKNOWLEDGE;
            }
            if (failuresRemaining.getAndUpdate(remaining -> Math.max(0, remaining - 1)) > 0) {
                return WakeupDisposition.RETRY;
            }
            try {
                runner.completeCallback(
                    change.providerCallbackId(),
                    PipelineJson.mapper().writeValueAsString(new DurableAwaitSignal(
                        change.executionId(), change.interactionId(), change.correlationId())));
                return WakeupDisposition.ACKNOWLEDGE;
            } catch (Exception failure) {
                return WakeupDisposition.RETRY;
            }
        }
    }

    private record HandledMessage(Message message, SqsMessageDisposition disposition) {
    }

    private final class ScenarioQueues implements AutoCloseable {
        private final String workQueueUrl;
        private final String awaitQueueUrl;
        private final String transitionRequestQueueUrl;
        private final String transitionResponseQueueUrl;
        private final String dlqQueueUrl;

        private ScenarioQueues(
            String workQueueUrl,
            String awaitQueueUrl,
            String transitionRequestQueueUrl,
            String transitionResponseQueueUrl,
            String dlqQueueUrl
        ) {
            this.workQueueUrl = workQueueUrl;
            this.awaitQueueUrl = awaitQueueUrl;
            this.transitionRequestQueueUrl = transitionRequestQueueUrl;
            this.transitionResponseQueueUrl = transitionResponseQueueUrl;
            this.dlqQueueUrl = dlqQueueUrl;
        }

        String workQueueUrl() {
            return workQueueUrl;
        }

        String awaitQueueUrl() {
            return awaitQueueUrl;
        }

        String transitionRequestQueueUrl() {
            return transitionRequestQueueUrl;
        }

        String transitionResponseQueueUrl() {
            return transitionResponseQueueUrl;
        }

        String dlqQueueUrl() {
            return dlqQueueUrl;
        }

        @Override
        public void close() {
            for (String queueUrl : List.of(
                workQueueUrl,
                awaitQueueUrl,
                transitionRequestQueueUrl,
                transitionResponseQueueUrl,
                dlqQueueUrl)) {
                sqsAdmin.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
            }
        }
    }

    private final class RuntimeFixture implements AutoCloseable {
        private final ScenarioQueues queues;
        private final SqsClient sqs;
        private final DynamoDbClient dynamo;
        private final PipelineOrchestratorConfig config;
        private final ExecutionStateStore executionStore;
        private final AwaitInteractionStore interactions;
        private final AwaitUnitStore units;
        private final DurableAwaitCoordinator awaitCoordinator;
        private final QueueAsyncCoordinator coordinator;
        private final ExecutionHooks workerHooks;
        private final PipelineControlPlane controlPlane;
        private final SqsWorkItemAction workAction;
        private final SqsTransitionWorkerAction transitionAction;
        private final SqsAwaitCompletionAction awaitAction;
        private final AtomicInteger transitionExecutions = new AtomicInteger();
        private final AtomicReference<AwaitSuspendedException> seededAwait = new AtomicReference<>();

        private RuntimeFixture(ScenarioQueues queues, WorkerOutcome outcome, int maxRetries) throws Exception {
            this.queues = queues;
            this.sqs = sqsClient();
            this.dynamo = dynamoClient();
            this.config = config(queues, maxRetries);
            this.executionStore = DynamoAwaitLifecycleTestStores.executionStore(dynamo, TABLE_PREFIX);
            this.interactions = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
                .interactionStore(dynamo, TABLE_PREFIX);
            this.units = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
                .unitStore(dynamo, TABLE_PREFIX);
            this.awaitCoordinator = new DurableAwaitCoordinator(interactions, units);

            SqsWorkDispatcher dispatcher = AwsShapedCoordinatorTestComponents.workDispatcher(sqs, config);
            DeadLetterPublisher deadLetter = AwsShapedCoordinatorTestComponents.deadLetterPublisher(sqs, config);
            this.coordinator = coordinator(config, executionStore, dispatcher, deadLetter, awaitCoordinator);
            LocalPipelineControlPlane localControlPlane = new LocalPipelineControlPlane();
            localControlPlane.queueAsyncCoordinator = coordinator;
            localControlPlane.initializeQueueMode();
            this.controlPlane = localControlPlane;

            SqsPipelineTransitionWorker transitionWorker =
                AwsShapedCoordinatorTestComponents.transitionWorker(sqs, config);
            PipelineTransitionWorkerSelector selector = mock(PipelineTransitionWorkerSelector.class);
            when(selector.select(any())).thenReturn(transitionWorker);
            PipelineExecutionService coordinatorService = new PipelineExecutionService();
            coordinatorService.controlPlane = controlPlane;
            coordinatorService.transitionWorkerSelector = selector;
            localControlPlane.pipelineExecutionService = coordinatorService;
            this.workAction = AwsShapedCoordinatorTestComponents.workItemAction(coordinatorService);
            this.awaitAction = AwsShapedAwaitTestComponents.completionAction(coordinatorService);

            PipelineExecutionService workerService = workerService(outcome);
            this.workerHooks = workerService.executionHooks;
            this.transitionAction = AwsShapedCoordinatorTestComponents.transitionWorkerAction(
                sqs, config, workerService);
        }

        private PipelineExecutionService workerService(WorkerOutcome outcome) throws Exception {
            PipelineExecutionService service = new PipelineExecutionService();
            PipelineRunner runner = mock(PipelineRunner.class);
            PipelineStepResolver stepResolver = mock(PipelineStepResolver.class);
            PipelineStepConfig stepConfig = mock(PipelineStepConfig.class);
            service.orchestratorConfig = config;
            service.pipelineRunner = runner;
            service.pipelineStepResolver = stepResolver;
            service.pipelineStepConfig = stepConfig;
            service.awaitCoordinator = awaitCoordinator;
            service.executionHooks = new ExecutionHooks();
            service.executionInputPolicy = new ExecutionInputPolicy();
            service.transitionPayloadCodec = new JsonTransitionPayloadCodec();
            service.releaseIdentityResolver = new PipelineReleaseIdentityResolver();
            when(stepResolver.loadPipelineSteps()).thenReturn(List.of(new Object(), new Object(), new Object()));
            PipelineRunContext telemetryContext = mock(PipelineRunContext.class);
            when(runner.runFromStepUntilWithContext(any(), any(), anyInt(), anyInt()))
                .thenAnswer(invocation -> {
                    int executionNumber = transitionExecutions.incrementAndGet();
                    return switch (outcome) {
                        case WAIT -> new PipelineRunner.ExecutionResult(
                            Multi.createFrom().failure(awaitSuspension()), telemetryContext);
                        case WAIT_THEN_COMPLETE -> executionNumber == 1
                            ? new PipelineRunner.ExecutionResult(
                                Multi.createFrom().failure(awaitSuspension()), telemetryContext)
                            : new PipelineRunner.ExecutionResult(
                                Multi.createFrom().item("approved-result"), telemetryContext);
                        case COMPLETE -> new PipelineRunner.ExecutionResult(
                            Multi.createFrom().item("approved-result"), telemetryContext);
                        case FAIL -> new PipelineRunner.ExecutionResult(
                            Multi.createFrom().failure(new IllegalStateException("worker failed")), telemetryContext);
                    };
                });
            markStartupHealthy(service);
            return service;
        }

        private AwaitSuspendedException awaitSuspension() {
            return Optional.ofNullable(seededAwait.get())
                .orElseThrow(() -> new IllegalStateException("await state was not seeded"));
        }

        private void seedScalarAwait(
            String tenantId,
            String executionId,
            String interactionId,
            String correlationId
        ) {
            long now = System.currentTimeMillis();
            long ttl = now / 1_000L + 3_600L;
            String unitId = "unit-" + executionId;
            units.importRecord(new AwaitUnitRecord(
                    tenantId,
                    unitId,
                    executionId,
                    "AwaitApproval",
                    1,
                    "ONE_TO_ONE",
                    1L,
                    AwaitUnitStatus.WAITING_EXTERNAL,
                    interactionId,
                    1,
                    0,
                    Set.of(),
                    true,
                    now,
                    now,
                    ttl))
                .await().atMost(Duration.ofSeconds(10));
            interactions.importRecord(new AwaitInteractionRecord(
                    tenantId,
                    executionId,
                    "AwaitApproval",
                    1,
                    String.class.getName(),
                    interactionId,
                    correlationId,
                    executionId + ":0:0",
                    "await-" + executionId,
                    1L,
                    AwaitInteractionStatus.DISPATCHED,
                    Map.of("request", "approval"),
                    null,
                    unitId,
                    null,
                    null,
                    null,
                    null,
                    "local-proof",
                    Map.of(),
                    now + 60_000L,
                    now,
                    now,
                    ttl))
                .await().atMost(Duration.ofSeconds(10));
            seededAwait.set(new AwaitSuspendedException(tenantId, executionId, unitId, 1));
        }

        private SemanticCheckpoint semanticCheckpoint(
            String tenantId,
            String executionId,
            String interactionId
        ) {
            ExecutionRecord<Object, Object> execution = executionStore.getExecution(tenantId, executionId)
                .await().atMost(Duration.ofSeconds(10))
                .orElseThrow(() -> new IllegalStateException("Execution checkpoint is missing"));
            Optional<AwaitInteractionRecord> interaction = interactions.get(tenantId, interactionId)
                .await().atMost(Duration.ofSeconds(10));
            return new SemanticCheckpoint(
                executionId,
                execution.status(),
                execution.currentStepIndex(),
                execution.version(),
                interaction.isPresent(),
                interaction.map(record -> record.status().name()).orElse(""));
        }

        private boolean bindProviderCallback(
            String tenantId,
            AwaitBridge bridge,
            long driverGeneration
        ) {
            try {
                dynamo.updateItem(UpdateItemRequest.builder()
                    .tableName(TABLE_PREFIX + "_interaction")
                    .key(Map.of(
                        "tenant_id", AttributeValue.fromS(tenantId),
                        "interaction_id", AttributeValue.fromS(bridge.interactionId())))
                    .updateExpression("SET #callback = :callback, #generation = :generation")
                    .conditionExpression("(#status = :dispatched OR #status = :completed) AND "
                        + "(attribute_not_exists(#generation) OR #generation < :generation OR "
                        + "(#generation = :generation AND #callback = :callback))")
                    .expressionAttributeNames(Map.of(
                        "#callback", PROVIDER_CALLBACK_ID,
                        "#generation", PROVIDER_DRIVER_GENERATION,
                        "#status", "status"))
                    .expressionAttributeValues(Map.of(
                        ":callback", AttributeValue.fromS(bridge.providerCallbackId()),
                        ":generation", AttributeValue.fromN(Long.toString(driverGeneration)),
                        ":dispatched", AttributeValue.fromS(AwaitInteractionStatus.DISPATCHED.name()),
                        ":completed", AttributeValue.fromS(AwaitInteractionStatus.COMPLETED.name())))
                    .build());
                return true;
            } catch (ConditionalCheckFailedException superseded) {
                return false;
            }
        }

        private Optional<ProviderCallbackBinding> providerCallbackBinding(
            String tenantId,
            String interactionId
        ) {
            Map<String, AttributeValue> item = dynamo.getItem(GetItemRequest.builder()
                    .tableName(TABLE_PREFIX + "_interaction")
                    .key(Map.of(
                        "tenant_id", AttributeValue.fromS(tenantId),
                        "interaction_id", AttributeValue.fromS(interactionId)))
                    .consistentRead(true)
                    .build())
                .item();
            if (!item.containsKey(PROVIDER_CALLBACK_ID)
                || !item.containsKey(PROVIDER_DRIVER_GENERATION)) {
                return Optional.empty();
            }
            return Optional.of(new ProviderCallbackBinding(
                item.get(PROVIDER_CALLBACK_ID).s(),
                Long.parseLong(item.get(PROVIDER_DRIVER_GENERATION).n())));
        }

        private void completeAwait(
            String tenantId,
            String executionId,
            String interactionId,
            String correlationId,
            String actor
        ) throws Exception {
            send(queues.awaitQueueUrl(), PipelineJson.mapper().writeValueAsString(
                new SqsAwaitCompletionEnvelope(
                    tenantId,
                    interactionId,
                    correlationId,
                    "",
                    "completion-" + executionId,
                    Map.of("decision", "approved"),
                    actor)));
            assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
                invokeNext(queues.awaitQueueUrl(), message ->
                    awaitAction.handle(message, Duration.ofSeconds(10))).disposition());
            assertEquals(ExecutionStatus.QUEUED,
                controlPlane.getExecutionStatus(tenantId, executionId)
                    .await().atMost(Duration.ofSeconds(10)).status());
            assertEquals(AwaitInteractionStatus.COMPLETED,
                interactions.get(tenantId, interactionId)
                    .await().atMost(Duration.ofSeconds(10)).orElseThrow().status());
        }

        private HandledMessage invokeWorkWithTransition(String executionId) throws Exception {
            Message work = take(queues.workQueueUrl());
            return invokeWorkMessageWithTransition(work, executionId);
        }

        private HandledMessage invokeWorkMessageWithTransition(Message work, String executionId) throws Exception {
            ExecutionWorkItem workItem = PipelineJson.mapper().readValue(work.body(), ExecutionWorkItem.class);
            assertEquals(executionId, workItem.executionId());
            int executionsBefore = transitionExecutions.get();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<HandledMessage> worker = executor.submit(() -> {
                    Message request = take(queues.transitionRequestQueueUrl());
                    assertPinnedTransition(request.body(), executionId);
                    return invokeMessage(
                        queues.transitionRequestQueueUrl(), request, transitionAction::handle);
                });
                HandledMessage handled = invokeMessage(queues.workQueueUrl(), work, workAction::handle);
                if (transitionExecutions.get() == executionsBefore && !worker.isDone()) {
                    worker.cancel(true);
                    ExecutionStatus status = controlPlane.getExecutionStatus(
                            workItem.tenantId(), workItem.executionId())
                        .await().atMost(Duration.ofSeconds(10)).status();
                    throw new AssertionError(
                        "work action did not invoke the transition worker: disposition="
                            + handled.disposition() + ", status=" + status);
                }
                assertEquals(SqsMessageDisposition.ACKNOWLEDGE,
                    worker.get(15, TimeUnit.SECONDS).disposition());
                return handled;
            } finally {
                executor.shutdownNow();
            }
        }

        private void assertPinnedTransition(String requestBody, String executionId) throws Exception {
            JsonNode request = PipelineJson.mapper().readTree(requestBody);
            TransitionCommandEnvelope command = PipelineJson.mapper().readValue(
                request.path("commandEnvelope").asText(), TransitionCommandEnvelope.class);
            assertEquals(executionId, command.executionId());
            assertEquals(PIPELINE_ID, command.pipelineId());
            assertEquals(CONTRACT_VERSION, command.contractVersion());
            assertEquals(RELEASE_VERSION, command.releaseVersion());
        }

        private HandledMessage invokeNext(
            String queueUrl,
            Function<SqsInboundMessage, Uni<SqsMessageDisposition>> action
        ) {
            return invokeMessage(queueUrl, take(queueUrl), action);
        }

        private HandledMessage invokeMessage(
            String queueUrl,
            Message message,
            Function<SqsInboundMessage, Uni<SqsMessageDisposition>> action
        ) {
            SqsMessageDisposition disposition = action.apply(SqsInboundMessage.from(message))
                .await().atMost(Duration.ofSeconds(15));
            if (disposition == SqsMessageDisposition.ACKNOWLEDGE) {
                delete(queueUrl, message);
            }
            return new HandledMessage(message, disposition);
        }

        private Message take(String queueUrl) {
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            do {
                Optional<Message> message = receive(queueUrl, 2, 1).stream().findFirst();
                if (message.isPresent()) {
                    return message.orElseThrow();
                }
            } while (System.nanoTime() < deadline);
            throw new IllegalStateException("No message available on " + queueUrl);
        }

        private List<Message> receive(String queueUrl, int waitSeconds, int limit) {
            return sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .waitTimeSeconds(waitSeconds)
                    .visibilityTimeout(20)
                    .maxNumberOfMessages(limit)
                    .build())
                .messages();
        }

        private void send(String queueUrl, String body) {
            sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody(body).build());
        }

        private void delete(String queueUrl, Message message) {
            assertNotNull(message.receiptHandle());
            sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build());
        }

        @Override
        public void close() {
            workerHooks.shutdownKillSwitchExecutor();
            dynamo.close();
            sqs.close();
        }
    }

    private static QueueAsyncCoordinator coordinator(
        PipelineOrchestratorConfig config,
        ExecutionStateStore store,
        WorkDispatcher dispatcher,
        DeadLetterPublisher deadLetterPublisher,
        AwaitCoordinator awaitCoordinator
    ) {
        ExecutionInputPolicy inputPolicy = new ExecutionInputPolicy();
        inputPolicy.orchestratorConfig = config;
        ExecutionFailureHandler failureHandler = new ExecutionFailureHandler();
        failureHandler.orchestratorConfig = config;
        ExecutionResultShapeResolver shapeResolver = mock(ExecutionResultShapeResolver.class);
        when(shapeResolver.resolve()).thenReturn(ExecutionResultShape.SINGLE);
        AwaitLiveCompletionRegistry liveCompletions = mock(AwaitLiveCompletionRegistry.class);
        lenient().when(liveCompletions.signal(any())).thenReturn(Uni.createFrom().item(false));
        CheckpointPublicationService checkpoints = mock(CheckpointPublicationService.class);
        ObjectPublishCompletionService objectPublish = mock(ObjectPublishCompletionService.class);
        lenient().when(checkpoints.enabled()).thenReturn(false);
        lenient().when(objectPublish.enabled()).thenReturn(false);

        QueueAsyncCoordinator coordinator = new QueueAsyncCoordinator();
        coordinator.orchestratorConfig = config;
        coordinator.executionInputPolicy = inputPolicy;
        coordinator.executionFailureHandler = failureHandler;
        coordinator.executionResultShapeResolver = shapeResolver;
        coordinator.checkpointPublicationService = checkpoints;
        coordinator.objectPublishCompletionService = objectPublish;
        coordinator.awaitCoordinator = awaitCoordinator;
        coordinator.awaitLiveCompletionRegistry = liveCompletions;
        coordinator.transitionWorkerExecutor = new org.pipelineframework.orchestrator.TransitionWorkerExecutor(
            config, new PipelineInvocationRuntime());
        coordinator.transitionPayloadCodec = new JsonTransitionPayloadCodec();
        coordinator.releaseIdentityResolver = new PipelineReleaseIdentityResolver();
        coordinator.controlPlaneAdmissionPolicy = new LocalControlPlaneAdmissionPolicy(config);
        coordinator.segmentBoundaryLedger = new SegmentBoundaryLedger(new InMemoryControlPlaneJournal());
        coordinator.executionStateStore = store;
        coordinator.workDispatcher = dispatcher;
        coordinator.deadLetterPublisher = deadLetterPublisher;
        return coordinator;
    }

    private static PipelineOrchestratorConfig config(ScenarioQueues queues, int maxRetries) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.SqsConfig sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        PipelineOrchestratorConfig.SqsWorkerConfig workerSqs = mock(PipelineOrchestratorConfig.SqsWorkerConfig.class);
        PipelineOrchestratorConfig.WorkerConfig worker = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        lenient().when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        lenient().when(config.defaultTenant()).thenReturn("default");
        lenient().when(config.pipelineId()).thenReturn(PIPELINE_ID);
        lenient().when(config.releaseVersion()).thenReturn(RELEASE_VERSION);
        lenient().when(config.executionTtlDays()).thenReturn(1);
        lenient().when(config.leaseMs()).thenReturn(5_000L);
        lenient().when(config.maxRetries()).thenReturn(maxRetries);
        lenient().when(config.maxCircuitDeferral()).thenReturn(Optional.empty());
        lenient().when(config.retryDelay()).thenReturn(Duration.ofMillis(10));
        lenient().when(config.retryMultiplier()).thenReturn(1.0d);
        lenient().when(config.sweepInterval()).thenReturn(Duration.ofSeconds(30));
        lenient().when(config.sweepLimit()).thenReturn(7);
        lenient().when(config.idempotencyPolicy()).thenReturn(OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY);
        lenient().when(config.stateProvider()).thenReturn("");
        lenient().when(config.dispatcherProvider()).thenReturn("");
        lenient().when(config.dlqProvider()).thenReturn("");
        lenient().when(config.strictStartup()).thenReturn(false);
        lenient().when(config.queueUrl()).thenReturn(Optional.of(queues.workQueueUrl()));
        lenient().when(config.dlqUrl()).thenReturn(Optional.of(queues.dlqQueueUrl()));
        lenient().when(config.sqs()).thenReturn(sqs);
        lenient().when(sqs.localLoopback()).thenReturn(false);
        lenient().when(sqs.region()).thenReturn(Optional.empty());
        lenient().when(sqs.endpointOverride()).thenReturn(Optional.empty());
        lenient().when(config.workerSqs()).thenReturn(workerSqs);
        lenient().when(workerSqs.requestQueueUrl()).thenReturn(Optional.of(queues.transitionRequestQueueUrl()));
        lenient().when(workerSqs.responseQueueUrl()).thenReturn(Optional.of(queues.transitionResponseQueueUrl()));
        lenient().when(workerSqs.pipelineId()).thenReturn(Optional.of(PIPELINE_ID));
        lenient().when(workerSqs.contractVersion()).thenReturn(Optional.of(CONTRACT_VERSION));
        lenient().when(workerSqs.releaseVersion()).thenReturn(Optional.of(RELEASE_VERSION));
        lenient().when(workerSqs.artifactId()).thenReturn(Optional.empty());
        lenient().when(workerSqs.artifactDigest()).thenReturn(Optional.empty());
        lenient().when(workerSqs.serverEnabled()).thenReturn(true);
        lenient().when(workerSqs.requestTimeout()).thenReturn(Duration.ofSeconds(10));
        lenient().when(workerSqs.visibilityTimeout()).thenReturn(Duration.ofSeconds(20));
        lenient().when(workerSqs.signatureTolerance()).thenReturn(Duration.ofMinutes(2));
        lenient().when(workerSqs.sharedSecret()).thenReturn(Optional.of(WORKER_SECRET));
        lenient().when(workerSqs.sharedSecretRef()).thenReturn(Optional.empty());
        lenient().when(config.worker()).thenReturn(worker);
        lenient().when(worker.executionMode()).thenReturn(TransitionWorkerExecutionMode.SAME_THREAD);
        lenient().when(worker.maxInFlight()).thenReturn(64);
        lenient().when(worker.saturatedDelay()).thenReturn(Duration.ofMillis(25));
        lenient().when(worker.allowedPayloadPrefixes()).thenReturn(List.of("org.pipelineframework.", "java.lang."));
        lenient().when(worker.artifactId()).thenReturn(Optional.empty());
        lenient().when(worker.artifactDigest()).thenReturn(Optional.empty());
        lenient().when(worker.sqs()).thenReturn(workerSqs);
        return config;
    }

    private static final class DurableAwaitCoordinator extends AwaitCoordinator {
        private final AwaitInteractionStore interactions;
        private final AwaitUnitStore units;

        private DurableAwaitCoordinator(AwaitInteractionStore interactions, AwaitUnitStore units) {
            this.interactions = interactions;
            this.units = units;
        }

        @Override
        public Uni<Void> importSuspension(TransitionAwaitSuspension suspension) {
            return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<TransitionAwaitSuspension> suspensionSnapshot(AwaitSuspendedException suspended) {
            return Uni.createFrom().item(new TransitionAwaitSuspension(
                suspended.tenantId(),
                suspended.executionId(),
                suspended.unitId(),
                suspended.stepIndex()));
        }

        @Override
        public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command) {
            return interactions.complete(command);
        }

        @Override
        public Uni<AwaitUnitRecord> recordCompletion(AwaitInteractionRecord record, long nowEpochMs) {
            return units.markCompleted(record.tenantId(), record.unitId(), nowEpochMs)
                .map(updated -> updated.orElseThrow(() ->
                    new IllegalStateException("Await unit completion lost OCC race")));
        }

        @Override
        public Uni<AwaitUnitRecord> getUnit(String tenantId, String unitId) {
            return units.get(tenantId, unitId)
                .map(record -> record.orElseThrow(() -> new IllegalStateException("Await unit not found")));
        }

        @Override
        public Uni<Object> loadResumePayload(String tenantId, String unitId) {
            return getUnit(tenantId, unitId)
                .onItem().transformToUni(unit -> interactions.get(tenantId, unit.primaryInteractionId()))
                .map(record -> record.orElseThrow(() ->
                    new IllegalStateException("Await interaction not found for unit " + unitId)))
                .map(AwaitInteractionRecord::responsePayload);
        }

        @Override
        public Uni<List<AwaitInteractionRecord>> findByUnit(String tenantId, String unitId) {
            return interactions.findByUnit(tenantId, unitId);
        }

        @Override
        public Uni<List<AwaitInteractionRecord>> queryPending(
            String tenantId,
            String assignee,
            String group,
            String stepId,
            int limit
        ) {
            return interactions.queryPending(tenantId, assignee, group, stepId, limit);
        }

        @Override
        public Uni<List<AwaitInteractionRecord>> findTimedOut(long nowEpochMs, int limit) {
            return interactions.findTimedOut(nowEpochMs, limit);
        }

        @Override
        public Uni<Optional<AwaitInteractionRecord>> markTimedOut(
            AwaitInteractionRecord record,
            long nowEpochMs
        ) {
            return interactions.markTimedOut(
                record.tenantId(), record.interactionId(), record.version(), nowEpochMs);
        }

        @Override
        public Uni<List<org.pipelineframework.orchestrator.AwaitItemContinuationCommand>>
                findDueItemContinuations(long nowEpochMs, int limit) {
            return ((org.pipelineframework.awaitable.store.AwaitItemContinuationWorkStore) interactions)
                .findDueItemContinuations(nowEpochMs, limit);
        }

        @Override
        public Uni<Void> rescheduleItemContinuation(
            org.pipelineframework.orchestrator.AwaitItemContinuationCommand command,
            int nextAttempt,
            long dueEpochMs
        ) {
            return ((org.pipelineframework.awaitable.store.AwaitItemContinuationWorkStore) interactions)
                .rescheduleItemContinuation(command, nextAttempt, dueEpochMs);
        }

        @Override
        public Uni<Void> completeItemContinuation(
            org.pipelineframework.orchestrator.AwaitItemContinuationCommand command
        ) {
            return ((org.pipelineframework.awaitable.store.AwaitItemContinuationWorkStore) interactions)
                .completeItemContinuation(command);
        }
    }
}

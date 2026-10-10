package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.controlplane.*;
import org.pipelineframework.orchestrator.release.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;

/** The existing bounded native sweep repairs a lost first enqueue without creating another root. */
@Testcontainers(disabledWithoutDocker = true)
class NativeExecutionAdmissionSweepIT {
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8")).withServices("dynamodb", "sqs");

    @Test
    void lostFirstEnqueueFreshInquiryAndNativeSweepPreserveOriginalUuidAndAllDurableRows() throws Exception {
        var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()));
        try (var dynamo = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint()).region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(credentials).build();
             var sqs = SqsClient.builder().endpointOverride(LOCALSTACK.getEndpoint()).region(Region.of(LOCALSTACK.getRegion()))
                 .credentialsProvider(credentials).build()) {
            String prefix = "native_admission_" + UUID.randomUUID().toString().replace("-", "");
            table(dynamo, prefix + "_execution", "tenant_id", "execution_id");
            table(dynamo, prefix + "_execution_key", "tenant_execution_key", null);
            table(dynamo, prefix + "_execution_payload", "payload_id", "payload_part");
            String queue = sqs.createQueue(request -> request.queueName(prefix)).queueUrl();
            var config = mock(PipelineOrchestratorConfig.class);
            when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
            when(config.idempotencyPolicy()).thenReturn(OrchestratorIdempotencyPolicy.CLIENT_KEY_REQUIRED);
            when(config.executionTtlDays()).thenReturn(1);
            when(config.sweepLimit()).thenReturn(10);
            when(config.queueUrl()).thenReturn(Optional.of(queue));
            var sqsConfig = mock(PipelineOrchestratorConfig.SqsConfig.class);
            when(config.sqs()).thenReturn(sqsConfig);
            when(sqsConfig.localLoopback()).thenReturn(false);
            var state = (DynamoExecutionStateStore) DynamoAwaitLifecycleTestStores.executionStore(dynamo, prefix);
            var input = new ExecutionInputPolicy();
            input.orchestratorConfig = config;
            var shape = mock(ExecutionResultShapeResolver.class);
            when(shape.resolve()).thenReturn(ExecutionResultShape.SINGLE);
            var journal = new InMemoryControlPlaneJournal();
            var ledger = new SegmentBoundaryLedger(journal);
            var attempts = new AtomicInteger();
            var lost = mock(WorkDispatcher.class);
            when(lost.enqueueNow(any())).thenAnswer(call -> {
                attempts.incrementAndGet();
                return io.smallrye.mutiny.Uni.createFrom().failure(new IllegalStateException("first enqueue lost before send"));
            });
            var activationCalls = new AtomicInteger();
            var flow = new QueueAsyncSubmissionFlow(config, input, shape, state, lost,
                new LocalControlPlaneAdmissionPolicy(config), () -> "pipeline", () -> "contract", () -> "release",
                () -> ledger, submission -> {
                    activationCalls.incrementAndGet();
                    return io.smallrye.mutiny.Uni.createFrom().failure(new IllegalStateException("admission must not activate"));
                }, submission -> io.smallrye.mutiny.Uni.createFrom().item(Optional.empty()));
            var evidence = ExecutionAdmissionReleaseEvidence.snapshot(new PipelineReleaseRecord("tenant", "pipeline", "contract", "release",
                PipelineReleaseStatus.REGISTERED, new PipelineReleaseDescriptor(1, "pipeline", "contract", "release", "artifact", List.of()),
                "artifact", "sha256:artifact", "file:/artifact.jar", 1, "artifact", null, 1, 1, 0));
            var intent = new ExecutionAdmissionIntent(1, "tenant", "pipeline", "client-key", "contract", "release", "UNI",
                "java.lang.String", JsonTransitionPayloadCodec.ENCODING, "\"request\"".getBytes(java.nio.charset.StandardCharsets.UTF_8), false);
            assertThrows(IllegalStateException.class, () -> flow.admit(io.smallrye.mutiny.Uni.createFrom().item("request"), intent, evidence, state)
                .await().indefinitely());
            assertEquals(1, attempts.get());
            assertEquals(0, activationCalls.get());
            assertTrue(sqs.receiveMessage(request -> request.queueUrl(queue).waitTimeSeconds(0)).messages().isEmpty());
            var replacement = (DynamoExecutionStateStore) DynamoAwaitLifecycleTestStores.executionStore(dynamo, prefix);
            var receipt = replacement.lookupAdmission("tenant", "pipeline", "client-key").await().indefinitely().orElseThrow();
            var execution = replacement.getExecution("tenant", receipt.executionId()).await().indefinitely().orElseThrow();
            assertEquals(ExecutionStatus.QUEUED, execution.status());
            assertEquals(0, execution.version());
            assertEquals(0, execution.attempt());
            assertNull(execution.leaseOwner());
            assertEquals("pipeline", execution.pipelineId());
            assertEquals("contract", execution.contractVersion());
            assertEquals("release", execution.releaseVersion());
            var rows = rows(dynamo, prefix);
            var facts = journal.projection("tenant", receipt.executionId()).await().indefinitely();
            assertEquals(receipt, replacement.inspectExistingAdmission(intent).await().indefinitely().orElseThrow().receipt());
            assertFalse(replacement.inspectExistingAdmission(intent).await().indefinitely().orElseThrow().newlyCreated());
            assertEquals(rows, rows(dynamo, prefix));
            assertEquals(facts, journal.projection("tenant", receipt.executionId()).await().indefinitely());
            assertEquals(1, attempts.get());
            assertTrue(sqs.receiveMessage(request -> request.queueUrl(queue).waitTimeSeconds(0)).messages().isEmpty());
            var dispatcher = AwsShapedCoordinatorTestComponents.workDispatcher(sqs, config);
            var awaits = mock(AwaitCoordinator.class);
            when(awaits.findTimedOut(anyLong(), anyInt())).thenReturn(io.smallrye.mutiny.Uni.createFrom().item(List.of()));
            var nativeSweep = new QueueAsyncSweepFlow(config, replacement, dispatcher, new AwaitTimeoutFlow(awaits, replacement, () -> ledger));
            long now = System.currentTimeMillis();
            assertTrue(now >= execution.nextDueEpochMs(), "wall clock moved behind the actual queued due timestamp");
            var swept = nativeSweep.sweepOnce(now).await().indefinitely();
            assertEquals(1, swept.dispatchedExecutionCount());
            var messages = sqs.receiveMessage(request -> request.queueUrl(queue).maxNumberOfMessages(10).waitTimeSeconds(0)).messages();
            assertEquals(1, messages.size());
            var work = PipelineJson.mapper().readValue(messages.getFirst().body(), ExecutionWorkItem.class);
            assertEquals(new ExecutionWorkItem("tenant", receipt.executionId()), work);
            assertEquals(rows, rows(dynamo, prefix));
            assertEquals(facts, journal.projection("tenant", receipt.executionId()).await().indefinitely());
            assertEquals(0, activationCalls.get());
            sqs.deleteQueue(request -> request.queueUrl(queue));
        }
    }

    private static Map<String, Set<Map<String, AttributeValue>>> rows(DynamoDbClient client, String prefix) {
        return Map.of("execution", scan(client, prefix + "_execution"), "key", scan(client, prefix + "_execution_key"),
            "payload", scan(client, prefix + "_execution_payload"));
    }
    private static Set<Map<String, AttributeValue>> scan(DynamoDbClient client, String table) {
        return Set.copyOf(client.scan(request -> request.tableName(table).consistentRead(true)).items());
    }
    private static void table(DynamoDbClient client, String table, String hash, String range) {
        var attributes = new java.util.ArrayList<AttributeDefinition>();
        var schema = new java.util.ArrayList<KeySchemaElement>();
        attributes.add(AttributeDefinition.builder().attributeName(hash).attributeType(ScalarAttributeType.S).build());
        schema.add(KeySchemaElement.builder().attributeName(hash).keyType(KeyType.HASH).build());
        if (range != null) {
            attributes.add(AttributeDefinition.builder().attributeName(range).attributeType(ScalarAttributeType.S).build());
            schema.add(KeySchemaElement.builder().attributeName(range).keyType(KeyType.RANGE).build());
        }
        client.createTable(request -> request.tableName(table).attributeDefinitions(attributes).keySchema(schema)
            .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build()));
        client.waiter().waitUntilTableExists(request -> request.tableName(table));
    }
}

package org.pipelineframework.awsproof;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionNames;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionOutput;
import org.pipelineframework.aws.durable.model.AwsDurableStartResponse;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.DescribeStacksRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.Execution;
import software.amazon.awssdk.services.lambda.model.ExecutionStatus;
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionRequest;
import software.amazon.awssdk.services.lambda.model.GetEventSourceMappingRequest;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.ListDurableExecutionsByFunctionRequest;
import software.amazon.awssdk.services.lambda.model.PublishVersionRequest;
import software.amazon.awssdk.services.lambda.model.StopDurableExecutionRequest;
import software.amazon.awssdk.services.lambda.model.UpdateAliasRequest;
import software.amazon.awssdk.services.lambda.model.UpdateEventSourceMappingRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;
import software.amazon.awssdk.services.sts.StsClient;

import static org.assertj.core.api.Assertions.assertThat;

/** Opt-in real-AWS gate. It is inert unless the runner supplies tpf.proof.stack. */
class DeployedAwsDurableProofIT {
    private static final Duration FLOW_TIMEOUT = Duration.ofMinutes(5);
    private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();
    private static final List<Evidence> EVIDENCE = new CopyOnWriteArrayList<>();
    private static final Map<String, String> START_TENANTS = new ConcurrentHashMap<>();

    private static ProofStack stack;
    private static LambdaClient lambda;
    private static DynamoDbClient dynamo;
    private static SqsClient sqs;

    @BeforeAll
    static void connect() {
        String stackName = System.getProperty("tpf.proof.stack", "");
        Assumptions.assumeTrue(!stackName.isBlank(), "deployed proof stack was not requested");
        Region region = Region.of(System.getProperty("tpf.proof.region", "us-east-2"));
        try (StsClient sts = StsClient.builder().region(region).build()) {
            String arn = sts.getCallerIdentity().arn();
            assertThat(arn).as("deployed proof must never run with account-root credentials")
                .doesNotEndWith(":root");
        }
        try (CloudFormationClient cloudFormation = CloudFormationClient.builder().region(region).build()) {
            stack = ProofStack.load(cloudFormation, stackName);
        }
        lambda = LambdaClient.builder().region(region).build();
        dynamo = DynamoDbClient.builder().region(region).build();
        sqs = SqsClient.builder().region(region).build();
    }

    @AfterAll
    static void closeAndReport() throws Exception {
        if (lambda != null) {
            lambda.close();
        }
        if (dynamo != null) {
            dynamo.close();
        }
        if (sqs != null) {
            sqs.close();
        }
        String reportPath = System.getProperty("tpf.proof.report", "");
        if (!reportPath.isBlank() && stack != null) {
            Map<String, Object> report = new LinkedHashMap<>();
            report.put("schemaVersion", 1);
            report.put("issue", 976);
            report.put("stack", stack.name());
            report.put("region", System.getProperty("tpf.proof.region", "us-east-2"));
            report.put("generatedAt", Instant.now().toString());
            report.put("evidence", List.copyOf(EVIDENCE));
            report.put("scenarioCatalog", ProofFaultScenarioCatalog.all());
            Files.writeString(Path.of(reportPath), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        }
    }

    @Test
    void repeatedIngressAndSubmitUncertaintyPreserveOneTpfExecution() throws Exception {
        evidence("idempotent-ingress", () -> {
            String suffix = suffix();
            AwsDurableExecutionInput input = input(suffix, 1);
            String executionName = AwsDurableExecutionNames.durableExecutionName(
                input.tenantId(), input.idempotencyKey(), input.generation());
            arm("ingress-after-provider-start", executionName);
            arm("submit-before-tpf-commit", "*");
            arm("submit-after-tpf-commit", "*");
            var lostResponse = invokeRaw(stack.output("IngressFunctionName"), input);
            assertThat(lostResponse.functionError()).isNotBlank();
            AwsDurableStartResponse second = start(input);

            assertThat(second.durableExecutionName()).isEqualTo(executionName);
            AwaitRequest await = awaitRequest(second);
            assertThat(await.tenantId()).isEqualTo(input.tenantId());
            assertThat(executionCount(input.tenantId())).isEqualTo(1);
            complete(await, "approval-" + suffix);
            assertSucceeded(execution(executionName));
        });
    }

    @Test
    void duplicateWorkAndTransitionDeliveryDoNotRepeatTheAwaitTransition() throws Exception {
        evidence("worker-redelivery", () -> {
            setQueueMapping(stack.output("WorkQueueMappingId"), false);
            try {
                AwsDurableExecutionInput input = input(suffix(), 1);
                AwsDurableStartResponse start = start(input);
                String workBody = receiveBody(stack.output("WorkQueueUrl"), false);
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(stack.output("WorkQueueUrl"))
                    .messageBody(workBody).build());
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(stack.output("WorkQueueUrl"))
                    .messageBody(workBody).build());
                arm("work-before-action", "*");
                arm("work-after-action", "*");
                setQueueMapping(stack.output("WorkQueueMappingId"), true);
                AwaitRequest await = awaitRequest(start);
                assertThat(interactionCount(await.tenantId(), await.executionId())).isEqualTo(1);
                complete(await, "duplicate-work");
                assertSucceeded(execution(start.durableExecutionName()));
            } finally {
                setQueueMapping(stack.output("WorkQueueMappingId"), true);
            }
        });

        evidence("transition-redelivery", () -> {
            setQueueMapping(stack.output("TransitionQueueMappingId"), false);
            try {
                AwsDurableStartResponse start = start(input(suffix(), 1));
                String transition = receiveBody(stack.output("TransitionRequestQueueUrl"), false);
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(stack.output("TransitionRequestQueueUrl"))
                    .messageBody(transition).build());
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(stack.output("TransitionRequestQueueUrl"))
                    .messageBody(transition).build());
                arm("transition-before-action", "*");
                arm("transition-after-action", "*");
                setQueueMapping(stack.output("TransitionQueueMappingId"), true);
                AwaitRequest await = awaitRequest(start);
                assertThat(interactionCount(await.tenantId(), await.executionId())).isEqualTo(1);
                complete(await, "duplicate-transition");
                assertSucceeded(execution(start.durableExecutionName()));
            } finally {
                setQueueMapping(stack.output("TransitionQueueMappingId"), true);
            }
        });
    }

    @Test
    void bothBindingOrdersAndDelayedStreamsConverge() throws Exception {
        evidence("binding-races", () -> {
            for (String point : List.of("bind-before-provider-binding", "bind-after-provider-binding")) {
                arm(point, "*");
                AwsDurableStartResponse start = start(input(suffix(), 1));
                AwaitRequest await = awaitRequest(start);
                complete(await, point);
                waitFor(() -> binding(await, 1), FLOW_TIMEOUT);
                assertSucceeded(execution(start.durableExecutionName()));
            }
        });

        evidence("stream-reconciliation", () -> {
            setMapping(stack.output("AwaitStreamMappingId"), false);
            setMapping(stack.output("BindingStreamMappingId"), false);
            try {
                AwsDurableStartResponse start = start(input(suffix(), 1));
                AwaitRequest await = awaitRequest(start);
                complete(await, "completion-with-streams-disabled");
                invoke(stack.output("ReconcilerFunctionName"), Map.of());
                assertSucceeded(execution(start.durableExecutionName()));
            } finally {
                setMapping(stack.output("AwaitStreamMappingId"), true);
                setMapping(stack.output("BindingStreamMappingId"), true);
            }
        });
    }

    @Test
    void callbackCrashesAndDuplicateCompletionAreRecoveredWithoutDoubleAdmission() throws Exception {
        evidence("callback-uncertainty", () -> {
            for (String point : List.of(
                "callback-api-throttled",
                "wakeup-before-provider-callback",
                "wakeup-after-provider-callback")) {
                AwsDurableStartResponse start = start(input(suffix(), 1));
                AwaitRequest await = awaitRequest(start);
                arm(point, await.executionId());
                complete(await, point);
                complete(await, point);
                assertSucceeded(execution(start.durableExecutionName()));
                assertThat(deliveryEvidence(await, 1)).isPresent();
            }
        });
    }

    @Test
    void randomizedCallbackAndBindingRacesConverge() throws Exception {
        long seed = Long.getLong("tpf.proof.random-seed", System.currentTimeMillis());
        int repetitions = Integer.getInteger("tpf.proof.random-repetitions", 10);
        assertThat(repetitions).isBetween(1, 100);
        evidence("callback-races-randomized-seed-" + seed, () -> {
            Random random = new Random(seed);
            List<String> bindingFaults = List.of(
                "bind-before-provider-binding",
                "bind-after-provider-binding");
            List<String> callbackFaults = List.of(
                "callback-api-throttled",
                "wakeup-before-provider-callback",
                "wakeup-after-provider-callback");
            for (int repetition = 0; repetition < repetitions; repetition++) {
                String suffix = "random-" + repetition + "-" + suffix();
                AwsDurableExecutionInput input = input(suffix, 1);
                arm(bindingFaults.get(random.nextInt(bindingFaults.size())), "*");
                AwsDurableStartResponse start = start(input);
                AwaitRequest await = awaitRequest(start);
                if (random.nextBoolean()) {
                    waitFor(() -> binding(await, 1), FLOW_TIMEOUT);
                }
                arm(callbackFaults.get(random.nextInt(callbackFaults.size())), await.executionId());
                int duplicateDeliveries = 1 + random.nextInt(3);
                for (int delivery = 0; delivery < duplicateDeliveries; delivery++) {
                    complete(await, "randomized-" + repetition);
                }
                if (random.nextBoolean()) {
                    invoke(stack.output("ReconcilerFunctionName"), Map.of());
                }
                var terminal = assertSucceeded(execution(start.durableExecutionName()));
                assertTerminalAwaitResult(terminal, await.executionId(), 1);
                assertThat(executionCount(input.tenantId())).isEqualTo(1);
                assertThat(interactionCount(await.tenantId(), await.executionId())).isEqualTo(1);
                assertThat(deliveryEvidence(await, 1)).isPresent();
            }
        });
    }

    @Test
    void expiredProviderCallbackStartsAReplacementGeneration() throws Exception {
        evidence("callback-expiry-recovery", () -> {
            String suffix = "callback-expiry-" + suffix();
            AwsDurableExecutionInput input = new AwsDurableExecutionInput(
                "tenant-" + suffix,
                "execution-key-" + suffix,
                "aws-durable-proof",
                "1",
                "proof-release-1",
                "{\"request\":\"" + suffix + "\",\"callbackExpiryProbe\":true}",
                Optional.empty(),
                1);
            AwsDurableStartResponse first = start(input);
            AwaitRequest await = awaitRequest(first);
            Execution firstExecution = execution(first.durableExecutionName());
            waitFor(() -> lambda.getDurableExecution(GetDurableExecutionRequest.builder()
                    .durableExecutionArn(firstExecution.durableExecutionArn()).build()).status()
                    == ExecutionStatus.FAILED
                ? Optional.of(Boolean.TRUE) : Optional.empty(), FLOW_TIMEOUT);

            invoke(stack.output("ReconcilerFunctionName"), Map.of(
                "detail", Map.of(
                    "status", "FAILED",
                    "durableExecutionArn", firstExecution.durableExecutionArn())));
            waitFor(() -> binding(await, 2), FLOW_TIMEOUT);
            complete(await, "callback-expiry-replacement");
            String replacementName = AwsDurableExecutionNames.durableExecutionName(
                await.tenantId(), await.executionId(), 2);
            assertSucceeded(execution(replacementName));
        });
    }

    @Test
    void generationFenceAndHistoryReconstructionRejectStaleBindings() throws Exception {
        evidence("generation-fence", () -> {
            String suffix = suffix();
            AwsDurableExecutionInput firstInput = input(suffix, 1);
            AwsDurableStartResponse first = start(firstInput);
            AwaitRequest await = awaitRequest(first);
            waitFor(() -> binding(await, 1), FLOW_TIMEOUT);
            AwsDurableStartResponse replacement = start(input(suffix, 2));
            waitFor(() -> binding(await, 2), FLOW_TIMEOUT);
            complete(await, "new-generation");
            assertSucceeded(execution(replacement.durableExecutionName()));
            assertThat(execution(first.durableExecutionName()).status()).isEqualTo(ExecutionStatus.RUNNING);
            lambda.stopDurableExecution(StopDurableExecutionRequest.builder()
                .durableExecutionArn(execution(first.durableExecutionName()).durableExecutionArn())
                .build());
        });

        evidence("binding-reconstruction", () -> {
            AwsDurableStartResponse start = start(input(suffix(), 1));
            AwaitRequest await = awaitRequest(start);
            Map<String, AttributeValue> binding = waitFor(() -> binding(await, 1), FLOW_TIMEOUT);
            dynamo.deleteItem(DeleteItemRequest.builder().tableName(stack.output("BindingTableName"))
                .key(Map.of("pk", binding.get("pk"), "sk", binding.get("sk"))).build());
            invoke(stack.output("ReconcilerFunctionName"), Map.of());
            waitFor(() -> binding(await, 1), FLOW_TIMEOUT);
            complete(await, "reconstructed-binding");
            assertSucceeded(execution(start.durableExecutionName()));
        });
    }

    @Test
    void mixedStreamBatchAndClosedCallbacksHaveDeterministicDisposition() throws Exception {
        evidence("partial-stream-batch", () -> {
            AwsDurableStartResponse start = start(input(suffix(), 1));
            AwaitRequest await = awaitRequest(start);
            complete(await, "partial-batch");
            assertSucceeded(execution(start.durableExecutionName()));
            JsonNode response = invoke(stack.output("AwaitWakeupFunctionName"), syntheticStreamBatch(await, 1));
            assertThat(response.path("batchItemFailures").toString()).contains("200");
            assertThat(response.path("batchItemFailures").toString()).doesNotContain("100");
        });

        evidence("stream-retained-failure", () -> {
            String malformed = "malformed-" + suffix();
            dynamo.putItem(PutItemRequest.builder()
                .tableName(stack.output("BindingTableName"))
                .item(Map.of(
                    "pk", AttributeValue.fromS(malformed),
                    "sk", AttributeValue.fromS("GEN#00000000000000000001"),
                    "record_type", AttributeValue.fromS("BINDING")))
                .build());
            waitFor(() -> queueDepth(stack.output("StreamFailureQueueUrl")) > 0
                ? Optional.of(Boolean.TRUE) : Optional.empty(), FLOW_TIMEOUT);
        });

        evidence("closed-callback", () -> {
            AwsDurableStartResponse start = start(input(suffix(), 1));
            AwaitRequest await = awaitRequest(start);
            Execution running = execution(start.durableExecutionName());
            lambda.stopDurableExecution(StopDurableExecutionRequest.builder()
                .durableExecutionArn(running.durableExecutionArn()).build());
            complete(await, "closed-callback");
            invoke(stack.output("ReconcilerFunctionName"), Map.of());
            assertThat(waitFor(() -> deliveryEvidence(await, 1), FLOW_TIMEOUT)).isNotEmpty();
            String replacementName = AwsDurableExecutionNames.durableExecutionName(
                await.tenantId(), await.executionId(), 2);
            assertSucceeded(execution(replacementName));
        });
    }

    @Test
    void parkedExecutionSurvivesAliasVersionChange() throws Exception {
        evidence("version-survival", () -> {
            AwsDurableStartResponse start = start(input(suffix(), 1));
            AwaitRequest await = awaitRequest(start);
            Execution parked = execution(start.durableExecutionName());
            String originalVersion = functionVersion(parked.functionArn());
            var published = lambda.publishVersion(PublishVersionRequest.builder()
                .functionName(stack.output("DurableFunctionName")).build());
            try {
                lambda.updateAlias(UpdateAliasRequest.builder()
                    .functionName(stack.output("DurableFunctionName"))
                    .name("proof-live")
                    .functionVersion(published.version()).build());
                complete(await, "version-survival");
                var terminal = assertSucceeded(execution(start.durableExecutionName()));
                assertThat(terminal.functionArn()).endsWith(":" + originalVersion);
            } finally {
                lambda.updateAlias(UpdateAliasRequest.builder()
                    .functionName(stack.output("DurableFunctionName"))
                    .name("proof-live")
                    .functionVersion(originalVersion).build());
            }
        });
    }

    @Test
    void malformedAwaitCompletionMovesToDlqWithoutSemanticMutation() throws Exception {
        evidence("poison-dlq", () -> {
            String queueUrl = stack.output("AwaitCompletionQueueUrl");
            String mappingId = stack.output("AwaitQueueMappingId");
            setQueueMapping(mappingId, false);
            try {
                setQueueVisibility(queueUrl, 1);
                sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody("{malformed-proof-message")
                    .build());
                setQueueMapping(mappingId, true);
                waitFor(() -> queueDepth(stack.output("AwaitCompletionDlqUrl")) > 0
                    ? Optional.of(Boolean.TRUE) : Optional.empty(), Duration.ofMinutes(2));
                assertThat(queueDepth(stack.output("AwaitCompletionDlqUrl"))).isGreaterThan(0);
            } finally {
                setQueueMapping(mappingId, false);
                setQueueVisibility(queueUrl, 720);
                setQueueMapping(mappingId, true);
            }
        });
    }

    @Test
    void workerRetryExhaustionKeepsTpfDlqAndRedriveAuthority() throws Exception {
        evidence("tpf-retry-redrive", () -> {
            String suffix = "retry-" + suffix();
            AwsDurableExecutionInput input = input(suffix, 1);
            armPersistent("pipeline-transition", suffix);
            AwsDurableStartResponse first = start(input);
            Map<String, AttributeValue> failed = waitFor(() -> executionForTenant(input.tenantId())
                .filter(item -> "FAILED".equals(item.get("status").s())), Duration.ofMinutes(8));
            assertThat(queueDepth(stack.output("WorkDlqUrl"))).isGreaterThan(0);
            String executionId = failed.get("execution_id").s();
            long version = Long.parseLong(failed.get("version").n());
            clearPersistent("pipeline-transition", suffix);

            AwsDurableDriverCheckpoint checkpoint = new AwsDurableDriverCheckpoint(
                new AwsDurableExecutionCheckpoint(
                    input.tenantId(), executionId, input.pipelineId(), input.contractVersion(),
                    input.releaseVersion()),
                1);
            AwsDurableActionResponse redrive = invokeAction(AwsDurableActionRequest.redrive(
                checkpoint, version, "deployed proof retry exhaustion"));
            assertThat(redrive.executionStatus()).contains("QUEUED");

            AwsDurableExecutionInput replacementInput = new AwsDurableExecutionInput(
                input.tenantId(), executionId, input.pipelineId(), input.contractVersion(), input.releaseVersion(),
                "{}", Optional.of(executionId), 2);
            AwsDurableStartResponse replacement = start(replacementInput);
            AwaitRequest await = awaitRequest(replacement);
            complete(await, "redrive-" + suffix);
            assertSucceeded(execution(replacement.durableExecutionName()));
            assertThat(first.durableExecutionName()).isNotEqualTo(replacement.durableExecutionName());
        });
    }

    @Test
    void awsSchedulesAwaitDeadlineButTpfAdmitsTheSemanticTimeout() throws Exception {
        evidence("await-deadline", () -> {
            String suffix = "deadline-" + suffix();
            AwsDurableExecutionInput input = new AwsDurableExecutionInput(
                "tenant-" + suffix,
                "execution-key-" + suffix,
                "aws-durable-proof",
                "1",
                "proof-release-1",
                "{\"request\":\"" + suffix + "\",\"deadlineProbe\":true}",
                Optional.empty(),
                1);
            AwsDurableStartResponse start = start(input);
            AwaitRequest await = awaitRequest(start);

            var failed = waitFor(() -> {
                var current = lambda.getDurableExecution(GetDurableExecutionRequest.builder()
                    .durableExecutionArn(execution(start.durableExecutionName()).durableExecutionArn())
                    .includeExecutionData(true)
                    .build());
                return current.status() == ExecutionStatus.FAILED
                    ? Optional.of(current) : Optional.empty();
            }, FLOW_TIMEOUT);
            assertThat(failed.error()).isNotNull();
            assertThat(waitFor(() -> executionItem(await.tenantId(), await.executionId())
                .filter(item -> "FAILED".equals(item.get("status").s())), Duration.ofMinutes(8))).isNotEmpty();
        });
    }

    @Test
    void providerHistoryLossRecoversFromTheTpfSemanticCheckpoint() throws Exception {
        evidence("provider-history-loss-recovery", () -> {
            AwsDurableExecutionInput input = input("history-loss-" + suffix(), 1);
            AwsDurableStartResponse first = start(input);
            AwaitRequest await = awaitRequest(first);
            waitFor(() -> binding(await, 1), FLOW_TIMEOUT);
            Execution providerExecution = execution(first.durableExecutionName());
            lambda.stopDurableExecution(StopDurableExecutionRequest.builder()
                .durableExecutionArn(providerExecution.durableExecutionArn())
                .build());
            waitFor(() -> {
                var current = lambda.getDurableExecution(GetDurableExecutionRequest.builder()
                    .durableExecutionArn(providerExecution.durableExecutionArn()).build());
                return current.status() == ExecutionStatus.STOPPED
                    ? Optional.of(Boolean.TRUE) : Optional.empty();
            }, FLOW_TIMEOUT);

            // Model the public history API returning ResourceNotFound after retention expiry. The
            // replacement must be derived from the retained TPF checkpoint and provider binding,
            // without inspecting the expired provider history.
            arm("provider-history-unavailable", await.executionId());
            invoke(stack.output("ReconcilerFunctionName"), Map.of());
            waitFor(() -> binding(await, 2), FLOW_TIMEOUT);
            complete(await, "provider-history-loss");

            String replacementName = AwsDurableExecutionNames.durableExecutionName(
                await.tenantId(), await.executionId(), 2);
            var terminal = assertSucceeded(execution(replacementName));
            assertTerminalAwaitResult(terminal, await.executionId(), 2);
            assertThat(executionCount(input.tenantId())).isEqualTo(1);
        });
    }

    private static void evidence(String group, ThrowingRunnable action) throws Exception {
        long started = System.currentTimeMillis();
        try {
            action.run();
            EVIDENCE.add(new Evidence(group, "PASSED", System.currentTimeMillis() - started, ""));
        } catch (Exception | AssertionError failure) {
            EVIDENCE.add(new Evidence(group, "FAILED", System.currentTimeMillis() - started, failure.toString()));
            throw failure;
        }
    }

    private static AwsDurableExecutionInput input(String suffix, long generation) {
        return new AwsDurableExecutionInput(
            "tenant-" + suffix,
            "execution-key-" + suffix,
            "aws-durable-proof",
            "1",
            "proof-release-1",
            "{\"request\":\"" + suffix + "\"}",
            Optional.empty(),
            generation);
    }

    private static AwsDurableStartResponse start(AwsDurableExecutionInput input) throws Exception {
        AwsDurableStartResponse response = JSON.treeToValue(
            invoke(stack.output("IngressFunctionName"), input), AwsDurableStartResponse.class);
        START_TENANTS.put(response.durableExecutionName(), input.tenantId());
        return response;
    }

    private static JsonNode invoke(String function, Object payload) throws Exception {
        var response = invokeRaw(function, payload);
        assertThat(response.functionError()).as("function error from %s", function).isNull();
        assertThat(response.statusCode()).isEqualTo(200);
        return JSON.readTree(response.payload().asUtf8String());
    }

    private static software.amazon.awssdk.services.lambda.model.InvokeResponse invokeRaw(
        String function,
        Object payload
    ) throws Exception {
        return lambda.invoke(InvokeRequest.builder()
            .functionName(function)
            .invocationType(InvocationType.REQUEST_RESPONSE)
            .payload(SdkBytes.fromUtf8String(JSON.writeValueAsString(payload)))
            .build());
    }

    private static AwsDurableActionResponse invokeAction(AwsDurableActionRequest request) throws Exception {
        return JSON.treeToValue(invoke(stack.output("ActionFunctionName"), request), AwsDurableActionResponse.class);
    }

    private static Execution execution(String executionName) {
        return waitFor(() -> lambda.listDurableExecutionsByFunction(
                ListDurableExecutionsByFunctionRequest.builder()
                    .functionName(stack.output("DurableFunctionName"))
                    .durableExecutionName(executionName)
                    .maxItems(10)
                    .build()).durableExecutions().stream().findFirst(), FLOW_TIMEOUT);
    }

    private static software.amazon.awssdk.services.lambda.model.GetDurableExecutionResponse assertSucceeded(
        Execution execution
    ) {
        return waitFor(() -> {
            var current = lambda.getDurableExecution(GetDurableExecutionRequest.builder()
                .durableExecutionArn(execution.durableExecutionArn())
                .includeExecutionData(true)
                .build());
            if (current.status() == ExecutionStatus.FAILED
                || current.status() == ExecutionStatus.TIMED_OUT
                || current.status() == ExecutionStatus.STOPPED) {
                throw new AssertionError("durable execution failed: " + current.error());
            }
            return current.status() == ExecutionStatus.SUCCEEDED ? Optional.of(current) : Optional.empty();
        }, FLOW_TIMEOUT);
    }

    private static void assertTerminalAwaitResult(
        software.amazon.awssdk.services.lambda.model.GetDurableExecutionResponse execution,
        String expectedExecutionId,
        long expectedGeneration
    ) throws Exception {
        AwsDurableExecutionOutput output = JSON.readValue(execution.result(), AwsDurableExecutionOutput.class);
        assertThat(output.executionId()).isEqualTo(expectedExecutionId);
        assertThat(output.generation()).isEqualTo(expectedGeneration);
        assertThat(JSON.readTree(output.resultJson()).asText()).isEqualTo("approved-result");
    }

    private static AwaitRequest awaitRequest(AwsDurableStartResponse start) {
        String expectedTenant = Optional.ofNullable(START_TENANTS.get(start.durableExecutionName()))
            .orElseThrow(() -> new IllegalStateException(
                "proof start is missing its test tenant: " + start.durableExecutionName()));
        return waitFor(() -> {
            var response = sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(stack.output("AwaitRequestQueueUrl"))
                .waitTimeSeconds(10)
                .maxNumberOfMessages(1)
                .build());
            if (response.messages().isEmpty()) {
                return Optional.empty();
            }
            var message = response.messages().getFirst();
            sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(stack.output("AwaitRequestQueueUrl"))
                .receiptHandle(message.receiptHandle())
                .build());
            try {
                JsonNode value = JSON.readTree(message.body());
                AwaitRequest request = new AwaitRequest(
                    value.path("tenantId").asText(),
                    value.path("executionId").asText(),
                    value.path("interactionId").asText(),
                    value.path("correlationId").asText(),
                    value.path("resumeToken").asText());
                return request.tenantId().equals(expectedTenant)
                    ? Optional.of(request)
                    : Optional.empty();
            } catch (Exception failure) {
                throw new IllegalStateException("could not read proof Await request", failure);
            }
        }, FLOW_TIMEOUT);
    }

    private static String receiveBody(String queueUrl, boolean delete) {
        return waitFor(() -> {
            var response = sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl).waitTimeSeconds(10).maxNumberOfMessages(1)
                .attributeNames(QueueAttributeName.ALL).build());
            if (response.messages().isEmpty()) {
                return Optional.empty();
            }
            var message = response.messages().getFirst();
            if (delete) {
                sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl).receiptHandle(message.receiptHandle()).build());
            } else {
                sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                    .queueUrl(queueUrl).receiptHandle(message.receiptHandle()).visibilityTimeout(0).build());
            }
            return Optional.of(message.body());
        }, FLOW_TIMEOUT);
    }

    private static void complete(AwaitRequest await, String key) throws Exception {
        Map<String, Object> envelope = Map.of(
            "tenantId", await.tenantId(),
            "interactionId", await.interactionId(),
            "correlationId", await.correlationId(),
            "resumeToken", await.resumeToken(),
            "idempotencyKey", key,
            "responsePayload", "approved-result",
            "actor", "deployed-proof");
        sqs.sendMessage(SendMessageRequest.builder()
            .queueUrl(stack.output("AwaitCompletionQueueUrl"))
            .messageBody(JSON.writeValueAsString(envelope)).build());
    }

    private static int executionCount(String tenantId) {
        return dynamo.query(QueryRequest.builder()
            .tableName(stack.output("ExecutionTableName"))
            .keyConditionExpression("tenant_id = :tenant")
            .expressionAttributeValues(Map.of(":tenant", AttributeValue.fromS(tenantId)))
            .consistentRead(true).build()).count();
    }

    private static int interactionCount(String tenantId, String executionId) {
        return (int) dynamo.query(QueryRequest.builder()
            .tableName(stack.output("AwaitInteractionTableName"))
            .keyConditionExpression("tenant_id = :tenant")
            .filterExpression("execution_id = :execution")
            .expressionAttributeValues(Map.of(
                ":tenant", AttributeValue.fromS(tenantId),
                ":execution", AttributeValue.fromS(executionId)))
            .consistentRead(true).build()).items().size();
    }

    private static Optional<Map<String, AttributeValue>> executionItem(String tenantId, String executionId) {
        var item = dynamo.getItem(GetItemRequest.builder()
            .tableName(stack.output("ExecutionTableName"))
            .key(Map.of(
                "tenant_id", AttributeValue.fromS(tenantId),
                "execution_id", AttributeValue.fromS(executionId)))
            .consistentRead(true).build()).item();
        return item.isEmpty() ? Optional.empty() : Optional.of(item);
    }

    private static Optional<Map<String, AttributeValue>> executionForTenant(String tenantId) {
        return dynamo.query(QueryRequest.builder()
            .tableName(stack.output("ExecutionTableName"))
            .keyConditionExpression("tenant_id = :tenant")
            .expressionAttributeValues(Map.of(":tenant", AttributeValue.fromS(tenantId)))
            .consistentRead(true)
            .limit(1)
            .build()).items().stream().findFirst();
    }

    private static Optional<Map<String, AttributeValue>> binding(AwaitRequest await, long generation) {
        String sort = String.format(java.util.Locale.ROOT, "GEN#%020d", generation);
        var item = dynamo.getItem(GetItemRequest.builder()
            .tableName(stack.output("BindingTableName"))
            .key(Map.of(
                "pk", AttributeValue.fromS(await.tenantId() + "#" + await.interactionId()),
                "sk", AttributeValue.fromS(sort)))
            .consistentRead(true).build()).item();
        return item.isEmpty() ? Optional.empty() : Optional.of(item);
    }

    private static Optional<Map<String, AttributeValue>> deliveryEvidence(AwaitRequest await, long generation) {
        var item = dynamo.getItem(GetItemRequest.builder()
            .tableName(stack.output("BindingTableName"))
            .key(Map.of(
                "pk", AttributeValue.fromS(await.tenantId() + "#" + await.interactionId()),
                "sk", AttributeValue.fromS("DELIVERED#" + generation)))
            .consistentRead(true).build()).item();
        return item.isEmpty() ? Optional.empty() : Optional.of(item);
    }

    private static void arm(String point, String correlation) {
        dynamo.putItem(PutItemRequest.builder()
            .tableName(stack.output("FaultTableName"))
            .item(Map.of(
                "pk", AttributeValue.fromS(point + "#" + correlation),
                "sk", AttributeValue.fromS("ARMED")))
            .build());
    }

    private static void armPersistent(String point, String correlation) {
        dynamo.putItem(PutItemRequest.builder()
            .tableName(stack.output("FaultTableName"))
            .item(Map.of(
                "pk", AttributeValue.fromS(point + "#" + correlation),
                "sk", AttributeValue.fromS("PERSISTENT")))
            .build());
    }

    private static void clearPersistent(String point, String correlation) {
        dynamo.deleteItem(DeleteItemRequest.builder()
            .tableName(stack.output("FaultTableName"))
            .key(Map.of(
                "pk", AttributeValue.fromS(point + "#" + correlation),
                "sk", AttributeValue.fromS("PERSISTENT")))
            .build());
    }

    private static int queueDepth(String queueUrl) {
        String value = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
            .queueUrl(queueUrl)
            .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
            .build()).attributes().getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0");
        return Integer.parseInt(value);
    }

    private static void setQueueVisibility(String queueUrl, int seconds) {
        sqs.setQueueAttributes(SetQueueAttributesRequest.builder()
            .queueUrl(queueUrl)
            .attributes(Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, Integer.toString(seconds)))
            .build());
        waitFor(() -> Integer.toString(seconds).equals(sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.VISIBILITY_TIMEOUT)
                .build()).attributes().get(QueueAttributeName.VISIBILITY_TIMEOUT))
            ? Optional.of(Boolean.TRUE) : Optional.empty(), Duration.ofMinutes(2));
    }

    private static void setMapping(String uuid, boolean enabled) {
        lambda.updateEventSourceMapping(UpdateEventSourceMappingRequest.builder()
            .uuid(uuid).enabled(enabled).build());
        waitFor(() -> {
            String state = lambda.getEventSourceMapping(GetEventSourceMappingRequest.builder().uuid(uuid).build()).state();
            return (enabled ? "Enabled" : "Disabled").equals(state)
                ? Optional.of(Boolean.TRUE) : Optional.empty();
        }, Duration.ofMinutes(2));
        if (!enabled) {
            try {
                Thread.sleep(25_000L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("event-source quiescence wait was interrupted", interrupted);
            }
        }
    }

    private static void setQueueMapping(String uuid, boolean enabled) {
        setMapping(uuid, enabled);
        if (!enabled) {
            try {
                Thread.sleep(45_000L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("SQS event-source quiescence wait was interrupted", interrupted);
            }
        }
    }

    private static Map<String, Object> syntheticStreamBatch(AwaitRequest await, long generation) {
        Map<String, Object> image = Map.of(
            "record_type", Map.of("S", "BINDING"),
            "tenant_id", Map.of("S", await.tenantId()),
            "execution_id", Map.of("S", await.executionId()),
            "interaction_id", Map.of("S", await.interactionId()),
            "correlation_id", Map.of("S", await.correlationId()),
            "generation", Map.of("N", Long.toString(generation)));
        return Map.of("Records", List.of(
            Map.of("eventID", "good-record", "eventName", "MODIFY",
                "dynamodb", Map.of("SequenceNumber", "100", "NewImage", image, "OldImage", image)),
            Map.of("eventID", "bad-record", "eventName", "MODIFY",
                "dynamodb", Map.of("SequenceNumber", "200", "NewImage",
                    Map.of("record_type", Map.of("S", "BINDING"))))));
    }

    private static String functionVersion(String functionArn) {
        return functionArn.substring(functionArn.lastIndexOf(':') + 1);
    }

    private static String suffix() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static <T> T waitFor(Supplier<Optional<T>> probe, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        RuntimeException lastFailure = new IllegalStateException("condition was not yet true");
        while (System.nanoTime() < deadline) {
            try {
                Optional<T> value = probe.get();
                if (value.isPresent()) {
                    return value.orElseThrow();
                }
            } catch (RuntimeException failure) {
                lastFailure = failure;
            }
            try {
                Thread.sleep(1_000L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("proof wait was interrupted", interrupted);
            }
        }
        throw new IllegalStateException("proof condition did not converge within " + timeout, lastFailure);
    }

    private record AwaitRequest(
        String tenantId,
        String executionId,
        String interactionId,
        String correlationId,
        String resumeToken
    ) {
    }

    private record Evidence(String group, String status, long durationMs, String detail) {
    }

    private record ProofStack(String name, Map<String, String> outputs) {
        static ProofStack load(CloudFormationClient cloudFormation, String name) {
            var described = cloudFormation.describeStacks(DescribeStacksRequest.builder().stackName(name).build())
                .stacks().getFirst();
            Map<String, String> outputs = new LinkedHashMap<>();
            described.outputs().forEach(output -> outputs.put(output.outputKey(), output.outputValue()));
            return new ProofStack(name, Map.copyOf(outputs));
        }

        String output(String name) {
            return Optional.ofNullable(outputs.get(name))
                .orElseThrow(() -> new IllegalStateException("stack output is missing: " + name));
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}

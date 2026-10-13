package org.pipelineframework.aws.durable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.junit.jupiter.api.Assertions.*;

class AwsDurableDeploymentHardeningTest {
    private JsonNode template() throws Exception {
        try (var input = getClass().getResourceAsStream("/aws-durable-queue-async.yaml")) {
            return new ObjectMapper(new YAMLFactory()).readTree(input);
        }
    }

    @Test
    void computeDisablesOnlyTransitionConsumptionAndRequiresExclusiveConsumerValidation() throws Exception {
        var template = template(); var resources = template.path("Resources");
        assertFalse(template.path("Parameters").path("TransitionWorkerPlacement").has("Default"));
        assertEquals("FunctionTransitionConsumer", resources.path("TransitionWorkerFunction").path("Condition").asText());
        assertFalse(resources.path("TransitionWorkerFunction").path("Properties").has("Events"));
        assertEquals("FunctionTransitionConsumer", resources.path("TransitionWorkerFunctionTransition").path("Condition").asText());
        assertEquals("FunctionTransitionConsumer", resources.path("TransitionWorkerVersion").path("Condition").asText());
        for (String host : List.of("DurableFunction", "IngressFunction", "ActionFunction", "WorkFunction", "AwaitCompletionFunction", "AwaitWakeupFunction", "ExecutionWakeupFunction", "DueWorkFunction")) {
            assertFalse(resources.path(host).has("Condition"), host + " remains hosted for COMPUTE workers");
        }
        var rule = template.path("Rules").path("ExclusiveComputeTransitionConsumer");
        assertEquals("TransitionWorkerPlacement", rule.path("RuleCondition").get(0).asText());
        assertEquals("COMPUTE", rule.path("RuleCondition").get(1).asText());
        assertEquals("ComputeTransitionConsumerValidation", rule.path("Assertions").get(0).path("Assert").get(0).asText());
        assertEquals("validated", rule.path("Assertions").get(0).path("Assert").get(1).asText());
    }

    @Test
    void everyFunctionHasFiniteEncryptedLogsErrorsThrottlesAndAnOwnedAlarmDestination() throws Exception {
        var template = template(); var resources = template.path("Resources");
        assertFalse(template.path("Parameters").path("AlarmTopicArn").has("Default"));
        var entries = resources.fields();
        while (entries.hasNext()) {
            var entry = entries.next(); var resource = entry.getValue();
            if (entry.getKey().endsWith("Function")) {
                var logs = resources.path(entry.getKey() + "Logs");
                assertEquals("AWS::Logs::LogGroup", logs.path("Type").asText(), entry.getKey());
                assertEquals("LogRetentionDays", logs.path("Properties").path("RetentionInDays").asText());
                assertEquals("HostKmsKeyArn", logs.path("Properties").path("KmsKeyId").asText());
                assertEquals("Retain", logs.path("DeletionPolicy").asText());
                assertEquals(entry.getKey() + "Logs", resource.path("DependsOn").asText());
                assertTrue(resources.has(entry.getKey() + "ErrorsAlarm"));
                assertTrue(resources.has(entry.getKey() + "ThrottlesAlarm"));
            }
            if ("AWS::CloudWatch::Alarm".equals(resource.path("Type").asText())) {
                var properties = resource.path("Properties");
                if (properties.has("<<")) {
                    assertTrue(properties.path("<<").asText().endsWith("AlarmProperties"));
                } else {
                    assertEquals("AlarmTopicArn", properties.path("AlarmActions").get(0).asText(), entry.getKey());
                    assertEquals("AlarmTopicArn", properties.path("OKActions").get(0).asText(), entry.getKey());
                }
            }
        }
    }

    @Test
    void signingSecretsRemainExactVersionReferencesWithScopedReadsAndDecrypt() throws Exception {
        var template = template(); var resources = template.path("Resources");
        var parameters = template.path("Parameters");
        assertFalse(parameters.has("ResumeTokenSecret")); assertFalse(parameters.has("WorkerSharedSecret"));
        var env = resources.path("ActionFunction").path("Properties").path("Environment").path("Variables");
        assertFalse(env.has("PIPELINE_ORCHESTRATOR_RESUME_TOKEN_SECRET"));
        assertFalse(env.has("PIPELINE_ORCHESTRATOR_WORKER_SQS_SHARED_SECRET"));
        assertEquals("ResumeTokenSecretVersion", env.path("TPF_AWS_DURABLE_RESUME_TOKEN_SECRET_VERSION").asText());
        assertEquals("WorkerSharedSecretVersion", env.path("TPF_AWS_DURABLE_WORKER_SECRET_VERSION").asText());
        for (String role : List.of("ActionRole", "WorkerRole", "WakeupRole", "DueWorkRole")) {
            var statements = resources.path(role).path("Properties").path("Policies").get(0).path("PolicyDocument").path("Statement");
            boolean read = false, decrypt = false;
            for (var statement : statements) {
                if (statement.path("Action").asText().equals("secretsmanager:GetSecretValue")) {
                    read = true;
                    assertEquals("SigningSecretAccessArns", statement.path("Resource").asText());
                }
                if (statement.path("Resource").asText().equals("SigningKmsAccessArns")) {
                    decrypt = true;
                    assertEquals("kms:Decrypt", statement.path("Action").asText());
                    assertTrue(statement.path("Condition").toString().contains("kms:EncryptionContext:SecretARN"));
                    assertTrue(statement.path("Condition").toString().contains("secretsmanager.${AWS::Region}.amazonaws.com"));
                }
            }
            assertTrue(read && decrypt, role);
        }
        var assertions = template.path("Rules").path("CurrentSigningReferencesAccessible").path("Assertions");
        for (int index = 0; index < 3; index++) {
            assertEquals(index == 2 ? "SigningKmsAccessArns" : "SigningSecretAccessArns", assertions.get(index).path("Assert").get(0).asText());
            assertEquals(List.of("ResumeTokenSecretArn", "WorkerSharedSecretArn", "SigningKmsKeyArn").get(index), assertions.get(index).path("Assert").get(1).asText());
        }
        assertFalse(parameters.path("SigningSecretAccessArns").has("Default"));
        assertFalse(parameters.path("SigningKmsAccessArns").has("Default"));
    }

    @Test
    void workerAvailabilityReceivesTheExactGeneratedContractAndReleaseArtifactIdentity() throws Exception {
        var template = template();
        assertTrue(template.path("Parameters").has("ContractVersion"), "Worker availability requires the generated semantic contract identity");
        assertFalse(template.path("Parameters").path("ContractVersion").has("Default"));
        assertTrue(template.path("Parameters").has("ReleaseArtifactId"));
        assertFalse(template.path("Parameters").path("ReleaseArtifactId").has("Default"));
        var encoded = template.path("Resources").path("ActionFunction").path("Properties")
            .path("Environment").path("Variables").path("TPF_AWS_DURABLE_CONFIGURATION").asText();
        var configuration = new ObjectMapper().readTree(encoded);
        assertEquals("${ContractVersion}", configuration.path("worker.sqs.contract-version").asText());
        assertEquals("${ReleaseArtifactId}", configuration.path("worker.sqs.artifact-id").asText());
    }

    @Test
    void explicitSemanticConfigurationAndHostLimitsAreWiredBeforeAdmission() throws Exception {
        var template = template(); var env = template.path("Resources").path("ActionFunction").path("Properties").path("Environment").path("Variables");
        var configuration = new ObjectMapper().readTree(env.path("TPF_AWS_DURABLE_CONFIGURATION").asText());
        assertEquals("QUEUE_ASYNC", configuration.path("mode").asText());
        assertEquals("dynamo", configuration.path("state-provider").asText());
        assertEquals("true", configuration.path("strict-startup").asText());
        assertEquals("CLIENT_KEY_REQUIRED", configuration.path("idempotency-policy").asText());
        assertEquals("${ReleaseArtifactBucket}", configuration.path("releases.storage.s3.bucket").asText());
        for (String table : List.of("execution", "execution-key", "execution-payload", "await-interaction", "await-interaction-key", "await-completion-lifecycle", "await-unit", "await-admission", "release", "worker")) {
            var reference = configuration.path("dynamo." + table + "-table").asText();
            assertTrue(reference.startsWith("${") && reference.endsWith("}"));
            var parameter = reference.substring(2, reference.length() - 1);
            assertFalse(template.path("Parameters").path(parameter).has("Default"));
        }
        assertEquals("DeploymentPreflightValidation", env.path("TPF_AWS_DURABLE_DEPLOYMENT_VALIDATION").asText());
        assertEquals("DurableEstimatedOperations", env.path("TPF_AWS_DURABLE_ESTIMATED_OPERATIONS").asText());
        assertEquals("DurableEstimatedWrittenStateBytes", env.path("TPF_AWS_DURABLE_ESTIMATED_WRITTEN_STATE_BYTES").asText());
        assertEquals(31622400, template.path("Parameters").path("DurableExecutionTimeoutSeconds").path("MaxValue").asInt());
        assertFalse(template.path("Parameters").path("DeploymentPreflightValidation").has("Default"));
        assertEquals("validated", template.path("Rules").path("DeploymentPreflightValidated").path("Assertions").get(0).path("Assert").get(1).asText());
        assertEquals("HostKmsKeyArn", template.path("Resources").path("DurableFunction").path("Properties").path("DurableConfig").path("KMSKeyArn").asText());
        assertTrue(template.path("Resources").path("CallbackBindingTable").path("Properties").path("DeletionProtectionEnabled").asBoolean());
    }

    @Test
    void retainedExecutionRecoveryAndDueWorkUsePinnedCodeAndIndependentBoundedHosts() throws Exception {
        var template = template(); var resources = template.path("Resources");
        assertEquals("ActionVersion", resources.path("DurableFunction").path("Properties").path("Environment")
            .path("Variables").path("TPF_AWS_DURABLE_ACTION_FUNCTION").asText());
        for (String name : List.of("ActionFunction", "ActionVersion", "DurableVersion")) {
            assertEquals("Retain", resources.path(name).path("DeletionPolicy").asText());
            assertEquals("Retain", resources.path(name).path("UpdateReplacePolicy").asText());
        }
        assertEquals("ActionCodeSha256", resources.path("ActionVersion").path("Properties").path("CodeSha256").asText());
        assertFalse(template.path("Parameters").path("VersionPublicationId").has("Default"));
        assertFalse(template.path("Parameters").path("ExecutionRecoveryMigrationValidation").has("Default"));
        assertEquals("validated", template.path("Rules").path("ExecutionRecoveryMigrationValidated")
            .path("Assertions").get(0).path("Assert").get(1).asText());
        var execution = resources.path("ExecutionWakeupFunction").path("Properties");
        assertEquals("ExecutionStateStreamArn", execution.path("Environment").path("Variables").path("TPF_AWS_DURABLE_EXECUTION_STREAM_ARN").asText());
        assertEquals("ExecutionStateStreamArn", resources.path("ExecutionWakeupFunctionExecutionState").path("Properties").path("EventSourceArn").asText());
        var due = resources.path("DueWorkFunction").path("Properties");
        assertEquals("DueWorkRole.Arn", due.path("Role").asText());
        assertEquals(2, due.path("ReservedConcurrentExecutions").asInt());
        assertEquals(2, resources.path("DueWorkFunctionTick").path("Properties").path("ScalingConfig").path("MaximumConcurrency").asInt());
        assertTrue(resources.path("DueWorkQueue").path("Properties").path("VisibilityTimeout").asInt() >= 6 * 120);
        var tick = resources.path("DueWorkRule").path("Properties").path("Targets").get(0);
        assertEquals("DueWorkQueue.Arn", tick.path("Arn").asText());
        assertEquals("DueWorkDlq.Arn", tick.path("DeadLetterConfig").path("Arn").asText());
        assertEquals(10, tick.path("RetryPolicy").path("MaximumRetryAttempts").asInt());
        assertEquals("DueWorkRule.Arn", resources.path("DueWorkQueuePolicy").path("Properties").path("PolicyDocument")
            .path("Statement").get(0).path("Condition").path("ArnEquals").path("aws:SourceArn").asText());
        assertTrue(resources.has("DueWorkDlqAlarm")); assertTrue(resources.has("DueWorkAgeAlarm"));
        assertTrue(resources.has("ExecutionStateDroppedEventAlarm"));
    }

    @Test
    void releaseContractRejectsMutableConsumersMissingIsolationAndUnpinnedManifestIdentity() throws Exception {
        var original = template();
        validateReleaseContract(original);
        var mutableConsumer = original.deepCopy();
        ((ObjectNode) mutableConsumer.path("Resources").path("WorkFunctionWork").path("Properties"))
            .put("FunctionName", "WorkFunction");
        assertThrows(AssertionError.class, () -> validateReleaseContract(mutableConsumer));
        var missingIsolation = original.deepCopy();
        ((ObjectNode) missingIsolation.path("Rules")).remove("ReleaseIsolationValidated");
        assertThrows(AssertionError.class, () -> validateReleaseContract(missingIsolation));
        var missingIdentity = original.deepCopy();
        ((ObjectNode) missingIdentity.path("Parameters")).remove("ContractVersion");
        assertThrows(AssertionError.class, () -> validateReleaseContract(missingIdentity));
        var mutableRepair = original.deepCopy();
        ((ObjectNode) mutableRepair.path("Resources").path("DurableTerminalRule").path("Properties")
            .path("Targets").get(0)).put("Arn", "ReconciliationFunction.Arn");
        assertThrows(AssertionError.class, () -> validateReleaseContract(mutableRepair));
        var incompleteManifest = original.deepCopy();
        ((com.fasterxml.jackson.databind.node.ArrayNode) incompleteManifest.path("Outputs")
            .path("DeploymentReleaseManifest").path("Value"))
            .set(0, new ObjectMapper().getNodeFactory().textNode("{\"schemaVersion\":1}"));
        assertThrows(AssertionError.class, () -> validateReleaseContract(incompleteManifest));
    }

    private void validateReleaseContract(JsonNode template) throws Exception {
        var resources = template.path("Resources");
        for (String name : List.of("Durable", "Action", "Ingress", "Work", "AwaitCompletion", "TransitionWorker", "AwaitWakeup", "ExecutionWakeup", "DueWork", "Reconciliation")) {
            var version = resources.path(name + "Version");
            assertEquals("AWS::Lambda::Version", version.path("Type").asText(), name);
            assertEquals("Retain", version.path("DeletionPolicy").asText());
            assertEquals("Retain", version.path("UpdateReplacePolicy").asText());
            assertEquals(name + "Function", version.path("Properties").path("FunctionName").asText());
            assertEquals(name.equals("Durable") || name.equals("Ingress") ? "DurableCodeSha256" : "ActionCodeSha256",
                version.path("Properties").path("CodeSha256").asText());
            assertTrue(version.path("Properties").path("Description").asText().contains("${VersionPublicationId}"));
            assertEquals("FunctionUpdate", version.path("Properties").path("RuntimePolicy").path("UpdateRuntimeOn").asText());
        }
        assertEquals("VersionPublicationId", resources.path("ActionFunction").path("Properties").path("Environment")
            .path("Variables").path("TPF_AWS_DURABLE_PUBLICATION_ID").asText());
        var mappings = Map.of("WorkFunctionWork", "WorkVersion", "AwaitCompletionFunctionAwaitCompletion", "AwaitCompletionVersion",
            "TransitionWorkerFunctionTransition", "TransitionWorkerVersion", "AwaitWakeupFunctionAwaitState", "AwaitWakeupVersion",
            "ExecutionWakeupFunctionExecutionState", "ExecutionWakeupVersion", "DueWorkFunctionTick", "DueWorkVersion");
        for (var mapping : mappings.entrySet()) {
            var resource = resources.path(mapping.getKey());
            assertEquals("AWS::Lambda::EventSourceMapping", resource.path("Type").asText());
            assertEquals(mapping.getValue(), resource.path("Properties").path("FunctionName").asText());
        }
        assertEquals("StreamMapping", resources.path("AwaitWakeupFunctionCallbackBinding").path("Properties").path("<<").asText());
        assertEquals("ReconciliationVersion", resources.path("DurableTerminalRule").path("Properties").path("Targets").get(0).path("Arn").asText());
        assertEquals("ReconciliationVersion", resources.path("DurableTerminalPermission").path("Properties").path("FunctionName").asText());
        assertEquals("ReconciliationVersion.Version", resources.path("ReconciliationInvokeConfig").path("Properties").path("Qualifier").asText());
        assertEquals("IngressVersion.Version", resources.path("IngressInvokeConfig").path("Properties").path("Qualifier").asText());
        for (String name : List.of("ReleaseIsolationMode", "ReleaseIsolationValidation", "ContractVersion", "ReleaseArtifactDigest", "VersionPublicationId")) {
            assertTrue(template.path("Parameters").has(name));
            assertFalse(template.path("Parameters").path(name).has("Default"));
        }
        assertEquals(List.of("FRESH_RELEASE_ISOLATED_STACK", "DRAINED_RELEASE_MIGRATION"),
            new ObjectMapper().convertValue(template.path("Parameters").path("ReleaseIsolationMode").path("AllowedValues"), List.class));
        assertTrue(template.path("Rules").has("ReleaseIsolationValidated"));
        assertEquals("ReleaseIsolationValidation", template.path("Rules").path("ReleaseIsolationValidated")
            .path("Assertions").get(0).path("Assert").get(0).asText());
        var manifest = new ObjectMapper().readTree(template.path("Outputs").path("DeploymentReleaseManifest").path("Value").get(0).asText());
        assertEquals(1, manifest.path("schemaVersion").asInt());
        for (var identity : Map.of("pipelineId", "PipelineId", "contractVersion", "ContractVersion", "releaseId", "ReleaseVersion",
            "artifactId", "ReleaseArtifactId", "artifactSha256", "ReleaseArtifactDigest").entrySet()) {
            assertEquals("${" + identity.getValue() + "}", manifest.path(identity.getKey()).asText());
        }
        assertEquals(10, manifest.path("functionVersionArns").size());
        assertEquals("${ResumeTokenSecretVersion}", manifest.path("signing").path("resumeToken").path("versionId").asText());
        assertEquals("${WorkerSharedSecretVersion}", manifest.path("signing").path("worker").path("versionId").asText());
        var entries = resources.fields();
        while (entries.hasNext()) {
            var resource = entries.next().getValue();
            if (List.of("AWS::IAM::Role", "AWS::SQS::Queue", "AWS::SQS::QueuePolicy", "AWS::Lambda::EventSourceMapping",
                "AWS::Lambda::EventInvokeConfig", "AWS::Lambda::Permission", "AWS::Lambda::Alias", "AWS::Events::Rule").contains(resource.path("Type").asText())) {
                assertEquals("Retain", resource.path("DeletionPolicy").asText());
                assertEquals("Retain", resource.path("UpdateReplacePolicy").asText());
            }
        }
    }

    @Test
    void providerBudgetAlarmsUseActualPerExecutionMetricsAndMaximumInsteadOfSummedExecutions() throws Exception {
        var resources = template().path("Resources");
        var names = List.of("DurableOperationBudgetAlarm", "DurableWrittenStateBudgetAlarm");
        var metrics = List.of("DurableExecutionOperations", "DurableExecutionStorageWrittenBytes");
        var thresholds = List.of(2700L, 90_000_000L);
        for (int index = 0; index < names.size(); index++) {
            var properties = resources.path(names.get(index)).path("Properties");
            assertEquals("DurableFunctionAlarmProperties", properties.path("<<").asText());
            assertEquals(metrics.get(index), properties.path("MetricName").asText());
            assertEquals("Maximum", properties.path("Statistic").asText());
            assertEquals(thresholds.get(index).longValue(), properties.path("Threshold").asLong());
            assertEquals("GreaterThanOrEqualToThreshold", properties.path("ComparisonOperator").asText());
        }
        var inherited = resources.path("DurableFunctionErrorsAlarm").path("Properties");
        assertEquals("AWS/Lambda", inherited.path("Namespace").asText());
        assertEquals("FunctionName", inherited.path("Dimensions").get(0).path("Name").asText());
        assertEquals("${AWS::StackName}-durable", inherited.path("Dimensions").get(0).path("Value").asText());
        assertEquals("AlarmTopicArn", inherited.path("AlarmActions").get(0).asText());
        assertEquals("AlarmTopicArn", inherited.path("OKActions").get(0).asText());
    }

    @Test
    void mechanicalAlarmsObserveEmittedMetricsWithBoundedDimensions() throws Exception {
        var resources = template().path("Resources");
        for (String name : List.of("WakeupProviderUnknownAlarm", "WakeupTransientFailureAlarm", "ReplacementRejectedAlarm", "ReplacementFailedAlarm", "RepairFailedAlarm", "CallbackBindingConflictAlarm",
            "CallbackRegistrationConflictAlarm", "GenerationClaimConflictAlarm", "StaleGenerationRejectionAlarm")) {
            var properties = resources.path(name).path("Properties");
            assertEquals("TPF/AwsDurable", properties.path("Namespace").asText());
            assertEquals("HostOutcomeCount", properties.path("MetricName").asText());
            assertEquals(2, properties.path("Dimensions").size());
            assertEquals("Stage", properties.path("Dimensions").get(0).path("Name").asText());
            assertEquals("Outcome", properties.path("Dimensions").get(1).path("Name").asText());
        }
    }

    @Test
    void completionAlarmsObserveNativeSamplesWithoutInventingAGlobalInventoryOrCallbackLatency() throws Exception {
        var template = template();
        var threshold = template.path("Parameters").path("CompletionReleaseLatencyAlarmMilliseconds");
        assertFalse(threshold.has("Default"));
        assertEquals(1, threshold.path("MinValue").asInt());
        var resources = template.path("Resources");
        var unknown = resources.path("CompletionObservationUnknownAlarm").path("Properties");
        assertEquals("CompletionObservationCount", unknown.path("MetricName").asText());
        assertEquals("UNKNOWN", unknown.path("Dimensions").get(1).path("Value").asText());
        var latency = resources.path("CompletionReleaseLatencyAlarm").path("Properties");
        assertEquals("AdmissionToReleaseLatencyMilliseconds", latency.path("MetricName").asText());
        assertEquals("Maximum", latency.path("Statistic").asText());
        assertEquals("CompletionReleaseLatencyAlarmMilliseconds", latency.path("Threshold").asText());
        assertEquals("RELEASED", latency.path("Dimensions").get(1).path("Value").asText());
        for (var properties : List.of(unknown, latency)) {
            assertEquals("ActionFunctionAlarmProperties", properties.path("<<").asText());
            assertEquals("TPF/AwsDurable", properties.path("Namespace").asText());
            assertEquals(2, properties.path("Dimensions").size());
            assertEquals("Stage", properties.path("Dimensions").get(0).path("Name").asText());
            assertEquals("COMPLETION", properties.path("Dimensions").get(0).path("Value").asText());
            assertTrue(properties.path("AlarmDescription").asText().contains("sample"));
        }
    }

    @Test
    void callbackLatencyAlarmsDistinguishObservedSamplesFromUnknownTimestamps() throws Exception {
        var template = template();
        var threshold = template.path("Parameters").path("CallbackResumeLatencyAlarmMilliseconds");
        assertFalse(threshold.has("Default"));
        assertEquals(1, threshold.path("MinValue").asInt());
        var resources = template.path("Resources");
        var latency = resources.path("CallbackResumeObservedLatencyAlarm").path("Properties");
        assertEquals("CallbackToFirstResumedActionMilliseconds", latency.path("MetricName").asText());
        assertEquals("Maximum", latency.path("Statistic").asText());
        assertEquals("CallbackResumeLatencyAlarmMilliseconds", latency.path("Threshold").asText());
        assertEquals("MEASURED", latency.path("Dimensions").get(1).path("Value").asText());
        var unknown = resources.path("CallbackResumeUnknownAlarm").path("Properties");
        assertEquals("CallbackResumeUnknownCount", unknown.path("MetricName").asText());
        assertEquals("UNKNOWN", unknown.path("Dimensions").get(1).path("Value").asText());
        for (var properties : List.of(latency, unknown)) {
            assertEquals("ActionFunctionAlarmProperties", properties.path("<<").asText());
            assertEquals("TPF/AwsDurable", properties.path("Namespace").asText());
            assertEquals(2, properties.path("Dimensions").size());
            assertEquals("CALLBACK_RESUME", properties.path("Dimensions").get(0).path("Value").asText());
            assertTrue(properties.path("AlarmDescription").asText().contains("sample"));
        }
    }

    @Test
    void failedIngressRepairInvocationAndEventDeliveryHaveBoundedRetryDestinations() throws Exception {
        var resources = template().path("Resources");
        for (String name : List.of("IngressInvokeConfig", "ReconciliationInvokeConfig")) {
            var properties = resources.path(name).path("Properties");
            assertEquals(3600, properties.path("MaximumEventAgeInSeconds").asInt());
            assertEquals(2, properties.path("MaximumRetryAttempts").asInt());
            assertFalse(properties.path("DestinationConfig").path("OnFailure").path("Destination").asText().isBlank());
        }
        var target = resources.path("DurableTerminalRule").path("Properties").path("Targets").get(0);
        assertEquals("RepairFailureDlq.Arn", target.path("DeadLetterConfig").path("Arn").asText());
        assertEquals(5, target.path("RetryPolicy").path("MaximumRetryAttempts").asInt());
        assertEquals("DurableTerminalRule.Arn", resources.path("RepairFailureQueuePolicy").path("Properties").path("PolicyDocument")
            .path("Statement").get(0).path("Condition").path("ArnEquals").path("aws:SourceArn").asText());
        assertTrue(resources.has("DurableDlqAlarm")); assertTrue(resources.has("RepairFailureDlqAlarm"));
        var stream = resources.path("AwaitWakeupFunctionAwaitState").path("Properties");
        assertEquals("EventCount", stream.path("MetricsConfig").path("Metrics").get(0).asText());
        for (String name : List.of("AwaitStateDroppedEventAlarm", "CallbackBindingDroppedEventAlarm")) {
            var properties = resources.path(name).path("Properties");
            assertEquals("DroppedEventCount", properties.path("MetricName").asText());
            assertEquals("EventSourceMappingUUID", properties.path("Dimensions").get(0).path("Name").asText());
        }
        for (String name : List.of("StreamDestinationDeliveryFailureAlarm", "IngressDestinationDeliveryFailureAlarm", "RepairDestinationDeliveryFailureAlarm")) {
            assertEquals("DestinationDeliveryFailures", resources.path(name).path("Properties").path("MetricName").asText());
        }
        assertEquals("DeadLetterErrors", resources.path("DurableDeadLetterDeliveryFailureAlarm").path("Properties").path("MetricName").asText());
    }

    @Test
    void repairAgeAlarmsCoverSamplesAndUnknownWithoutClaimingInventory() throws Exception {
        var template = template();
        var threshold = template.path("Parameters").path("RepairAgeAlarmMilliseconds");
        assertFalse(threshold.has("Default"));
        assertEquals(1, threshold.path("MinValue").asInt());
        var resources = template.path("Resources");
        var age = resources.path("RepairObservedAgeAlarm").path("Properties");
        assertEquals("RepairAgeMilliseconds", age.path("MetricName").asText());
        assertEquals("Maximum", age.path("Statistic").asText());
        assertEquals("RepairAgeAlarmMilliseconds", age.path("Threshold").asText());
        assertEquals("REPAIR_AGE_SAMPLED", age.path("Dimensions").get(1).path("Value").asText());
        var unknown = resources.path("RepairObservedAgeUnknownAlarm").path("Properties");
        assertEquals("RepairAgeUnknownCount", unknown.path("MetricName").asText());
        assertEquals("REPAIR_AGE_UNKNOWN", unknown.path("Dimensions").get(1).path("Value").asText());
        for (var properties : List.of(age, unknown)) {
            assertEquals("ReconciliationFunctionAlarmProperties", properties.path("<<").asText());
            assertEquals("TPF/AwsDurable", properties.path("Namespace").asText());
            assertEquals(2, properties.path("Dimensions").size());
            assertEquals("REPAIR", properties.path("Dimensions").get(0).path("Value").asText());
            assertTrue(properties.path("AlarmDescription").asText().contains("sample"));
        }
        var inherited = resources.path("ReconciliationFunctionErrorsAlarm").path("Properties");
        assertEquals("AlarmTopicArn", inherited.path("AlarmActions").get(0).asText());
        assertEquals("notBreaching", inherited.path("TreatMissingData").asText());
    }
}

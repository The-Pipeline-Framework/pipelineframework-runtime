package org.pipelineframework.aws.durable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import static org.junit.jupiter.api.Assertions.*;

class AwsDurableSharedRestDeploymentTest {
    @Test
    void completionBindingsExposeTheExistingNativeQueue() throws Exception {
        var shared = template("aws-durable-shared-rest.yaml");
        var queue = shared.required("Resources").required("AwaitCompletionQueue");
        assertEquals("AWS::SQS::Queue", queue.required("Type").asText());
        try (var input = getClass().getResourceAsStream("/aws-durable-shared-rest.yaml")) {
            assertNotNull(input);
            var nodes = new Yaml().compose(new InputStreamReader(input, StandardCharsets.UTF_8));
            assertIntrinsic(nodes, "!Ref", "AwaitCompletionQueue", "Outputs", "AwaitCompletionQueueUrl", "Value");
            assertIntrinsic(nodes, "!GetAtt", "AwaitCompletionQueue.Arn", "Outputs", "AwaitCompletionQueueArn", "Value");
            assertIntrinsic(nodes, "!GetAtt", "AwaitCompletionQueue.Arn", "Resources", "AwaitCompletionFunctionAwaitCompletion",
                    "Properties", "EventSourceArn");
        }
    }

    private static void assertIntrinsic(Node node, String tag, String target, String... path) {
        for (String key : path) {
            var mapping = assertInstanceOf(MappingNode.class, node);
            node = mapping.getValue().stream()
                    .filter(pair -> pair.getKeyNode() instanceof ScalarNode scalar
                            && key.equals(scalar.getValue()))
                    .findFirst().orElseThrow().getValueNode();
        }
        assertEquals(tag, node.getTag().getValue(), String.join(".", path));
        assertEquals(target, assertInstanceOf(ScalarNode.class, node).getValue());
    }

    @Test
    void sharedAndAttachmentOwnershipProduceIndependentInventories() throws Exception {
        JsonNode shared = template("aws-durable-shared-rest.yaml");
        JsonNode worker = template("aws-durable-pinned-worker.yaml");
        assertEquals("AWS::LanguageExtensions", shared.path("Transform").asText());
        assertFalse(worker.has("Transform"));
        var resources = shared.required("Resources");
        var attachment = worker.required("Resources");
        int platformResources = resources.size() - 1; // The retained-grant loop adds policies only for retained publications.
        assertTrue(resources.has("Fn::ForEach::RetainedWorkerTargetReadPolicies"));
        assertEquals(Map.of("platform", 58, "expense", 66, "both", 74, "retained", 82),
            Map.of("platform", platformResources, "expense", platformResources + attachment.size(),
                "both", platformResources + 2 * attachment.size(),
                "retained", platformResources + 3 * attachment.size()));
        assertFalse(resources.has("TransitionWorkerFunction"));
        assertFalse(resources.has("TransitionRequestQueue"));
        assertFalse(shared.path("Parameters").has("PipelineId"));
        assertFalse(shared.path("Parameters").has("ReleaseVersion"));
        assertEquals("AWS_IAM", resources.path("LifecycleUrl").path("Properties").path("AuthType").asText());
        assertEquals("true", resources.path("LifecycleFunction").path("Properties").path("Environment")
            .path("Variables").path("TPF_AWS_DURABLE_REST_LIFECYCLE_ONLY").asText());
        assertFalse(resources.path("CoordinationRole").toString().contains("lambda:UpdateAlias"));
        assertFalse(attachment.path("WorkerRole").toString().contains("dynamodb:"));
        assertFalse(attachment.path("WorkerRole").toString().contains("lambda:UpdateAlias"));
    }

    @Test
    void refreshableWorkerReadsUseSeparateAwsAuthorizationScopes() throws Exception {
        var role = template("aws-durable-shared-rest.yaml").path("Resources").path("CoordinationRole");
        var statements = role.path("Properties").path("Policies").findValues("Statement");
        var refreshableStatements = statements.stream().flatMap(JsonNode::valueStream)
            .filter(JsonNode::isArray)
            .filter(branch -> "RefreshableTargets".equals(branch.path(0).asText()))
            .map(branch -> branch.path(1)).toList();

        assertEquals(
            java.util.List.of("arn:${AWS::Partition}:lambda:${AWS::Region}:${AWS::AccountId}:function:${WorkerFunctionNamePrefix}*-worker"),
            refreshableStatements.stream().filter(statement -> "lambda:GetAlias".equals(statement.path("Action").asText()))
                .map(statement -> statement.path("Resource").asText()).toList());
        assertEquals(
            java.util.List.of("arn:${AWS::Partition}:lambda:${AWS::Region}:${AWS::AccountId}:function:${WorkerFunctionNamePrefix}*:release"),
            refreshableStatements.stream().filter(statement -> "lambda:GetFunctionUrlConfig".equals(statement.path("Action").asText()))
                .map(statement -> statement.path("Resource").asText()).toList());
    }

    @Test
    void sharedRestTemplateBoundsSqsConcurrencyExposesDlqOutputsAndWiresTenantQuotas() throws Exception {
        var shared = template("aws-durable-shared-rest.yaml");
        var params = shared.required("Parameters");
        assertEquals(10, params.required("WorkMaxConcurrency").required("Default").asInt());
        assertEquals(2, params.required("WorkMaxConcurrency").required("MinValue").asInt());
        assertEquals(5, params.required("AwaitCompletionMaxConcurrency").required("Default").asInt());
        assertEquals(2, params.required("DueWorkMaxConcurrency").required("Default").asInt());
        assertEquals(0, params.required("MaxInFlightTransitionsPerTenant").required("Default").asInt());
        assertEquals("false", params.required("DistributedTransitionAdmission").required("Default").asText());

        try (var input = getClass().getResourceAsStream("/aws-durable-shared-rest.yaml")) {
            assertNotNull(input);
            var nodes = new Yaml().compose(new InputStreamReader(input, StandardCharsets.UTF_8));
            assertIntrinsic(nodes, "!Ref", "WorkMaxConcurrency",
                "Resources", "WorkFunctionWork", "Properties", "ScalingConfig", "MaximumConcurrency");
            assertIntrinsic(nodes, "!Ref", "AwaitCompletionMaxConcurrency",
                "Resources", "AwaitCompletionFunctionAwaitCompletion", "Properties", "ScalingConfig", "MaximumConcurrency");
            assertIntrinsic(nodes, "!Ref", "DueWorkMaxConcurrency",
                "Resources", "DueWorkFunctionTick", "Properties", "ScalingConfig", "MaximumConcurrency");
            assertIntrinsic(nodes, "!Ref", "MaxInFlightTransitionsPerTenant",
                "Resources", "WorkFunction", "Properties", "Environment", "Variables",
                "PIPELINE_ORCHESTRATOR_QUOTAS_MAX_IN_FLIGHT_TRANSITIONS_PER_TENANT");
            assertIntrinsic(nodes, "!Ref", "DistributedTransitionAdmission",
                "Resources", "WorkFunction", "Properties", "Environment", "Variables",
                "PIPELINE_ORCHESTRATOR_QUOTAS_DISTRIBUTED_TRANSITION_ADMISSION");
            assertIntrinsic(nodes, "!GetAtt", "WorkDlq.QueueName", "Outputs", "WorkDlqName", "Value");
            assertIntrinsic(nodes, "!GetAtt", "AwaitCompletionDlq.QueueName", "Outputs", "AwaitCompletionDlqName", "Value");
            assertIntrinsic(nodes, "!GetAtt", "DurableDlq.QueueName", "Outputs", "DurableDlqName", "Value");
            assertIntrinsic(nodes, "!GetAtt", "RepairFailureDlq.QueueName", "Outputs", "RepairFailureDlqName", "Value");
            assertIntrinsic(nodes, "!GetAtt", "StreamFailureDlq.QueueName", "Outputs", "StreamFailureDlqName", "Value");
        }
    }

    private static JsonNode template(String name) throws Exception {
        try (var input = AwsDurableSharedRestDeploymentTest.class.getResourceAsStream("/" + name)) {
            assertNotNull(input);
            return new ObjectMapper(new YAMLFactory()).readTree(input);
        }
    }
}

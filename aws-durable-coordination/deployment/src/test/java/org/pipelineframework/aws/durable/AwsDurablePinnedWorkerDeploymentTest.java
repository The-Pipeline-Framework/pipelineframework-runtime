package org.pipelineframework.aws.durable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AwsDurablePinnedWorkerDeploymentTest {
    @Test
    void attachmentOwnsOnlyWorkerResourcesAndUsesOnlyLaneAndCredentialPermissions() throws Exception {
        try (var input = getClass().getResourceAsStream("/aws-durable-pinned-worker.yaml")) {
            var template = new ObjectMapper(new YAMLFactory()).readTree(input);
            assertFalse(template.has("Transform"));
            var resources = template.required("Resources");
            var types = new HashSet<String>();
            resources.forEach(resource -> types.add(resource.required("Type").asText()));
            assertEquals(Set.of("AWS::Logs::LogGroup", "AWS::IAM::Role", "AWS::Lambda::Function",
                "AWS::Lambda::Version", "AWS::Lambda::Alias", "AWS::Lambda::Url", "AWS::Lambda::Permission"), types);
            assertEquals(8, resources.size());
            assertEquals("AWS_IAM", resources.path("WorkerUrl").path("Properties").path("AuthType").asText());
            assertFalse(resources.path("WorkerAlias").path("Properties").has("RoutingConfig"));
            assertEquals("ConfigurationDigest", resources.path("WorkerVersion").path("Properties")
                .path("Description").asText());
            assertEquals("Retain", resources.path("WorkerVersion").path("DeletionPolicy").asText());
            for (String name : Set.of("CoordinatorUrlPermission", "CoordinatorInvokePermission")) {
                var permission = resources.path(name).path("Properties");
                assertEquals("CoordinatorInvokerRoleArn", permission.path("Principal").asText());
                assertEquals("WorkerAlias", permission.path("FunctionName").asText());
            }
            assertTrue(resources.path("CoordinatorInvokePermission").path("Properties")
                .path("InvokedViaFunctionUrl").asBoolean());
            var statements = resources.path("WorkerRole").path("Properties").path("Policies").get(0)
                .path("PolicyDocument").required("Statement");
            var credential = statements.required(0);
            assertEquals("WorkerUsesParameter", credential.required(0).asText());
            assertCredential(credential.required(1), "ssm:GetParameter", "WorkerParameterArn");
            assertCredential(credential.required(2), "secretsmanager:GetSecretValue", "WorkerSecretArn");

            var actions = new HashSet<String>();
            for (int i = 1; i < statements.size(); i++) {
                var action = statements.required(i).required("Action");
                if (action.isArray()) action.forEach(value -> actions.add(value.asText()));
                else actions.add(action.asText());
            }
            assertEquals(Set.of("lambda:InvokeFunctionUrl", "lambda:InvokeFunction",
                "logs:CreateLogStream", "logs:PutLogEvents"), actions);
            var callbackPolicy = statements.get(4);
            assertEquals("CoordinatorLifecycleAliasArn", callbackPolicy.path("Resource").asText());
            assertEquals("true", callbackPolicy.path("Condition").path("Bool")
                .path("lambda:InvokedViaFunctionUrl").asText());
        }
    }

    private static void assertCredential(JsonNode statement, String action, String resource) {
        assertEquals("Allow", statement.required("Effect").asText());
        assertEquals(action, statement.required("Action").asText());
        assertEquals(resource, statement.required("Resource").asText());
    }
}

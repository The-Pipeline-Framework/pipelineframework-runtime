package org.pipelineframework.aws.durable;

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
                "AWS::Lambda::Version", "AWS::Lambda::EventSourceMapping"), types);
            assertEquals(5, resources.size());
            assertEquals("aws-durable-pinned-worker", resources.path("WorkerFunction").path("Properties")
                .path("Environment").path("Variables").path("QUARKUS_LAMBDA_HANDLER").asText());
            assertEquals("WorkerVersion", resources.path("WorkerEvents").path("Properties")
                .path("FunctionName").asText());
            assertEquals("Retain", resources.path("WorkerVersion").path("DeletionPolicy").asText());
            var actions = new HashSet<String>();
            resources.path("WorkerRole").path("Properties").path("Policies").get(0)
                .path("PolicyDocument").path("Statement").forEach(statement -> {
                    var action = statement.required("Action");
                    if (action.isArray()) action.forEach(value -> actions.add(value.asText()));
                    else actions.add(action.asText());
                });
            assertEquals(Set.of("sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes",
                "sqs:SendMessage", "secretsmanager:GetSecretValue", "logs:CreateLogStream", "logs:PutLogEvents"), actions);
        }
    }
}

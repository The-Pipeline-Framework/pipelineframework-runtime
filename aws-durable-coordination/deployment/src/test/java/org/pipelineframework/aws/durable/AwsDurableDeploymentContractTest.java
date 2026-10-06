package org.pipelineframework.aws.durable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwsDurableDeploymentContractTest {
    private JsonNode resources() throws Exception {
        try (var template = getClass().getResourceAsStream("/aws-durable-queue-async.yaml")) {
            return new ObjectMapper(new YAMLFactory()).readTree(template).path("Resources");
        }
    }

    @Test
    void stackReplacementCannotDeleteParkedCodeOrMechanicalRecoveryEvidence() throws Exception {
        JsonNode resources = resources();
        for (String name : new String[]{"DurableFunction", "DurableVersion", "CallbackBindingTable"}) {
            assertEquals("Retain", resources.path(name).path("DeletionPolicy").asText(), name);
            assertEquals("Retain", resources.path(name).path("UpdateReplacePolicy").asText(), name);
        }
    }

    @Test
    void lambdaQueueVisibilityHasRetryHeadroom() throws Exception {
        JsonNode resources = resources();
        assertVisibility(resources, "WorkQueue", "WorkFunction");
        assertVisibility(resources, "TransitionRequestQueue", "TransitionWorkerFunction");
        assertVisibility(resources, "AwaitCompletionQueue", "AwaitCompletionFunction");
    }

    @Test
    void everyQueueAndBindingTableHasExplicitEncryption() throws Exception {
        JsonNode resources = resources();
        var entries = resources.fields();
        while (entries.hasNext()) {
            var entry = entries.next();
            if ("AWS::SQS::Queue".equals(entry.getValue().path("Type").asText())) {
                assertTrue(entry.getValue().path("Properties").path("SqsManagedSseEnabled").asBoolean(),
                    entry.getKey());
            }
        }
        assertTrue(resources.path("CallbackBindingTable").path("Properties")
            .path("SSESpecification").path("SSEEnabled").asBoolean());
    }

    @Test
    void streamConsumerCanWriteItsConfiguredFailureDestination() throws Exception {
        JsonNode statements = resources().path("WakeupRole").path("Properties")
            .path("Policies").get(0).path("PolicyDocument").path("Statement");
        boolean destinationAllowed = false;
        for (JsonNode statement : statements) {
            if ("sqs:SendMessage".equals(statement.path("Action").asText())
                && "StreamFailureDlq.Arn".equals(statement.path("Resource").asText())) {
                destinationAllowed = true;
            }
        }
        assertTrue(destinationAllowed, "Stream failure destination must have a scoped send grant");
    }

    private void assertVisibility(JsonNode resources, String queue, String function) {
        JsonNode properties = resources.path(function).path("Properties");
        int timeout = properties.path("Timeout").asInt(
            resources.path("ActionFunction").path("Properties").path("Timeout").asInt());
        assertTrue(timeout > 0, function);
        assertTrue(resources.path(queue).path("Properties").path("VisibilityTimeout").asInt()
            >= 6 * timeout, queue + " must allow Lambda retry headroom");
    }
}

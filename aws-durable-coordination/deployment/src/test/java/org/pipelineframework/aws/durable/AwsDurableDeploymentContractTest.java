package org.pipelineframework.aws.durable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwsDurableDeploymentContractTest {
    private JsonNode template() throws Exception {
        try (var template = getClass().getResourceAsStream("/aws-durable-queue-async.yaml")) {
            return new ObjectMapper(new YAMLFactory()).readTree(template);
        }
    }

    private JsonNode resources() throws Exception {
        return template().path("Resources");
    }

    @Test
    void deploymentRequiresExplicitGenerationMigrationValidation() throws Exception {
        JsonNode template = template();
        JsonNode validation = template.path("Parameters").path("CallbackGenerationMigrationValidation");
        assertEquals("String", validation.path("Type").asText());
        assertFalse(validation.has("Default"), "Migration validation must never be assumed");
        assertEquals(1, validation.path("AllowedValues").size());
        assertEquals("validated", validation.path("AllowedValues").get(0).asText());
        JsonNode assertions = template.path("Rules").path("CallbackGenerationMigrationValidated").path("Assertions");
        assertEquals(1, assertions.size());
        JsonNode comparison = assertions.get(0).path("Assert");
        assertEquals(2, comparison.size());
        assertEquals("CallbackGenerationMigrationValidation", comparison.get(0).asText());
        assertEquals("validated", comparison.get(1).asText());
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

    @Test
    void registrationIndexSelectsRegistrationsBeforeEvaluatingTheLimit() throws Exception {
        JsonNode resources = resources();
        JsonNode indexes = resources.path("CallbackBindingTable").path("Properties")
            .path("GlobalSecondaryIndexes");
        boolean registrationIndexFound = false;
        boolean legacyIndexRetained = false;
        for (JsonNode index : indexes) {
            if ("provider-registration-index".equals(index.path("IndexName").asText())) {
                registrationIndexFound = true;
                assertEquals("provider_execution_arn", index.path("KeySchema").get(0).path("AttributeName").asText());
                assertEquals("HASH", index.path("KeySchema").get(0).path("KeyType").asText());
                assertEquals("sk", index.path("KeySchema").get(1).path("AttributeName").asText());
                assertEquals("RANGE", index.path("KeySchema").get(1).path("KeyType").asText());
                assertEquals("ALL", index.path("Projection").path("ProjectionType").asText());
            }
            legacyIndexRetained |= "provider-execution-index".equals(index.path("IndexName").asText());
        }
        assertTrue(registrationIndexFound);
        assertTrue(legacyIndexRetained, "Retained host versions still need their original index");
        boolean indexGranted = false;
        for (JsonNode statement : resources.path("WakeupRole").path("Properties").path("Policies").get(0)
                .path("PolicyDocument").path("Statement")) {
            for (JsonNode resource : statement.path("Resource")) {
                indexGranted |= "${CallbackBindingTable.Arn}/index/provider-registration-index"
                    .equals(resource.asText());
            }
        }
        assertTrue(indexGranted, "Targeted repair must be authorised to query its new index");
    }

    @Test
    void callbackWritersCanConditionTransactionsOnGenerationAuthority() throws Exception {
        JsonNode resources = resources();
        for (String role : new String[]{"ActionRole", "WakeupRole"}) {
            boolean granted = false;
            for (JsonNode statement : resources.path(role).path("Properties").path("Policies").get(0)
                    .path("PolicyDocument").path("Statement")) {
                boolean checksGeneration = false;
                for (JsonNode action : statement.path("Action")) {
                    checksGeneration |= "dynamodb:ConditionCheckItem".equals(action.asText());
                }
                if (checksGeneration) {
                    granted |= "CallbackBindingTable.Arn".equals(statement.path("Resource").asText());
                    for (JsonNode resource : statement.path("Resource")) {
                        granted |= "CallbackBindingTable.Arn".equals(resource.asText());
                    }
                }
            }
            assertTrue(granted, role + " must condition writes on the callback table generation fence");
        }
    }

    @Test
    void wakeupRoutesExactSemanticAndMechanicalStreamSources() throws Exception {
        JsonNode function = resources().path("AwaitWakeupFunction").path("Properties");
        JsonNode variables = function.path("Environment").path("Variables");
        assertEquals("AwaitInteractionStreamArn", variables.path("TPF_AWS_DURABLE_AWAIT_STREAM_ARN").asText());
        assertEquals("CallbackBindingTable.StreamArn", variables.path("TPF_AWS_DURABLE_BINDING_STREAM_ARN").asText());
        assertEquals(variables.path("TPF_AWS_DURABLE_AWAIT_STREAM_ARN"),
            function.path("Events").path("AwaitState").path("Properties").path("Stream"));
        assertEquals(variables.path("TPF_AWS_DURABLE_BINDING_STREAM_ARN"),
            function.path("Events").path("CallbackBinding").path("Properties").path("Stream"));
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

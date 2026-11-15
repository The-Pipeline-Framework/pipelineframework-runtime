package org.pipelineframework.aws.durable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AwsDurableSharedRestDeploymentTest {
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

    private static JsonNode template(String name) throws Exception {
        try (var input = AwsDurableSharedRestDeploymentTest.class.getResourceAsStream("/" + name)) {
            assertNotNull(input);
            return new ObjectMapper(new YAMLFactory()).readTree(input);
        }
    }
}

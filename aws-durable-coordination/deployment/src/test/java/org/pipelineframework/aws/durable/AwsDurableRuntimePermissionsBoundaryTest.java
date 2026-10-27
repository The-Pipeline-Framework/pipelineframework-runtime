package org.pipelineframework.aws.durable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwsDurableRuntimePermissionsBoundaryTest {
    private static final Set<String> ROLES = Set.of(
        "DueWorkRole", "DurableRole", "IngressRole", "ActionRole", "WorkerRole", "WakeupRole");

    private JsonNode template() throws Exception {
        try (var input = getClass().getResourceAsStream("/aws-durable-queue-async.yaml")) {
            return new ObjectMapper(new YAMLFactory()).readTree(input);
        }
    }

    @Test
    void boundaryIsOptionalExactCustomerPolicyAndDoesNotChangeProfileDefault() throws Exception {
        JsonNode template = template();
        JsonNode parameter = template.path("Parameters").path("RuntimePermissionsBoundaryArn");
        assertEquals(74, template.path("Parameters").size());
        assertEquals("String", parameter.path("Type").asText());
        assertEquals("", parameter.path("Default").asText());
        assertEquals("HARDENED", template.path("Parameters").path("DeploymentProfile").path("Default").asText());
        Pattern pattern = Pattern.compile(parameter.path("AllowedPattern").asText());
        assertTrue(pattern.matcher("").matches());
        assertTrue(pattern.matcher("arn:aws:iam::123456789012:policy/tpf/runtime-boundary").matches());
        for (String invalid : new String[]{"*", "arn:aws:iam::123456789012:role/runtime", "arn:aws:iam::123456789012:policy/*", "arn:aws:iam::aws:policy/AdministratorAccess"}) {
            assertFalse(pattern.matcher(invalid).matches(), invalid);
        }
    }

    @Test
    void everyExplicitRoleUsesSameConditionalBoundaryWithoutChangingRetention() throws Exception {
        JsonNode template = template();
        Set<String> observed = new HashSet<>();
        var fields = template.path("Resources").fields();
        while (fields.hasNext()) {
            var entry = fields.next();
            JsonNode resource = entry.getValue();
            if (!"AWS::IAM::Role".equals(resource.path("Type").asText())) continue;
            observed.add(entry.getKey());
            JsonNode boundary = resource.path("Properties").path("PermissionsBoundary");
            assertEquals(3, boundary.size(), entry.getKey());
            assertEquals("RuntimePermissionsBoundarySelected", boundary.get(0).asText());
            assertEquals("RuntimePermissionsBoundaryArn", boundary.get(1).asText());
            assertEquals("AWS::NoValue", boundary.get(2).asText());
            assertEquals("Retain", resource.path("DeletionPolicy").asText());
            assertEquals("Retain", resource.path("UpdateReplacePolicy").asText());
        }
        assertEquals(ROLES, observed);
    }

    @Test
    void emptyBoundaryOmitsRolePropertyInCompatibleHardenedGraph() throws Exception {
        JsonNode template = DeploymentProfileTestSupport.hardened(template());
        for (String name : ROLES) {
            assertFalse(template.path("Resources").path(name).path("Properties").has("PermissionsBoundary"), name);
        }
    }
}

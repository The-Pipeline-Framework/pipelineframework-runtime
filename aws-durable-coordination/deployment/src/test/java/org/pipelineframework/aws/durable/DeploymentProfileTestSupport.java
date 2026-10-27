package org.pipelineframework.aws.durable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Set;

/** Resolve only the new profile choices before applying all existing hardened assertions. */
final class DeploymentProfileTestSupport {
    private static final Set<String> TRUE_DEFAULT = Set.of("HardenedProfile", "SecretsManagerSigning", "HardenedFunctionTransition");
    private static final Set<String> FALSE_DEFAULT = Set.of("RestrictedPilotProfile", "RuntimePermissionsBoundarySelected");

    private DeploymentProfileTestSupport() {}

    static JsonNode hardened(JsonNode node) {
        JsonNode resolved = resolve(node.deepCopy());
        var output = resolved.path("Outputs").path("DeploymentReleaseManifest").path("Value");
        if (output.isArray() && output.size() == 2 && output.get(1).has("SigningManifest")) {
            String signing = output.get(1).path("SigningManifest").get(0).asText();
            String semanticKeys = output.get(1).path("SemanticKeysManifest").get(0).asText();
            ((ArrayNode) output).set(0, com.fasterxml.jackson.databind.node.TextNode.valueOf(output.get(0).asText().replace("${SigningManifest}", signing).replace("${SemanticKeysManifest}", semanticKeys)));
        }
        return resolved;
    }

    private static JsonNode resolve(JsonNode node) {
        if (node.isArray() && node.size() == 3 && node.get(0).isTextual()) {
            String condition = node.get(0).asText();
            if (TRUE_DEFAULT.contains(condition)) return resolve(node.get(1));
            if (FALSE_DEFAULT.contains(condition)) return resolve(node.get(2));
        }
        if (node.isObject()) {
            var names = new ArrayList<String>();
            node.fieldNames().forEachRemaining(names::add);
            for (String name : names) {
                JsonNode value = resolve(node.get(name));
                if ("AWS::NoValue".equals(value.asText())) ((ObjectNode) node).remove(name);
                else ((ObjectNode) node).set(name, value);
            }
        } else if (node.isArray()) {
            var values = new ArrayList<JsonNode>();
            for (JsonNode item : node) {
                JsonNode value = resolve(item);
                if (!"AWS::NoValue".equals(value.asText())) values.add(value);
            }
            ((ArrayNode) node).removeAll().addAll(values);
        }
        return node;
    }
}

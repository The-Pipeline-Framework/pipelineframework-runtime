package org.pipelineframework.release.maven;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

/** Component ownership, rather than coincidentally equal snapshot labels, determines nested-build pins. */
class HostedFixtureVersionOwnershipTest {
    @Test
    void hostedCoordinatesUseTheirOwnersVersionFilters() throws Exception {
        var expected = Map.of(
            "pipelineframework-runtime-core", "@tpf.contracts.version@",
            "pipelineframework-compiler", "@tpf.compiler.version@",
            "pipelineframework", "@project.version@",
            "pipelineframework-aws-durable-coordination-host", "@project.version@",
            "pipelineframework-release-maven-plugin", "@project.version@");
        var seen = new HashSet<String>();
        var factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        try (var files = Files.walk(Path.of("src/it"))) {
            for (var pom : files.filter(path -> path.getFileName().toString().equals("pom.xml"))
                    .filter(path -> path.getName(2).toString().startsWith("hosted-aws-durable-")).toList()) {
                var elements = factory.newDocumentBuilder().parse(pom.toFile()).getElementsByTagNameNS("*", "*");
                for (int i = 0; i < elements.getLength(); i++) {
                    var coordinate = (Element) elements.item(i);
                    if (!Set.of("dependency", "plugin", "path").contains(coordinate.getLocalName())
                            || !"org.pipelineframework".equals(child(coordinate, "groupId"))) continue;
                    String artifact = child(coordinate, "artifactId");
                    assertTrue(expected.containsKey(artifact), "Declare the semantic owner of " + artifact + " in " + pom);
                    assertEquals(expected.get(artifact), child(coordinate, "version"),
                        "Wrong component version filter for " + artifact + " in " + pom);
                    seen.add(artifact);
                }
            }
        }
        assertEquals(expected.keySet(), seen, "The guard must inspect all hosted fixture component references");
    }

    /** Reads only direct coordinate children, so nested plugin configuration cannot supply a version. */
    private static String child(Element parent, String name) {
        for (var node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (node instanceof Element element && name.equals(element.getLocalName())) return element.getTextContent().strip();
        }
        return "";
    }
}

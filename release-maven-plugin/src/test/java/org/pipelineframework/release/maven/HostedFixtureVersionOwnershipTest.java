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
import javax.xml.parsers.ParserConfigurationException;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

/** Component ownership, rather than coincidentally equal snapshot labels, determines nested-build pins. */
class HostedFixtureVersionOwnershipTest {
    /** Checks each framework coordinate against the Maven version filter for its owning repository. */
    @Test
    void hostedCoordinatesUseTheirOwnersVersionFilters() throws Exception {
        var expected = Map.of(
            "pipelineframework-runtime-core", "@tpf.contracts.version@",
            "pipelineframework-compiler", "@tpf.compiler.version@",
            "pipelineframework", "@project.version@",
            "pipelineframework-aws-durable-coordination-host", "@project.version@",
            "pipelineframework-release-maven-plugin", "@project.version@");
        var seen = new HashSet<String>();
        var factory = pomFactory();
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

    /** Keeps the fixture provider on the same version at application runtime and during compilation. */
    @Test
    void hostedCommandUsesItsProviderVersion() throws Exception {
        var fixture = Path.of("src/it/hosted-aws-durable-command");
        var parser = pomFactory().newDocumentBuilder();
        var provider = parser.parse(fixture.resolve("provider/pom.xml").toFile()).getDocumentElement();
        String group = child(provider, "groupId");
        String artifact = child(provider, "artifactId");
        String version = child(provider, "version");
        assertTrue(!version.isBlank(), "The fixture provider must declare its version");

        var app = parser.parse(fixture.resolve("app/pom.xml").toFile());
        var referenceParents = Map.of("dependency", "dependencies", "path", "annotationProcessorPaths");
        for (var reference : referenceParents.entrySet()) {
            var elements = app.getElementsByTagNameNS("*", reference.getKey());
            boolean seen = false;
            for (int i = 0; i < elements.getLength(); i++) {
                var coordinate = (Element) elements.item(i);
                var parent = coordinate.getParentNode();
                if (!reference.getValue().equals(parent.getLocalName())
                        || ("dependency".equals(reference.getKey()) && parent.getParentNode() != app.getDocumentElement())
                        || !group.equals(child(coordinate, "groupId"))
                        || !artifact.equals(child(coordinate, "artifactId"))) {
                    continue;
                }
                assertEquals(version, child(coordinate, "version"),
                    "Wrong hosted Command fixture provider version in " + reference.getKey());
                seen = true;
            }
            assertTrue(seen, "Missing hosted Command fixture provider " + reference.getKey());
        }
    }

    /** Parses namespaced fixture POMs without resolving external resources. */
    private static DocumentBuilderFactory pomFactory() throws ParserConfigurationException {
        var factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        return factory;
    }

    /** Reads only direct coordinate children, so nested plugin configuration cannot supply a version. */
    private static String child(Element parent, String name) {
        for (var node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (node instanceof Element element && name.equals(element.getLocalName())) return element.getTextContent().strip();
        }
        return "";
    }
}

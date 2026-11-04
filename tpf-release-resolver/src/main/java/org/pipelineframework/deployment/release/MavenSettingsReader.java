package org.pipelineframework.deployment.release;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

final class MavenSettingsReader {
    record Repository(String id, URI url) {}
    record Server(String username, String password) {}
    record Settings(Optional<Path> localRepository, List<Repository> repositories, Map<String, Server> servers) {}

    Settings read(Path source) {
        if (source == null || !Files.isRegularFile(source)) {
            return new Settings(Optional.empty(), List.of(), Map.of());
        }
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            Element root = factory.newDocumentBuilder().parse(source.toFile()).getDocumentElement();
            Optional<Path> local = childText(root, "localRepository").map(MavenSettingsReader::path);
            Set<String> active = new HashSet<>();
            child(root, "activeProfiles").ifPresent(profiles -> elements(profiles, "activeProfile")
                .forEach(element -> active.add(element.getTextContent().trim())));
            List<Repository> repositories = new ArrayList<>();
            child(root, "profiles").ifPresent(profiles -> elements(profiles, "profile").forEach(profile -> {
                String id = childText(profile, "id").orElse("");
                if (!active.isEmpty() && !active.contains(id)) return;
                child(profile, "repositories").ifPresent(values -> elements(values, "repository").forEach(repository -> {
                    String repositoryId = childText(repository, "id").orElse("");
                    childText(repository, "url").ifPresent(url -> repositories.add(new Repository(repositoryId, URI.create(url))));
                }));
            }));
            Map<String, Server> servers = new HashMap<>();
            child(root, "servers").ifPresent(values -> elements(values, "server").forEach(server -> {
                String id = childText(server, "id").orElse("");
                String username = childText(server, "username").orElse("");
                String password = childText(server, "password").orElse("");
                if (!id.isBlank() && !username.isBlank()) servers.put(id, new Server(username, password));
            }));
            return new Settings(local, List.copyOf(repositories), Map.copyOf(servers));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to read Maven settings " + source, e);
        }
    }

    private static Path path(String value) {
        String expanded = value.replace("${user.home}", System.getProperty("user.home"));
        if (expanded.startsWith("~/")) expanded = System.getProperty("user.home") + expanded.substring(1);
        return Path.of(expanded).toAbsolutePath().normalize();
    }

    private static Optional<Element> child(Element parent, String name) {
        for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (node instanceof Element element && element.getTagName().equals(name)) return Optional.of(element);
        }
        return Optional.empty();
    }

    private static Optional<String> childText(Element parent, String name) {
        return child(parent, name).map(element -> element.getTextContent().trim()).filter(value -> !value.isBlank());
    }

    private static List<Element> elements(Element parent, String name) {
        List<Element> result = new ArrayList<>();
        NodeList nodes = parent.getChildNodes();
        for (int index = 0; index < nodes.getLength(); index++) {
            if (nodes.item(index) instanceof Element element && element.getTagName().equals(name)) result.add(element);
        }
        return result;
    }
}

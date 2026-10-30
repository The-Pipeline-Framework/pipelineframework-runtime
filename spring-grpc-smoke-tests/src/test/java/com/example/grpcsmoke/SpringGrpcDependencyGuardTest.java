package com.example.grpcsmoke;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpringGrpcDependencyGuardTest {
    private static final List<String> FORBIDDEN = List.of(
        "io.quarkus.", "jakarta.enterprise.", "io.smallrye.mutiny",
        "org.eclipse.microprofile.", "jakarta.ws.rs.");

    @Test
    void authoredAndGeneratedSourcesStaySpringOnly() throws Exception {
        Path module = Path.of(System.getProperty("user.dir"));
        Path generated = Path.of(System.getProperty("project.build.directory"), "generated-sources");
        assertTrue(Files.isDirectory(generated));
        for (Path root : List.of(module.resolve("src/main/java"), generated)) {
            try (var sources = Files.walk(root)) {
                for (Path source : sources.filter(path -> path.toString().endsWith(".java")).toList()) {
                    String content = Files.readString(source);
                    for (String token : FORBIDDEN) {
                        assertFalse(content.contains(token), source + " contains " + token);
                    }
                }
            }
        }
    }
}

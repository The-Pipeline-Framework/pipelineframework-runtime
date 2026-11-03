package org.pipelineframework.deployment.release;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Optional;

/** Reads Docker's external credential configuration without placing secrets in a Release. */
final class DockerConfigCredentialResolver implements CredentialResolver {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path configFile;

    DockerConfigCredentialResolver() {
        this(Path.of(System.getenv().getOrDefault(
            "DOCKER_CONFIG", Path.of(System.getProperty("user.home"), ".docker").toString()), "config.json"));
    }

    DockerConfigCredentialResolver(Path configFile) {
        this.configFile = configFile;
    }

    @Override
    public Optional<Credential> resolve(CredentialReference reference) {
        if (!Files.isRegularFile(configFile)) return Optional.empty();
        try {
            JsonNode root = JSON.readTree(configFile.toFile());
            JsonNode registry = root.path("auths").path(reference.name());
            if (!registry.isMissingNode() && registry.hasNonNull("auth")) {
                String decoded = new String(Base64.getDecoder().decode(registry.path("auth").asText()), StandardCharsets.UTF_8);
                int separator = decoded.indexOf(':');
                if (separator >= 0) return Optional.of(new Credential.Basic(
                    decoded.substring(0, separator), decoded.substring(separator + 1)));
            }
            String helper = root.path("credHelpers").path(reference.name()).asText(root.path("credsStore").asText(""));
            return helper.isBlank() ? Optional.empty() : helperCredential(helper, reference.name());
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Failed to resolve Docker credential for " + reference.name(), e);
        }
    }

    private static Optional<Credential> helperCredential(String helper, String registry) throws IOException {
        Process process = new ProcessBuilder("docker-credential-" + helper, "get").start();
        try (var input = process.getOutputStream()) {
            input.write(registry.getBytes(StandardCharsets.UTF_8));
        }
        try {
            if (process.waitFor() != 0) return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading Docker credential helper", e);
        }
        JsonNode response = JSON.readTree(process.getInputStream());
        String username = response.path("Username").asText();
        String secret = response.path("Secret").asText();
        if (secret.isBlank()) return Optional.empty();
        if (username.equals("<token>")) return Optional.of(new Credential.Bearer(secret));
        return Optional.of(new Credential.Basic(username, secret));
    }
}

package org.pipelineframework.deployment.release;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactResolver;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactUri;

public final class OciArtifactResolverProvider implements ArtifactResolverProvider<OciResolverConfiguration> {
    private static final String ACCEPT = String.join(", ",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json");

    @Override public String scheme() { return "oci"; }
    @Override public Class<OciResolverConfiguration> configurationType() { return OciResolverConfiguration.class; }

    @Override
    public PipelineReleaseArtifactResolver create(OciResolverConfiguration configuration, CredentialResolver credentials) {
        HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
        return artifact -> open(client, configuration, credentials, artifact);
    }

    private static ByteArrayInputStream open(
        HttpClient client,
        OciResolverConfiguration configuration,
        CredentialResolver credentials,
        PipelineReleaseArtifactDescriptor artifact
    ) throws IOException {
        PipelineReleaseArtifactUri parsed = PipelineReleaseArtifactUri.parse(artifact.uri());
        String value = parsed.value().substring("oci://".length());
        int slash = value.indexOf('/');
        int at = value.lastIndexOf('@');
        String registry = value.substring(0, slash);
        String repository = value.substring(slash + 1, at);
        String digest = value.substring(at + 1);
        String protocol = configuration.insecureRegistries().contains(registry) ? "http" : "https";
        URI endpoint = URI.create(protocol + "://" + registry + "/v2/" + repository + "/manifests/" + digest);
        HttpRequest.Builder request = HttpRequest.newBuilder(endpoint).header("Accept", ACCEPT).GET();
        Optional<Credential> credential = Optional.ofNullable(configuration.registryCredentials().get(registry))
            .flatMap(credentials::resolve);
        if (credential.isEmpty() && configuration.credentialSource().equals("docker-config")) {
            credential = new DockerConfigCredentialResolver().resolve(new CredentialReference(registry));
        }
        credential.ifPresent(resolvedCredential -> addAuthorization(request, resolvedCredential));
        try {
            HttpResponse<byte[]> response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != 200) {
                throw new IOException("OCI registry returned HTTP " + response.statusCode() + " for " + artifact.uri());
            }
            return new ByteArrayInputStream(response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while resolving " + artifact.uri(), e);
        }
    }

    private static void addAuthorization(HttpRequest.Builder request, Credential credential) {
        if (credential instanceof Credential.Bearer bearer) {
            request.header("Authorization", "Bearer " + bearer.token());
        } else if (credential instanceof Credential.Basic basic) {
            String value = java.util.Base64.getEncoder().encodeToString(
                (basic.username() + ":" + basic.password()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            request.header("Authorization", "Basic " + value);
        }
    }
}

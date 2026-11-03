package org.pipelineframework.deployment.release;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactResolver;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactUri;

public final class OciArtifactResolverProvider implements ArtifactResolverProvider<OciResolverConfiguration> {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final int BODY_LIMIT = 4 * 1024 * 1024;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern CHALLENGE_PARAMETER =
        Pattern.compile("\\s*([a-zA-Z]+)\\s*=\\s*\"([^\"\\r\\n]*)\"\\s*(?:,|$)");
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
        HttpClient tokenClient = HttpClient.newHttpClient();
        return artifact -> open(client, tokenClient, configuration, credentials, artifact);
    }

    private static ByteArrayInputStream open(
        HttpClient client,
        HttpClient tokenClient,
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
        HttpRequest.Builder request = manifestRequest(endpoint);
        Optional<Credential> credential = Optional.ofNullable(configuration.registryCredentials().get(registry))
            .flatMap(credentials::resolve);
        if (credential.isEmpty() && configuration.credentialSource().equals("docker-config")) {
            credential = new DockerConfigCredentialResolver().resolve(new CredentialReference(registry));
        }
        credential.ifPresent(resolvedCredential -> addAuthorization(request, resolvedCredential));
        try {
            HttpResponse<byte[]> response = send(client, request.build());
            if (response.statusCode() == 401) {
                var challenge = response.headers().allValues("WWW-Authenticate").stream()
                    .filter(valueHeader -> valueHeader.regionMatches(true, 0, "Bearer ", 0, 7)).findFirst();
                if (challenge.isPresent()) {
                    Credential.Bearer token = fetchToken(tokenClient, endpoint, repository, challenge.get(), credential);
                    HttpRequest.Builder retry = manifestRequest(endpoint);
                    addAuthorization(retry, token);
                    response = send(tokenClient, retry.build());
                }
            }
            if (response.statusCode() != 200) {
                throw new IOException("OCI registry returned HTTP " + response.statusCode() + " for " + artifact.uri());
            }
            return new ByteArrayInputStream(response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while resolving " + artifact.uri(), e);
        }
    }

    private static HttpRequest.Builder manifestRequest(URI endpoint) {
        return HttpRequest.newBuilder(endpoint).timeout(REQUEST_TIMEOUT).header("Accept", ACCEPT).GET();
    }

    private static Credential.Bearer fetchToken(
        HttpClient client, URI registry, String repository, String challenge, Optional<Credential> credential
    ) throws IOException, InterruptedException {
        var parameters = new HashMap<String, String>();
        var matcher = CHALLENGE_PARAMETER.matcher(challenge.substring(7));
        int end = 0;
        while (matcher.find() && matcher.start() == end) {
            if (parameters.putIfAbsent(matcher.group(1).toLowerCase(Locale.ROOT), matcher.group(2)) != null) {
                throw new IOException("Duplicate OCI Bearer challenge parameter");
            }
            end = matcher.end();
        }
        if (end != challenge.length() - 7 || !parameters.containsKey("realm")) {
            throw new IOException("Invalid OCI Bearer challenge");
        }
        try {
            URI realm = URI.create(parameters.get("realm"));
            boolean sameOrigin = realm.getHost() != null && registry.getScheme().equalsIgnoreCase(realm.getScheme())
                && registry.getHost().equalsIgnoreCase(realm.getHost())
                && port(registry) == port(realm);
            if (realm.getHost() == null || realm.getUserInfo() != null || realm.getFragment() != null
                || (!"https".equalsIgnoreCase(realm.getScheme()) && !sameOrigin)) {
                throw new IOException("Invalid OCI token endpoint");
            }
            String scope = "repository:" + repository + ":pull";
            if (!scope.equals(parameters.getOrDefault("scope", scope))) {
                throw new IOException("OCI Bearer challenge requested an unexpected scope");
            }
            String query = (parameters.containsKey("service")
                ? "service=" + URLEncoder.encode(parameters.get("service"), StandardCharsets.UTF_8) + "&" : "")
                + "scope=" + URLEncoder.encode(scope, StandardCharsets.UTF_8);
            URI endpoint = URI.create(realm + (realm.getRawQuery() == null ? "?" : "&") + query);
            var request = HttpRequest.newBuilder(endpoint).timeout(REQUEST_TIMEOUT).GET();
            if (sameOrigin) {
                credential.filter(Credential.Basic.class::isInstance)
                    .ifPresent(value -> addAuthorization(request, value));
            }
            var response = send(client, request.build());
            if (response.statusCode() != 200) {
                throw new IOException("OCI token endpoint returned HTTP " + response.statusCode());
            }
            var json = JSON.readTree(response.body());
            if (json == null) throw new IOException("Empty OCI token response");
            String token = json.path("token").asText(json.path("access_token").asText(""));
            if (token.isBlank() || !token.matches("[A-Za-z0-9._~+/=-]+")) throw new IOException("OCI token response did not contain a token");
            return new Credential.Bearer(token);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid OCI token challenge or response", e);
        }
    }

    private static int port(URI uri) {
        return uri.getPort() >= 0 ? uri.getPort() : "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
    }

    private static HttpResponse<byte[]> send(HttpClient client, HttpRequest request)
        throws IOException, InterruptedException {
        var response = client.sendAsync(request, OciArtifactResolverProvider::boundedBody);
        try {
            return response.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            response.cancel(true);
            throw new IOException("Timed out while receiving OCI response", e);
        } catch (InterruptedException e) {
            response.cancel(true);
            throw e;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException failure) throw failure;
            throw new IOException("Could not receive OCI response", e.getCause());
        }
    }

    private static HttpResponse.BodySubscriber<byte[]> boundedBody(HttpResponse.ResponseInfo info) {
        var delegate = HttpResponse.BodySubscribers.ofByteArray();
        return new HttpResponse.BodySubscriber<>() {
            private Flow.Subscription subscription;
            private long remaining = BODY_LIMIT;

            @Override public CompletionStage<byte[]> getBody() { return delegate.getBody(); }

            @Override public void onSubscribe(Flow.Subscription value) {
                subscription = value;
                if (info.headers().firstValueAsLong("Content-Length").orElse(0) > BODY_LIMIT) {
                    tooLarge();
                } else {
                    delegate.onSubscribe(value);
                }
            }

            @Override public void onNext(List<ByteBuffer> buffers) {
                for (ByteBuffer buffer : buffers) remaining -= buffer.remaining();
                if (remaining < 0) tooLarge();
                else delegate.onNext(buffers);
            }

            @Override public void onError(Throwable error) { delegate.onError(error); }
            @Override public void onComplete() { delegate.onComplete(); }

            private void tooLarge() {
                subscription.cancel();
                delegate.onError(new IOException("OCI response exceeds " + BODY_LIMIT + " bytes"));
            }
        };
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

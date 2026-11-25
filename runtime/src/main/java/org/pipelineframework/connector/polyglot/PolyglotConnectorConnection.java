package org.pipelineframework.connector.polyglot;

import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.pipelineframework.connector.ResolvedConnection;

/**
 * Host-owned ADR-0021 connection for a Tier-2 Polyglot Custom Connector Host.
 *
 * <p>Resolved through {@link org.pipelineframework.connector.ConnectionResolver} so host URIs,
 * HMAC signing keys, and scoped external API secrets stay strictly outside pipeline inputs and
 * pure business functions.</p>
 */
public record PolyglotConnectorConnection(
    URI hostUri,
    String sharedSecret,
    Map<String, String> authorizedSecrets,
    boolean allowLoopbackHttp
) implements ResolvedConnection {

    public PolyglotConnectorConnection {
        hostUri = validateOrigin(hostUri, allowLoopbackHttp);
        if (Objects.requireNonNull(sharedSecret, "sharedSecret must not be null").isBlank()) {
            throw new IllegalArgumentException("sharedSecret must not be blank");
        }
        Objects.requireNonNull(authorizedSecrets, "authorizedSecrets must not be null");
        authorizedSecrets.forEach((key, value) -> {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("authorized secret names must not be blank");
            }
            if (value == null) {
                throw new IllegalArgumentException("authorized secret value for '" + key + "' must not be null");
            }
        });
        authorizedSecrets = Map.copyOf(authorizedSecrets);
    }

    public PolyglotConnectorConnection(URI hostUri, String sharedSecret, Map<String, String> authorizedSecrets) {
        this(hostUri, sharedSecret, authorizedSecrets, false);
    }

    private static URI validateOrigin(URI raw, boolean allowLoopbackHttp) {
        URI uri = Objects.requireNonNull(raw, "hostUri must not be null").normalize();
        String scheme = uri.getScheme();
        String host = uri.getHost();
        if (!uri.isAbsolute() || scheme == null || host == null
            || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException(
                "Polyglot connector hostUri must be an absolute HTTP(S) origin without userInfo, query, or fragment");
        }
        String lowerScheme = scheme.toLowerCase(Locale.ROOT);
        if ("https".equals(lowerScheme)) {
            return uri;
        }
        if ("http".equals(lowerScheme) && allowLoopbackHttp && isLoopbackHost(host)) {
            return uri;
        }
        throw new IllegalArgumentException(
            "Polyglot connector hostUri requires HTTPS (plaintext HTTP is only permitted on loopback when explicitly enabled)");
    }

    private static boolean isLoopbackHost(String host) {
        String normalized = host.toLowerCase(Locale.ROOT);
        return "127.0.0.1".equals(normalized)
            || "localhost".equals(normalized)
            || "::1".equals(normalized)
            || "[::1]".equals(normalized);
    }
}

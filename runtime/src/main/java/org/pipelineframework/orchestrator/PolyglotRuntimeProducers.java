package org.pipelineframework.orchestrator;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import io.quarkus.arc.DefaultBean;
import io.quarkus.arc.lookup.LookupIfProperty;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.connector.polyglot.PolyglotConnectorConnection;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Quarkus CDI default producers for ADR-0092 polyglot runtime infrastructure:
 * {@link ForeignDispatchIntentStore} ({@code memory} / {@code dynamo} / {@code noop}) and
 * {@link ConnectionResolver} for Tier-2 {@link PolyglotConnectorConnection} endpoints.
 *
 * <p>Activated only when {@code pipeline.polyglot.enabled=true} and annotated {@link DefaultBean}
 * so applications can override either bean with custom Vault/Secrets-Manager resolvers or stores.</p>
 */
@ApplicationScoped
public class PolyglotRuntimeProducers {

    @Inject
    PolyglotRuntimeConfig config;

    @Inject
    Instance<DynamoDbClient> dynamoClients;

    @Produces
    @ApplicationScoped
    @DefaultBean
    @LookupIfProperty(name = "pipeline.polyglot.enabled", stringValue = "true")
    public ForeignDispatchIntentStore foreignDispatchIntentStore() {
        return createIntentStore(config, dynamoClients);
    }

    public ForeignDispatchIntentStore createIntentStore(
        PolyglotRuntimeConfig runtimeConfig,
        Instance<DynamoDbClient> clients
    ) {
        Objects.requireNonNull(runtimeConfig, "config must not be null");
        String provider = runtimeConfig.dispatchIntentStore() == null
            ? "memory"
            : runtimeConfig.dispatchIntentStore().trim().toLowerCase(Locale.ROOT);
        return switch (provider) {
            case "memory" -> new InMemoryForeignDispatchIntentStore();
            case "noop" -> ForeignDispatchIntentStore.noop();
            case "dynamo" -> {
                if (clients == null || clients.isUnsatisfied()) {
                    throw new IllegalStateException(
                        "pipeline.polyglot.dispatch-intent-store=dynamo requires a DynamoDbClient CDI bean");
                }
                if (clients.isAmbiguous()) {
                    throw new IllegalStateException(
                        "Multiple DynamoDbClient CDI beans found for pipeline.polyglot.dispatch-intent-store=dynamo");
                }
                yield new DynamoForeignDispatchIntentStore(clients.get(), runtimeConfig.dispatchIntentTable());
            }
            default -> throw new IllegalArgumentException(
                "Unsupported pipeline.polyglot.dispatch-intent-store '" + runtimeConfig.dispatchIntentStore()
                    + "' (expected memory, dynamo, or noop)");
        };
    }

    @Produces
    @ApplicationScoped
    @DefaultBean
    @LookupIfProperty(name = "pipeline.polyglot.enabled", stringValue = "true")
    public ConnectionResolver polyglotConnectionResolver() {
        return createConnectionResolver(config);
    }

    public ConnectionResolver createConnectionResolver(PolyglotRuntimeConfig runtimeConfig) {
        Objects.requireNonNull(runtimeConfig, "config must not be null");
        return new ConfiguredPolyglotConnectionResolver(runtimeConfig);
    }

    static final class ConfiguredPolyglotConnectionResolver implements ConnectionResolver {
        private final PolyglotRuntimeConfig config;

        ConfiguredPolyglotConnectionResolver(PolyglotRuntimeConfig config) {
            this.config = config;
        }

        @Override
        public <T extends ResolvedConnection> CompletionStage<T> resolve(ConnectionResolutionRequest<T> request) {
            if (!request.connectionType().equals(PolyglotConnectorConnection.class)) {
                return CompletableFuture.failedStage(new ConnectionResolutionException(
                    "Configured polyglot ConnectionResolver only resolves PolyglotConnectorConnection, got "
                        + request.connectionType().getName()));
            }
            String name = request.reference().value();
            PolyglotRuntimeConfig.ConnectorHostConfig connectorCfg = config.connector();
            PolyglotRuntimeConfig.NamedConnectionConfig named = connectorCfg == null || connectorCfg.connections() == null
                ? null
                : connectorCfg.connections().get(name);

            Optional<String> rawUri = firstPresent(
                named == null ? Optional.empty() : named.hostUri(),
                connectorCfg == null ? Optional.empty() : connectorCfg.hostUri());
            if (rawUri.isEmpty()) {
                return CompletableFuture.failedStage(new ConnectionResolutionException(
                    "No host URI configured for polyglot connector connection '" + name
                        + "'; set pipeline.polyglot.connector.connections." + name
                        + ".host-uri or pipeline.polyglot.connector.host-uri"));
            }
            Optional<String> rawSecret = firstPresent(
                named == null ? Optional.empty() : named.sharedSecret(),
                connectorCfg == null ? Optional.empty() : connectorCfg.sharedSecret(),
                config.sharedSecret());
            if (rawSecret.isEmpty()) {
                return CompletableFuture.failedStage(new ConnectionResolutionException(
                    "No shared secret configured for polyglot connector connection '" + name
                        + "'; set pipeline.polyglot.connector.shared-secret or pipeline.polyglot.shared-secret"));
            }

            Map<String, String> mergedSecrets = new LinkedHashMap<>();
            if (connectorCfg != null && connectorCfg.secrets() != null) {
                mergedSecrets.putAll(connectorCfg.secrets());
            }
            if (named != null && named.secrets() != null) {
                mergedSecrets.putAll(named.secrets());
            }
            try {
                PolyglotConnectorConnection connection = new PolyglotConnectorConnection(
                    URI.create(rawUri.orElseThrow().trim()),
                    rawSecret.orElseThrow().trim(),
                    Map.copyOf(mergedSecrets),
                    config.allowLoopbackHttp());
                return CompletableFuture.completedStage(request.connectionType().cast(connection));
            } catch (RuntimeException invalid) {
                return CompletableFuture.failedStage(new ConnectionResolutionException(
                    "Invalid polyglot connector connection '" + name + "': " + invalid.getMessage(), invalid));
            }
        }

        @SafeVarargs
        private static Optional<String> firstPresent(Optional<String>... candidates) {
            for (Optional<String> candidate : candidates) {
                if (candidate != null && candidate.isPresent() && !candidate.orElseThrow().isBlank()) {
                    return candidate;
                }
            }
            return Optional.empty();
        }
    }
}

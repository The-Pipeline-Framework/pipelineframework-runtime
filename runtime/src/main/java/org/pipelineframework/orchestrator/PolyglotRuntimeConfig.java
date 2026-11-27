package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * Production Quarkus configuration for class-free ADR-0092 polyglot applications
 * ({@code CanonicalFunctionHost*} steps and Tier-2 {@code PolyglotConnectorProvider} hosts).
 */
@ConfigMapping(prefix = "pipeline.polyglot")
@Unremovable
public interface PolyglotRuntimeConfig {

    /**
     * Enables automatic Quarkus CDI wiring for class-free canonical v3 polyglot steps and connectors.
     *
     * @return {@code true} when polyglot auto-wiring is enabled
     */
    @WithDefault("false")
    boolean enabled();

    /**
     * Default base URI of the foreign function host (HTTPS required unless {@link #allowLoopbackHttp()} is true).
     *
     * @return default foreign function host URI
     */
    @WithName("function-host-uri")
    Optional<String> functionHostUri();

    /**
     * Optional per-step foreign function host base URIs keyed by normalized kebab-case step ID
     * (for example {@code validate-invoice}).
     *
     * @return per-step target URIs
     */
    @WithName("step-uris")
    Map<String, String> stepUris();

    /**
     * Shared HMAC-SHA256 secret used to authenticate requests to foreign function hosts.
     *
     * @return shared secret
     */
    @WithName("shared-secret")
    Optional<String> sharedSecret();

    /**
     * Permits plain {@code http://} targets strictly for loopback addresses ({@code localhost},
     * {@code 127.0.0.1}, {@code ::1}). Defaults to {@code false} so production deployments enforce HTTPS.
     *
     * @return {@code true} only when loopback HTTP is explicitly allowed
     */
    @WithName("allow-loopback-http")
    @WithDefault("false")
    boolean allowLoopbackHttp();

    /**
     * Per-invocation request timeout for unary and streaming foreign function steps.
     *
     * @return request timeout
     */
    @WithName("request-timeout")
    @WithDefault("PT30S")
    Duration requestTimeout();

    /**
     * Maximum idle interval between NDJSON frames for {@code ONE_TO_MANY} foreign function streams.
     *
     * @return stream idle timeout
     */
    @WithName("stream-idle-timeout")
    @WithDefault("PT30S")
    Duration streamIdleTimeout();

    /**
     * Maximum response payload size in bytes accepted from foreign function hosts.
     *
     * @return max response bytes
     */
    @WithName("max-response-bytes")
    @WithDefault("4194304")
    int maxResponseBytes();

    /**
     * Pre-send {@link ForeignDispatchIntentStore} backend: {@code memory}, {@code dynamo}, or {@code noop}.
     *
     * @return store provider name
     */
    @WithName("dispatch-intent-store")
    @WithDefault("memory")
    String dispatchIntentStore();

    /**
     * DynamoDB table name used when {@link #dispatchIntentStore()} is {@code dynamo}.
     *
     * @return DynamoDB table name
     */
    @WithName("dispatch-intent-table")
    @WithDefault("tpf_foreign_dispatch_intent")
    String dispatchIntentTable();

    /**
     * Tier-2 Polyglot Custom Connector host settings.
     *
     * @return connector host configuration
     */
    @WithName("connector")
    ConnectorHostConfig connector();

    /** Default and named Tier-2 {@code polyglot.connector} host connection settings. */
    interface ConnectorHostConfig {

        /**
         * Default base URI for Tier-2 {@code polyglot.connector} connections.
         *
         * @return default connector host URI
         */
        @WithName("host-uri")
        Optional<String> hostUri();

        /**
         * Shared HMAC-SHA256 secret for Tier-2 {@code polyglot.connector} requests
         * (falls back to {@link PolyglotRuntimeConfig#sharedSecret()} when absent).
         *
         * @return connector HMAC secret
         */
        @WithName("shared-secret")
        Optional<String> sharedSecret();

        /**
         * Scoped provider secrets delivered strictly to authenticated Tier-2 connector hosts
         * (never exposed to pure business functions).
         *
         * @return default connector secrets
         */
        @WithName("secrets")
        Map<String, String> secrets();

        /**
         * Named connection overrides keyed by the {@code config.connection} name declared in {@code pipeline.yaml}.
         *
         * @return named connector host connections
         */
        @WithName("connections")
        Map<String, NamedConnectionConfig> connections();
    }

    /** Per-connection Tier-2 {@code polyglot.connector} override. */
    interface NamedConnectionConfig {

        /**
         * Base URI for this named polyglot connector host connection.
         *
         * @return named connection host URI
         */
        @WithName("host-uri")
        Optional<String> hostUri();

        /**
         * Shared HMAC-SHA256 secret for this named polyglot connector host connection.
         *
         * @return named connection shared secret
         */
        @WithName("shared-secret")
        Optional<String> sharedSecret();

        /**
         * Scoped provider secrets merged for this named polyglot connector host connection.
         *
         * @return named connection secrets
         */
        @WithName("secrets")
        Map<String, String> secrets();
    }
}

package org.pipelineframework.connector.polyglot;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandDispatchIdentity;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.CommandReconciliationInvocation;
import org.pipelineframework.connector.CommandReconciliationResult;
import org.pipelineframework.connector.CommandRecoveryTarget;
import org.pipelineframework.connector.CommandReference;
import org.pipelineframework.connector.CommandReferencePurpose;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;

/**
 * Tier-2 Polyglot Custom Query/Command Connector Host SPI (ADR-0092).
 *
 * <p>Delegates only provider-specific {@code query}, {@code dispatch}, and {@code reconcile}
 * calls to an authenticated Node/Python custom Connector host over HMAC-SHA256 HTTP while
 * TPF's Java engine retains 100% authority over {@code CommandEffectStore},
 * {@code QueryCaptureStore}, {@code providerIdempotencyKey}, pre-send
 * {@code CommandRecoveryBinding}, {@code RETURN_RECORDED} replay, automatic {@code AMBIGUOUS}
 * reconciliation, ADR-0021 host {@link ConnectionResolver} secret delivery, and fail-closed
 * canonical v3 schema validation.</p>
 */
public final class PolyglotConnectorProvider
    implements ConnectorProvider<PolyglotConnectorProvider.ProviderConfig> {

    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("polyglot.connector");
    public static final String CONNECTOR_OPERATION_HEADER = "X-TPF-Connector-Operation";
    public static final String IDEMPOTENCY_KEY_HEADER = "X-TPF-Idempotency-Key";
    public static final String QUERY_PATH_PREFIX = "/tpf/connector/query/";
    public static final String COMMAND_PATH_PREFIX = "/tpf/connector/command/";

    private static final ObjectMapper JSON = PipelineJson.mapper().copy()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final int DEFAULT_TIMEOUT_MS = 5_000;
    private static final ConnectorConfigSchema<ProviderConfig> PROVIDER_SCHEMA =
        ConnectorConfigSchema.record(ProviderConfig.class, "polyglot.connector.provider", 1);
    private static final ConnectorConfigSchema<OperationConfig> OPERATION_SCHEMA =
        ConnectorConfigSchema.record(OperationConfig.class, "polyglot.connector.operation", 1);

    /** Provider binding configuration bound via {@link ConnectorConfigurationDocument}. */
    public record ProviderConfig(
        String connection,
        Optional<String> recoveryTargetIdentity,
        Optional<Integer> timeoutMs
    ) {
        public ProviderConfig {
            if (Objects.requireNonNull(connection, "connection must not be null").isBlank()) {
                throw new IllegalArgumentException("connection must not be blank");
            }
            recoveryTargetIdentity = Objects.requireNonNull(recoveryTargetIdentity, "recoveryTargetIdentity")
                .filter(v -> !v.isBlank());
            timeoutMs = Objects.requireNonNull(timeoutMs, "timeoutMs");
        }

        Duration effectiveTimeout() {
            int ms = timeoutMs.filter(v -> v > 0).orElse(DEFAULT_TIMEOUT_MS);
            return Duration.ofMillis(ms);
        }

        String effectiveRecoveryTargetIdentity(String operationId) {
            return recoveryTargetIdentity.orElse("polyglot-connector:" + connection + ":" + operationId);
        }
    }

    /** Per-operation configuration bound on Query/Command step descriptors. */
    public record OperationConfig(Optional<String> scope) {
        public OperationConfig {
            scope = Objects.requireNonNull(scope, "scope").filter(v -> !v.isBlank());
        }
    }

    private final PolyglotConnectorSpec spec;
    private final HttpClient httpClient;
    private final List<ConnectorOperation> operations;
    private final AtomicReference<Optional<ActiveState>> active = new AtomicReference<>(Optional.empty());

    public PolyglotConnectorProvider(PolyglotConnectorSpec spec) {
        this(spec, HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(Duration.ofSeconds(3))
            .build());
    }

    PolyglotConnectorProvider(PolyglotConnectorSpec spec, HttpClient httpClient) {
        this.spec = Objects.requireNonNull(spec, "spec must not be null");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        List<ConnectorOperation> built = new ArrayList<>();
        spec.queries().forEach(q -> built.add(new PolyglotQueryOperation(q)));
        spec.commands().forEach(c -> built.add(new PolyglotCommandOperation(c)));
        this.operations = List.copyOf(built);
    }

    @Override
    public ConnectorProviderId id() {
        return PROVIDER_ID;
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return operations;
    }

    @Override
    public Optional<ConnectorConfigSchema<ProviderConfig>> configurationSchema() {
        return Optional.of(PROVIDER_SCHEMA);
    }

    @Override
    public CompletionStage<Void> start(ConnectorRuntimeContext context, ProviderConfig configuration) {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(configuration, "configuration must not be null");
        active.set(Optional.of(new ActiveState(context, configuration)));
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        active.set(Optional.empty());
        return CompletableFuture.completedStage(null);
    }

    private ActiveState requireActive() {
        return active.get().orElseThrow(() -> new IllegalStateException("PolyglotConnectorProvider is not active"));
    }

    private CompletionStage<PolyglotConnectorConnection> resolveConnection(
        ActiveState state, ConnectorExecutionContext executionContext
    ) {
        ConnectionResolver resolver = state.context().connectionResolver().orElseThrow(
            () -> new ConnectionResolutionException("No host ConnectionResolver is configured for polyglot.connector"));
        return resolver.resolve(new ConnectionResolutionRequest<>(
            new ConnectionRef(state.config().connection()),
            PolyglotConnectorConnection.class,
            executionContext));
    }

    private CompletionStage<HttpResponse<byte[]>> postSigned(
        ActiveState state,
        PolyglotConnectorConnection conn,
        String path,
        String operationId,
        Optional<String> idempotencyKey,
        ObjectNode body
    ) {
        byte[] payload;
        try {
            payload = JSON.writeValueAsBytes(body);
        } catch (IOException serializationError) {
            return CompletableFuture.failedStage(serializationError);
        }
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            conn.sharedSecret(), "POST", path, timestamp, nonce, payload);
        String base = conn.hostUri().toString();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(base + path))
            .timeout(state.config().effectiveTimeout())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header(CONNECTOR_OPERATION_HEADER, operationId)
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature)
            .POST(HttpRequest.BodyPublishers.ofByteArray(payload));
        idempotencyKey.ifPresent(key -> builder.header(IDEMPOTENCY_KEY_HEADER, key));
        ClassLoader callerLoader = Thread.currentThread().getContextClassLoader();
        CompletableFuture<HttpResponse<byte[]>> delivered = new CompletableFuture<>();
        httpClient.sendAsync(builder.build(), boundedByteArrayHandler(spec.maxResponseBytes()))
            .whenComplete((response, failure) -> {
                Thread current = Thread.currentThread();
                ClassLoader previous = current.getContextClassLoader();
                current.setContextClassLoader(callerLoader);
                try {
                    if (failure != null) {
                        delivered.completeExceptionally(failure);
                    } else {
                        delivered.complete(response);
                    }
                } finally {
                    current.setContextClassLoader(previous);
                }
            });
        return delivered;
    }

    private static HttpResponse.BodyHandler<byte[]> boundedByteArrayHandler(int maxBytes) {
        return responseInfo -> new HttpResponse.BodySubscriber<>() {
            private final CompletableFuture<byte[]> body = new CompletableFuture<>();
            private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            private Flow.Subscription subscription;
            private int totalBytes;
            private boolean exceeded;

            @Override
            public void onSubscribe(Flow.Subscription sub) {
                this.subscription = sub;
                sub.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(List<ByteBuffer> items) {
                if (exceeded) {
                    return;
                }
                for (ByteBuffer item : items) {
                    int remaining = item.remaining();
                    if (remaining > maxBytes - totalBytes) {
                        exceeded = true;
                        if (subscription != null) {
                            subscription.cancel();
                        }
                        body.completeExceptionally(
                            new IOException("Polyglot connector response exceeded " + maxBytes + " bytes"));
                        return;
                    }
                    byte[] chunk = new byte[remaining];
                    item.get(chunk);
                    buffer.write(chunk, 0, remaining);
                    totalBytes += remaining;
                }
            }

            @Override
            public void onError(Throwable throwable) {
                body.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                if (!exceeded) {
                    body.complete(buffer.toByteArray());
                }
            }

            @Override
            public CompletionStage<byte[]> getBody() {
                return body;
            }
        };
    }

    private JsonNode validateCanonical(String canonicalTypeId, JsonNode node) {
        if (node == null || node.isNull()) {
            throw new IllegalArgumentException("canonical payload for " + canonicalTypeId + " must not be null");
        }
        try {
            String canonicalJson = spec.catalogue().validateAndCanonicalize(canonicalTypeId, JSON.writeValueAsString(node));
            return strictReader().readTree(canonicalJson);
        } catch (IOException parseFailure) {
            throw new IllegalArgumentException("invalid JSON for " + canonicalTypeId, parseFailure);
        }
    }

    private JsonNode parseStrictObject(byte[] body) throws IOException {
        if (body == null || body.length > spec.maxResponseBytes()) {
            throw new IOException("Polyglot connector response missing or exceeded " + spec.maxResponseBytes() + " bytes");
        }
        JsonNode root = strictReader().readTree(body);
        if (root == null || !root.isObject()) {
            throw new IOException("Polyglot connector response must be a JSON object");
        }
        return root;
    }

    private static ObjectReader strictReader() {
        return JSON.reader()
            .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    }

    private final class PolyglotQueryOperation implements QueryOperation<JsonNode, OperationConfig, JsonNode> {
        private final PolyglotConnectorSpec.QueryOperationSpec opSpec;

        private PolyglotQueryOperation(PolyglotConnectorSpec.QueryOperationSpec opSpec) {
            this.opSpec = opSpec;
        }

        @Override public String id() { return opSpec.operationId(); }
        @Override public int majorVersion() { return opSpec.majorVersion(); }
        @Override public QueryCapabilities capabilities() { return QueryCapabilities.conservative(); }
        @Override public Optional<ConnectorConfigSchema<OperationConfig>> configurationSchema() {
            return Optional.of(OPERATION_SCHEMA);
        }

        @Override
        public CompletionStage<QueryOutcome<JsonNode>> query(QueryInvocation<JsonNode, OperationConfig, JsonNode> invocation) {
            final ActiveState state;
            try {
                state = requireActive();
            } catch (RuntimeException inactive) {
                return CompletableFuture.completedStage(
                    new QueryOutcome.TemporarilyUnavailable<>("polyglot-connector-inactive"));
            }
            final JsonNode validatedInput;
            try {
                validatedInput = validateCanonical(opSpec.inputCanonicalTypeId(), invocation.input());
            } catch (RuntimeException invalidInput) {
                return CompletableFuture.completedStage(
                    new QueryOutcome.TerminalFailure<>("polyglot-query-invalid-input"));
            }

            return resolveConnection(state, invocation.executionContext()).handle((conn, connErr) -> {
                if (connErr != null || conn == null) {
                    return CompletableFuture.<QueryOutcome<JsonNode>>completedStage(
                        new QueryOutcome.TemporarilyUnavailable<>("polyglot-connection-unavailable"));
                }
                ObjectNode req = JSON.createObjectNode();
                req.put("operation", opSpec.operationId());
                req.set("input", validatedInput);
                req.set("secrets", JSON.valueToTree(conn.authorizedSecrets()));
                invocation.configuration().scope().ifPresent(s -> req.put("scope", s));

                return postSigned(state, conn, QUERY_PATH_PREFIX + opSpec.operationId(),
                        opSpec.operationId(), Optional.empty(), req)
                    .<QueryOutcome<JsonNode>>handle((resp, transportErr) -> {
                        if (transportErr != null) {
                            return new QueryOutcome.TemporarilyUnavailable<>("polyglot-query-transport-error");
                        }
                        int status = resp.statusCode();
                        if (status == 401 || status == 403) {
                            return new QueryOutcome.AuthenticationRequired<>("polyglot-query-unauthorized");
                        }
                        if (status != 200) {
                            return new QueryOutcome.TemporarilyUnavailable<>("polyglot-query-http-" + status);
                        }
                        JsonNode envelope;
                        try {
                            envelope = parseStrictObject(resp.body());
                        } catch (IOException malformed) {
                            return new QueryOutcome.TerminalFailure<>("polyglot-query-invalid-response");
                        }
                        return switch (envelope.path("outcome").asText("")) {
                            case "FOUND" -> {
                                try {
                                    JsonNode canonical = validateCanonical(
                                        opSpec.outputCanonicalTypeId(), envelope.get("payload"));
                                    yield new QueryOutcome.Found<>(canonical);
                                } catch (RuntimeException schemaViolation) {
                                    yield new QueryOutcome.TerminalFailure<>("polyglot-connector-schema-violation");
                                }
                            }
                            case "NOT_FOUND" -> new QueryOutcome.NotFound<>(
                                envelope.path("code").asText("not-found"));
                            case "AUTH_REQUIRED" -> new QueryOutcome.AuthenticationRequired<>(
                                envelope.path("code").asText("auth-required"));
                            case "TEMPORARILY_UNAVAILABLE" -> new QueryOutcome.TemporarilyUnavailable<>(
                                envelope.path("code").asText("temporarily-unavailable"));
                            case "TERMINAL_FAILURE" -> new QueryOutcome.TerminalFailure<>(
                                envelope.path("code").asText("terminal-failure"));
                            default -> new QueryOutcome.TerminalFailure<>("polyglot-query-invalid-outcome");
                        };
                    });
            }).thenCompose(stage -> stage);
        }
    }

    private final class PolyglotCommandOperation implements CommandOperation<JsonNode, OperationConfig, JsonNode> {
        private final PolyglotConnectorSpec.CommandOperationSpec opSpec;
        private final CommandCapabilities capabilities;

        private PolyglotCommandOperation(PolyglotConnectorSpec.CommandOperationSpec opSpec) {
            this.opSpec = opSpec;
            Set<String> refKinds = opSpec.receiptReferenceKind().map(Set::of).orElse(Set.of());
            this.capabilities = new CommandCapabilities(
                true,
                true,
                opSpec.reconciliationSupported(),
                CommandExecutionPosture.UNSPECIFIED,
                CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED,
                opSpec.reconciliationSupported(),
                refKinds);
        }

        @Override public String id() { return opSpec.operationId(); }
        @Override public int majorVersion() { return opSpec.majorVersion(); }
        @Override public CommandCapabilities capabilities() { return capabilities; }
        @Override public Optional<ConnectorConfigSchema<OperationConfig>> configurationSchema() {
            return Optional.of(OPERATION_SCHEMA);
        }

        @Override
        public Optional<CommandRecoveryTarget> recoveryTarget(CommandInvocation<JsonNode, OperationConfig> invocation) {
            if (!opSpec.reconciliationSupported()) {
                return Optional.empty();
            }
            ActiveState state = requireActive();
            String identity = state.config().effectiveRecoveryTargetIdentity(opSpec.operationId());
            Map<String, Object> configMap = invocation.configuration() != null
                && invocation.configuration().scope().isPresent()
                ? Map.of("scope", invocation.configuration().scope().orElseThrow())
                : Map.of();
            ConnectorConfigurationSnapshot snapshot = ConnectorConfigurationSnapshot.from(
                OPERATION_SCHEMA, new ConnectorConfigurationDocument(configMap), false);
            return Optional.of(new CommandRecoveryTarget(identity, snapshot));
        }

        @Override
        public CompletionStage<CommandOutcome<JsonNode>> dispatch(CommandInvocation<JsonNode, OperationConfig> invocation) {
            final ActiveState state;
            try {
                state = requireActive();
            } catch (RuntimeException inactive) {
                return CompletableFuture.completedStage(
                    new CommandOutcome.RetryableFailure<>("polyglot-connector-inactive", List.of()));
            }
            CommandDispatchIdentity identity = invocation.dispatchIdentity().orElse(null);
            if (identity == null) {
                return CompletableFuture.completedStage(
                    new CommandOutcome.TerminalFailure<>("polyglot-command-missing-dispatch-identity", List.of()));
            }
            final JsonNode validatedInput;
            try {
                validatedInput = validateCanonical(opSpec.inputCanonicalTypeId(), invocation.input());
            } catch (RuntimeException invalidInput) {
                return CompletableFuture.completedStage(
                    new CommandOutcome.TerminalFailure<>("polyglot-command-invalid-input", List.of()));
            }

            return resolveConnection(state, invocation.executionContext()).handle((conn, connErr) -> {
                if (connErr != null || conn == null) {
                    return CompletableFuture.<CommandOutcome<JsonNode>>completedStage(
                        new CommandOutcome.RetryableFailure<>("polyglot-connection-unavailable", List.of()));
                }
                ObjectNode req = JSON.createObjectNode();
                req.put("operation", opSpec.operationId());
                req.set("input", validatedInput);
                req.set("dispatchIdentity", encodeDispatchIdentity(identity));
                req.set("secrets", JSON.valueToTree(conn.authorizedSecrets()));
                if (invocation.configuration() != null) {
                    invocation.configuration().scope().ifPresent(s -> req.put("scope", s));
                }

                String path = COMMAND_PATH_PREFIX + opSpec.operationId() + "/dispatch";
                return postSigned(state, conn, path, opSpec.operationId(),
                        Optional.of(identity.providerIdempotencyKey()), req)
                    .<CommandOutcome<JsonNode>>handle((resp, transportErr) -> {
                        if (transportErr != null) {
                            return ambiguous("polyglot-command-dispatch-ambiguous");
                        }
                        int status = resp.statusCode();
                        if (status == 401 || status == 403) {
                            return new CommandOutcome.TerminalFailure<>(
                                "polyglot-command-unauthorized", List.of());
                        }
                        if (status != 200) {
                            return ambiguous("polyglot-command-http-" + status);
                        }
                        JsonNode envelope;
                        try {
                            envelope = parseStrictObject(resp.body());
                        } catch (IOException malformed) {
                            return ambiguous("polyglot-command-invalid-response");
                        }
                        return switch (envelope.path("outcome").asText("")) {
                            case "SUCCEEDED" -> {
                                try {
                                    JsonNode canonical = validateCanonical(
                                        opSpec.outputCanonicalTypeId(), envelope.get("payload"));
                                    List<CommandReference> refs = extractReceiptReferences(envelope);
                                    yield new CommandOutcome.Succeeded<>(
                                        canonical,
                                        new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false),
                                        refs);
                                } catch (RuntimeException schemaViolation) {
                                    yield ambiguous("polyglot-connector-schema-violation");
                                }
                            }
                            case "RETRYABLE_FAILURE" -> new CommandOutcome.RetryableFailure<>(
                                envelope.path("code").asText("retryable-failure"), List.of());
                            case "TERMINAL_FAILURE" -> new CommandOutcome.TerminalFailure<>(
                                envelope.path("code").asText("terminal-failure"), List.of());
                            default -> ambiguous(envelope.path("code").asText("polyglot-command-ambiguous"));
                        };
                    });
            }).thenCompose(stage -> stage);
        }

        @Override
        public CompletionStage<CommandReconciliationResult<JsonNode>> reconcile(
            CommandReconciliationInvocation<JsonNode, OperationConfig> invocation
        ) {
            if (!opSpec.reconciliationSupported()) {
                return CompletableFuture.completedStage(
                    new CommandReconciliationResult.Unresolved<>("reconciliation-not-supported"));
            }
            final ActiveState state;
            try {
                state = requireActive();
            } catch (RuntimeException inactive) {
                return CompletableFuture.completedStage(
                    new CommandReconciliationResult.Unresolved<>("polyglot-connector-inactive"));
            }
            CommandDispatchIdentity identity = invocation.invocation().dispatchIdentity().orElse(null);
            if (identity == null) {
                return CompletableFuture.completedStage(
                    new CommandReconciliationResult.Unresolved<>("reconcile-missing-dispatch-identity"));
            }

            return resolveConnection(state, invocation.invocation().executionContext()).handle((conn, connErr) -> {
                if (connErr != null || conn == null) {
                    return CompletableFuture.<CommandReconciliationResult<JsonNode>>completedStage(
                        new CommandReconciliationResult.Unresolved<>("polyglot-connection-unavailable"));
                }
                ObjectNode req = JSON.createObjectNode();
                req.put("operation", opSpec.operationId());
                req.put("recoveryTargetIdentity", invocation.binding().target().identity());
                req.set("dispatchIdentity", encodeDispatchIdentity(identity));
                req.set("secrets", JSON.valueToTree(conn.authorizedSecrets()));

                String path = COMMAND_PATH_PREFIX + opSpec.operationId() + "/reconcile";
                return postSigned(state, conn, path, opSpec.operationId(),
                        Optional.of(identity.providerIdempotencyKey()), req)
                    .<CommandReconciliationResult<JsonNode>>handle((resp, transportErr) -> {
                        if (transportErr != null) {
                            return new CommandReconciliationResult.Unresolved<>("reconcile-transport-error");
                        }
                        if (resp.statusCode() != 200) {
                            return new CommandReconciliationResult.Unresolved<>("reconcile-http-" + resp.statusCode());
                        }
                        JsonNode envelope;
                        try {
                            envelope = parseStrictObject(resp.body());
                        } catch (IOException malformed) {
                            return new CommandReconciliationResult.Unresolved<>("reconcile-invalid-response");
                        }
                        if (!"CONFIRMED_SUCCEEDED".equals(envelope.path("outcome").asText(""))) {
                            return new CommandReconciliationResult.Unresolved<>(
                                envelope.path("code").asText("reconcile-unresolved"));
                        }
                        JsonNode canonical;
                        try {
                            canonical = validateCanonical(opSpec.outputCanonicalTypeId(), envelope.get("payload"));
                        } catch (RuntimeException schemaViolation) {
                            return new CommandReconciliationResult.Unresolved<>("reconcile-schema-violation");
                        }
                        String refValue = envelope.path("receiptReference").asText("");
                        String refKind = opSpec.receiptReferenceKind().orElse("");
                        if (refValue.isBlank() || refKind.isBlank()) {
                            return new CommandReconciliationResult.Unresolved<>("reconcile-missing-receipt");
                        }
                        CommandReference receipt = new CommandReference(
                            refKind, refValue, CommandReferencePurpose.RECONCILIATION);
                        CommandOutcome.Succeeded<JsonNode> succeeded = new CommandOutcome.Succeeded<>(
                            canonical,
                            new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false),
                            List.of(receipt));
                        return new CommandReconciliationResult.ConfirmedSucceeded<>(
                            invocation.binding(), succeeded, receipt);
                    });
            }).thenCompose(stage -> stage);
        }

        private List<CommandReference> extractReceiptReferences(JsonNode envelope) {
            String refValue = envelope.path("receiptReference").asText("");
            if (refValue.isBlank() || opSpec.receiptReferenceKind().isEmpty()) {
                return List.of();
            }
            return List.of(new CommandReference(
                opSpec.receiptReferenceKind().orElseThrow(), refValue, CommandReferencePurpose.RECONCILIATION));
        }

        private static CommandOutcome<JsonNode> ambiguous(String code) {
            return new CommandOutcome.Ambiguous<>(code, CommandConfirmation.none(), Set.of(), List.of());
        }

        private static ObjectNode encodeDispatchIdentity(CommandDispatchIdentity identity) {
            ObjectNode node = JSON.createObjectNode();
            node.put("commandId", identity.commandId());
            node.put("occurrenceId", identity.occurrenceId());
            node.put("attemptId", identity.attemptId());
            node.put("providerIdempotencyKey", identity.providerIdempotencyKey());
            return node;
        }
    }

    private record ActiveState(ConnectorRuntimeContext context, ProviderConfig config) {
    }
}

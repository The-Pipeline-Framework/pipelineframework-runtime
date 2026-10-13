package org.pipelineframework.aws.durable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.JsonFactory;
import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

/** Validates the deployment-owned Lambda configuration before resolving pinned signing keys. */
public final class AwsDurableDeploymentConfigSourceFactory implements ConfigSourceFactory {
    private static final String PREFIX = "tpf.aws.durable.";
    private final Function<GetSecretValueRequest, GetSecretValueResponse> secrets;

    public AwsDurableDeploymentConfigSourceFactory() {
        this(request -> {
            try (var client = SecretsManagerClient.builder()
                .overrideConfiguration(config -> config.apiCallTimeout(Duration.ofSeconds(15))
                    .apiCallAttemptTimeout(Duration.ofSeconds(5))).build()) {
                return client.getSecretValue(request);
            }
        });
    }

    AwsDurableDeploymentConfigSourceFactory(Function<GetSecretValueRequest, GetSecretValueResponse> secrets) {
        this.secrets = Objects.requireNonNull(secrets);
    }

    @Override
    public Iterable<ConfigSource> getConfigSources(ConfigSourceContext originalContext) {
        Map<String, String> settings = deploymentSettings(originalContext);
        ConfigSourceContext context = new ConfigSourceContext() {
            @Override public ConfigValue getValue(String name) {
                return settings.containsKey(name)
                    ? ConfigValue.builder().withName(name).withValue(settings.get(name))
                        .withConfigSourceName("aws-durable-deployment-settings").withConfigSourceOrdinal(500).build()
                    : originalContext.getValue(name);
            }
            @Override public java.util.Iterator<String> iterateNames() { return originalContext.iterateNames(); }
        };
        if (value(context, PREFIX + "deployment-validation").isEmpty()) {
            if (!settings.isEmpty() || value(context, PREFIX + "resume-token-secret-arn").isPresent()
                || value(context, PREFIX + "worker-secret-arn").isPresent()) {
                throw new IllegalArgumentException("AWS Durable deployment requires preflight validation");
            }
            return List.of(); // Plain coordinator packages and ordinary builds do not load application secrets.
        }
        requireEqual(context, PREFIX + "deployment-validation", "validated");
        requireEqual(context, "pipeline.orchestrator.mode", "QUEUE_ASYNC");
        requireEqual(context, "pipeline.orchestrator.coordination-host", "AWS_DURABLE");
        requireEqual(context, "pipeline.orchestrator.process-loops-disabled", "true");
        requireEqual(context, "pipeline.orchestrator.strict-startup", "true");
        String idempotencyPolicy = required(context, "pipeline.orchestrator.idempotency-policy");
        if (!Set.of("CLIENT_KEY_REQUIRED", "OPTIONAL_CLIENT_KEY").contains(idempotencyPolicy)) {
            throw new IllegalArgumentException("AWS Durable admission recovery requires a client key policy");
        }
        requireEqual(context, "pipeline.orchestrator.state-provider", "dynamo");
        requireEqual(context, "pipeline.orchestrator.dispatcher-provider", "sqs");
        requireEqual(context, "pipeline.orchestrator.dlq-provider", "sqs");
        requireEqual(context, "pipeline.orchestrator.releases.registry.provider", "dynamo");
        requireEqual(context, "pipeline.orchestrator.releases.storage.provider", "s3");
        for (String table : List.of("execution", "execution-key", "execution-payload", "await-interaction",
            "await-interaction-key", "await-completion-lifecycle", "await-unit", "await-admission", "release", "worker")) {
            if (!required(context, "pipeline.orchestrator.dynamo." + table + "-table")
                .matches("[A-Za-z0-9_.-]{3,255}")) {
                throw new IllegalArgumentException("Invalid semantic table configuration: " + table);
            }
        }
        for (String property : List.of("pipeline.orchestrator.pipeline-id", "pipeline.orchestrator.release-version",
            "pipeline.orchestrator.releases.storage.s3.bucket", "pipeline.orchestrator.queue-url",
            "pipeline.orchestrator.dlq-url", "pipeline.orchestrator.worker.sqs.request-queue-url",
            "pipeline.orchestrator.worker.sqs.response-queue-url", "pipeline.orchestrator.worker.sqs.pipeline-id",
            "pipeline.orchestrator.worker.sqs.contract-version", "pipeline.orchestrator.worker.sqs.release-version",
            "pipeline.orchestrator.worker.sqs.artifact-id", "pipeline.orchestrator.worker.sqs.artifact-digest",
            PREFIX + "binding-table")) {
            required(context, property);
        }
        new AwsDurableHostLimits(
            Math.toIntExact(number(context, PREFIX + "estimated-operations")),
            number(context, PREFIX + "estimated-written-state-bytes"),
            Duration.ofSeconds(number(context, PREFIX + "execution-timeout-seconds")),
            Duration.ofDays(number(context, PREFIX + "retention-days")),
            Duration.ofSeconds(number(context, PREFIX + "worker-timeout-seconds")),
            Duration.ofSeconds(number(context, PREFIX + "sqs-visibility-seconds")));
        if (number(context, PREFIX + "callback-retention-seconds") <= 0
            || number(context, PREFIX + "action-timeout-seconds") <= 0
            || number(context, PREFIX + "action-timeout-seconds") > 900) {
            throw new IllegalArgumentException("Callback retention and bounded action timeout must be positive");
        }
        for (String property : List.of("pipeline.orchestrator.resume-token-secret",
            "pipeline.orchestrator.worker.sqs.shared-secret", "pipeline.orchestrator.worker.sqs.shared-secret-ref")) {
            if (value(context, property).isPresent()) {
                throw new IllegalArgumentException("Pinned deployment must not also configure " + property);
            }
        }
        // Validate both references before the first network call; absent or floating versions fail closed.
        GetSecretValueRequest resume = request(context, "resume-token");
        GetSecretValueRequest worker = request(context, "worker");
        var resolved = new java.util.HashMap<>(settings);
        resolved.put("pipeline.orchestrator.resume-token-secret", resolve(resume));
        resolved.put("pipeline.orchestrator.worker.sqs.shared-secret", resolve(worker));
        return List.of(new PinnedSecrets(resolved));
    }

    private Map<String, String> deploymentSettings(ConfigSourceContext context) {
        Optional<String> encoded = value(context, PREFIX + "configuration");
        if (encoded.isEmpty()) {
            return Map.of();
        }
        Set<String> allowed = Set.of("mode", "strict-startup", "state-provider", "dispatcher-provider", "dlq-provider",
            "pipeline-id", "release-version", "idempotency-policy", "coordination-host", "process-loops-disabled", "queue-url", "dlq-url",
            "dynamo.region", "dynamo.execution-table", "dynamo.execution-key-table", "dynamo.execution-payload-table",
            "dynamo.await-interaction-table", "dynamo.await-interaction-key-table", "dynamo.await-completion-lifecycle-table", "dynamo.await-unit-table",
            "dynamo.await-admission-table", "dynamo.release-table", "dynamo.worker-table", "releases.registry.provider",
            "releases.storage.provider", "releases.storage.s3.bucket", "worker.sqs.pipeline-id",
            "worker.sqs.contract-version", "worker.sqs.release-version", "worker.sqs.artifact-id",
            "worker.sqs.artifact-digest", "worker.sqs.visibility-timeout",
            "worker.sqs.request-queue-url", "worker.sqs.response-queue-url");
        try {
            var mapper = new ObjectMapper(JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build());
            var object = mapper.readTree(encoded.orElseThrow());
            if (!object.isObject()) throw new IllegalArgumentException("Deployment settings must be an object");
            var settings = new java.util.HashMap<String, String>();
            var entries = object.fields();
            while (entries.hasNext()) {
                var entry = entries.next();
                if (!allowed.contains(entry.getKey()) || !entry.getValue().isTextual()) {
                    throw new IllegalArgumentException("Unsupported deployment setting");
                }
                settings.put("pipeline.orchestrator." + entry.getKey(), entry.getValue().asText());
            }
            return Map.copyOf(settings);
        } catch (Exception invalid) {
            throw new IllegalArgumentException("Invalid AWS Durable deployment settings");
        }
    }

    private GetSecretValueRequest request(ConfigSourceContext context, String purpose) {
        String arn = required(context, PREFIX + purpose + "-secret-arn");
        String version = required(context, PREFIX + purpose + "-secret-version");
        if (!arn.matches("arn:[a-z-]+:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+-[A-Za-z0-9]{6}")
            || !version.matches("[A-Za-z0-9-]{32,64}")) {
            throw new IllegalArgumentException("A full Secrets Manager ARN and immutable version are required for " + purpose);
        }
        return GetSecretValueRequest.builder().secretId(arn).versionId(version).build();
    }

    private String resolve(GetSecretValueRequest request) {
        final GetSecretValueResponse response;
        try {
            response = secrets.apply(request);
        } catch (RuntimeException failure) {
            // Provider errors can contain sensitive request/context values; do not attach or log them.
            throw new IllegalStateException("Pinned deployment signing key could not be loaded");
        }
        if (response == null || !request.secretId().equals(response.arn())
            || !request.versionId().equals(response.versionId())
            || response.secretString() == null || response.secretString().length() < 32) {
            throw new IllegalStateException("Pinned signing key response has invalid identity, version or length");
        }
        return response.secretString();
    }

    private void requireEqual(ConfigSourceContext context, String name, String expected) {
        if (!required(context, name).equals(expected)) {
            throw new IllegalArgumentException("Unsupported deployment configuration: " + name);
        }
    }

    private long number(ConfigSourceContext context, String name) {
        try {
            return Long.parseLong(required(context, name));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("Invalid deployment limit: " + name);
        }
    }

    private String required(ConfigSourceContext context, String name) {
        return Optional.ofNullable(context.getValue(name))
            .filter(config -> !config.isDefault())
            .map(ConfigValue::getValue).filter(v -> !v.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("Explicit deployment configuration required: " + name));
    }

    private Optional<String> value(ConfigSourceContext context, String name) {
        return Optional.ofNullable(context.getValue(name)).map(ConfigValue::getValue);
    }

    private record PinnedSecrets(Map<String, String> values) implements ConfigSource {
        private PinnedSecrets { values = Map.copyOf(values); }
        @Override public Map<String, String> getProperties() { return values; }
        @Override public Set<String> getPropertyNames() { return values.keySet(); }
        @Override public String getValue(String name) { return values.get(name); }
        @Override public String getName() { return "aws-durable-pinned-signing-keys"; }
        @Override public int getOrdinal() { return 500; }
        @Override public String toString() { return getName(); }
    }
}

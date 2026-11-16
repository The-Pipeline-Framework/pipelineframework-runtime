package org.pipelineframework.aws.durable;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.aws.durable.AwsDurableDeploymentConfigSourceFactory.PinnedParameter;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig.RegisteredRestTargetConfig;
import org.pipelineframework.orchestrator.RegisteredRestWorkerTargetSource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.ssm.SsmClient;

/** Reads complete, immutable versions at a fixed deployment-owned routing key. */
@ApplicationScoped
public class AwsDurableRegisteredRestTargetSource implements RegisteredRestWorkerTargetSource {
    private static final String TARGET_PREFIX = "pipeline.orchestrator.worker.targets.";
    static final String CREDENTIAL_PREFIX = "config:tpf.aws.durable.rest-parameter.";
    private static final int MAX_BYTES = 256 * 1024;
    private static final long REFRESH_NANOS = Duration.ofSeconds(5).toNanos();
    private static final long RETRY_NANOS = Duration.ofSeconds(1).toNanos();
    private static final Set<String> FIELDS = Set.of("entry-id", "tenant-id", "pipeline-id", "contract-version",
        "release-version", "worker-id", "endpoint", "shared-secret-ref", "artifact-id", "artifact-digest");

    @ConfigProperty(name = "tpf.aws.durable.refreshable-rest-targets", defaultValue = "false")
    boolean refreshable;
    @ConfigProperty(name = "tpf.aws.durable.rest-target-key")
    Optional<String> key;
    @ConfigProperty(name = "pipeline.orchestrator.releases.storage.s3.bucket")
    Optional<String> bucket;
    @ConfigProperty(name = "tpf.aws.durable.rest-worker-parameter-arn-prefix")
    Optional<String> parameterArnPrefix;

    private record Cached(Map<String, RegisteredRestTargetConfig> targets, long expiresAt) {}
    private volatile Cached cached;
    private final Map<String, String> credentials = new ConcurrentHashMap<>();
    private final LongSupplier clock;
    private Uni<Map<String, RegisteredRestTargetConfig>> inFlight;
    private S3Client s3;
    private SsmClient parameters;

    public AwsDurableRegisteredRestTargetSource() {
        this(null, null, System::nanoTime);
    }

    AwsDurableRegisteredRestTargetSource(S3Client s3, SsmClient parameters, LongSupplier clock) {
        this.s3 = s3;
        this.parameters = parameters;
        this.clock = clock;
    }

    @Override
    public boolean enabled() {
        return refreshable;
    }

    @Override
    public Uni<Map<String, RegisteredRestTargetConfig>> snapshot() {
        return Uni.createFrom().deferred(() -> {
            Cached observed = cached;
            return fresh(observed) ? cachedResult(observed) : refresh();
        });
    }

    private boolean fresh(Cached observed) {
        return observed != null && clock.getAsLong() - observed.expiresAt() < 0;
    }

    private static Uni<Map<String, RegisteredRestTargetConfig>> cachedResult(Cached observed) {
        if (observed.targets() == null) {
            return Uni.createFrom().failure(new IllegalStateException("Trusted REST routing snapshot is unavailable"));
        }
        return Uni.createFrom().item(observed.targets());
    }

    private synchronized Uni<Map<String, RegisteredRestTargetConfig>> refresh() {
        Cached observed = cached;
        if (fresh(observed)) return cachedResult(observed);
        if (inFlight == null) {
            inFlight = Uni.createFrom().item(this::readTrusted)
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .onItemOrFailure().invoke(this::finishRefresh)
                .memoize().indefinitely();
        }
        return inFlight;
    }

    private Map<String, RegisteredRestTargetConfig> readTrusted() {
        try {
            return read();
        } catch (IOException | RuntimeException failure) {
            throw new IllegalStateException("Trusted REST routing snapshot is unavailable");
        }
    }

    private synchronized void finishRefresh(Map<String, RegisteredRestTargetConfig> targets, Throwable failure) {
        cached = new Cached(targets, clock.getAsLong() + (failure == null ? REFRESH_NANOS : RETRY_NANOS));
        inFlight = null;
    }

    private Map<String, RegisteredRestTargetConfig> read() throws IOException {
        if (s3 == null) s3 = S3Client.builder().overrideConfiguration(config ->
            config.apiCallTimeout(Duration.ofSeconds(10)).apiCallAttemptTimeout(Duration.ofSeconds(4))).build();
        var coordinate = GetObjectRequest.builder().bucket(bucket.orElseThrow())
            .key(AwsDurableDeploymentConfigSourceFactory.routingKey(key.orElseThrow())).build();
        try (var stream = s3.getObject(coordinate)) {
            var response = stream.response();
            byte[] bytes = stream.readNBytes(MAX_BYTES + 1);
            if (bytes.length > MAX_BYTES) {
                stream.abort();
                throw new IllegalArgumentException("Shared REST routing properties exceed 256 KiB");
            }
            return parse(bytes, response.versionId(), response.metadata().get("digest"), parameterArnPrefix.orElseThrow());
        }
    }

    static Map<String, RegisteredRestTargetConfig> parse(byte[] bytes, String version, String digest, String parameterPrefix)
        throws IOException {
        if (version == null || version.isBlank() || "null".equals(version) || bytes.length > MAX_BYTES
            || !sha256(bytes).equals(digest)) {
            throw new IllegalArgumentException("Invalid immutable REST routing object");
        }
        var properties = new Properties() {
            @Override
            public synchronized Object put(Object key, Object value) {
                if (containsKey(key)) throw new IllegalArgumentException("Repeated REST routing property");
                return super.put(key, value);
            }
        };
        properties.load(new StringReader(new String(bytes, StandardCharsets.UTF_8)));
        var grouped = new HashMap<String, Map<String, String>>();
        for (String key : properties.stringPropertyNames()) {
            int field = key.lastIndexOf('.');
            if (!key.startsWith(TARGET_PREFIX) || field <= TARGET_PREFIX.length()) {
                throw new IllegalArgumentException("Invalid registered REST routing property");
            }
            String name = key.substring(field + 1);
            if (!FIELDS.contains(name) && !"protocol".equals(name)) {
                throw new IllegalArgumentException("Invalid registered REST routing property");
            }
            String group = key.substring(TARGET_PREFIX.length(), field);
            if (!group.matches("[A-Za-z0-9_-]+")) throw new IllegalArgumentException("Invalid REST routing entry key");
            grouped.computeIfAbsent(group, ignored -> new HashMap<>()).put(name, properties.getProperty(key));
        }
        var targets = new HashMap<String, RegisteredRestTargetConfig>();
        grouped.forEach((id, fields) -> {
            String protocol = fields.getOrDefault("protocol", "rest");
            if (!fields.keySet().containsAll(FIELDS) || !"rest".equals(protocol)) {
                throw new IllegalArgumentException("Incomplete or unsupported REST routing entry");
            }
            credentialRequest(fields.get("shared-secret-ref"), parameterPrefix);
            targets.put(id, new Target(Optional.of(fields.get("entry-id")), fields.get("tenant-id"),
                fields.get("pipeline-id"), fields.get("contract-version"), fields.get("release-version"),
                fields.get("worker-id"), protocol, fields.get("endpoint"), fields.get("shared-secret-ref"),
                fields.get("artifact-id"), fields.get("artifact-digest")));
        });
        return Map.copyOf(targets);
    }

    static PinnedParameter credentialRequest(String reference, String parameterPrefix) {
        if (!reference.startsWith(CREDENTIAL_PREFIX)) {
            throw new IllegalArgumentException("A pinned REST signing credential is required");
        }
        int separator = reference.lastIndexOf('@');
        if (separator <= CREDENTIAL_PREFIX.length()) {
            throw new IllegalArgumentException("A full parameter ARN and pinned version are required");
        }
        var request = AwsDurableDeploymentConfigSourceFactory.parameterRequest(
            reference.substring(CREDENTIAL_PREFIX.length(), separator), reference.substring(separator + 1), "REST worker");
        if (!request.arn().startsWith(parameterPrefix)) {
            throw new IllegalArgumentException("REST signing credential is outside the configured namespace");
        }
        return request;
    }

    @Override
    public Uni<Void> prepareCredential(String reference) {
        return Uni.createFrom().deferred(() -> {
            if (credentials.containsKey(reference)) return Uni.createFrom().voidItem();
            return Uni.createFrom().item(() -> credentials.computeIfAbsent(reference, this::readCredential))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()).replaceWithVoid();
        });
    }

    @Override
    public Optional<String> preparedCredential(String reference) {
        if (!reference.startsWith(CREDENTIAL_PREFIX)) return Optional.empty();
        String value = credentials.get(reference);
        if (value == null) throw new IllegalStateException("Pinned REST credential was not prepared");
        return Optional.of(value);
    }

    private String readCredential(String reference) {
        var request = credentialRequest(reference, parameterArnPrefix.orElseThrow());
        try {
            return AwsDurableDeploymentConfigSourceFactory.verifiedParameter(request, parameterClient().getParameter(request.read()));
        } catch (RuntimeException failure) {
            throw new IllegalStateException("Pinned REST signing credential could not be loaded");
        }
    }

    private synchronized SsmClient parameterClient() {
        if (parameters == null) parameters = SsmClient.builder().overrideConfiguration(config ->
            config.apiCallTimeout(Duration.ofSeconds(10)).apiCallAttemptTimeout(Duration.ofSeconds(4))).build();
        return parameters;
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    @PreDestroy
    void close() {
        if (s3 != null) s3.close();
        if (parameters != null) parameters.close();
    }

    private record Target(Optional<String> entryId, String tenantId, String pipelineId, String contractVersion,
                          String releaseVersion, String workerId, String protocol, String endpoint,
                          String sharedSecretRef, String artifactId, String artifactDigest)
        implements RegisteredRestTargetConfig {}
}

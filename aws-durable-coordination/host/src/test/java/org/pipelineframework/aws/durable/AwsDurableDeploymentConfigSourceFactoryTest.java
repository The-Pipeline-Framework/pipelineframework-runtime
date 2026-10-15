package org.pipelineframework.aws.durable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.config.PropertiesConfigSource;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;
import software.amazon.awssdk.services.ssm.model.Parameter;
import software.amazon.awssdk.services.ssm.model.ParameterType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AwsDurableDeploymentConfigSourceFactoryTest {
    private static final String VERSION = "01234567890123456789012345678901";
    private static final String SECRET = "a-pinned-signing-key-with-at-least-32-characters";

    @Test
    void discoversTheFactoryWithoutLoadingAwsOrSecretsInOrdinaryBuilds() {
        assertThat(ServiceLoader.load(ConfigSourceFactory.class).stream()
            .map(ServiceLoader.Provider::type)).contains(AwsDurableDeploymentConfigSourceFactory.class);
        var requests = new ArrayList<GetSecretValueRequest>();
        build(Map.of(), requests);
        assertThat(requests).isEmpty();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {
        "tpf.aws.durable.resume-token-secret-arn", "tpf.aws.durable.worker-secret-arn",
        "tpf.aws.durable.resume-token-secret-version", "tpf.aws.durable.worker-secret-version",
        "pipeline.orchestrator.resume-token-secret", "pipeline.orchestrator.worker.sqs.shared-secret",
        "pipeline.orchestrator.worker.sqs.shared-secret-ref", "tpf.aws.durable.configuration"})
    void partialSecretConfigurationRequiresPreflightBeforeAnyRead(String property) {
        var requests = new ArrayList<GetSecretValueRequest>();
        var values = Map.of(property, property.endsWith("configuration") ? "{}" : SECRET);
        assertThatThrownBy(() -> build(values, requests)).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires preflight validation").hasMessageNotContaining(SECRET);
        assertThat(requests).isEmpty();
    }

    @Test
    void resolvesBothExactVersionsOnceIntoExistingRuntimeConfiguration() {
        var requests = new ArrayList<GetSecretValueRequest>();
        SmallRyeConfig config = build(validConfiguration(), requests);
        assertThat(config.getValue("pipeline.orchestrator.resume-token-secret", String.class)).isEqualTo(SECRET);
        assertThat(config.getValue("pipeline.orchestrator.worker.sqs.shared-secret", String.class)).isEqualTo(SECRET);
        assertThat(requests).hasSize(2).allSatisfy(request -> {
            assertThat(request.versionId()).isEqualTo(VERSION);
            assertThat(request.versionStage()).isNull();
            assertThat(request.secretId()).startsWith("arn:aws:secretsmanager:");
        });
        config.getValue("pipeline.orchestrator.resume-token-secret", String.class);
        assertThat(requests).hasSize(2);
        var runtime = config.getConfigMapping(PipelineOrchestratorConfig.class);
        assertThat(runtime.stateProvider()).isEqualTo("dynamo");
        assertThat(runtime.releases().registry().provider()).isEqualTo("dynamo");
        assertThat(runtime.releases().storage().provider()).isEqualTo("s3");
        assertThat(runtime.workerSqs().sharedSecret()).contains(SECRET);
        var source = java.util.stream.StreamSupport.stream(config.getConfigSources().spliterator(), false)
            .filter(candidate -> candidate.getName().equals("aws-durable-pinned-signing-keys")).findFirst().orElseThrow();
        assertThat(source.toString()).doesNotContain(SECRET);
        assertThatThrownBy(() -> source.getProperties().put("anything", "value"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void selectsPinnedSsmVersionsWithoutReadingSecretsManager() {
        var secretRequests = new ArrayList<GetSecretValueRequest>();
        var parameterRequests = new ArrayList<GetParameterRequest>();
        var config = buildSsm(validSsmConfiguration(), secretRequests, parameterRequests,
            this::validParameterResponse);
        assertThat(config.getValue("pipeline.orchestrator.resume-token-secret", String.class)).isEqualTo(SECRET);
        assertThat(config.getValue("pipeline.orchestrator.worker.sqs.shared-secret", String.class)).isEqualTo(SECRET);
        assertThat(secretRequests).isEmpty();
        assertThat(parameterRequests).hasSize(2).allSatisfy(request -> {
            assertThat(request.name()).startsWith("arn:aws:ssm:eu-west-1:123456789012:parameter/tpf/pilot/").endsWith(":7");
            assertThat(request.withDecryption()).isTrue();
        });
        config.getValue("pipeline.orchestrator.resume-token-secret", String.class);
        assertThat(parameterRequests).hasSize(2);
    }

    @Test
    void ssmSelectionRejectsInvalidAndMixedReferencesBeforeEitherProviderRead() {
        var valid = validSsmConfiguration();
        for (String missing : List.of("tpf.aws.durable.resume-token-parameter-arn",
            "tpf.aws.durable.resume-token-parameter-version", "tpf.aws.durable.worker-parameter-arn",
            "tpf.aws.durable.worker-parameter-version")) {
            var values = new HashMap<>(valid); values.remove(missing);
            assertNoProviderRead(values);
        }
        for (Map.Entry<String, String> invalid : List.of(
            Map.entry("tpf.aws.durable.signing-source", "UNKNOWN"),
            Map.entry("tpf.aws.durable.resume-token-parameter-arn", "name-only"),
            Map.entry("tpf.aws.durable.resume-token-parameter-version", "latest"),
            Map.entry("tpf.aws.durable.resume-token-parameter-version", "0"),
            Map.entry("tpf.aws.durable.resume-token-parameter-version", "-1"),
            Map.entry("tpf.aws.durable.resume-token-parameter-version", "9223372036854775808"),
            Map.entry("tpf.aws.durable.resume-token-secret-arn", "arn:aws:secretsmanager:eu-west-1:123456789012:secret:other-abcdef"),
            Map.entry("pipeline.orchestrator.worker.sqs.shared-secret-ref", "env:worker"),
            Map.entry("pipeline.orchestrator.resume-token-secret", SECRET))) {
            var values = new HashMap<>(valid); values.put(invalid.getKey(), invalid.getValue());
            assertNoProviderRead(values);
        }
        var same = new HashMap<>(valid);
        same.put("tpf.aws.durable.worker-parameter-arn", same.get("tpf.aws.durable.resume-token-parameter-arn"));
        assertNoProviderRead(same);
        var smWithSsmField = validConfiguration();
        smWithSsmField.put("tpf.aws.durable.worker-parameter-version", "7");
        assertNoProviderRead(smWithSsmField);
        var noPreflight = new HashMap<>(valid); noPreflight.remove("tpf.aws.durable.deployment-validation");
        assertNoProviderRead(noPreflight);
    }

    @Test
    void ssmFlatParameterNameMatchesExactArnResource() {
        var values = validSsmConfiguration();
        values.put("tpf.aws.durable.resume-token-parameter-arn",
            "arn:aws:ssm:eu-west-1:123456789012:parameter/resume-token");
        values.put("tpf.aws.durable.worker-parameter-arn",
            "arn:aws:ssm:eu-west-1:123456789012:parameter/worker");
        var config = buildSsm(values, new ArrayList<>(), new ArrayList<>(), request -> {
            String arn = request.name().substring(0, request.name().lastIndexOf(':'));
            return GetParameterResponse.builder().parameter(Parameter.builder().arn(arn)
                .name(arn.substring(arn.indexOf(":parameter/") + ":parameter/".length()))
                .version(7L).type(ParameterType.SECURE_STRING).value(SECRET).build()).build();
        });
        assertThat(config.getValue("pipeline.orchestrator.resume-token-secret", String.class)).isEqualTo(SECRET);
        assertThat(config.getValue("pipeline.orchestrator.worker.sqs.shared-secret", String.class)).isEqualTo(SECRET);
    }

    @Test
    void ssmResponseIdentityTypeVersionAndLengthFailuresRevealNoKey() {
        var valid = validSsmConfiguration();
        var request = GetParameterRequest.builder()
            .name(valid.get("tpf.aws.durable.resume-token-parameter-arn") + ":7").withDecryption(true).build();
        var good = validParameterResponse(request).parameter();
        var oversized = "é".repeat(2050); // 2-byte UTF-8 characters: 4,100 bytes.
        for (Parameter bad : List.of(
            good.toBuilder().arn("arn:aws:ssm:eu-west-1:123456789012:parameter/other").build(),
            good.toBuilder().name("/other").build(),
            good.toBuilder().version(8L).build(),
            good.toBuilder().type(ParameterType.STRING).build(),
            good.toBuilder().value("short").build(),
            good.toBuilder().value(" ".repeat(32)).build(),
            good.toBuilder().value(oversized).build(),
            good.toBuilder().value((String) null).build())) {
            var parameterRequests = new ArrayList<GetParameterRequest>();
            assertThatThrownBy(() -> buildSsm(valid, new ArrayList<>(), parameterRequests,
                ignored -> GetParameterResponse.builder().parameter(bad).build()))
                .isInstanceOf(IllegalStateException.class).hasMessageNotContaining(SECRET);
            assertThat(parameterRequests).hasSize(1); // First invalid read prevents the second read and any config source.
        }
        assertThatThrownBy(() -> buildSsm(valid, new ArrayList<>(), new ArrayList<>(),
            ignored -> GetParameterResponse.builder().build())).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> buildSsm(valid, new ArrayList<>(), new ArrayList<>(),
            ignored -> null)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void ssmSecondReadFailureDoesNotPublishPartialKeysOrProviderError() {
        var parameterRequests = new ArrayList<GetParameterRequest>();
        assertThatThrownBy(() -> buildSsm(validSsmConfiguration(), new ArrayList<>(), parameterRequests,
            request -> {
                if (request.name().contains("worker")) throw new IllegalStateException(SECRET);
                return validParameterResponse(request);
            })).isInstanceOf(IllegalStateException.class).hasMessageNotContaining(SECRET).hasNoCause();
        assertThat(parameterRequests).hasSize(2);
    }

    @Test
    void ssmSecondResponseIdentityMismatchFailsAfterBothExactReads() {
        var parameterRequests = new ArrayList<GetParameterRequest>();
        assertThatThrownBy(() -> buildSsm(validSsmConfiguration(), new ArrayList<>(), parameterRequests,
            request -> {
                var response = validParameterResponse(request);
                if (request.name().contains("worker")) {
                    return response.toBuilder().parameter(response.parameter().toBuilder().version(8L).build()).build();
                }
                return response;
            })).isInstanceOf(IllegalStateException.class).hasMessageNotContaining(SECRET);
        assertThat(parameterRequests).hasSize(2);
    }

    @Test
    void missingTablesWrongProvidersPlaintextAndFloatingVersionsFailBeforeAnyRead() {
        for (String property : List.of("pipeline.orchestrator.dynamo.execution-payload-table",
            "pipeline.orchestrator.worker.sqs.response-queue-url", "pipeline.orchestrator.worker.sqs.contract-version",
            "pipeline.orchestrator.worker.sqs.artifact-id", "pipeline.orchestrator.worker.sqs.artifact-digest",
            "tpf.aws.durable.worker-secret-version")) {
            var values = validConfiguration(); values.remove(property);
            var requests = new ArrayList<GetSecretValueRequest>();
            assertThatThrownBy(() -> build(values, requests)).isInstanceOf(IllegalArgumentException.class);
            assertThat(requests).isEmpty();
        }
        for (Map.Entry<String, String> invalid : Map.of(
            "pipeline.orchestrator.state-provider", "memory",
            "pipeline.orchestrator.idempotency-policy", "SERVER_KEY_ONLY",
            "pipeline.orchestrator.resume-token-secret", SECRET,
            "pipeline.orchestrator.worker.sqs.shared-secret-ref", "env:secret",
            "tpf.aws.durable.worker-secret-version", "AWSCURRENT",
            "tpf.aws.durable.estimated-operations", "3001",
            "tpf.aws.durable.sqs-visibility-seconds", "900",
            "tpf.aws.durable.retention-days", "91").entrySet()) {
            var values = validConfiguration(); values.put(invalid.getKey(), invalid.getValue());
            var requests = new ArrayList<GetSecretValueRequest>();
            assertThatThrownBy(() -> build(values, requests)).isInstanceOf(IllegalArgumentException.class);
            assertThat(requests).isEmpty();
        }
    }

    @Test
    void providerDenialAndWrongResponseVersionFailClosedWithoutExposingSecret() {
        var values = validConfiguration();
        assertThatThrownBy(() -> new SmallRyeConfigBuilder().withSources(new PropertiesConfigSource(values, "deployment", 300))
            .withSources(new AwsDurableDeploymentConfigSourceFactory(request -> {
                throw new IllegalStateException(SECRET);
            })).build()).isInstanceOf(IllegalStateException.class).hasMessageNotContaining(SECRET).hasNoCause();
        assertThatThrownBy(() -> new SmallRyeConfigBuilder().withSources(new PropertiesConfigSource(values, "deployment", 300))
            .withSources(new AwsDurableDeploymentConfigSourceFactory(request -> GetSecretValueResponse.builder()
                .arn(request.secretId()).versionId("wrong-version").secretString(SECRET).build())).build())
            .isInstanceOf(IllegalStateException.class).hasMessageNotContaining(SECRET);
    }

    @Test
    void compactDeploymentSettingsPreserveRuntimeMappingWithoutDuplicatingLongEnvironmentNames() throws Exception {
        var values = validConfiguration();
        var compact = new HashMap<String, String>();
        values.entrySet().removeIf(entry -> {
            if (!entry.getKey().startsWith("pipeline.orchestrator.")) return false;
            compact.put(entry.getKey().substring("pipeline.orchestrator.".length()), entry.getValue());
            return true;
        });
        values.put("tpf.aws.durable.configuration", new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(compact));
        var requests = new ArrayList<GetSecretValueRequest>();
        var config = build(values, requests);
        assertThat(config.getConfigMapping(PipelineOrchestratorConfig.class).dynamo().executionTable()).isEqualTo("test_execution");
        assertThat(config.getConfigMapping(PipelineOrchestratorConfig.class).idempotencyPolicy().name()).isEqualTo("CLIENT_KEY_REQUIRED");
        assertThat(config.getConfigMapping(PipelineOrchestratorConfig.class).workerSqs().contractVersion())
            .contains("sha256:" + "a".repeat(64));
        assertThat(config.getConfigMapping(PipelineOrchestratorConfig.class).workerSqs().artifactId()).contains("test-artifact");
        assertThat(config.getValue("pipeline.orchestrator.resume-token-secret", String.class)).isEqualTo(SECRET);
        assertThat(requests).hasSize(2);
        values.remove("tpf.aws.durable.deployment-validation");
        assertThatThrownBy(() -> build(values, new ArrayList<>())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void malformedDuplicateOrPlaintextCompactSettingsFailBeforeReadsWithoutEchoingValues() {
        for (String encoded : List.of("[]", "{\"mode\":\"QUEUE_ASYNC\",\"mode\":\"SYNC\"}",
            "{\"resume-token-secret\":\"" + SECRET + "\"}", "{\"mode\":12}")) {
            var values = validConfiguration(); values.put("tpf.aws.durable.configuration", encoded);
            var requests = new ArrayList<GetSecretValueRequest>();
            assertThatThrownBy(() -> build(values, requests)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining(SECRET).hasNoCause();
            assertThat(requests).isEmpty();
        }
    }

    private SmallRyeConfig build(Map<String, String> values, List<GetSecretValueRequest> requests) {
        return new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class).withSources(new PropertiesConfigSource(values, "deployment", 300))
            .withSources(new AwsDurableDeploymentConfigSourceFactory(request -> {
                requests.add(request);
                return GetSecretValueResponse.builder().arn(request.secretId()).versionId(request.versionId())
                    .secretString(SECRET).build();
            })).build();
    }

    private SmallRyeConfig buildSsm(Map<String, String> values, List<GetSecretValueRequest> secrets,
        List<GetParameterRequest> parameters,
        java.util.function.Function<GetParameterRequest, GetParameterResponse> response) {
        return new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class)
            .withSources(new PropertiesConfigSource(values, "deployment", 300))
            .withSources(new AwsDurableDeploymentConfigSourceFactory(request -> {
                secrets.add(request);
                throw new AssertionError("Secrets Manager must not be read in SSM mode");
            }, request -> {
                parameters.add(request);
                return response.apply(request);
            })).build();
    }

    private GetParameterResponse validParameterResponse(GetParameterRequest request) {
        String arn = request.name().substring(0, request.name().lastIndexOf(':'));
        String name = "/" + arn.substring(arn.indexOf(":parameter/") + ":parameter/".length());
        return GetParameterResponse.builder().parameter(Parameter.builder().arn(arn).name(name)
            .version(7L).type(ParameterType.SECURE_STRING).value(SECRET).build()).build();
    }

    private void assertNoProviderRead(Map<String, String> values) {
        var secrets = new ArrayList<GetSecretValueRequest>();
        var parameters = new ArrayList<GetParameterRequest>();
        assertThatThrownBy(() -> buildSsm(values, secrets, parameters, this::validParameterResponse))
            .isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining(SECRET);
        assertThat(secrets).isEmpty();
        assertThat(parameters).isEmpty();
    }

    private Map<String, String> validConfiguration() {
        var values = new HashMap<String, String>();
        values.put("tpf.aws.durable.deployment-validation", "validated");
        values.put("pipeline.orchestrator.mode", "QUEUE_ASYNC");
        values.put("pipeline.orchestrator.coordination-host", "AWS_DURABLE");
        values.put("pipeline.orchestrator.process-loops-disabled", "true");
        values.put("pipeline.orchestrator.strict-startup", "true");
        values.put("pipeline.orchestrator.idempotency-policy", "CLIENT_KEY_REQUIRED");
        values.put("pipeline.orchestrator.state-provider", "dynamo");
        values.put("pipeline.orchestrator.dispatcher-provider", "sqs");
        values.put("pipeline.orchestrator.dlq-provider", "sqs");
        values.put("pipeline.orchestrator.releases.registry.provider", "dynamo");
        values.put("pipeline.orchestrator.releases.storage.provider", "s3");
        for (String table : List.of("execution", "execution-key", "execution-payload", "await-interaction",
            "await-interaction-key", "await-completion-lifecycle", "await-unit", "await-admission", "release", "worker")) {
            values.put("pipeline.orchestrator.dynamo." + table + "-table", "test_" + table);
        }
        for (String name : List.of("pipeline.orchestrator.pipeline-id", "pipeline.orchestrator.release-version",
            "pipeline.orchestrator.releases.storage.s3.bucket", "pipeline.orchestrator.queue-url",
            "pipeline.orchestrator.dlq-url", "pipeline.orchestrator.worker.sqs.request-queue-url",
            "pipeline.orchestrator.worker.sqs.response-queue-url", "tpf.aws.durable.binding-table")) values.put(name, "test-value");
        values.put("pipeline.orchestrator.worker.sqs.pipeline-id", "test-value");
        values.put("pipeline.orchestrator.worker.sqs.contract-version", "sha256:" + "a".repeat(64));
        values.put("pipeline.orchestrator.worker.sqs.release-version", "test-value");
        values.put("pipeline.orchestrator.worker.sqs.artifact-id", "test-artifact");
        values.put("pipeline.orchestrator.worker.sqs.artifact-digest", "b".repeat(64));
        values.put("tpf.aws.durable.estimated-operations", "3000");
        values.put("tpf.aws.durable.estimated-written-state-bytes", "104857600");
        values.put("tpf.aws.durable.execution-timeout-seconds", "31622400");
        values.put("tpf.aws.durable.retention-days", "90");
        values.put("tpf.aws.durable.worker-timeout-seconds", "900");
        values.put("tpf.aws.durable.sqs-visibility-seconds", "5400");
        values.put("tpf.aws.durable.callback-retention-seconds", "7776000");
        values.put("tpf.aws.durable.action-timeout-seconds", "120");
        for (String purpose : List.of("resume-token", "worker")) {
            values.put("tpf.aws.durable." + purpose + "-secret-arn",
                "arn:aws:secretsmanager:eu-west-1:123456789012:secret:" + purpose + "-abcdef");
            values.put("tpf.aws.durable." + purpose + "-secret-version", VERSION);
        }
        return values;
    }

    private Map<String, String> validSsmConfiguration() {
        var values = validConfiguration();
        for (String purpose : List.of("resume-token", "worker")) {
            values.remove("tpf.aws.durable." + purpose + "-secret-arn");
            values.remove("tpf.aws.durable." + purpose + "-secret-version");
            values.put("tpf.aws.durable." + purpose + "-parameter-arn",
                "arn:aws:ssm:eu-west-1:123456789012:parameter/tpf/pilot/" + purpose);
            values.put("tpf.aws.durable." + purpose + "-parameter-version", "7");
        }
        values.put("tpf.aws.durable.signing-source", "SSM_STANDARD");
        return values;
    }
}

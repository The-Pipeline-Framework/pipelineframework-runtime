package org.pipelineframework.aws.durable;

import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableHostedProfile;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.orchestrator.AsyncExecutionSubmission;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.CoordinationHost;
import org.pipelineframework.orchestrator.ExecutionClientAdmission;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.orchestrator.release.PipelineReleaseEntryBinding;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.GetFunctionConfigurationRequest;

/** Connects the existing Quarkus submission facade to the original AWS Durable coordinator. */
@ApplicationScoped
@LookupIfProperty(name = "tpf.aws.durable.http.enabled", stringValue = "true")
public class AwsDurableHttpSubmission implements AsyncExecutionSubmission {

    private static final String COORDINATOR_TRIGGER = "AWS_DURABLE_COORDINATOR";
    private static final Duration ADMISSION_TIMEOUT = Duration.ofSeconds(8);
    private static final Duration AWS_REQUEST_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);
    private static final Pattern FUNCTION_FAMILY = Pattern.compile(
        "arn:[a-z0-9-]+:lambda:[a-z0-9-]+:[0-9]{12}:function:[A-Za-z0-9_-]+");

    @Inject PipelineControlPlane controlPlane;
    @Inject PipelineOrchestratorConfig config;
    @Inject ControlPlaneAdmissionPolicy admissionPolicy;
    @Inject PipelineReleaseIdentityResolver identity;
    @Inject PipelineReleaseRegistry releases;
    @Inject TransitionPayloadCodec codec;
    @Inject ObjectMapper mapper;
    @Inject LambdaClient lambda;
    @Inject @ConfigProperty(name = "tpf.aws.durable.http.coordinator-function-family")
    Optional<String> coordinatorFunctionFamily;

    @Override
    public Uni<RunAsyncAcceptedDto> submit(Object input, String tenantId, String idempotencyKey, boolean outputStreaming) {
        var context = PipelineContextHolder.get();
        boolean hasPipelineContext = context != null
            && (context.versionTag() != null || context.replayMode() != null || context.cachePolicy() != null);
        return Uni.createFrom().deferred(() -> {
            if (config.mode() != OrchestratorMode.QUEUE_ASYNC || config.coordinationHost() != CoordinationHost.AWS_DURABLE
                || !config.strictStartup() || !config.processLoopsDisabled()) {
                throw new IllegalStateException("HTTP Durable admission requires strict event-hosted AWS Durable queue mode");
            }
            if (outputStreaming || input instanceof Multi<?> || hasPipelineContext) {
                throw new IllegalArgumentException("HTTP Durable admission supports only unary input and output without a caller context");
            }
            if (tenantId == null || tenantId.isBlank() || idempotencyKey == null || idempotencyKey.isBlank()) {
                throw new IllegalArgumentException("HTTP Durable admission requires an explicit tenant and client idempotency key");
            }
            String tenant = tenantId.trim();
            String key = idempotencyKey.trim();
            String pipeline = identity.pipelineId(config);
            String releaseVersion = identity.releaseVersion(config);
            var decision = admissionPolicy.admit(new ControlPlaneAdmissionRequest(tenant,
                ControlPlaneAdmissionOperation.SUBMIT_EXECUTION, pipeline, releaseVersion, null, "api", true));
            if (!decision.allowed()) {
                throw new ControlPlaneAdmissionException(decision);
            }
            Uni<?> payload = input instanceof Uni<?> unary ? unary : Uni.createFrom().item(input);
            return payload.map(codec::encode).chain(encoded -> submitTyped(tenant, key, pipeline, releaseVersion, encoded));
        }).ifNoItem().after(ADMISSION_TIMEOUT).failWith(() ->
            new IllegalStateException("Native admission was not observed within the HTTP deadline"));
    }

    private Uni<RunAsyncAcceptedDto> submitTyped(String tenant, String key, String pipeline, String releaseVersion,
        SerializedTransitionPayload input) {
        var contract = identity.contract();
        if (contract.steps().isEmpty() || !contract.steps().getFirst().inputTypeId().equals(input.payloadTypeId())
            || !AwsDurableHostedProfile.TYPED_JSON_ENCODING.equals(input.payloadEncoding())) {
            throw new IllegalArgumentException("HTTP input does not match the compiled Durable entry type");
        }
        String fingerprint = ExecutionClientAdmission.fingerprint(input);
        return controlPlane.getExecutionAdmissionByClientKey(tenant, key, pipeline, releaseVersion).chain(existing -> {
            if (existing.isPresent()) {
                return Uni.createFrom().item(accepted(existing.orElseThrow(), tenant, pipeline, releaseVersion, fingerprint, true));
            }
            return releases.active(tenant, pipeline).chain(found -> {
                var release = found.orElseThrow(() -> new IllegalStateException("An active AWS Durable Release is required"));
                requireMatchingHttpRelease(release, tenant, pipeline, releaseVersion);
                return start(release, key, input)
                    .chain(duplicate -> awaitAdmission(tenant, key, pipeline, releaseVersion).map(admitted ->
                        accepted(admitted, tenant, pipeline, releaseVersion, fingerprint, duplicate)));
            });
        });
    }

    private void requireMatchingHttpRelease(PipelineReleaseRecord release, String tenant, String pipeline, String releaseVersion) {
        var deployment = release.descriptor().deployment();
        if (!tenant.equals(release.tenantId()) || !pipeline.equals(release.pipelineId())
            || !releaseVersion.equals(release.releaseVersion()) || !identity.contract().equals(release.contract())
            || deployment == null || !AwsDurableHostedProfile.PROFILE_ID.equals(deployment.profile())
            || release.contract().steps().isEmpty()) {
            throw new IllegalStateException("This HTTP entry does not match the active AWS Durable Release");
        }
    }

    private Uni<Boolean> start(PipelineReleaseRecord release, String key, SerializedTransitionPayload input) {
        var entry = release.descriptor().deployment().entries().stream()
            .filter(value -> COORDINATOR_TRIGGER.equals(value.trigger())
                && AwsDurableHostedProfile.ENTRIES_BY_TRIGGER.get(COORDINATOR_TRIGGER).entryClass().equals(value.entryClass()))
            .findFirst().orElseThrow(() -> new IllegalStateException("The active Release does not declare a Durable coordinator"));
        String inputJson = serializeInput(input);
        return releases.getEntryBinding(release.tenantId(), release.pipelineId(), release.contractVersion(),
            release.releaseVersion(), entry.entryId()).chain(found -> {
                var binding = found.orElseThrow(() -> new IllegalStateException("The Durable coordinator binding is unavailable"));
                return Uni.createFrom().item(() -> startPinned(release, entry.entryId(), binding, key, inputJson))
                    .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
            });
    }

    private String serializeInput(SerializedTransitionPayload input) {
        try {
            return PipelineJson.mapper().writeValueAsString(input);
        } catch (JsonProcessingException invalid) {
            throw new IllegalArgumentException("HTTP input could not be serialized", invalid);
        }
    }

    private boolean startPinned(PipelineReleaseRecord release, String entryId, PipelineReleaseEntryBinding binding,
        String key, String inputJson) {
        if (!binding.key().equals(List.of(release.tenantId(), release.pipelineId(), release.contractVersion(),
            release.releaseVersion(), entryId))) {
            throw new IllegalStateException("The Durable coordinator binding does not match the selected entry");
        }
        binding.requireRelease(release);
        String family = coordinatorFunctionFamily.filter(value -> FUNCTION_FAMILY.matcher(value).matches())
            .orElseThrow(() -> new IllegalStateException("An exact trusted Durable coordinator function family ARN is required"));
        if (!"aws-lambda".equals(binding.provider()) || !binding.resourceId().matches(Pattern.quote(family) + ":[1-9][0-9]*")) {
            throw new IllegalStateException("HTTP admission requires a numbered version of the trusted Durable coordinator");
        }
        var artifact = release.descriptor().artifacts().stream()
            .filter(value -> binding.artifactId().equals(value.artifactId())).findFirst().orElseThrow();
        byte[] codeDigest = Base64.getDecoder().decode(binding.codeDigest());
        if (!"lambda-zip".equals(artifact.kind()) || codeDigest.length != 32
            || !artifact.digest().equals("sha256:" + HexFormat.of().formatHex(codeDigest))) {
            throw new IllegalStateException("The Durable coordinator digest does not match the Release ZIP");
        }
        String version = binding.resourceId().substring(family.length() + 1);
        var observed = lambda.getFunctionConfiguration(GetFunctionConfigurationRequest.builder()
            .functionName(family).qualifier(version)
            .overrideConfiguration(override -> override.apiCallTimeout(AWS_REQUEST_TIMEOUT)).build());
        if (!binding.codeDigest().equals(observed.codeSha256()) || !version.equals(observed.version())) {
            throw new IllegalStateException("The numbered Durable coordinator code could not be verified");
        }
        var input = new AwsDurableExecutionInput(release.tenantId(), key, release.pipelineId(),
            release.contractVersion(), release.releaseVersion(), inputJson, Optional.empty(), 1,
            Optional.of(UUID.randomUUID().toString()));
        return new AwsDurableExecutionStarter(lambda, mapper, family, version).startForHttpAdmission(input, AWS_REQUEST_TIMEOUT);
    }

    private Uni<ExecutionClientAdmission> awaitAdmission(String tenant, String key, String pipeline, String releaseVersion) {
        return controlPlane.getExecutionAdmissionByClientKey(tenant, key, pipeline, releaseVersion).chain(found -> found
            .map(value -> Uni.createFrom().item(value))
            .orElseGet(() -> Uni.createFrom().voidItem().onItem().delayIt().by(POLL_INTERVAL)
                .chain(() -> awaitAdmission(tenant, key, pipeline, releaseVersion))));
    }

    private RunAsyncAcceptedDto accepted(ExecutionClientAdmission admission, String tenant, String pipeline,
        String releaseVersion, String fingerprint, boolean duplicate) {
        var checkpoint = admission.checkpoint();
        if (!tenant.equals(checkpoint.tenantId()) || !pipeline.equals(checkpoint.pipelineId())
            || !identity.contractVersion().equals(checkpoint.contractVersion()) || !releaseVersion.equals(checkpoint.releaseVersion())) {
            throw new IllegalStateException("Native admission does not match the selected tenant and Release");
        }
        if (admission.inputFingerprint().filter(fingerprint::equals).isEmpty()) {
            throw new IllegalArgumentException("Client idempotency key does not identify the same initial input");
        }
        return admission.accepted(duplicate);
    }
}

package org.pipelineframework.aws.durable;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.aws.durable.model.LambdaVersionConfigurationEvidence;
import org.pipelineframework.orchestrator.RegisteredRestWorkerTargets;
import org.pipelineframework.orchestrator.RestWorkerBindingVerifier;
import org.pipelineframework.orchestrator.release.PipelineReleaseEntryBinding;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.FunctionUrlAuthType;

/** Checks immutable version evidence on cold start or expiry of trusted routing state. */
@ApplicationScoped
public class AwsDurableRestWorkerBindingVerifier implements RestWorkerBindingVerifier {
    private static final long MAX_AGE_NANOS = Duration.ofSeconds(60).toNanos();
    private static final String VERSION_ARN = "arn:[^:]+:lambda:[^:]+:[0-9]{12}:function:[A-Za-z0-9_-]+:[1-9][0-9]*";

    @Inject LambdaClient lambda;
    @Inject
    @ConfigProperty(name = "tpf.aws.durable.rest-signing-region")
    Optional<String> region;

    private final ConcurrentMap<PipelineReleaseEntryBinding, Long> verifiedAt = new ConcurrentHashMap<>();

    @Override
    public Uni<Void> verify(PipelineReleaseEntryBinding binding) {
        if (region.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        Long last = verifiedAt.get(binding);
        if (last != null && System.nanoTime() - last < MAX_AGE_NANOS) {
            return Uni.createFrom().voidItem();
        }
        return Uni.createFrom().item(() -> verifiedAt.compute(binding, (key, previous) -> {
            if (previous == null || System.nanoTime() - previous >= MAX_AGE_NANOS) {
                verifyCurrent(key);
                return System.nanoTime();
            }
            return previous;
        })).runSubscriptionOn(io.smallrye.mutiny.infrastructure.Infrastructure.getDefaultWorkerPool())
            .replaceWithVoid();
    }

    void verifyCurrent(PipelineReleaseEntryBinding binding) {
        verifyPublication(lambda, region.orElseThrow(), binding);
    }

    /** Publishes a verified REST binding through the existing immutable registry operation. */
    public Uni<PipelineReleaseEntryBinding> publish(PipelineReleaseRegistry registry, PipelineReleaseEntryBinding binding) {
        return Uni.createFrom().item(() -> {
            verifyCurrent(binding);
            return binding;
        }).runSubscriptionOn(io.smallrye.mutiny.infrastructure.Infrastructure.getDefaultWorkerPool())
            .chain(registry::registerEntryBinding);
    }

    /** Use the same observation check before publishing a routing binding. */
    public static void verifyPublication(LambdaClient lambda, String region, PipelineReleaseEntryBinding binding) {
        String arn = binding.resourceId();
        if (!arn.matches(VERSION_ARN) || !arn.split(":")[3].equals(region)) {
            throw new IllegalStateException("REST worker requires a numbered Lambda version in the configured Region");
        }
        int separator = arn.lastIndexOf(':');
        String function = arn.substring(0, separator);
        String version = arn.substring(separator + 1);
        String expectedConfiguration = binding.configurationDigest().orElseThrow(() ->
            new IllegalStateException("REST worker configuration digest is missing"));
        var alias = lambda.getAlias(request -> request.functionName(function).name("release"));
        var url = lambda.getFunctionUrlConfig(request -> request.functionName(function).qualifier("release"));
        var observed = lambda.getFunctionConfiguration(request -> request.functionName(function).qualifier(version));
        var dispatch = binding.dispatchBinding().orElseThrow();
        if (!version.equals(alias.functionVersion())
            || alias.routingConfig() != null && !alias.routingConfig().additionalVersionWeights().isEmpty()
            || url.authType() != FunctionUrlAuthType.AWS_IAM
            || url.functionUrl() == null || !RegisteredRestWorkerTargets.normalizeOrigin(url.functionUrl(), false)
                .equals(RegisteredRestWorkerTargets.normalizeOrigin(dispatch.endpoint(), false))
            || !version.equals(observed.version()) || !binding.codeDigest().equals(observed.codeSha256())
            || !LambdaVersionConfigurationEvidence.matches(expectedConfiguration, observed.configSha256(), observed.revisionId())) {
            throw new IllegalStateException("REST worker alias, URL or numbered version differs from trusted publication");
        }
    }
}

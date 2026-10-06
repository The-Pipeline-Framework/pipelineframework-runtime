package org.pipelineframework.awsproof;

import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.jboss.logging.Logger;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableStartResponse;

@ApplicationScoped
final class ProofDriverRecoveryService {
    private static final Logger LOG = Logger.getLogger(ProofDriverRecoveryService.class);
    private static final String PIPELINE_ID = "aws-durable-proof";
    private static final String CONTRACT_VERSION = "1";
    private static final String RELEASE_VERSION = "proof-release-1";

    @Inject
    ProofCallbackBindingRepository bindings;

    @Inject
    ProofDurableCallbackClient callbacks;

    @Inject
    ProofDurableExecutionStarter starter;

    boolean recoverClosed(String providerExecutionArn) {
        Objects.requireNonNull(providerExecutionArn, "providerExecutionArn");
        Optional<AwsDurableCallbackBinding> candidate = bindings.scanOpen(100).stream()
            .filter(binding -> providerExecutionArn.equals(binding.providerExecutionArn()))
            .findFirst();
        return candidate.map(this::recoverClosed).orElse(false);
    }

    int recoverClosedBindings() {
        int recovered = 0;
        for (AwsDurableCallbackBinding binding : bindings.scanOpen(100)) {
            try {
                if (callbacks.state(binding) == ProofProviderCallbackState.CLOSED && recover(binding)) {
                    recovered++;
                }
            } catch (RuntimeException failure) {
                LOG.warnf(failure,
                    "Replacement recovery failed for execution=%s generation=%d",
                    binding.awaitIdentity().executionId(), binding.awaitIdentity().generation());
            }
        }
        return recovered;
    }

    private boolean recoverClosed(AwsDurableCallbackBinding binding) {
        return callbacks.state(binding) == ProofProviderCallbackState.CLOSED && recover(binding);
    }

    private boolean recover(AwsDurableCallbackBinding binding) {
        AwsDurableExecutionInput input = AwsDurableExecutionInput.resume(
            binding.awaitIdentity(), PIPELINE_ID, CONTRACT_VERSION, RELEASE_VERSION);
        AwsDurableStartResponse response = starter.start(input);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            return false;
        }
        bindings.recordDelivered(binding, "REPLACEMENT_GENERATION_STARTED");
        return true;
    }
}

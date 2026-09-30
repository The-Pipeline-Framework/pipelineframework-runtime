package org.pipelineframework.awsproof;

import java.util.Objects;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.pipelineframework.awsproof.model.ProofExecutionInput;
import org.pipelineframework.awsproof.model.ProofExecutionNames;
import org.pipelineframework.awsproof.model.ProofStartResponse;

@Named("proof-ingress")
@ApplicationScoped
public final class ProofIngressHandler implements RequestHandler<ProofExecutionInput, ProofStartResponse> {
    @Inject
    ProofDurableExecutionStarter starter;

    @Inject
    ProofFaultInjector faults;

    @Override
    public ProofStartResponse handleRequest(ProofExecutionInput input, Context context) {
        Objects.requireNonNull(input, "input");
        String executionName = ProofExecutionNames.durableExecutionName(
            input.tenantId(), input.idempotencyKey(), input.generation());
        faults.failIfArmed("ingress-before-provider-start", executionName);
        ProofStartResponse response = starter.start(input);
        faults.failIfArmed("ingress-after-provider-start", executionName);
        return response;
    }
}

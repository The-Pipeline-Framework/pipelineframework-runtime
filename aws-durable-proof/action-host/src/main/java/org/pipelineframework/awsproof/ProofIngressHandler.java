package org.pipelineframework.awsproof;

import java.util.Objects;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionNames;
import org.pipelineframework.aws.durable.model.AwsDurableStartResponse;

@Named("proof-ingress")
@ApplicationScoped
public final class ProofIngressHandler implements RequestHandler<AwsDurableExecutionInput, AwsDurableStartResponse> {
    @Inject
    ProofDurableExecutionStarter starter;

    @Inject
    ProofFaultInjector faults;

    @Override
    public AwsDurableStartResponse handleRequest(AwsDurableExecutionInput input, Context context) {
        Objects.requireNonNull(input, "input");
        String executionName = AwsDurableExecutionNames.durableExecutionName(
            input.tenantId(), input.idempotencyKey(), input.generation());
        faults.failIfArmed("ingress-before-provider-start", executionName);
        AwsDurableStartResponse response = starter.start(input);
        faults.failIfArmed("ingress-after-provider-start", executionName);
        return response;
    }
}

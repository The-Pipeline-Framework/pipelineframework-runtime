package org.pipelineframework.awsproof;

import java.util.Objects;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.pipelineframework.awaitable.AwaitCompletionDescriptorRegistry;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;

/** One synchronous Lambda invocation maps to one existing bounded TPF or host-mechanical action. */
@Named("proof-action-gateway")
@ApplicationScoped
public final class ProofActionGatewayHandler implements RequestHandler<AwsDurableActionRequest, AwsDurableActionResponse> {
    @Inject
    ProofControlPlaneActionAdapter controlPlaneActions;

    @Inject
    ProofDurableHostActionAdapter durableHostActions;

    @Inject
    AwaitCompletionDescriptorRegistry descriptorRegistry;

    @Inject
    ProofAwaitDescriptorFactory descriptorFactory;

    @Override
    public AwsDurableActionResponse handleRequest(AwsDurableActionRequest request, Context context) {
        Objects.requireNonNull(request, "request");
        descriptorRegistry.register(descriptorFactory.create());
        return switch (request.operation()) {
            case SUBMIT, STATUS, RESULT, REDRIVE, SWEEP, QUERY_PENDING_AWAIT, READ_AWAIT_CHECKPOINT,
                READ_EXECUTION_AWAITS ->
                controlPlaneActions.handle(request);
            case REGISTER_CALLBACK, BIND_CALLBACK -> durableHostActions.handle(request);
        };
    }
}

package org.pipelineframework.aws.durable;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;

/** Named Quarkus Lambda entrypoint for bounded control-plane and callback-binding actions. */
@Named("aws-durable-action")
@ApplicationScoped
public final class AwsDurableActionGatewayHandler
    implements RequestHandler<AwsDurableActionRequest, AwsDurableActionResponse> {
    @Inject
    AwsDurableHostServices services;

    @Override
    public AwsDurableActionResponse handleRequest(AwsDurableActionRequest request, Context context) {
        return services.actions().invoke(request);
    }
}

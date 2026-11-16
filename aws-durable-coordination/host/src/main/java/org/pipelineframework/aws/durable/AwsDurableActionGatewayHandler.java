package org.pipelineframework.aws.durable;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.aws.durable.model.AwsDurableOperation;
import org.pipelineframework.orchestrator.ExecutionDispatchPin;
import org.pipelineframework.orchestrator.RegisteredRestWorkerTargets;
import org.pipelineframework.orchestrator.worker.PipelineWorkerAvailabilityRequest;
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

    @Inject RegisteredRestWorkerTargets registeredTargets;
    @Inject ObjectMapper mapper;

    @Override
    public AwsDurableActionResponse handleRequest(AwsDurableActionRequest request, Context context) {
        if (request.operation() == AwsDurableOperation.CHECK_REGISTERED_WORKER) {
            return checkRegisteredWorker(request);
        }
        return services.actions().invoke(request);
    }

    private AwsDurableActionResponse checkRegisteredWorker(AwsDurableActionRequest request) {
        BindingIdentity identity;
        try {
            identity = mapper.readValue(request.inputJson().orElseThrow(), BindingIdentity.class);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("An exact worker binding identity is required");
        }
        var target = new PipelineWorkerAvailabilityRequest(request.tenantId(), request.pipelineId().orElseThrow(),
            request.contractVersion().orElseThrow(), request.releaseVersion().orElseThrow(), "", "");
        var pin = new ExecutionDispatchPin(identity.entryId(), identity.deploymentOperationId());
        return AwsDurableActionResponse.bound(registeredTargets.checkPinned(target, pin)
            .await().atMost(java.time.Duration.ofSeconds(30)));
    }

    public record BindingIdentity(String entryId, String deploymentOperationId) {}
}

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
    @Inject org.pipelineframework.PipelineExecutionService executionService;

    @Override
    public AwsDurableActionResponse handleRequest(AwsDurableActionRequest request, Context context) {
        if (request.operation() == AwsDurableOperation.CHECK_REGISTERED_WORKER) {
            return checkRegisteredWorker(request);
        }
        if (request.operation() == AwsDurableOperation.COMPLETE_HUMAN_AWAIT) return completeHumanAwait(request);
        return services.actions().invoke(request);
    }

    private AwsDurableActionResponse completeHumanAwait(AwsDurableActionRequest request) {
        var identity = AwsDurableControlPlaneActions.scopedInteraction(request);
        var pin = AwsDurableControlPlaneActions.semanticPin(request);
        AwsDurableHumanCompletionRequest input;
        try {
            input = org.pipelineframework.config.pipeline.PipelineJson.mapper().readValue(
                request.inputJson().orElseThrow(() -> new IllegalArgumentException("inputJson is required")),
                AwsDurableHumanCompletionRequest.class);
        } catch (java.io.IOException malformed) {
            throw new IllegalArgumentException("Invalid canonical human completion request", malformed);
        }
        var command = new org.pipelineframework.awaitable.AwaitCompletionCommand(pin.tenantId(), identity.interactionId(),
            identity.correlationId(), null, input.idempotencyKey(), input.responsePayload(), input.actor(), System.currentTimeMillis());
        var accepted = executionService.completeHumanAwaitInteraction(pin, command).await().atMost(java.time.Duration.ofSeconds(30));
        try {
            return AwsDurableActionResponse.result(mapper.writeValueAsString(
                org.pipelineframework.awaitable.dto.AwaitDtoMapper.toCompletionResponse(accepted)));
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Cannot serialize human completion receipt", failure);
        }
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

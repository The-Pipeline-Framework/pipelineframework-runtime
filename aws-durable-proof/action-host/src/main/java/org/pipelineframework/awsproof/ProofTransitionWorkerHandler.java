package org.pipelineframework.awsproof;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import org.pipelineframework.orchestrator.SqsTransitionWorkerAction;

@Named("proof-transition-worker")
@ApplicationScoped
public final class ProofTransitionWorkerHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    @Inject
    SqsTransitionWorkerAction action;

    @Inject
    ProofSqsBatchAdapter batches;

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        return batches.handle("transition", event, action::handle, Duration.ofMinutes(5));
    }
}

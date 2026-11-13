package org.pipelineframework.aws.durable;

import java.time.Duration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import org.pipelineframework.orchestrator.SqsTransitionWorkerAction;

/** Executes authenticated transitions in one independently deployed, immutable application runtime. */
@Named("aws-durable-pinned-worker")
@ApplicationScoped
public final class AwsDurablePinnedWorkerHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    @Inject
    SqsTransitionWorkerAction action;

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        return new AwsDurableSqsBatchAdapter().handle(event, action::handle, Duration.ofMinutes(5));
    }
}

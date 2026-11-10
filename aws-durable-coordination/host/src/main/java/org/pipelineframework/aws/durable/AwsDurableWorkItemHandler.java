package org.pipelineframework.aws.durable;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import org.pipelineframework.orchestrator.SqsWorkItemAction;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.CoordinationHost;

@Named("aws-durable-work-item")
@ApplicationScoped
public final class AwsDurableWorkItemHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    @Inject
    SqsWorkItemAction action;
    @Inject
    Instance<AwsDurableReleaseRouter> releases;
    @Inject
    PipelineOrchestratorConfig config;

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        return new AwsDurableSqsBatchAdapter().handle(event, message -> {
            if (config.coordinationHost() == CoordinationHost.NATIVE) return action.handle(message);
            if (releases == null || !releases.isResolvable()) throw new IllegalStateException("AWS Durable release routing is unavailable");
            return releases.get().work(message, context, action::handle);
        }, Duration.ofMinutes(5));
    }
}

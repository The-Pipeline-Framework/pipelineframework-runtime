package org.pipelineframework.awsproof;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import org.pipelineframework.awaitable.AwaitCompletionDescriptorRegistry;
import org.pipelineframework.orchestrator.SqsWorkItemAction;

@Named("proof-work-item")
@ApplicationScoped
public final class ProofWorkItemHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    @Inject
    SqsWorkItemAction action;

    @Inject
    ProofSqsBatchAdapter batches;

    @Inject
    AwaitCompletionDescriptorRegistry descriptorRegistry;

    @Inject
    ProofAwaitDescriptorFactory descriptorFactory;

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        descriptorRegistry.register(descriptorFactory.create());
        return batches.handle("work", event, action::handle, Duration.ofMinutes(5));
    }
}

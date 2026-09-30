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
import org.pipelineframework.awaitable.sqs.SqsAwaitCompletionAction;

@Named("proof-await-completion")
@ApplicationScoped
public final class ProofAwaitCompletionHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    @Inject
    SqsAwaitCompletionAction action;

    @Inject
    ProofSqsBatchAdapter batches;

    @Inject
    AwaitCompletionDescriptorRegistry descriptorRegistry;

    @Inject
    ProofAwaitDescriptorFactory descriptorFactory;

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        Duration timeout = Duration.ofSeconds(30);
        descriptorRegistry.register(descriptorFactory.create());
        return batches.handle("await", event, message -> action.handle(message, timeout), timeout);
    }
}

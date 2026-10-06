package org.pipelineframework.aws.durable;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import org.pipelineframework.awaitable.sqs.SqsAwaitCompletionAction;

@Named("aws-durable-await-completion")
@ApplicationScoped
public final class AwsDurableAwaitCompletionHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    @Inject
    SqsAwaitCompletionAction action;

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        Duration timeout = Duration.ofSeconds(30);
        return new AwsDurableSqsBatchAdapter().handle(event, message -> action.handle(message, timeout), timeout);
    }
}

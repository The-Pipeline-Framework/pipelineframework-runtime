package org.pipelineframework.aws.durable;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;

@Named("aws-durable-await-wakeup")
@ApplicationScoped
public final class AwsDurableAwaitWakeupHandler implements RequestHandler<DynamodbEvent, StreamsEventResponse> {
    @Inject
    AwsDurableHostServices services;

    @Override
    public StreamsEventResponse handleRequest(DynamodbEvent event, Context context) {
        return new AwsDurableAwaitStreamBatchAdapter().handle(event, services.streamProcessor()::process);
    }
}

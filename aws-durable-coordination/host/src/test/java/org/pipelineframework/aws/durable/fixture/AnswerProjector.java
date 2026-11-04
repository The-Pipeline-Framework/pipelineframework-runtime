package org.pipelineframework.aws.durable.fixture;

import org.pipelineframework.awaitable.AwaitCompletionMetadata;
import org.pipelineframework.awaitable.AwaitCompletionProjector;

public class AnswerProjector implements AwaitCompletionProjector<Request, Answer, Prepared> {
    @Override public Prepared project(Request request, Answer answer, AwaitCompletionMetadata metadata) {
        return new Prepared(request.id(), answer.value());
    }
}

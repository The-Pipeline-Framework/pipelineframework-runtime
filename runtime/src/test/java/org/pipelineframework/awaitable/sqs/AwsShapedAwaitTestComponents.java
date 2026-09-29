package org.pipelineframework.awaitable.sqs;

import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitTelemetry;

/** Test-only access to the bounded SQS await-completion action. */
public final class AwsShapedAwaitTestComponents {

    private AwsShapedAwaitTestComponents() {
    }

    public static SqsAwaitCompletionAction completionAction(PipelineExecutionService executionService) {
        return new SqsAwaitCompletionAction(executionService, AwaitTelemetry.disabled());
    }
}

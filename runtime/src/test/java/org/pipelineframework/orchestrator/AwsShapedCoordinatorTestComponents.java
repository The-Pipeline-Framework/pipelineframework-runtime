package org.pipelineframework.orchestrator;

import static org.mockito.Mockito.mock;

import java.time.Duration;

import jakarta.enterprise.event.Event;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.invocation.PipelineInvocationRuntime;

import software.amazon.awssdk.services.sqs.SqsClient;

/** Test-only access to package-scoped SQS coordinator components. */
public final class AwsShapedCoordinatorTestComponents {

    private AwsShapedCoordinatorTestComponents() {
    }

    public static SqsWorkDispatcher workDispatcher(
        SqsClient client,
        PipelineOrchestratorConfig config
    ) {
        @SuppressWarnings("unchecked")
        Event<ExecutionWorkItem> executionWorkEvent = mock(Event.class);
        return new SqsWorkDispatcher(client, config, executionWorkEvent);
    }

    public static SqsDeadLetterPublisher deadLetterPublisher(
        SqsClient client,
        PipelineOrchestratorConfig config
    ) {
        return new SqsDeadLetterPublisher(client, config);
    }

    public static SqsPipelineTransitionWorker transitionWorker(
        SqsClient client,
        PipelineOrchestratorConfig config
    ) {
        return new SqsPipelineTransitionWorker(client, config, new PipelineInvocationRuntime());
    }

    public static SqsWorkItemAction workItemAction(PipelineExecutionService executionService) {
        return new SqsWorkItemAction(executionService, Duration.ofSeconds(10));
    }

    public static SqsTransitionWorkerAction transitionWorkerAction(
        SqsClient client,
        PipelineOrchestratorConfig config,
        PipelineExecutionService executionService
    ) {
        return new SqsTransitionWorkerAction(
            config,
            executionService,
            new LocalControlPlaneSecretResolver(),
            client);
    }
}

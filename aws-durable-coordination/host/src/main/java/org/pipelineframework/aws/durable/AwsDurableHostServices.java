package org.pipelineframework.aws.durable;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.awaitable.store.DynamoAwaitInteractionKeyDecoder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.lambda.LambdaClient;

/** Application composition root for generated AWS Durable Lambda handlers. */
@ApplicationScoped
public final class AwsDurableHostServices {
    @Inject
    PipelineControlPlane controlPlane;

    @Inject
    AwsDurableInputDecoder inputDecoder;

    @Inject
    ObjectMapper mapper;

    @Inject
    DynamoDbClient dynamo;

    @Inject
    LambdaClient lambda;

    public AwsDurableControlPlaneActions actions() {
        return new AwsDurableControlPlaneActions(
            controlPlane,
            bindings(),
            inputDecoder,
            mapper,
            duration("TPF_AWS_DURABLE_ACTION_TIMEOUT_SECONDS", 30),
            duration("TPF_AWS_DURABLE_CALLBACK_RETENTION_SECONDS", 7_776_000));
    }

    public AwsDurableAwaitStreamProcessor streamProcessor() {
        return new AwsDurableAwaitStreamProcessor(bindings(), actions(), wakeups(),
            requiredEnvironment("TPF_AWS_DURABLE_AWAIT_STREAM_ARN"),
            requiredEnvironment("TPF_AWS_DURABLE_BINDING_STREAM_ARN"), this::observeAwaitStream);
    }

    Optional<AwsDurableAwaitCheckpoint> observeAwaitStream(
        com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord record
    ) {
        var keys = Optional.ofNullable(record.getDynamodb())
            .map(com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord::getKeys)
            .orElseThrow(() -> new IllegalArgumentException("DynamoDB Stream keys are unavailable"));
        var locator = new DynamoAwaitInteractionKeyDecoder().decode(keys.entrySet().stream()
            .collect(Collectors.toMap(java.util.Map.Entry::getKey,
                entry -> Optional.ofNullable(entry.getValue().getS())
                    .orElseThrow(() -> new IllegalArgumentException("DynamoDB key must be a string")))));
        return checkpointReader().read(locator.tenantId(), locator.interactionId())
            .filter(checkpoint -> locator.tenantId().equals(checkpoint.tenantId())
                && locator.interactionId().equals(checkpoint.interactionId()));
    }

    public AwsDurableTerminalReconciler terminalReconciler() {
        return new AwsDurableTerminalReconciler(
            bindings(), actions(), replacementService(), new LambdaAwsDurableProviderExecutionInspector(lambda));
    }

    private AwsDurableWakeupService wakeups() {
        return new AwsDurableWakeupService(checkpointReader(), bindings(),
            new AwsDurableCallbackClient(lambda, mapper), replacementService());
    }

    private AwsDurableReplacementService replacementService() {
        return new AwsDurableReplacementService(checkpointReader(), new AwsDurableExecutionStarter(
            lambda, mapper, requiredEnvironment("TPF_AWS_DURABLE_FUNCTION"),
            requiredEnvironment("TPF_AWS_DURABLE_QUALIFIER")), bindings());
    }

    private AwsDurableAwaitCheckpointReader checkpointReader() {
        return (tenantId, interactionId) -> controlPlane.getAwaitSemanticCheckpoint(tenantId, interactionId)
            .await().atMost(duration("TPF_AWS_DURABLE_ACTION_TIMEOUT_SECONDS", 30))
            .map(checkpoint -> new AwsDurableAwaitCheckpoint(
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.interactionId(),
                checkpoint.correlationId(), checkpoint.unitId(), checkpoint.stepId(), checkpoint.status().name(),
                checkpoint.pipelineId(), checkpoint.contractVersion(), checkpoint.releaseVersion()));
    }

    private AwsDurableCallbackBindingRepository bindings() {
        return new AwsDurableCallbackBindingRepository(
            dynamo, requiredEnvironment("TPF_AWS_DURABLE_BINDING_TABLE"));
    }

    private static Duration duration(String name, long defaultSeconds) {
        String configured = System.getenv(name);
        return Duration.ofSeconds(configured == null || configured.isBlank()
            ? defaultSeconds
            : Long.parseLong(configured));
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required");
        }
        return value;
    }
}

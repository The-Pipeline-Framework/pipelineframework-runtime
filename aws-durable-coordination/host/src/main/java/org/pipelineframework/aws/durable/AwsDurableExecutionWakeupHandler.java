package org.pipelineframework.aws.durable;

import java.util.Optional;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import org.pipelineframework.orchestrator.DynamoExecutionKeyDecoder;

/** Execution Stream keys locate semantic observations; images cannot authorise callback failure. */
@Named("aws-durable-execution-wakeup")
@ApplicationScoped
public final class AwsDurableExecutionWakeupHandler implements RequestHandler<DynamodbEvent, StreamsEventResponse> {
    @Inject
    AwsDurableHostServices services;
    private final String streamArn;

    public AwsDurableExecutionWakeupHandler() {
        this(Optional.ofNullable(System.getenv("TPF_AWS_DURABLE_EXECUTION_STREAM_ARN")).orElse(""));
    }

    AwsDurableExecutionWakeupHandler(String streamArn) {
        this.streamArn = java.util.Objects.requireNonNull(streamArn, "streamArn");
    }

    @Override
    public StreamsEventResponse handleRequest(DynamodbEvent event, Context context) {
        return new AwsDurableAwaitStreamBatchAdapter().handle(event, this::process);
    }

    AwsDurableWakeupDisposition process(DynamodbEvent.DynamodbStreamRecord record) {
        if (streamArn.isBlank()) throw new IllegalStateException("TPF_AWS_DURABLE_EXECUTION_STREAM_ARN is required");
        if (!streamArn.equals(record.getEventSourceARN())) throw new IllegalArgumentException("unexpected execution Stream source");
        if ("REMOVE".equals(record.getEventName())) return AwsDurableWakeupDisposition.ACKNOWLEDGE;
        if (!"INSERT".equals(record.getEventName()) && !"MODIFY".equals(record.getEventName())) {
            throw new IllegalArgumentException("unsupported execution Stream event");
        }
        var keys = Optional.ofNullable(record.getDynamodb()).map(value -> value.getKeys())
            .orElseThrow(() -> new IllegalArgumentException("execution Stream keys are unavailable"));
        var execution = new DynamoExecutionKeyDecoder().decode(keys.entrySet().stream()
            .collect(Collectors.toMap(java.util.Map.Entry::getKey, entry -> Optional.ofNullable(entry.getValue().getS())
                .orElseThrow(() -> new IllegalArgumentException("execution key must be a string")))));
        return services.terminalCallbacks().settleExecution(execution.tenantId(), execution.executionId());
    }
}

package org.pipelineframework.aws.durable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;

/**
 * Joins TPF Await Stream records with disposable provider callback registrations.
 * Either side may arrive first; registration records repair the TPF-first crash window.
 */
public final class AwsDurableAwaitStreamProcessor {
    private final AwsDurableCallbackBindingRepository bindings;
    private final AwsDurableActionInvoker actions;
    private final AwsDurableWakeupService wakeups;

    public AwsDurableAwaitStreamProcessor(
        AwsDurableCallbackBindingRepository bindings,
        AwsDurableActionInvoker actions,
        AwsDurableWakeupService wakeups
    ) {
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.actions = Objects.requireNonNull(actions, "actions");
        this.wakeups = Objects.requireNonNull(wakeups, "wakeups");
    }

    public AwsDurableWakeupDisposition process(DynamodbEvent.DynamodbStreamRecord record) {
        Objects.requireNonNull(record, "record");
        Map<String, AttributeValue> image = Optional.ofNullable(record.getDynamodb())
            .map(com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord::getNewImage)
            .orElse(Map.of());
        if (image.isEmpty()) {
            return AwsDurableWakeupDisposition.ACKNOWLEDGE;
        }
        return AwsDurableCallbackBindingRepository.REGISTRATION.equals(text(image, "record_type").orElse(""))
            ? processRegistration(image)
            : processAwait(image);
    }

    private AwsDurableWakeupDisposition processAwait(Map<String, AttributeValue> image) {
        Optional<String> tenantId = text(image, "tenant_id");
        Optional<String> executionId = text(image, "execution_id");
        Optional<String> interactionId = text(image, "interaction_id");
        Optional<String> correlationId = text(image, "correlation_id");
        Optional<String> status = text(image, "status");
        if (tenantId.isEmpty() || executionId.isEmpty() || interactionId.isEmpty()
            || correlationId.isEmpty() || status.isEmpty()) {
            return AwsDurableWakeupDisposition.ACKNOWLEDGE;
        }
        Optional<AwsDurableCallbackRegistration> registration = bindings.findLatestRegistration(
            tenantId.orElseThrow(), executionId.orElseThrow());
        if (registration.isEmpty()) {
            return AwsDurableWakeupDisposition.RETRY;
        }
        AwsDurableCallbackRegistration current = registration.orElseThrow();
        AwsDurableAwaitIdentity identity = new AwsDurableAwaitIdentity(
            tenantId.orElseThrow(), executionId.orElseThrow(), interactionId.orElseThrow(),
            correlationId.orElseThrow(), current.checkpoint().generation());
        bindings.bind(current.bind(identity));
        return "COMPLETED".equals(status.orElseThrow())
            ? wakeups.wake(identity)
            : AwsDurableWakeupDisposition.ACKNOWLEDGE;
    }

    private AwsDurableWakeupDisposition processRegistration(Map<String, AttributeValue> image) {
        Optional<AwsDurableCallbackRegistration> decoded = decodeRegistration(image);
        if (decoded.isEmpty()) {
            return AwsDurableWakeupDisposition.ACKNOWLEDGE;
        }
        AwsDurableCallbackRegistration registration = decoded.orElseThrow();
        var response = actions.invoke(AwsDurableActionRequest.executionAwaits(registration.checkpoint()));
        Optional<AwsDurableAwaitCheckpoint> latest = response.awaitCheckpoints().stream().findFirst();
        if (latest.isEmpty()) {
            return AwsDurableWakeupDisposition.RETRY;
        }
        AwsDurableAwaitCheckpoint semantic = latest.orElseThrow();
        AwsDurableAwaitIdentity identity = new AwsDurableAwaitIdentity(
            semantic.tenantId(), semantic.executionId(), semantic.interactionId(), semantic.correlationId(),
            registration.checkpoint().generation());
        bindings.bind(registration.bind(identity));
        return "COMPLETED".equals(semantic.status())
            ? wakeups.wake(identity)
            : AwsDurableWakeupDisposition.ACKNOWLEDGE;
    }

    private static Optional<AwsDurableCallbackRegistration> decodeRegistration(Map<String, AttributeValue> image) {
        try {
            var checkpoint = new org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint(
                new org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint(
                    required(image, "tenant_id"), required(image, "execution_id"),
                    required(image, "pipeline_id"), required(image, "contract_version"),
                    required(image, "release_version")),
                Long.parseLong(requiredNumber(image, "generation")));
            return Optional.of(new AwsDurableCallbackRegistration(
                checkpoint,
                required(image, "provider_execution_name"),
                required(image, "provider_execution_arn"),
                required(image, "provider_callback_id"),
                Long.parseLong(requiredNumber(image, "created_at_epoch_ms")),
                Long.parseLong(requiredNumber(image, "expires_at_epoch_s"))));
        } catch (IllegalArgumentException malformed) {
            return Optional.empty();
        }
    }

    private static Optional<String> text(Map<String, AttributeValue> image, String name) {
        return Optional.ofNullable(image.get(name)).map(AttributeValue::getS).filter(value -> !value.isBlank());
    }

    private static String required(Map<String, AttributeValue> image, String name) {
        return text(image, name).orElseThrow(() -> new IllegalArgumentException(name + " is required"));
    }

    private static String requiredNumber(Map<String, AttributeValue> image, String name) {
        return Optional.ofNullable(image.get(name)).map(AttributeValue::getN)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException(name + " is required"));
    }
}

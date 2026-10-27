package org.pipelineframework.aws.durable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

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
    private final String awaitStreamArn;
    private final String bindingStreamArn;
    private final Function<DynamodbEvent.DynamodbStreamRecord, Optional<AwsDurableAwaitCheckpoint>> observations;
    private final Function<AwsDurableCallbackRegistration, Optional<AwsDurableWakeupDisposition>> terminalSettlements;
    private final Predicate<AwsDurableAwaitCheckpoint> terminalWithoutRegistration;

    public AwsDurableAwaitStreamProcessor(
        AwsDurableCallbackBindingRepository bindings,
        AwsDurableActionInvoker actions,
        AwsDurableWakeupService wakeups,
        String awaitStreamArn,
        String bindingStreamArn,
        Function<DynamodbEvent.DynamodbStreamRecord, Optional<AwsDurableAwaitCheckpoint>> observations
    ) {
        this(bindings, actions, wakeups, awaitStreamArn, bindingStreamArn, observations, ignored -> Optional.empty());
    }

    public AwsDurableAwaitStreamProcessor(AwsDurableCallbackBindingRepository bindings, AwsDurableActionInvoker actions,
        AwsDurableWakeupService wakeups, String awaitStreamArn, String bindingStreamArn,
        Function<DynamodbEvent.DynamodbStreamRecord, Optional<AwsDurableAwaitCheckpoint>> observations,
        Function<AwsDurableCallbackRegistration, Optional<AwsDurableWakeupDisposition>> terminalSettlements) {
        this(bindings, actions, wakeups, awaitStreamArn, bindingStreamArn, observations, terminalSettlements,
            ignored -> false);
    }

    AwsDurableAwaitStreamProcessor(AwsDurableCallbackBindingRepository bindings, AwsDurableActionInvoker actions,
        AwsDurableWakeupService wakeups, String awaitStreamArn, String bindingStreamArn,
        Function<DynamodbEvent.DynamodbStreamRecord, Optional<AwsDurableAwaitCheckpoint>> observations,
        Function<AwsDurableCallbackRegistration, Optional<AwsDurableWakeupDisposition>> terminalSettlements,
        Predicate<AwsDurableAwaitCheckpoint> terminalWithoutRegistration) {
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.actions = Objects.requireNonNull(actions, "actions");
        this.wakeups = Objects.requireNonNull(wakeups, "wakeups");
        this.awaitStreamArn = requiredArn(awaitStreamArn, "awaitStreamArn");
        this.bindingStreamArn = requiredArn(bindingStreamArn, "bindingStreamArn");
        if (awaitStreamArn.equals(bindingStreamArn)) {
            throw new IllegalArgumentException("semantic and mechanical streams must be distinct");
        }
        this.observations = Objects.requireNonNull(observations, "observations");
        this.terminalSettlements = Objects.requireNonNull(terminalSettlements, "terminalSettlements");
        this.terminalWithoutRegistration = Objects.requireNonNull(terminalWithoutRegistration,
            "terminalWithoutRegistration");
    }

    public AwsDurableWakeupDisposition process(DynamodbEvent.DynamodbStreamRecord record) {
        Objects.requireNonNull(record, "record");
        boolean semantic = awaitStreamArn.equals(record.getEventSourceARN());
        if (!semantic && !bindingStreamArn.equals(record.getEventSourceARN())) {
            throw new IllegalArgumentException("unexpected DynamoDB Stream source");
        }
        if ("REMOVE".equals(record.getEventName())) {
            return AwsDurableWakeupDisposition.ACKNOWLEDGE;
        }
        if (!"INSERT".equals(record.getEventName()) && !"MODIFY".equals(record.getEventName())) {
            throw new IllegalArgumentException("unsupported DynamoDB Stream event");
        }
        if (semantic) {
            return observations.apply(record).map(this::processAwait).orElse(AwsDurableWakeupDisposition.RETRY);
        }
        Map<String, AttributeValue> image = Optional.ofNullable(record.getDynamodb())
            .map(com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord::getNewImage)
            .orElse(Map.of());
        return switch (required(image, AwsDurableCallbackBindingRepository.RECORD_TYPE)) {
            case AwsDurableCallbackBindingRepository.REGISTRATION -> processRegistration(image);
            case AwsDurableCallbackBindingRepository.BINDING,
                 AwsDurableCallbackBindingRepository.DELIVERY,
                 AwsDurableCallbackBindingRepository.CALLBACK_ACCEPTED,
                 AwsDurableCallbackBindingRepository.DRIVER_RESUMED,
                 AwsDurableCallbackBindingRepository.CALLBACK_LATENCY,
                 AwsDurableCallbackBindingRepository.REPAIR_FIRST_OBSERVED,
                 "GENERATION_CLAIM", "GENERATION_SUCCESSOR", "RECOVERY", "ADMISSION" -> AwsDurableWakeupDisposition.ACKNOWLEDGE;
            default -> throw new IllegalStateException("unrecognised mechanical evidence; retry with a compatible host");
        };
    }

    private AwsDurableWakeupDisposition processAwait(AwsDurableAwaitCheckpoint semantic) {
        Optional<AwsDurableCallbackRegistration> registration = bindings.findLatestRegistration(
            semantic.tenantId(), semantic.executionId());
        if (registration.isEmpty()) {
            // A completed execution need not ever have suspended its provider on a callback.
            // Retrying its scheduling projections can starve unrelated live callbacks.
            // A late registration still repairs itself from its own retained stream record.
            return terminalWithoutRegistration.test(semantic)
                ? AwsDurableWakeupDisposition.ACKNOWLEDGE : AwsDurableWakeupDisposition.RETRY;
        }
        AwsDurableCallbackRegistration current = registration.orElseThrow();
        if (current.awaitIdentity().filter(pinned -> !pinned.interactionId().equals(semantic.interactionId())
            || !pinned.correlationId().equals(semantic.correlationId())).isPresent()) {
            return AwsDurableWakeupDisposition.ACKNOWLEDGE; // Its pinned registration event repairs the other interaction.
        }
        AwsDurableAwaitIdentity identity = new AwsDurableAwaitIdentity(
            semantic.tenantId(), semantic.executionId(), semantic.interactionId(),
            semantic.correlationId(), current.checkpoint().generation(), current.occurrence());
        try {
            bindings.bind(current.bind(identity));
        } catch (AwsDurableStaleGenerationException stale) {
            // Completion still needs to replay an unresolved replacement start.
        }
        return "COMPLETED".equals(semantic.status())
            ? wakeups.wake(identity)
            : AwsDurableWakeupDisposition.ACKNOWLEDGE;
    }

    private static String requiredArn(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private AwsDurableWakeupDisposition processRegistration(Map<String, AttributeValue> image) {
        AwsDurableCallbackRegistration registration = decodeRegistration(image)
            .orElseThrow(() -> new IllegalStateException("malformed callback registration; retry repair"));
        var terminal = terminalSettlements.apply(registration);
        if (terminal.isPresent()) return terminal.orElseThrow();
        AwsDurableAwaitIdentity pinned = registration.awaitIdentity()
            .orElseThrow(() -> new IllegalStateException("legacy registration requires a validated protocol upgrade"));
        Optional<AwsDurableAwaitCheckpoint> latest = actions.invoke(AwsDurableActionRequest.awaitCheckpoint(pinned))
            .awaitCheckpoint();
        if (latest.isEmpty()) {
            return AwsDurableWakeupDisposition.RETRY;
        }
        AwsDurableAwaitCheckpoint semantic = latest.orElseThrow();
        AwsDurableAwaitIdentity identity = new AwsDurableAwaitIdentity(
            semantic.tenantId(), semantic.executionId(), semantic.interactionId(), semantic.correlationId(),
            registration.checkpoint().generation(), registration.occurrence());
        try {
            bindings.bind(registration.bind(identity));
        } catch (AwsDurableStaleGenerationException stale) {
            // Completion still needs to replay an unresolved replacement start.
        }
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
                Long.parseLong(requiredNumber(image, "expires_at_epoch_s")),
                text(image, "interaction_id").map(interaction -> new AwsDurableAwaitIdentity(checkpoint.tenantId(),
                    checkpoint.executionId(), interaction, required(image, "correlation_id"), checkpoint.generation(),
                    Long.parseLong(requiredNumber(image, "occurrence"))))));
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

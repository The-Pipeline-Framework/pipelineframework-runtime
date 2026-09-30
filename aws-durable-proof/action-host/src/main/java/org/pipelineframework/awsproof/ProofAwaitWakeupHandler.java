package org.pipelineframework.awsproof;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awsproof.model.ProofAwaitIdentity;

@Named("proof-await-wakeup")
@ApplicationScoped
public final class ProofAwaitWakeupHandler implements RequestHandler<DynamodbEvent, StreamsEventResponse> {
    @Inject
    ProofCallbackBindingRepository bindings;

    @Inject
    ProofWakeupService wakeups;

    @Inject
    ProofDurableBindingResolver resolver;

    @Override
    public StreamsEventResponse handleRequest(DynamodbEvent event, Context context) {
        List<StreamsEventResponse.BatchItemFailure> failures = new ArrayList<>();
        for (DynamodbEvent.DynamodbStreamRecord record : event.getRecords()) {
            String itemIdentifier = itemIdentifier(record);
            try {
                Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();
                Map<String, AttributeValue> oldImage = record.getDynamodb().getOldImage();
                Optional<ProofAwaitIdentity> identity = identity(newImage);
                if (identity.isEmpty() && (newImage == null || newImage.isEmpty())) {
                    identity = bindingIdentity(oldImage);
                    identity.ifPresent(this::reconstruct);
                }
                if (identity.isPresent()
                    && wakeups.wake(identity.orElseThrow()) == ProofWakeupDisposition.RETRY) {
                    failures.add(new StreamsEventResponse.BatchItemFailure(itemIdentifier));
                }
            } catch (RuntimeException failure) {
                failures.add(new StreamsEventResponse.BatchItemFailure(itemIdentifier));
            }
        }
        return new StreamsEventResponse(List.copyOf(failures));
    }

    static String itemIdentifier(DynamodbEvent.DynamodbStreamRecord record) {
        return Optional.ofNullable(record.getDynamodb())
            .map(com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord::getSequenceNumber)
            .filter(sequenceNumber -> !sequenceNumber.isBlank())
            .orElse("");
    }

    private Optional<ProofAwaitIdentity> identity(Map<String, AttributeValue> image) {
        if (image == null || image.isEmpty()) {
            return Optional.empty();
        }
        String recordType = text(image, ProofCallbackBindingRepository.RECORD_TYPE).orElse("");
        if (ProofCallbackBindingRepository.BINDING.equals(recordType)) {
            return Optional.of(new ProofAwaitIdentity(
                text(image, "tenant_id").orElseThrow(),
                text(image, "execution_id").orElseThrow(),
                text(image, "interaction_id").orElseThrow(),
                text(image, "correlation_id").orElseThrow(),
                number(image, "generation").orElseThrow()));
        }
        if (ProofCallbackBindingRepository.REGISTRATION.equals(recordType)
            || ProofCallbackBindingRepository.DELIVERY.equals(recordType)) {
            return Optional.empty();
        }
        String status = text(image, "status").orElse("");
        if (status.isBlank()) {
            return Optional.empty();
        }
        String tenantId = text(image, "tenant_id").orElseThrow();
        String executionId = text(image, "execution_id").orElseThrow();
        String interactionId = text(image, "interaction_id").orElseThrow();
        String correlationId = text(image, "correlation_id").orElseThrow();
        var registration = bindings.findLatestRegistration(tenantId, executionId)
            .or(() -> resolver.reconstructRegistration(tenantId, executionId).map(recovered -> {
                bindings.register(recovered);
                return recovered;
            }))
            .orElseThrow(() -> new IllegalStateException(
                "provider callback registration is not available yet"));
        ProofAwaitIdentity identity = new ProofAwaitIdentity(
            tenantId, executionId, interactionId, correlationId, registration.checkpoint().generation());
        bindings.bind(registration.bind(identity));
        return AwaitInteractionStatus.COMPLETED.name().equals(status)
            ? Optional.of(identity)
            : Optional.empty();
    }

    private Optional<ProofAwaitIdentity> bindingIdentity(Map<String, AttributeValue> image) {
        if (image == null || image.isEmpty()
            || !ProofCallbackBindingRepository.BINDING.equals(
                text(image, ProofCallbackBindingRepository.RECORD_TYPE).orElse(""))) {
            return Optional.empty();
        }
        return Optional.of(new ProofAwaitIdentity(
            text(image, "tenant_id").orElseThrow(),
            text(image, "execution_id").orElseThrow(),
            text(image, "interaction_id").orElseThrow(),
            text(image, "correlation_id").orElseThrow(),
            number(image, "generation").orElseThrow()));
    }

    private void reconstruct(ProofAwaitIdentity identity) {
        resolver.reconstruct(identity).ifPresent(bindings::bind);
    }

    private static Optional<String> text(Map<String, AttributeValue> image, String name) {
        return Optional.ofNullable(image.get(name)).map(AttributeValue::getS).filter(value -> !value.isBlank());
    }

    private static Optional<Long> number(Map<String, AttributeValue> image, String name) {
        return Optional.ofNullable(image.get(name)).map(AttributeValue::getN).map(Long::parseLong);
    }
}

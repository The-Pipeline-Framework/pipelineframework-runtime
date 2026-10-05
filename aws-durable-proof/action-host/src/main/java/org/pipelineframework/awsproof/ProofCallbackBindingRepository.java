package org.pipelineframework.awsproof;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

/** Proof fixture around the production binding repository; only broad fault reconciliation remains proof-only. */
@ApplicationScoped
final class ProofCallbackBindingRepository {
    static final String PK = org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository.PK;
    static final String SK = org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository.SK;
    static final String RECORD_TYPE =
        org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository.RECORD_TYPE;
    static final String BINDING = org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository.BINDING;
    static final String REGISTRATION =
        org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository.REGISTRATION;
    static final String DELIVERY = org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository.DELIVERY;

    @Inject
    DynamoDbClient dynamo;

    boolean bind(AwsDurableCallbackBinding binding) {
        return delegate().bind(binding);
    }

    boolean register(AwsDurableCallbackRegistration registration) {
        return delegate().register(registration);
    }

    Optional<AwsDurableCallbackRegistration> findRegistration(
        String tenantId,
        String executionId,
        long generation
    ) {
        return delegate().findRegistration(tenantId, executionId, generation);
    }

    Optional<AwsDurableCallbackRegistration> findLatestRegistration(String tenantId, String executionId) {
        return delegate().findLatestRegistration(tenantId, executionId);
    }

    Optional<AwsDurableCallbackBinding> find(String tenantId, String interactionId, long generation) {
        return delegate().find(tenantId, interactionId, generation);
    }

    Optional<AwsDurableCallbackBinding> findLatest(String tenantId, String interactionId) {
        return delegate().findLatest(tenantId, interactionId);
    }

    boolean delivered(AwsDurableCallbackBinding binding) {
        return delegate().delivered(binding);
    }

    void recordDelivered(AwsDurableCallbackBinding binding, String providerOutcome) {
        delegate().recordDelivered(binding, providerOutcome);
    }

    List<AwsDurableCallbackBinding> scanOpen(int limit) {
        if (limit <= 0) {
            return List.of();
        }
        List<AwsDurableCallbackBinding> bindings = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        do {
            ScanRequest.Builder request = ScanRequest.builder()
                .tableName(tableName())
                .filterExpression("#recordType = :binding")
                .expressionAttributeNames(Map.of("#recordType", RECORD_TYPE))
                .expressionAttributeValues(Map.of(":binding", AttributeValue.fromS(BINDING)))
                .limit(Math.max(100, limit - bindings.size()))
                .consistentRead(true);
            if (!lastEvaluatedKey.isEmpty()) {
                request.exclusiveStartKey(lastEvaluatedKey);
            }
            var response = dynamo.scan(request.build());
            for (Map<String, AttributeValue> item : response.items()) {
                var decoded = decodeBinding(item);
                if (decoded.isPresent() && !delivered(decoded.orElseThrow())) {
                    bindings.add(decoded.orElseThrow());
                    if (bindings.size() == limit) {
                        break;
                    }
                }
            }
            lastEvaluatedKey = response.lastEvaluatedKey();
        } while (bindings.size() < limit && lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
        return List.copyOf(bindings);
    }

    static Optional<AwsDurableCallbackBinding> decodeBinding(Map<String, AttributeValue> item) {
        return org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository.decodeBinding(item);
    }

    static boolean sameBindingAuthority(
        AwsDurableCallbackBinding existing,
        AwsDurableCallbackBinding candidate
    ) {
        return org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository
            .sameBindingAuthority(existing, candidate);
    }

    static boolean sameRegistrationAuthority(
        AwsDurableCallbackRegistration existing,
        AwsDurableCallbackRegistration candidate
    ) {
        return org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository
            .sameRegistrationAuthority(existing, candidate);
    }

    private org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository delegate() {
        return new org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository(dynamo, tableName());
    }

    private static String tableName() {
        String table = System.getenv("TPF_PROOF_BINDING_TABLE");
        if (table == null || table.isBlank()) {
            throw new IllegalStateException("TPF_PROOF_BINDING_TABLE is required");
        }
        return table;
    }
}

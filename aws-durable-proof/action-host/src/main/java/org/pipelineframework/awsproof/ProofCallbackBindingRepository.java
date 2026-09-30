package org.pipelineframework.awsproof;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.jboss.logging.Logger;
import org.pipelineframework.awsproof.model.ProofAwaitIdentity;
import org.pipelineframework.awsproof.model.ProofBindingStatus;
import org.pipelineframework.awsproof.model.ProofCallbackBinding;
import org.pipelineframework.awsproof.model.ProofCallbackRegistration;
import org.pipelineframework.awsproof.model.ProofExecutionCheckpoint;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

/** Immutable proof-only provider bindings and append-only delivery evidence. */
@ApplicationScoped
final class ProofCallbackBindingRepository {
    private static final Logger LOG = Logger.getLogger(ProofCallbackBindingRepository.class);
    static final String PK = "pk";
    static final String SK = "sk";
    static final String RECORD_TYPE = "record_type";
    static final String BINDING = "BINDING";
    static final String REGISTRATION = "REGISTRATION";
    static final String DELIVERY = "DELIVERY";

    @Inject
    DynamoDbClient dynamo;

    boolean bind(ProofCallbackBinding binding) {
        Objects.requireNonNull(binding, "binding");
        try {
            dynamo.putItem(PutItemRequest.builder()
                .tableName(tableName())
                .item(toItem(binding))
                .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK))
                .build());
            return true;
        } catch (ConditionalCheckFailedException conflict) {
            ProofCallbackBinding existing = find(
                binding.awaitIdentity().tenantId(),
                binding.awaitIdentity().interactionId(),
                binding.awaitIdentity().generation()).orElseThrow(() -> conflict);
            if (!sameBindingAuthority(existing, binding)) {
                throw new IllegalStateException("callback binding generation is already owned by another callback", conflict);
            }
            return false;
        }
    }

    boolean register(ProofCallbackRegistration registration) {
        Objects.requireNonNull(registration, "registration");
        try {
            dynamo.putItem(PutItemRequest.builder()
                .tableName(tableName())
                .item(toItem(registration))
                .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK))
                .build());
            return true;
        } catch (ConditionalCheckFailedException conflict) {
            ProofExecutionCheckpoint checkpoint = registration.checkpoint();
            ProofCallbackRegistration existing = findRegistration(
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation())
                .orElseThrow(() -> conflict);
            if (!sameRegistrationAuthority(existing, registration)) {
                throw new IllegalStateException(
                    "callback registration generation is already owned by another callback", conflict);
            }
            return false;
        }
    }

    Optional<ProofCallbackRegistration> findRegistration(
        String tenantId,
        String executionId,
        long generation
    ) {
        Map<String, AttributeValue> item = dynamo.getItem(GetItemRequest.builder()
            .tableName(tableName())
            .consistentRead(true)
            .key(Map.of(
                PK, string(tenantId + "#" + executionId),
                SK, string(ProofCallbackRegistration.registrationSortKey(generation))))
            .build()).item();
        return item == null || item.isEmpty()
            ? Optional.empty()
            : Optional.of(registrationFromItem(item));
    }

    Optional<ProofCallbackRegistration> findLatestRegistration(String tenantId, String executionId) {
        var response = dynamo.query(QueryRequest.builder()
            .tableName(tableName())
            .keyConditionExpression("#pk = :pk AND begins_with(#sk, :registration)")
            .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK))
            .expressionAttributeValues(Map.of(
                ":pk", string(tenantId + "#" + executionId),
                ":registration", string("REGISTRATION#")))
            .scanIndexForward(false)
            .consistentRead(true)
            .limit(1)
            .build());
        return response.items().stream().findFirst().map(this::registrationFromItem);
    }

    Optional<ProofCallbackBinding> find(String tenantId, String interactionId, long generation) {
        Map<String, AttributeValue> item = dynamo.getItem(GetItemRequest.builder()
            .tableName(tableName())
            .consistentRead(true)
            .key(Map.of(
                PK, string(tenantId + "#" + interactionId),
                SK, string(ProofCallbackBinding.generationSortKey(generation))))
            .build()).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(fromItem(item));
    }

    Optional<ProofCallbackBinding> findLatest(String tenantId, String interactionId) {
        var response = dynamo.query(QueryRequest.builder()
            .tableName(tableName())
            .keyConditionExpression("#pk = :pk AND begins_with(#sk, :generation)")
            .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK))
            .expressionAttributeValues(Map.of(
                ":pk", string(tenantId + "#" + interactionId),
                ":generation", string("GEN#")))
            .scanIndexForward(false)
            .consistentRead(true)
            .limit(1)
            .build());
        return response.items().stream().findFirst().map(ProofCallbackBindingRepository::fromItem);
    }

    boolean delivered(ProofCallbackBinding binding) {
        Map<String, AttributeValue> item = dynamo.getItem(GetItemRequest.builder()
            .tableName(tableName())
            .consistentRead(true)
            .key(Map.of(
                PK, string(binding.partitionKey()),
                SK, string(deliveredKey(binding.awaitIdentity().generation()))))
            .build()).item();
        return item != null && !item.isEmpty();
    }

    void recordDelivered(ProofCallbackBinding binding, String providerOutcome) {
        Map<String, AttributeValue> item = Map.of(
            PK, string(binding.partitionKey()),
            SK, string(deliveredKey(binding.awaitIdentity().generation())),
            RECORD_TYPE, string(DELIVERY),
            "provider_callback_id", string(binding.providerCallbackId()),
            "provider_outcome", string(required(providerOutcome, "providerOutcome")),
            "delivered_at_epoch_ms", number(Instant.now().toEpochMilli()),
            "attempt_id", string(UUID.randomUUID().toString()));
        try {
            dynamo.putItem(PutItemRequest.builder()
                .tableName(tableName())
                .item(item)
                .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK))
                .build());
        } catch (ConditionalCheckFailedException duplicate) {
            // Delivery evidence is immutable and idempotent for one generation.
        }
    }

    List<ProofCallbackBinding> scanOpen(int limit) {
        if (limit <= 0) {
            return List.of();
        }
        List<ProofCallbackBinding> bindings = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        do {
            ScanRequest.Builder request = ScanRequest.builder()
                .tableName(tableName())
                .filterExpression("#recordType = :binding")
                .expressionAttributeNames(Map.of("#recordType", RECORD_TYPE))
                .expressionAttributeValues(Map.of(":binding", string(BINDING)))
                .limit(Math.max(100, limit - bindings.size()))
                .consistentRead(true);
            if (!lastEvaluatedKey.isEmpty()) {
                request.exclusiveStartKey(lastEvaluatedKey);
            }
            var response = dynamo.scan(request.build());
            for (Map<String, AttributeValue> item : response.items()) {
                Optional<ProofCallbackBinding> decoded = decodeBinding(item);
                if (decoded.isEmpty()) {
                    continue;
                }
                ProofCallbackBinding binding = decoded.orElseThrow();
                if (!delivered(binding)) {
                    bindings.add(binding);
                    if (bindings.size() == limit) {
                        break;
                    }
                }
            }
            lastEvaluatedKey = response.lastEvaluatedKey();
        } while (bindings.size() < limit && lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
        return List.copyOf(bindings);
    }

    private Map<String, AttributeValue> toItem(ProofCallbackBinding binding) {
        ProofAwaitIdentity identity = binding.awaitIdentity();
        return Map.ofEntries(
            Map.entry(PK, string(binding.partitionKey())),
            Map.entry(SK, string(binding.sortKey())),
            Map.entry(RECORD_TYPE, string(BINDING)),
            Map.entry("tenant_id", string(identity.tenantId())),
            Map.entry("execution_id", string(identity.executionId())),
            Map.entry("interaction_id", string(identity.interactionId())),
            Map.entry("correlation_id", string(identity.correlationId())),
            Map.entry("generation", number(identity.generation())),
            Map.entry("provider_execution_name", string(binding.providerExecutionName())),
            Map.entry("provider_execution_arn", string(binding.providerExecutionArn())),
            Map.entry("provider_callback_id", string(binding.providerCallbackId())),
            Map.entry("binding_status", string(binding.status().name())),
            Map.entry("created_at_epoch_ms", number(binding.createdAtEpochMs())),
            Map.entry("expires_at_epoch_s", number(binding.expiresAtEpochS())));
    }

    private Map<String, AttributeValue> toItem(ProofCallbackRegistration registration) {
        ProofExecutionCheckpoint checkpoint = registration.checkpoint();
        return Map.ofEntries(
            Map.entry(PK, string(registration.partitionKey())),
            Map.entry(SK, string(registration.sortKey())),
            Map.entry(RECORD_TYPE, string(REGISTRATION)),
            Map.entry("tenant_id", string(checkpoint.tenantId())),
            Map.entry("execution_id", string(checkpoint.executionId())),
            Map.entry("pipeline_id", string(checkpoint.pipelineId())),
            Map.entry("contract_version", string(checkpoint.contractVersion())),
            Map.entry("release_version", string(checkpoint.releaseVersion())),
            Map.entry("generation", number(checkpoint.generation())),
            Map.entry("provider_execution_name", string(registration.providerExecutionName())),
            Map.entry("provider_execution_arn", string(registration.providerExecutionArn())),
            Map.entry("provider_callback_id", string(registration.providerCallbackId())),
            Map.entry("created_at_epoch_ms", number(registration.createdAtEpochMs())),
            Map.entry("expires_at_epoch_s", number(registration.expiresAtEpochS())));
    }

    static Optional<ProofCallbackBinding> decodeBinding(Map<String, AttributeValue> item) {
        try {
            return Optional.of(fromItem(item));
        } catch (IllegalArgumentException | IllegalStateException malformed) {
            LOG.warnf(malformed,
                "Skipping malformed proof callback binding during reconciliation pk=%s sk=%s",
                item.get(PK), item.get(SK));
            return Optional.empty();
        }
    }

    private static ProofCallbackBinding fromItem(Map<String, AttributeValue> item) {
        ProofAwaitIdentity identity = new ProofAwaitIdentity(
            text(item, "tenant_id"),
            text(item, "execution_id"),
            text(item, "interaction_id"),
            text(item, "correlation_id"),
            longValue(item, "generation"));
        return new ProofCallbackBinding(
            identity,
            text(item, "provider_execution_name"),
            text(item, "provider_execution_arn"),
            text(item, "provider_callback_id"),
            ProofBindingStatus.valueOf(text(item, "binding_status")),
            longValue(item, "created_at_epoch_ms"),
            longValue(item, "expires_at_epoch_s"));
    }

    private ProofCallbackRegistration registrationFromItem(Map<String, AttributeValue> item) {
        ProofExecutionCheckpoint checkpoint = new ProofExecutionCheckpoint(
            text(item, "tenant_id"),
            text(item, "execution_id"),
            true,
            text(item, "pipeline_id"),
            text(item, "contract_version"),
            text(item, "release_version"),
            longValue(item, "generation"));
        return new ProofCallbackRegistration(
            checkpoint,
            text(item, "provider_execution_name"),
            text(item, "provider_execution_arn"),
            text(item, "provider_callback_id"),
            longValue(item, "created_at_epoch_ms"),
            longValue(item, "expires_at_epoch_s"));
    }

    private String tableName() {
        return required(System.getenv("TPF_PROOF_BINDING_TABLE"), "TPF_PROOF_BINDING_TABLE");
    }

    static boolean sameBindingAuthority(ProofCallbackBinding existing, ProofCallbackBinding candidate) {
        return existing.awaitIdentity().equals(candidate.awaitIdentity())
            && existing.providerExecutionName().equals(candidate.providerExecutionName())
            && existing.providerExecutionArn().equals(candidate.providerExecutionArn())
            && existing.providerCallbackId().equals(candidate.providerCallbackId())
            && existing.status() == candidate.status();
    }

    static boolean sameRegistrationAuthority(
        ProofCallbackRegistration existing,
        ProofCallbackRegistration candidate
    ) {
        return existing.checkpoint().tenantId().equals(candidate.checkpoint().tenantId())
            && existing.checkpoint().executionId().equals(candidate.checkpoint().executionId())
            && existing.checkpoint().generation() == candidate.checkpoint().generation()
            && existing.providerExecutionName().equals(candidate.providerExecutionName())
            && existing.providerExecutionArn().equals(candidate.providerExecutionArn())
            && existing.providerCallbackId().equals(candidate.providerCallbackId());
    }

    private static String deliveredKey(long generation) {
        return "DELIVERED#" + generation;
    }

    private static AttributeValue string(String value) {
        return AttributeValue.builder().s(required(value, "attribute value")).build();
    }

    private static AttributeValue number(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String text(Map<String, AttributeValue> item, String name) {
        return required(Optional.ofNullable(item.get(name)).map(AttributeValue::s)
            .orElseThrow(() -> new IllegalStateException("binding attribute is missing: " + name)), name);
    }

    private static long longValue(Map<String, AttributeValue> item, String name) {
        String value = Optional.ofNullable(item.get(name)).map(AttributeValue::n)
            .orElseThrow(() -> new IllegalStateException("binding attribute is missing: " + name));
        return Long.parseLong(value);
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

package org.pipelineframework.aws.durable;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableBindingStatus;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

/** Immutable provider callback bindings and append-only delivery evidence. */
public final class AwsDurableCallbackBindingRepository {
    public static final String PROVIDER_EXECUTION_INDEX = "provider-execution-index";
    public static final String PK = "pk";
    public static final String SK = "sk";
    public static final String RECORD_TYPE = "record_type";
    public static final String BINDING = "BINDING";
    public static final String REGISTRATION = "REGISTRATION";
    public static final String DELIVERY = "DELIVERY";
    private static final int PROVIDER_LOOKUP_PAGE_SIZE = 25;
    private static final int PROVIDER_LOOKUP_MAX_PAGES = 8;

    private final DynamoDbClient dynamo;
    private final String tableName;

    public AwsDurableCallbackBindingRepository(DynamoDbClient dynamo, String tableName) {
        this.dynamo = Objects.requireNonNull(dynamo, "dynamo");
        this.tableName = required(tableName, "tableName");
    }

    public boolean bind(AwsDurableCallbackBinding binding) {
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
            AwsDurableCallbackBinding existing = find(
                binding.awaitIdentity().tenantId(),
                binding.awaitIdentity().interactionId(),
                binding.awaitIdentity().generation()).orElseThrow(() -> conflict);
            if (!sameBindingAuthority(existing, binding)) {
                throw new IllegalStateException("callback binding generation is already owned by another callback", conflict);
            }
            return false;
        }
    }

    public boolean register(AwsDurableCallbackRegistration registration) {
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
            AwsDurableDriverCheckpoint checkpoint = registration.checkpoint();
            AwsDurableCallbackRegistration existing = findRegistration(
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation())
                .orElseThrow(() -> conflict);
            if (!sameRegistrationAuthority(existing, registration)) {
                throw new IllegalStateException(
                    "callback registration generation is already owned by another callback", conflict);
            }
            return false;
        }
    }

    public Optional<AwsDurableCallbackRegistration> findRegistration(
        String tenantId,
        String executionId,
        long generation
    ) {
        Map<String, AttributeValue> item = dynamo.getItem(GetItemRequest.builder()
            .tableName(tableName())
            .consistentRead(true)
            .key(Map.of(
                PK, string(tenantId + "#" + executionId),
                SK, string(AwsDurableCallbackRegistration.registrationSortKey(generation))))
            .build()).item();
        return item == null || item.isEmpty()
            ? Optional.empty()
            : Optional.of(registrationFromItem(item));
    }

    public Optional<AwsDurableCallbackRegistration> findLatestRegistration(String tenantId, String executionId) {
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

    /** Resolves one provider terminal event without scanning callback state. */
    public Optional<AwsDurableCallbackRegistration> findRegistrationByProviderExecutionArn(
        String providerExecutionArn
    ) {
        String arn = required(providerExecutionArn, "providerExecutionArn");
        Map<String, AttributeValue> cursor = Map.of();
        for (int page = 0; page < PROVIDER_LOOKUP_MAX_PAGES; page++) {
            var request = QueryRequest.builder()
                .tableName(tableName())
                .indexName(PROVIDER_EXECUTION_INDEX)
                .keyConditionExpression("#providerArn = :providerArn")
                .filterExpression("#recordType = :registration")
                .expressionAttributeNames(Map.of(
                    "#providerArn", "provider_execution_arn", "#recordType", RECORD_TYPE))
                .expressionAttributeValues(Map.of(
                    ":providerArn", string(arn), ":registration", string(REGISTRATION)))
                .limit(PROVIDER_LOOKUP_PAGE_SIZE);
            if (!cursor.isEmpty()) {
                request.exclusiveStartKey(cursor);
            }
            var response = dynamo.query(request.build());
            Optional<AwsDurableCallbackRegistration> registration = response.items().stream()
                .filter(item -> REGISTRATION.equals(text(item, RECORD_TYPE)))
                .findFirst()
                .map(this::registrationFromItem);
            if (registration.isPresent()) {
                return registration;
            }
            cursor = response.lastEvaluatedKey();
            if (cursor.isEmpty()) {
                return Optional.empty();
            }
        }
        throw new IllegalStateException("provider registration lookup exceeded its bounded page budget");
    }

    public Optional<AwsDurableCallbackBinding> find(String tenantId, String interactionId, long generation) {
        Map<String, AttributeValue> item = dynamo.getItem(GetItemRequest.builder()
            .tableName(tableName())
            .consistentRead(true)
            .key(Map.of(
                PK, string(tenantId + "#" + interactionId),
                SK, string(AwsDurableCallbackBinding.generationSortKey(generation))))
            .build()).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(fromItem(item));
    }

    public Optional<AwsDurableCallbackBinding> findLatest(String tenantId, String interactionId) {
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
        return response.items().stream().findFirst().map(AwsDurableCallbackBindingRepository::fromItem);
    }

    public boolean delivered(AwsDurableCallbackBinding binding) {
        Map<String, AttributeValue> item = dynamo.getItem(GetItemRequest.builder()
            .tableName(tableName())
            .consistentRead(true)
            .key(Map.of(
                PK, string(binding.partitionKey()),
                SK, string(deliveredKey(binding.awaitIdentity().generation()))))
            .build()).item();
        return item != null && !item.isEmpty();
    }

    public void recordDelivered(AwsDurableCallbackBinding binding, String providerOutcome) {
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

    private Map<String, AttributeValue> toItem(AwsDurableCallbackBinding binding) {
        AwsDurableAwaitIdentity identity = binding.awaitIdentity();
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

    private Map<String, AttributeValue> toItem(AwsDurableCallbackRegistration registration) {
        AwsDurableDriverCheckpoint checkpoint = registration.checkpoint();
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

    public static Optional<AwsDurableCallbackBinding> decodeBinding(Map<String, AttributeValue> item) {
        try {
            return Optional.of(fromItem(item));
        } catch (IllegalArgumentException | IllegalStateException malformed) {
            return Optional.empty();
        }
    }

    private static AwsDurableCallbackBinding fromItem(Map<String, AttributeValue> item) {
        AwsDurableAwaitIdentity identity = new AwsDurableAwaitIdentity(
            text(item, "tenant_id"),
            text(item, "execution_id"),
            text(item, "interaction_id"),
            text(item, "correlation_id"),
            longValue(item, "generation"));
        return new AwsDurableCallbackBinding(
            identity,
            text(item, "provider_execution_name"),
            text(item, "provider_execution_arn"),
            text(item, "provider_callback_id"),
            AwsDurableBindingStatus.valueOf(text(item, "binding_status")),
            longValue(item, "created_at_epoch_ms"),
            longValue(item, "expires_at_epoch_s"));
    }

    private AwsDurableCallbackRegistration registrationFromItem(Map<String, AttributeValue> item) {
        AwsDurableDriverCheckpoint checkpoint = new AwsDurableDriverCheckpoint(
            new AwsDurableExecutionCheckpoint(
                text(item, "tenant_id"),
                text(item, "execution_id"),
                text(item, "pipeline_id"),
                text(item, "contract_version"),
                text(item, "release_version")),
            longValue(item, "generation"));
        return new AwsDurableCallbackRegistration(
            checkpoint,
            text(item, "provider_execution_name"),
            text(item, "provider_execution_arn"),
            text(item, "provider_callback_id"),
            longValue(item, "created_at_epoch_ms"),
            longValue(item, "expires_at_epoch_s"));
    }

    private String tableName() {
        return tableName;
    }

    public static boolean sameBindingAuthority(
        AwsDurableCallbackBinding existing,
        AwsDurableCallbackBinding candidate
    ) {
        return existing.awaitIdentity().equals(candidate.awaitIdentity())
            && existing.providerExecutionName().equals(candidate.providerExecutionName())
            && existing.providerExecutionArn().equals(candidate.providerExecutionArn())
            && existing.providerCallbackId().equals(candidate.providerCallbackId())
            && existing.status() == candidate.status();
    }

    public static boolean sameRegistrationAuthority(
        AwsDurableCallbackRegistration existing,
        AwsDurableCallbackRegistration candidate
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

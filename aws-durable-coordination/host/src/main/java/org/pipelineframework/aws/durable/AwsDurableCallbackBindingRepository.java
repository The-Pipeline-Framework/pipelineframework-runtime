package org.pipelineframework.aws.durable;

import java.time.Instant;
import java.util.List;
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
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

/** Immutable provider callback bindings and append-only delivery evidence. */
public final class AwsDurableCallbackBindingRepository {
    public static final String PROVIDER_EXECUTION_INDEX = "provider-execution-index";
    public static final String PROVIDER_REGISTRATION_INDEX = "provider-registration-index";
    public static final String PK = "pk";
    public static final String SK = "sk";
    public static final String RECORD_TYPE = "record_type";
    public static final String BINDING = "BINDING";
    public static final String REGISTRATION = "REGISTRATION";
    public static final String DELIVERY = "DELIVERY";

    private final DynamoDbClient dynamo;
    private final String tableName;

    public AwsDurableCallbackBindingRepository(DynamoDbClient dynamo, String tableName) {
        this.dynamo = Objects.requireNonNull(dynamo, "dynamo");
        this.tableName = required(tableName, "tableName");
    }

    /** Generation claims and successor fences never expire with disposable callback rows. */
    public AwsDurableGenerationAdmission claimGeneration(AwsDurableDriverCheckpoint checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        String partition = checkpoint.tenantId() + "#" + checkpoint.executionId();
        Map<String, AttributeValue> claim = Map.ofEntries(
            Map.entry(PK, string(partition)), Map.entry(SK, string(claimKey(checkpoint.generation()))),
            Map.entry(RECORD_TYPE, string("GENERATION_CLAIM")),
            Map.entry("schema_version", number(2)),
            Map.entry("pipeline_id", string(checkpoint.pipelineId())),
            Map.entry("contract_version", string(checkpoint.contractVersion())),
            Map.entry("release_version", string(checkpoint.releaseVersion())));
        var operations = new java.util.ArrayList<TransactWriteItem>();
        operations.add(TransactWriteItem.builder().put(immutablePut(claim)).build());
        if (checkpoint.generation() > 1) {
            long predecessor = checkpoint.generation() - 1;
            operations.add(TransactWriteItem.builder().conditionCheck(ConditionCheck.builder()
                .tableName(tableName).key(key(partition, claimKey(predecessor)))
                .conditionExpression("pipeline_id = :pipeline AND contract_version = :contract AND release_version = :release")
                .expressionAttributeValues(Map.of(":pipeline", string(checkpoint.pipelineId()),
                    ":contract", string(checkpoint.contractVersion()), ":release", string(checkpoint.releaseVersion())))
                .build()).build());
            operations.add(TransactWriteItem.builder().put(immutablePut(Map.of(
                PK, string(partition), SK, string(successorKey(predecessor)),
                RECORD_TYPE, string("GENERATION_SUCCESSOR"), "schema_version", number(2),
                "generation", number(checkpoint.generation())))).build());
        }
        try {
            dynamo.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(operations).build());
            return AwsDurableGenerationAdmission.CREATED;
        } catch (TransactionCanceledException conflict) {
            Map<String, AttributeValue> existing = read(partition, claimKey(checkpoint.generation()));
            if (existing.isEmpty()) {
                throw conflict; // Missing predecessor, contention or infrastructure failure remains retryable.
            }
            if (!existing.equals(claim)) {
                throw new IllegalStateException("generation is already owned by another semantic checkpoint", conflict);
            }
            if (!read(partition, successorKey(checkpoint.generation())).isEmpty()) {
                return AwsDurableGenerationAdmission.STALE;
            }
            return AwsDurableGenerationAdmission.REPLAYED;
        }
    }

    public boolean isCurrentGeneration(String tenantId, String executionId, long generation) {
        String partition = required(tenantId, "tenantId") + "#" + required(executionId, "executionId");
        return !read(partition, claimKey(generation)).isEmpty()
            && !isSuperseded(tenantId, executionId, generation);
    }

    public boolean isSuperseded(String tenantId, String executionId, long generation) {
        return !read(required(tenantId, "tenantId") + "#" + required(executionId, "executionId"),
            successorKey(generation)).isEmpty();
    }

    public boolean bind(AwsDurableCallbackBinding binding) {
        Objects.requireNonNull(binding, "binding");
        AwsDurableAwaitIdentity identity = binding.awaitIdentity();
        String partition = identity.tenantId() + "#" + identity.executionId();
        var operations = generationConditions(partition, identity.generation());
        operations.add(TransactWriteItem.builder().conditionCheck(ConditionCheck.builder()
            .tableName(tableName).key(key(partition,
                AwsDurableCallbackRegistration.registrationSortKey(identity.generation())))
            .conditionExpression("provider_callback_id = :callback AND provider_execution_arn = :arn AND provider_execution_name = :name")
            .expressionAttributeValues(Map.of(":callback", string(binding.providerCallbackId()),
                ":arn", string(binding.providerExecutionArn()), ":name", string(binding.providerExecutionName())))
            .build()).build());
        operations.add(TransactWriteItem.builder().put(immutablePut(toItem(binding))).build());
        try {
            dynamo.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(operations).build());
            return true;
        } catch (TransactionCanceledException conflict) {
            requireCurrentGeneration(identity.tenantId(), identity.executionId(), identity.generation());
            AwsDurableCallbackBinding existing = find(identity.tenantId(), identity.interactionId(),
                identity.generation()).orElseThrow(() -> conflict);
            if (!sameBindingAuthority(existing, binding)) {
                throw new IllegalStateException("callback binding generation is already owned by another callback", conflict);
            }
            return false;
        }
    }

    public boolean register(AwsDurableCallbackRegistration registration) {
        Objects.requireNonNull(registration, "registration");
        AwsDurableDriverCheckpoint checkpoint = registration.checkpoint();
        if (claimGeneration(checkpoint) == AwsDurableGenerationAdmission.STALE) {
            throw new AwsDurableStaleGenerationException();
        }
        var operations = generationConditions(registration.partitionKey(), checkpoint.generation());
        operations.add(TransactWriteItem.builder().put(immutablePut(toItem(registration))).build());
        try {
            dynamo.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(operations).build());
            return true;
        } catch (TransactionCanceledException conflict) {
            requireCurrentGeneration(checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
            AwsDurableCallbackRegistration existing = findRegistration(
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation()).orElseThrow(() -> conflict);
            if (!sameRegistrationAuthority(existing, registration)) {
                throw new IllegalStateException("callback registration generation is already owned by another callback", conflict);
            }
            return false;
        }
    }

    private java.util.ArrayList<TransactWriteItem> generationConditions(String partition, long generation) {
        return new java.util.ArrayList<>(List.of(
            TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName(tableName)
                .key(key(partition, claimKey(generation))).conditionExpression("attribute_exists(#pk)")
                .expressionAttributeNames(Map.of("#pk", PK)).build()).build(),
            TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName(tableName)
                .key(key(partition, successorKey(generation))).conditionExpression("attribute_not_exists(#pk)")
                .expressionAttributeNames(Map.of("#pk", PK)).build()).build()));
    }

    private Put immutablePut(Map<String, AttributeValue> item) {
        return Put.builder().tableName(tableName).item(item)
            .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
            .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK)).build();
    }

    private void requireCurrentGeneration(String tenantId, String executionId, long generation) {
        if (isSuperseded(tenantId, executionId, generation)) {
            throw new AwsDurableStaleGenerationException();
        }
        if (!isCurrentGeneration(tenantId, executionId, generation)) {
            throw new IllegalStateException("provider generation claim is unavailable");
        }
    }

    private Map<String, AttributeValue> read(String partition, String sort) {
        return dynamo.getItem(GetItemRequest.builder().tableName(tableName).consistentRead(true)
            .key(key(partition, sort)).build()).item();
    }

    private static Map<String, AttributeValue> key(String partition, String sort) {
        return Map.of(PK, string(partition), SK, string(sort));
    }

    private static String claimKey(long generation) {
        if (generation < 1) throw new IllegalArgumentException("generation must be positive");
        return "CLAIM#" + generation;
    }

    private static String successorKey(long generation) {
        if (generation < 1) throw new IllegalArgumentException("generation must be positive");
        return "SUCCESSOR#" + generation;
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
        var response = dynamo.query(QueryRequest.builder()
            .tableName(tableName())
            .indexName(PROVIDER_REGISTRATION_INDEX)
            .keyConditionExpression("#providerArn = :providerArn AND begins_with(#sk, :registration)")
            .expressionAttributeNames(Map.of("#providerArn", "provider_execution_arn", "#sk", SK))
            .expressionAttributeValues(Map.of(
                ":providerArn", string(required(providerExecutionArn, "providerExecutionArn")),
                ":registration", string("REGISTRATION#")))
            .limit(1)
            .build());
        return response.items().stream().findFirst().map(this::registrationFromItem);
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
        return existing.checkpoint().equals(candidate.checkpoint())
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

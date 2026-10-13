package org.pipelineframework.aws.durable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
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
    public static final String CALLBACK_ACCEPTED = "CALLBACK_ACCEPTED";
    public static final String DRIVER_RESUMED = "DRIVER_RESUMED";
    public static final String CALLBACK_LATENCY = "CALLBACK_LATENCY";
    public static final String REPAIR_FIRST_OBSERVED = "REPAIR_FIRST_OBSERVED";

    private final AwsDurableHostTelemetry telemetry;
    private final DynamoDbClient dynamo;
    private final String tableName;
    private final Clock clock;

    public AwsDurableCallbackBindingRepository(DynamoDbClient dynamo, String tableName) {
        this(dynamo, tableName, new AwsDurableHostTelemetry());
    }

    AwsDurableCallbackBindingRepository(DynamoDbClient dynamo, String tableName, AwsDurableHostTelemetry telemetry) {
        this(dynamo, tableName, telemetry, Clock.systemUTC());
    }

    AwsDurableCallbackBindingRepository(DynamoDbClient dynamo, String tableName,
        AwsDurableHostTelemetry telemetry, Clock clock) {
        this.telemetry = Objects.requireNonNull(telemetry);
        this.dynamo = Objects.requireNonNull(dynamo, "dynamo");
        this.tableName = required(tableName, "tableName");
        this.clock = Objects.requireNonNull(clock, "clock");
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
            observe(AwsDurableHostTelemetry.Stage.GENERATION, AwsDurableHostTelemetry.Outcome.CREATED,
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
            return AwsDurableGenerationAdmission.CREATED;
        } catch (TransactionCanceledException conflict) {
            Map<String, AttributeValue> existing = read(partition, claimKey(checkpoint.generation()));
            if (existing.isEmpty()) {
                throw conflict; // Missing predecessor, contention or infrastructure failure remains retryable.
            }
            if (!existing.equals(claim)) {
                observe(AwsDurableHostTelemetry.Stage.GENERATION, AwsDurableHostTelemetry.Outcome.CONFLICT,
                    checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
                throw new IllegalStateException("generation is already owned by another semantic checkpoint", conflict);
            }
            if (!read(partition, successorKey(checkpoint.generation())).isEmpty()) {
                observe(AwsDurableHostTelemetry.Stage.GENERATION, AwsDurableHostTelemetry.Outcome.STALE,
                    checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
                return AwsDurableGenerationAdmission.STALE;
            }
            observe(AwsDurableHostTelemetry.Stage.GENERATION, AwsDurableHostTelemetry.Outcome.REPLAYED,
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
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
                AwsDurableCallbackRegistration.registrationSortKey(identity.generation(), identity.occurrence())))
            .conditionExpression("provider_callback_id = :callback AND provider_execution_arn = :arn AND provider_execution_name = :name"
                + " AND (attribute_not_exists(interaction_id) OR (interaction_id = :interaction AND correlation_id = :correlation AND occurrence = :occurrence))")
            .expressionAttributeValues(Map.of(":callback", string(binding.providerCallbackId()),
                ":arn", string(binding.providerExecutionArn()), ":name", string(binding.providerExecutionName()),
                ":interaction", string(identity.interactionId()), ":correlation", string(identity.correlationId()),
                ":occurrence", number(identity.occurrence())))
            .build()).build());
        operations.add(TransactWriteItem.builder().put(immutablePut(toItem(binding))).build());
        try {
            dynamo.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(operations).build());
            observe(AwsDurableHostTelemetry.Stage.BINDING, AwsDurableHostTelemetry.Outcome.CREATED,
                identity.tenantId(), identity.executionId(), identity.generation());
            return true;
        } catch (TransactionCanceledException conflict) {
            requireCurrentGeneration(identity.tenantId(), identity.executionId(), identity.generation());
            AwsDurableCallbackBinding existing = find(identity.tenantId(), identity.interactionId(),
                identity.generation(), identity.occurrence()).orElseThrow(() -> conflict);
            if (!sameBindingAuthority(existing, binding)) {
                observe(AwsDurableHostTelemetry.Stage.BINDING, AwsDurableHostTelemetry.Outcome.CONFLICT,
                    identity.tenantId(), identity.executionId(), identity.generation());
                throw new IllegalStateException("callback binding generation is already owned by another callback", conflict);
            }
            observe(AwsDurableHostTelemetry.Stage.BINDING, AwsDurableHostTelemetry.Outcome.REPLAYED,
                identity.tenantId(), identity.executionId(), identity.generation());
            return false;
        }
    }

    public boolean registerAdmission(org.pipelineframework.aws.durable.model.AwsDurableAdmissionLocator locator) {
        Objects.requireNonNull(locator, "locator");
        var item = new java.util.HashMap<String, AttributeValue>(Map.ofEntries(
            Map.entry(PK, string(locator.partitionKey())), Map.entry(SK, string(locator.sortKey())),
            Map.entry(RECORD_TYPE, string("ADMISSION")), Map.entry("tenant_id", string(locator.tenantId())),
            Map.entry("client_key", string(locator.clientKey())), Map.entry("pipeline_id", string(locator.pipelineId())),
            Map.entry("contract_version", string(locator.contractVersion())), Map.entry("release_version", string(locator.releaseVersion())),
            Map.entry("generation", number(locator.generation())),
            Map.entry("provider_execution_name", string(locator.providerExecutionName())),
            Map.entry("provider_execution_arn", string(locator.providerExecutionArn()))));
        locator.executionId().ifPresent(id -> item.put("execution_id", string(id)));
        try {
            dynamo.putItem(software.amazon.awssdk.services.dynamodb.model.PutItemRequest.builder()
                .tableName(tableName).item(item)
                .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK)).build());
            return true;
        } catch (software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException conflict) {
            var existing = read(locator.partitionKey(), locator.sortKey());
            if (existing.isEmpty()) throw conflict;
            if (!locator.equals(admissionFromItem(existing))) {
                throw new IllegalStateException("admission locator is already owned", conflict);
            }
            return false;
        }
    }

    /**
     * Retains the first host observation of one provider execution for sampled repair age.
     * This is mechanical evidence only and is deliberately independent of callback retention.
     */
    public OptionalLong recordRepairFirstObservedAt(String providerExecutionArn) {
        String arn = required(providerExecutionArn, "providerExecutionArn");
        long observedAt = clock.millis();
        if (observedAt < 0) return OptionalLong.empty();
        String partition = repairPartition(arn);
        Map<String, AttributeValue> item = Map.ofEntries(
            Map.entry(PK, string(partition)), Map.entry(SK, string("REPAIR#FIRST_OBSERVED")),
            Map.entry(RECORD_TYPE, string(REPAIR_FIRST_OBSERVED)),
            Map.entry("repair_provider_execution_arn", string(arn)),
            Map.entry("first_observed_at_epoch_ms", number(observedAt)),
            Map.entry("timestamp_source", string("HOST_FIRST_OBSERVED")));
        try {
            dynamo.putItem(PutItemRequest.builder().tableName(tableName).item(item)
                .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK)).build());
            return OptionalLong.of(observedAt);
        } catch (ConditionalCheckFailedException alreadyObserved) {
            Map<String, AttributeValue> existing = read(partition, "REPAIR#FIRST_OBSERVED");
            String recordType = optionalText(existing, RECORD_TYPE).orElse("");
            String retainedArn = optionalText(existing, "repair_provider_execution_arn").orElse("");
            String timestampSource = optionalText(existing, "timestamp_source").orElse("");
            if (!REPAIR_FIRST_OBSERVED.equals(recordType) || !arn.equals(retainedArn)
                || !"HOST_FIRST_OBSERVED".equals(timestampSource)) {
                throw new IllegalStateException("repair observation key is owned by another provider execution", alreadyObserved);
            }
            OptionalLong firstObserved = optionalLong(existing, "first_observed_at_epoch_ms");
            if (firstObserved.isEmpty() || firstObserved.getAsLong() < 0) {
                throw new IllegalStateException("repair observation timestamp is missing or invalid", alreadyObserved);
            }
            return firstObserved;
        }
    }

    private static String repairPartition(String providerExecutionArn) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(providerExecutionArn.getBytes(StandardCharsets.UTF_8));
            return "REPAIR#" + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    public Optional<org.pipelineframework.aws.durable.model.AwsDurableAdmissionLocator> findAdmissionByProviderExecutionArn(String arn) {
        var response = dynamo.query(QueryRequest.builder().tableName(tableName).indexName(PROVIDER_REGISTRATION_INDEX)
            .keyConditionExpression("#arn = :arn AND begins_with(#sk, :admission)")
            .expressionAttributeNames(Map.of("#arn", "provider_execution_arn", "#sk", SK))
            .expressionAttributeValues(Map.of(":arn", string(required(arn, "providerExecutionArn")), ":admission", string("ADMISSION#")))
            .scanIndexForward(false).limit(1).build());
        return response.items().stream().findFirst().map(this::admissionFromItem);
    }

    private org.pipelineframework.aws.durable.model.AwsDurableAdmissionLocator admissionFromItem(Map<String, AttributeValue> item) {
        return new org.pipelineframework.aws.durable.model.AwsDurableAdmissionLocator(text(item, "tenant_id"),
            text(item, "client_key"), text(item, "pipeline_id"), text(item, "contract_version"), text(item, "release_version"),
            Optional.ofNullable(item.get("execution_id")).map(AttributeValue::s), Long.parseLong(item.get("generation").n()),
            text(item, "provider_execution_name"), text(item, "provider_execution_arn"));
    }

    public boolean registerRecovery(org.pipelineframework.aws.durable.model.AwsDurableRecoveryLocator locator) {
        var checkpoint = locator.checkpoint();
        if (claimGeneration(checkpoint) == AwsDurableGenerationAdmission.STALE) {
            observe(AwsDurableHostTelemetry.Stage.GENERATION, AwsDurableHostTelemetry.Outcome.STALE_REJECTED,
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
            throw new AwsDurableStaleGenerationException();
        }
        var item = Map.ofEntries(
            Map.entry(PK, string(locator.partitionKey())), Map.entry(SK, string(locator.sortKey())),
            Map.entry(RECORD_TYPE, string("RECOVERY")),
            Map.entry("tenant_id", string(checkpoint.tenantId())), Map.entry("execution_id", string(checkpoint.executionId())),
            Map.entry("pipeline_id", string(checkpoint.pipelineId())), Map.entry("contract_version", string(checkpoint.contractVersion())),
            Map.entry("release_version", string(checkpoint.releaseVersion())), Map.entry("generation", number(checkpoint.generation())),
            Map.entry("provider_execution_name", string(locator.providerExecutionName())),
            Map.entry("provider_execution_arn", string(locator.providerExecutionArn())));
        var operations = generationConditions(locator.partitionKey(), checkpoint.generation());
        operations.add(TransactWriteItem.builder().put(immutablePut(item)).build());
        try {
            dynamo.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(operations).build());
            return true;
        } catch (TransactionCanceledException conflict) {
            requireCurrentGeneration(checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
            var existing = read(locator.partitionKey(), locator.sortKey());
            if (existing.isEmpty()) throw conflict;
            if (!locator.equals(recoveryFromItem(existing))) throw new IllegalStateException("recovery locator is already owned", conflict);
            return false;
        }
    }

    public Optional<org.pipelineframework.aws.durable.model.AwsDurableRecoveryLocator> findRecoveryByProviderExecutionArn(String arn) {
        var response = dynamo.query(QueryRequest.builder().tableName(tableName)
            .indexName(PROVIDER_REGISTRATION_INDEX)
            .keyConditionExpression("#arn = :arn AND begins_with(#sk, :recovery)")
            .expressionAttributeNames(Map.of("#arn", "provider_execution_arn", "#sk", SK))
            .expressionAttributeValues(Map.of(":arn", string(required(arn, "providerExecutionArn")), ":recovery", string("RECOVERY#")))
            .scanIndexForward(false).limit(1).build());
        return response.items().stream().findFirst().map(this::recoveryFromItem);
    }

    private org.pipelineframework.aws.durable.model.AwsDurableRecoveryLocator recoveryFromItem(Map<String, AttributeValue> item) {
        return new org.pipelineframework.aws.durable.model.AwsDurableRecoveryLocator(
            new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint(text(item, "tenant_id"), text(item, "execution_id"),
                text(item, "pipeline_id"), text(item, "contract_version"), text(item, "release_version")),
                Long.parseLong(item.get("generation").n())), text(item, "provider_execution_name"), text(item, "provider_execution_arn"));
    }

    public boolean register(AwsDurableCallbackRegistration registration) {
        Objects.requireNonNull(registration, "registration");
        AwsDurableDriverCheckpoint checkpoint = registration.checkpoint();
        if (claimGeneration(checkpoint) == AwsDurableGenerationAdmission.STALE) {
            observe(AwsDurableHostTelemetry.Stage.GENERATION, AwsDurableHostTelemetry.Outcome.STALE_REJECTED,
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
            throw new AwsDurableStaleGenerationException();
        }
        var operations = generationConditions(registration.partitionKey(), checkpoint.generation());
        operations.add(TransactWriteItem.builder().put(immutablePut(toItem(registration))).build());
        try {
            dynamo.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(operations).build());
            observe(AwsDurableHostTelemetry.Stage.REGISTRATION, AwsDurableHostTelemetry.Outcome.CREATED,
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
            return true;
        } catch (TransactionCanceledException conflict) {
            requireCurrentGeneration(checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
            AwsDurableCallbackRegistration existing = findRegistration(
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation(), registration.occurrence()).orElseThrow(() -> conflict);
            if (!sameRegistrationAuthority(existing, registration)) {
                observe(AwsDurableHostTelemetry.Stage.REGISTRATION, AwsDurableHostTelemetry.Outcome.CONFLICT,
                    checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
                throw new IllegalStateException("callback registration generation is already owned by another callback", conflict);
            }
            observe(AwsDurableHostTelemetry.Stage.REGISTRATION, AwsDurableHostTelemetry.Outcome.REPLAYED,
                checkpoint.tenantId(), checkpoint.executionId(), checkpoint.generation());
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
            observe(AwsDurableHostTelemetry.Stage.GENERATION, AwsDurableHostTelemetry.Outcome.STALE_REJECTED,
                tenantId, executionId, generation);
            throw new AwsDurableStaleGenerationException();
        }
        if (!isCurrentGeneration(tenantId, executionId, generation)) {
            throw new IllegalStateException("provider generation claim is unavailable");
        }
    }

    private void observe(AwsDurableHostTelemetry.Stage stage, AwsDurableHostTelemetry.Outcome outcome,
        String tenantId, String executionId, long generation) {
        telemetry.record(stage, outcome, tenantId.length() + ":" + tenantId + executionId, generation);
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
        return findRegistration(tenantId, executionId, generation, 1);
    }

    public Optional<AwsDurableCallbackRegistration> findRegistration(
        String tenantId, String executionId, long generation, long occurrence
    ) {
        Map<String, AttributeValue> item = dynamo.getItem(GetItemRequest.builder()
            .tableName(tableName())
            .consistentRead(true)
            .key(Map.of(
                PK, string(tenantId + "#" + executionId),
                SK, string(AwsDurableCallbackRegistration.registrationSortKey(generation, occurrence))))
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
            .scanIndexForward(false)
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
        return find(tenantId, interactionId, generation, 1);
    }

    public Optional<AwsDurableCallbackBinding> find(String tenantId, String interactionId, long generation, long occurrence) {
        Map<String, AttributeValue> item = dynamo.getItem(GetItemRequest.builder()
            .tableName(tableName())
            .consistentRead(true)
            .key(Map.of(
                PK, string(tenantId + "#" + interactionId),
                SK, string(AwsDurableCallbackBinding.generationSortKey(generation, occurrence))))
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
                SK, string(deliveredKey(binding.awaitIdentity().generation(), binding.awaitIdentity().occurrence()))))
            .build()).item();
        return item != null && !item.isEmpty();
    }

    public void recordDelivered(AwsDurableCallbackBinding binding, String providerOutcome) {
        Map<String, AttributeValue> item = Map.of(
            PK, string(binding.partitionKey()),
            SK, string(deliveredKey(binding.awaitIdentity().generation(), binding.awaitIdentity().occurrence())),
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

    /** Persists only a normal provider API acknowledgement; generic delivery rows are not timing evidence. */
    public void recordCallbackAccepted(AwsDurableCallbackBinding binding, long acceptedAtEpochMs) {
        Objects.requireNonNull(binding, "binding");
        if (acceptedAtEpochMs < 0) return;
        AwsDurableAwaitIdentity identity = binding.awaitIdentity();
        try {
            Optional<AwsDurableCallbackRegistration> registration = timingRegistration(binding);
            if (registration.isEmpty()) return;
            Map<String, AttributeValue> item = timingFact(binding, registration.orElseThrow(), acceptedKey(identity), CALLBACK_ACCEPTED,
                binding.expiresAtEpochS(), Map.of("callback_success_observed_at_epoch_ms", number(acceptedAtEpochMs),
                    "timestamp_source", string("HOST_API_ACK_OBSERVATION")));
            putOnce(item);
            recordLatencyIfReady(binding);
        } catch (RuntimeException unavailable) {
            // Timing evidence is best effort and cannot change callback authority or disposition.
        }
    }

    /** Captures the first verified driver action after a callback wakeup, once per generation/occurrence. */
    public void recordDriverResumed(AwsDurableAwaitIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        try {
            Optional<AwsDurableCallbackBinding> binding = find(identity.tenantId(), identity.interactionId(),
                identity.generation(), identity.occurrence());
            if (binding.isEmpty() || !binding.orElseThrow().awaitIdentity().equals(identity)) return;
            AwsDurableCallbackBinding current = binding.orElseThrow();
            Optional<AwsDurableCallbackRegistration> registration = timingRegistration(current);
            if (registration.isEmpty()) return;
            long resumedAt = clock.millis();
            if (resumedAt < 0) return;
            Map<String, AttributeValue> item = timingFact(current, registration.orElseThrow(), resumedKey(identity), DRIVER_RESUMED,
                current.expiresAtEpochS(), Map.of("resumed_at_epoch_ms", number(resumedAt)));
            putOnce(item);
            recordLatencyIfReady(current);
        } catch (RuntimeException unavailable) {
            // Resume timing evidence is best effort; the durable driver action still succeeds.
        }
    }

    private void recordLatencyIfReady(AwsDurableCallbackBinding binding) {
        AwsDurableAwaitIdentity identity = binding.awaitIdentity();
        Map<String, AttributeValue> accepted = read(binding.partitionKey(), acceptedKey(identity));
        Map<String, AttributeValue> resumed = read(binding.partitionKey(), resumedKey(identity));
        Optional<AwsDurableCallbackRegistration> registration = timingRegistration(binding);
        if (registration.isEmpty()) return;
        if (accepted.isEmpty() || resumed.isEmpty()
            || !factMatches(accepted, binding, registration.orElseThrow(), CALLBACK_ACCEPTED)
            || !factMatches(resumed, binding, registration.orElseThrow(), DRIVER_RESUMED)
            || !"HOST_API_ACK_OBSERVATION".equals(text(accepted, "timestamp_source"))) return;
        long acceptedAt = longValue(accepted, "callback_success_observed_at_epoch_ms");
        long resumedAt = longValue(resumed, "resumed_at_epoch_ms");
        boolean measured = resumedAt >= acceptedAt;
        long latency = measured ? resumedAt - acceptedAt : -1;
        var markerValues = new java.util.HashMap<String, AttributeValue>();
        markerValues.put("latency_outcome", string(measured ? "MEASURED" : "UNKNOWN"));
        if (measured) markerValues.put("latency_ms", number(latency));
        Map<String, AttributeValue> marker = timingFact(binding, registration.orElseThrow(), latencyKey(identity), CALLBACK_LATENCY,
            binding.expiresAtEpochS(), markerValues);
        // This claim is exact locally; a crash after the claim and before emit may lose a sample.
        // The timing row never changes callback, Await, or execution authority.
        if (putOnce(marker)) {
            telemetry.recordCallbackResume(identity.tenantId() + "\u0000" + identity.executionId()
                + "\u0000" + identity.interactionId(), identity.generation(),
                measured ? java.util.OptionalLong.of(latency) : java.util.OptionalLong.empty());
        }
    }

    private Optional<AwsDurableCallbackRegistration> timingRegistration(AwsDurableCallbackBinding binding) {
        AwsDurableAwaitIdentity identity = binding.awaitIdentity();
        return findRegistration(identity.tenantId(), identity.executionId(), identity.generation(), identity.occurrence())
            .filter(registration -> registration.awaitIdentity().filter(identity::equals).isPresent())
            .filter(registration -> registration.providerCallbackId().equals(binding.providerCallbackId())
                && registration.providerExecutionArn().equals(binding.providerExecutionArn()));
    }

    private Map<String, AttributeValue> timingFact(AwsDurableCallbackBinding binding,
        AwsDurableCallbackRegistration registration, String sortKey,
        String type, long expiresAtEpochS, Map<String, AttributeValue> values) {
        AwsDurableAwaitIdentity identity = binding.awaitIdentity();
        var item = new java.util.HashMap<String, AttributeValue>();
        item.put(PK, string(identity.tenantId() + "#" + identity.interactionId()));
        item.put(SK, string(sortKey));
        item.put(RECORD_TYPE, string(type));
        item.put("tenant_id", string(identity.tenantId()));
        item.put("execution_id", string(identity.executionId()));
        item.put("interaction_id", string(identity.interactionId()));
        item.put("generation", number(identity.generation()));
        item.put("occurrence", number(identity.occurrence()));
        item.put("provider_execution_arn", string(binding.providerExecutionArn()));
        item.put("provider_callback_id", string(binding.providerCallbackId()));
        item.put("pipeline_id", string(registration.checkpoint().pipelineId()));
        item.put("contract_version", string(registration.checkpoint().contractVersion()));
        item.put("release_version", string(registration.checkpoint().releaseVersion()));
        item.put("expires_at_epoch_s", number(expiresAtEpochS));
        item.putAll(values);
        return Map.copyOf(item);
    }

    private static boolean factMatches(Map<String, AttributeValue> item, AwsDurableCallbackBinding binding,
        AwsDurableCallbackRegistration registration, String type) {
        AwsDurableAwaitIdentity identity = binding.awaitIdentity();
        return type.equals(text(item, RECORD_TYPE))
            && identity.tenantId().equals(text(item, "tenant_id"))
            && identity.executionId().equals(text(item, "execution_id"))
            && identity.interactionId().equals(text(item, "interaction_id"))
            && identity.generation() == longValue(item, "generation")
            && identity.occurrence() == longValue(item, "occurrence")
            && binding.providerExecutionArn().equals(text(item, "provider_execution_arn"))
            && binding.providerCallbackId().equals(text(item, "provider_callback_id"))
            && registration.checkpoint().pipelineId().equals(text(item, "pipeline_id"))
            && registration.checkpoint().contractVersion().equals(text(item, "contract_version"))
            && registration.checkpoint().releaseVersion().equals(text(item, "release_version"));
    }

    private boolean putOnce(Map<String, AttributeValue> item) {
        try {
            dynamo.putItem(PutItemRequest.builder().tableName(tableName).item(item)
                .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK)).build());
            return true;
        } catch (ConditionalCheckFailedException duplicate) {
            return false;
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
            Map.entry("occurrence", number(identity.occurrence())),
            Map.entry("provider_execution_name", string(binding.providerExecutionName())),
            Map.entry("provider_execution_arn", string(binding.providerExecutionArn())),
            Map.entry("provider_callback_id", string(binding.providerCallbackId())),
            Map.entry("binding_status", string(binding.status().name())),
            Map.entry("created_at_epoch_ms", number(binding.createdAtEpochMs())),
            Map.entry("expires_at_epoch_s", number(binding.expiresAtEpochS())));
    }

    private Map<String, AttributeValue> toItem(AwsDurableCallbackRegistration registration) {
        AwsDurableDriverCheckpoint checkpoint = registration.checkpoint();
        var item = new java.util.HashMap<>(Map.ofEntries(
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
            Map.entry("expires_at_epoch_s", number(registration.expiresAtEpochS()))));
        registration.awaitIdentity().ifPresent(identity -> {
            item.put("interaction_id", string(identity.interactionId()));
            item.put("correlation_id", string(identity.correlationId()));
            item.put("occurrence", number(identity.occurrence()));
            item.put("schema_version", number(3));
        });
        return Map.copyOf(item);
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
            longValue(item, "generation"), occurrenceFromItem(item));
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
            longValue(item, "expires_at_epoch_s"),
            item.containsKey("interaction_id") ? Optional.of(new AwsDurableAwaitIdentity(checkpoint.tenantId(),
                checkpoint.executionId(), text(item, "interaction_id"), text(item, "correlation_id"),
                checkpoint.generation(), occurrenceFromItem(item))) : Optional.empty());
    }

    private static long occurrenceFromItem(Map<String, AttributeValue> item) {
        return item.containsKey("occurrence") || (item.containsKey("schema_version") && longValue(item, "schema_version") >= 3)
            ? longValue(item, "occurrence") : 1;
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
            && existing.awaitIdentity().equals(candidate.awaitIdentity())
            && existing.providerExecutionName().equals(candidate.providerExecutionName())
            && existing.providerExecutionArn().equals(candidate.providerExecutionArn())
            && existing.providerCallbackId().equals(candidate.providerCallbackId());
    }

    private static String deliveredKey(long generation, long occurrence) {
        return "DELIVERED#" + generation + (occurrence == 1 ? "" : "#OCCURRENCE#" + occurrence);
    }

    private static String acceptedKey(AwsDurableAwaitIdentity identity) {
        return "CALLBACK_ACCEPTED#GEN#" + identity.generation() + "#OCC#" + identity.occurrence();
    }

    private static String resumedKey(AwsDurableAwaitIdentity identity) {
        return "DRIVER_RESUMED#GEN#" + identity.generation() + "#OCC#" + identity.occurrence();
    }

    private static String latencyKey(AwsDurableAwaitIdentity identity) {
        return "CALLBACK_LATENCY#GEN#" + identity.generation() + "#OCC#" + identity.occurrence();
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

    private static Optional<String> optionalText(Map<String, AttributeValue> item, String name) {
        return Optional.ofNullable(item.get(name)).map(AttributeValue::s).filter(value -> !value.isBlank());
    }

    private static OptionalLong optionalLong(Map<String, AttributeValue> item, String name) {
        try {
            return Optional.ofNullable(item.get(name)).map(AttributeValue::n).map(Long::parseLong)
                .map(OptionalLong::of).orElseGet(OptionalLong::empty);
        } catch (NumberFormatException invalid) {
            return OptionalLong.empty();
        }
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

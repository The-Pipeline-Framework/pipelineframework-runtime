package org.pipelineframework.awaitable.store;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.function.Supplier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionAdmission;
import org.pipelineframework.awaitable.AwaitCompletionCursor;
import org.pipelineframework.awaitable.AwaitCompletionPage;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitDurablePayloadResolver;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitCreateResult;
import org.pipelineframework.awaitable.AwaitInteractionNotFoundException;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionCursor;
import org.pipelineframework.awaitable.AwaitInteractionPage;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitInteractionTerminalException;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.AwaitItemContinuationCommand;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * DynamoDB-backed await interaction store.
 *
 * <p>The interaction table uses primary key {@code tenant_id, interaction_id} and expects ALL-projected GSIs:
 * {@code await-interaction-by-unit}, {@code await-interaction-pending-by-tenant},
 * {@code await-interaction-pending-by-assignee}, {@code await-interaction-pending-by-group},
 * {@code await-interaction-pending-by-step}, {@code await-interaction-pending-by-deadline},
 * {@code await-interaction-continuation-work}, and {@code await-interaction-by-execution}.
 * Runtime lookup paths query those indexes by await unit, tenant pending queue, tenant filter bucket,
 * or due active deadline instead of scanning the interaction table.
 */
@ApplicationScoped
public class DynamoAwaitInteractionStore implements
    AwaitInteractionStore,
    AwaitItemContinuationWorkStore,
    AwaitInteractionExecutionLookup {
    private static final Logger LOG = Logger.getLogger(DynamoAwaitInteractionStore.class);

    private static final String UNIT_INDEX = "await-interaction-by-unit";
    private static final String PENDING_TENANT_INDEX = "await-interaction-pending-by-tenant";
    private static final String PENDING_ASSIGNEE_INDEX = "await-interaction-pending-by-assignee";
    private static final String PENDING_GROUP_INDEX = "await-interaction-pending-by-group";
    private static final String PENDING_STEP_INDEX = "await-interaction-pending-by-step";
    private static final String PENDING_DEADLINE_INDEX = "await-interaction-pending-by-deadline";
    private static final String CONTINUATION_WORK_INDEX = "await-interaction-continuation-work";
    private static final String EXECUTION_INDEX = "await-interaction-by-execution";
    private static final String ACTIVE_DEADLINE_PARTITION = "active";
    private static final String READY_CONTINUATION_PARTITION = "ready";

    static final String TENANT_ID = "tenant_id";
    static final String INTERACTION_ID = "interaction_id";
    private static final String LOOKUP_KEY = "lookup_key";
    private static final String LOOKUP_KIND = "lookup_kind";
    private static final String EXECUTION_ID = "execution_id";
    private static final String STEP_ID = "step_id";
    private static final String STEP_INDEX = "step_index";
    private static final String OUTPUT_TYPE = "output_type";
    private static final String TRANSPORT_OUTPUT_TYPE = "transport_output_type";
    private static final String CORRELATION_ID = "correlation_id";
    private static final String CAUSATION_ID = "causation_id";
    private static final String IDEMPOTENCY_KEY = "idempotency_key";
    private static final String VERSION = "version";
    private static final String STATUS = "status";
    private static final String REQUEST_PAYLOAD_JSON = "request_payload_json";
    private static final String RESPONSE_PAYLOAD_JSON = "response_payload_json";
    private static final String COMMAND_ACKNOWLEDGEMENT_JSON = "command_acknowledgement_json";
    private static final String STAGED_COMPLETION_JSON = "staged_completion_json";
    private static final String UNIT_ID = "unit_id";
    private static final String ITEM_INDEX = "item_index";
    private static final String ACTOR = "actor";
    private static final String ASSIGNEE = "assignee";
    private static final String GROUP = "group_name";
    private static final String TRANSPORT_TYPE = "transport_type";
    private static final String TRANSPORT_METADATA_JSON = "transport_metadata_json";
    private static final String DEADLINE_EPOCH_MS = "deadline_epoch_ms";
    private static final String CREATED_AT_EPOCH_MS = "created_at_epoch_ms";
    private static final String UPDATED_AT_EPOCH_MS = "updated_at_epoch_ms";
    private static final String TTL_EPOCH_S = "ttl_epoch_s";
    private static final String QUERY_UNIT_KEY = "query_unit_key";
    private static final String QUERY_UNIT_SORT = "query_unit_sort";
    private static final String QUERY_PENDING_TENANT_KEY = "query_pending_tenant_key";
    private static final String QUERY_PENDING_ASSIGNEE_KEY = "query_pending_assignee_key";
    private static final String QUERY_PENDING_GROUP_KEY = "query_pending_group_key";
    private static final String QUERY_PENDING_STEP_KEY = "query_pending_step_key";
    private static final String QUERY_PENDING_DEADLINE_SORT = "query_pending_deadline_sort";
    private static final String QUERY_DEADLINE_KEY = "query_deadline_key";
    private static final String QUERY_DEADLINE_SORT = "query_deadline_sort";
    private static final String TIMEOUT_CONSEQUENCES_PENDING = "timeout_consequences_pending";
    private static final String TIMEOUT_CONSEQUENCES_PARTITION = "TIMEOUT_CONSEQUENCES";
    private static final String QUERY_CONTINUATION_KEY = "query_continuation_key";
    private static final String QUERY_CONTINUATION_DUE = "query_continuation_due_epoch_ms";
    private static final String QUERY_EXECUTION_KEY = "query_execution_key";
    private static final String QUERY_EXECUTION_SORT = "query_execution_sort";
    private static final String CONTINUATION_ATTEMPT = "continuation_attempt";
    private static final String CONTINUATION_COMPLETED = "continuation_completed";
    private static final String ENCODED_JAVA_CLASS = "_tpf_java_class";
    private static final String ENCODED_PAYLOAD = "_tpf_payload";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    AwaitDurablePayloadResolver durablePayloadResolver;

    private volatile DynamoDbClient client;

    /**
     * Default constructor for CDI.
     */
    public DynamoAwaitInteractionStore() {
    }

    DynamoAwaitInteractionStore(DynamoDbClient client, PipelineOrchestratorConfig orchestratorConfig) {
        this.client = client;
        this.orchestratorConfig = orchestratorConfig;
    }

    DynamoAwaitInteractionStore(
        DynamoDbClient client,
        PipelineOrchestratorConfig orchestratorConfig,
        AwaitDurablePayloadResolver durablePayloadResolver) {
        this.client = client;
        this.orchestratorConfig = orchestratorConfig;
        this.durablePayloadResolver = durablePayloadResolver;
    }

    @Override
    public String providerName() {
        return "dynamo";
    }

    @Override
    public int priority() {
        return -1000;
    }

    @Override
    public Uni<AwaitCreateResult> createOrGet(AwaitCreateCommand command) {
        return blocking(() -> createOrGetBlocking(command));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> get(String tenantId, String interactionId) {
        return blocking(() -> getBlocking(tenantId, interactionId, System.currentTimeMillis()));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> getMetadata(String tenantId, String interactionId) {
        return blocking(() -> {
            var item = dynamoClient().getItem(GetItemRequest.builder().tableName(interactionTable())
                .key(primaryKey(tenantId, interactionId)).consistentRead(true).build()).item();
            if (item.isEmpty()) return Optional.empty();
            var record = toMetadataRecord(item);
            return record.ttlEpochS() > 0 && record.ttlEpochS() <= Instant.now().getEpochSecond()
                ? Optional.empty() : Optional.of(record);
        });
    }

    @Override
    public Uni<AwaitInteractionRecord> importRecord(AwaitInteractionRecord record) {
        return blocking(() -> {
            try {
                dynamoClient().transactWriteItems(TransactWriteItemsRequest.builder()
                    .transactItems(
                        TransactWriteItem.builder().put(Put.builder()
                            .tableName(interactionTable())
                            .item(toItem(record))
                            .conditionExpression("attribute_not_exists(#tenant) AND attribute_not_exists(#interaction)")
                            .expressionAttributeNames(Map.of("#tenant", TENANT_ID, "#interaction", INTERACTION_ID))
                            .build()).build(),
                        TransactWriteItem.builder().put(lookupPut(
                            "idempotency",
                            record.tenantId(),
                            record.stepId() + ":" + record.idempotencyKey(),
                            record.interactionId(),
                            record.ttlEpochS())).build(),
                        TransactWriteItem.builder().put(lookupPut(
                            "correlation",
                            record.tenantId(),
                            record.correlationId(),
                            record.interactionId(),
                            record.ttlEpochS())).build())
                    .build());
                return record;
            } catch (TransactionCanceledException | ConditionalCheckFailedException ignored) {
                AwaitInteractionRecord existing = getBlocking(record.tenantId(), record.interactionId(), System.currentTimeMillis())
                    .orElseThrow(() -> new IllegalStateException(
                        "Await interaction import lost race but no interaction was found"));
                requireSameOwner(existing, record);
                if (org.pipelineframework.awaitable.AwaitUnitOccurrence.fromUnitId(record.unitId()).isPresent()) {
                    requireLookupOwner("idempotency", record.stepId() + ":" + record.idempotencyKey(), record);
                    requireLookupOwner("correlation", record.correlationId(), record);
                }
                return existing;
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> findByCorrelation(String tenantId, String correlationId) {
        return blocking(() -> findByLookup(
            lookupKey("correlation", tenantId, correlationId),
            tenantId,
            System.currentTimeMillis()));
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findByUnit(
        String tenantId,
        String unitId) {
        return blocking(() -> {
            Map<String, String> names = Map.of(
                "#unitKey", QUERY_UNIT_KEY,
                "#ttl", TTL_EPOCH_S);
            Map<String, AttributeValue> values = Map.of(
                ":unitKey", avS(scopedKey(tenantId, unitId)),
                ":nowSec", avN(Instant.now().getEpochSecond()),
                ":noExpiry", avN(0));
            List<AwaitInteractionRecord> records = new ArrayList<>();
            Map<String, AttributeValue> lastEvaluatedKey = null;
            do {
                QueryRequest.Builder request = QueryRequest.builder()
                    .tableName(interactionTable())
                    .indexName(UNIT_INDEX)
                    .keyConditionExpression("#unitKey = :unitKey")
                    .filterExpression("(attribute_not_exists(#ttl) OR #ttl = :noExpiry OR #ttl > :nowSec)")
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values)
                    .limit(queryPageLimit(100));
                if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
                    request.exclusiveStartKey(lastEvaluatedKey);
                }
                var response = dynamoClient().query(request.build());
                if (response.items() != null) {
                    for (Map<String, AttributeValue> item : response.items()) {
                        AwaitInteractionRecord record = toRecord(item);
                        if (Objects.equals(tenantId, record.tenantId())
                            && Objects.equals(unitId, record.unitId())
                            && !ttlExpired(record, System.currentTimeMillis())) {
                            records.add(record);
                        }
                    }
                }
                lastEvaluatedKey = response.lastEvaluatedKey();
            } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
            records.sort(java.util.Comparator
                .comparingInt((AwaitInteractionRecord record) -> record.itemIndex() == null
                    ? Integer.MAX_VALUE
                    : record.itemIndex())
                .thenComparing(record -> nullToEmpty(record.causationId()))
                .thenComparing(AwaitInteractionRecord::interactionId));
            return List.copyOf(records);
        });
    }

    @Override
    public Uni<AwaitInteractionPage> findByUnitPage(
        String tenantId, String unitId, int limit, Optional<AwaitInteractionCursor> cursor) {
        return blocking(() -> {
            if (limit < 1 || limit > 100) {
                throw new IllegalArgumentException("Await page limit must be between 1 and 100");
            }
            Objects.requireNonNull(cursor, "cursor").ifPresent(value -> value.requireScope(tenantId, unitId));
            QueryRequest.Builder request = QueryRequest.builder()
                .tableName(interactionTable()).indexName(UNIT_INDEX)
                .keyConditionExpression("#unitKey = :unitKey")
                .filterExpression("attribute_not_exists(#ttl) OR #ttl = :noExpiry OR #ttl > :nowSec")
                .expressionAttributeNames(Map.of("#unitKey", QUERY_UNIT_KEY, "#ttl", TTL_EPOCH_S))
                .expressionAttributeValues(Map.of(":unitKey", avS(scopedKey(tenantId, unitId)),
                    ":nowSec", avN(Instant.now().getEpochSecond()), ":noExpiry", avN(0)))
                .scanIndexForward(true).limit(limit);
            cursor.ifPresent(value -> request.exclusiveStartKey(decodeUnitPosition(value)));
            var response = dynamoClient().query(request.build());
            var records = response.items().stream().map(this::toRecord)
                .filter(record -> tenantId.equals(record.tenantId()) && unitId.equals(record.unitId()))
                .filter(record -> !ttlExpired(record, System.currentTimeMillis()))
                .toList();
            var lastKey = response.lastEvaluatedKey();
            Optional<AwaitInteractionCursor> next = lastKey.isEmpty() ? Optional.empty()
                : Optional.of(new AwaitInteractionCursor(tenantId, unitId, encodeUnitPosition(lastKey)));
            return new AwaitInteractionPage(records, next);
        });
    }

    private String encodeUnitPosition(Map<String, AttributeValue> key) {
        try {
            Map<String, String> values = new HashMap<>();
            key.forEach((name, value) -> values.put(name, value.s()));
            return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                PipelineJson.mapper().writeValueAsBytes(values));
        } catch (java.io.IOException failure) {
            throw new IllegalArgumentException("Cannot encode Await unit cursor", failure);
        }
    }

    private Map<String, AttributeValue> decodeUnitPosition(AwaitInteractionCursor cursor) {
        try {
            var values = PipelineJson.mapper().readValue(java.util.Base64.getUrlDecoder().decode(cursor.position()),
                new com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {});
            if (!values.keySet().equals(java.util.Set.of(TENANT_ID, INTERACTION_ID, QUERY_UNIT_KEY, QUERY_UNIT_SORT))
                || !cursor.tenantId().equals(values.get(TENANT_ID))
                || !scopedKey(cursor.tenantId(), cursor.unitId()).equals(values.get(QUERY_UNIT_KEY))
                || values.values().stream().anyMatch(value -> value == null || value.isBlank())) {
                throw new IllegalArgumentException("Await unit cursor has invalid scope or keys");
            }
            Map<String, AttributeValue> key = new HashMap<>();
            values.forEach((name, value) -> key.put(name, avS(value)));
            return Map.copyOf(key);
        } catch (java.io.IOException failure) {
            throw new IllegalArgumentException("Cannot decode Await unit cursor", failure);
        }
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findByExecution(
        String tenantId,
        String executionId,
        int limit
    ) {
        return blocking(() -> {
            if (limit <= 0) {
                return List.of();
            }
            List<AwaitInteractionRecord> records = new ArrayList<>();
            Map<String, AttributeValue> lastEvaluatedKey = null;
            do {
                QueryRequest.Builder request = QueryRequest.builder()
                    .tableName(interactionTable())
                    .indexName(EXECUTION_INDEX)
                    .keyConditionExpression("#executionKey = :executionKey")
                    .filterExpression("attribute_not_exists(#ttl) OR #ttl = :noExpiry OR #ttl > :nowSec")
                    .expressionAttributeNames(Map.of(
                        "#executionKey", QUERY_EXECUTION_KEY,
                        "#ttl", TTL_EPOCH_S))
                    .expressionAttributeValues(Map.of(
                        ":executionKey", avS(scopedKey(tenantId, executionId)),
                        ":nowSec", avN(Instant.now().getEpochSecond()), ":noExpiry", avN(0)))
                    .scanIndexForward(false)
                    .limit(queryPageLimit(limit));
                if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
                    request.exclusiveStartKey(lastEvaluatedKey);
                }
                var response = dynamoClient().query(request.build());
                if (response.items() != null) {
                    for (Map<String, AttributeValue> item : response.items()) {
                        AwaitInteractionRecord record = toRecord(item);
                        if (Objects.equals(record.tenantId(), tenantId)
                            && Objects.equals(record.executionId(), executionId)) {
                            records.add(record);
                        }
                        if (records.size() >= limit) {
                            break;
                        }
                    }
                }
                lastEvaluatedKey = response.lastEvaluatedKey();
            } while (records.size() < limit && lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
            return List.copyOf(records);
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markDispatching(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs) {
        return markDispatching(tenantId, interactionId, expectedVersion, null, nowEpochMs);
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markDispatching(
        String tenantId,
        String interactionId,
        long expectedVersion,
        Map<String, Object> transportMetadata,
        long nowEpochMs) {
        return blocking(() -> transitionStatus(
            tenantId,
            interactionId,
            expectedVersion,
            AwaitInteractionStatus.WAITING,
            AwaitInteractionStatus.DISPATCHING,
            null,
            null,
            transportMetadata,
            nowEpochMs));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markDispatched(
        String tenantId,
        String interactionId,
        long expectedVersion,
        Map<String, Object> transportMetadata,
        long nowEpochMs) {
        return blocking(() -> transitionStatus(
            tenantId,
            interactionId,
            expectedVersion,
            AwaitInteractionStatus.DISPATCHING,
            AwaitInteractionStatus.DISPATCHED,
            null,
            null,
            transportMetadata,
            nowEpochMs));
    }

    @Override
    public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command) {
        return blocking(() -> completeBlocking(command));
    }

    @Override
    public Uni<Optional<AwaitCompletionAdmission>> getCompletionAdmission(String tenantId, String interactionId) {
        return blocking(() -> completionLifecycleRepository().readAdmission(tenantId, interactionId));
    }

    @Override
    public Uni<AwaitCompletionPage> findOpenCompletions(
        String tenantId, int pageLimit, Optional<AwaitCompletionCursor> cursor
    ) {
        return blocking(() -> completionLifecycleRepository().findOpen(tenantId, pageLimit, cursor));
    }

    @Override
    public Uni<Void> retireCompletionAdmission(AwaitCompletionAdmission admission) {
        return blocking(() -> {
            completionLifecycleRepository().retire(admission);
            return Boolean.TRUE;
        }).replaceWithVoid();
    }

    @Override
    public boolean supportsCommandCompletion() {
        return true;
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> settleCommandDispatch(AwaitInteractionRecord expected,
        org.pipelineframework.awaitable.CommandDispatchSettlement settlement, long nowEpochMs) {
        return blocking(() -> {
            AwaitInteractionRecord next = expected.settleCommandDispatch(settlement, nowEpochMs);
            return transitionStatus(expected.tenantId(), expected.interactionId(), expected.version(), expected.status(),
                next.status(), serializePayload(expected, AwaitDurablePayloadResolver.Slot.RESPONSE, next.responsePayload()),
                next.actor(), next.transportMetadata(), nowEpochMs);
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> compareAndSetCommandCompletion(
        AwaitInteractionRecord expected, AwaitInteractionRecord replacement) {
        if (!expected.permitsCommandCompletionReplacement(replacement)) {
            return Uni.createFrom().failure(new IllegalArgumentException("Invalid Command completion transition"));
        }
        return blocking(() -> {
            Optional<AwaitInteractionRecord> durable = getBlocking(expected.tenantId(), expected.interactionId(),
                replacement.updatedAtEpochMs());
            if (durable.isEmpty()) {
                return Optional.empty();
            }
            AwaitInteractionRecord current = durable.orElseThrow();
            if (current.version() != expected.version() || current.status() != expected.status()) {
                return Optional.empty();
            }
            // Dynamo rehydrates canonical request/acknowledgement records on each read. Their
            // classes need not implement value-equals, so compare the persisted JSON shape while
            // the version/status condition below remains the atomic write authority.
            if (!PipelineJson.mapper().valueToTree(current).equals(PipelineJson.mapper().valueToTree(expected))) {
                throw new IllegalArgumentException("Command completion CAS expected state differs from durable state");
            }
            return transitionStatus(expected.tenantId(), expected.interactionId(), expected.version(),
                expected.status(), replacement.status(),
                serializePayload(replacement, AwaitDurablePayloadResolver.Slot.RESPONSE, replacement.responsePayload()),
                replacement.actor(), replacement.transportMetadata(), replacement.updatedAtEpochMs(), false,
                serializeOptionalPayload(replacement, AwaitDurablePayloadResolver.Slot.COMMAND_ACKNOWLEDGEMENT,
                    replacement.commandAcknowledgement()),
                serializeOptionalPayload(replacement, AwaitDurablePayloadResolver.Slot.STAGED_COMPLETION,
                    replacement.stagedCompletionPayload()),
                expected.stagedCompletionPayload().isPresent() && replacement.stagedCompletionPayload().isEmpty(),
                replacement.status() == AwaitInteractionStatus.COMPLETED
                    ? Optional.of(completionAdmission(replacement, replacement.updatedAtEpochMs())) : Optional.empty());
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> fail(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs) {
        return blocking(() -> transitionStatus(
            tenantId,
            interactionId,
            expectedVersion,
            AwaitInteractionStatus.FAILED,
            null,
            null,
            null,
            nowEpochMs));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> cancel(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs) {
        return blocking(() -> transitionStatus(
            tenantId,
            interactionId,
            expectedVersion,
            AwaitInteractionStatus.CANCELLED,
            null,
            null,
            null,
            nowEpochMs));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markTimedOut(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs) {
        return blocking(() -> transitionStatus(
            tenantId,
            interactionId,
            expectedVersion,
            AwaitInteractionStatus.TIMED_OUT,
            null,
            null,
            null,
            nowEpochMs));
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findTimedOut(long nowEpochMs, int limit) {
        return blocking(() -> {
            if (limit <= 0) return List.of();
            var response = dynamoClient().query(QueryRequest.builder().tableName(interactionTable())
                .indexName(PENDING_DEADLINE_INDEX)
                .keyConditionExpression("#deadlineKey = :deadlineKey AND #deadlineSort <= :deadlineCutoff")
                .expressionAttributeNames(Map.of("#deadlineKey", QUERY_DEADLINE_KEY, "#deadlineSort", QUERY_DEADLINE_SORT))
                .expressionAttributeValues(Map.of(":deadlineKey", avS(ACTIVE_DEADLINE_PARTITION),
                    ":deadlineCutoff", avS(deadlineUpperBound(nowEpochMs))))
                .limit(Math.min(limit, 100)).scanIndexForward(true).build());
            var records = new ArrayList<AwaitInteractionRecord>();
            for (var item : response.items()) {
                var record = toMetadataRecord(item);
                if (ttlExpired(record, nowEpochMs)) {
                    retireExpiredActiveDeadline(record, nowEpochMs);
                } else if (!record.status().terminal() && record.deadlineEpochMs() <= nowEpochMs) {
                    records.add(record);
                }
            }
            records.sort(java.util.Comparator.comparingLong(AwaitInteractionRecord::deadlineEpochMs));
            return List.copyOf(records.subList(0, Math.min(records.size(), limit)));
        });
    }

    private void retireExpiredActiveDeadline(AwaitInteractionRecord record, long nowEpochMs) {
        try {
            dynamoClient().updateItem(UpdateItemRequest.builder().tableName(interactionTable())
                .key(primaryKey(record.tenantId(), record.interactionId()))
                .conditionExpression("#deadlineKey = :active AND #ttl > :zero AND #ttl <= :now")
                .updateExpression("REMOVE #deadlineKey, #deadlineSort")
                .expressionAttributeNames(Map.of("#deadlineKey", QUERY_DEADLINE_KEY,
                    "#deadlineSort", QUERY_DEADLINE_SORT, "#ttl", TTL_EPOCH_S))
                .expressionAttributeValues(Map.of(":active", avS(ACTIVE_DEADLINE_PARTITION), ":zero", avN(0),
                    ":now", avN(Instant.ofEpochMilli(nowEpochMs).getEpochSecond())))
                .build());
        } catch (ConditionalCheckFailedException alreadyChanged) {
            // TTL deletion or a concurrent lifecycle change has already retired this observation.
        }
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findDueTimeoutConsequences(long nowEpochMs, int limit) {
        if (limit < 1 || limit > 100) throw new IllegalArgumentException("Timeout page limit must be between 1 and 100");
        return blocking(() -> {
            var response = dynamoClient().query(QueryRequest.builder().tableName(interactionTable())
                .indexName(PENDING_DEADLINE_INDEX)
                .keyConditionExpression("#deadlineKey = :work AND #deadlineSort <= :cutoff")
                .expressionAttributeNames(Map.of("#deadlineKey", QUERY_DEADLINE_KEY, "#deadlineSort", QUERY_DEADLINE_SORT))
                .expressionAttributeValues(Map.of(":work", avS(TIMEOUT_CONSEQUENCES_PARTITION),
                    ":cutoff", avS(deadlineUpperBound(nowEpochMs))))
                .limit(limit).scanIndexForward(true).build());
            var due = new ArrayList<AwaitInteractionRecord>();
            for (var item : response.items()) {
                var record = toMetadataRecord(item);
                if (record.status() != AwaitInteractionStatus.TIMED_OUT) {
                    throw new IllegalStateException("Timeout work index contains a non-timeout interaction");
                }
                if (ttlExpired(record, nowEpochMs)) {
                    // Logical semantic retention has expired; bounded cleanup prevents expired
                    // index entries from starving retained work while Dynamo TTL cleanup lags.
                    acknowledgeTimeoutConsequences(record, Optional.of(nowEpochMs));
                } else {
                    due.add(record);
                }
            }
            return List.copyOf(due);
        });
    }

    @Override
    public Uni<Void> completeTimeoutConsequences(AwaitInteractionRecord interaction) {
        Objects.requireNonNull(interaction, "interaction");
        return blocking(() -> {
            acknowledgeTimeoutConsequences(interaction, Optional.empty());
            return Boolean.TRUE;
        }).replaceWithVoid();
    }

    private void acknowledgeTimeoutConsequences(AwaitInteractionRecord interaction, Optional<Long> expiredAt) {
        if (interaction.status() != AwaitInteractionStatus.TIMED_OUT) {
            throw new IllegalArgumentException("Only a timed-out interaction can acknowledge timeout work");
        }
        var names = new HashMap<String, String>(Map.of("#version", VERSION, "#status", STATUS,
            "#execution", EXECUTION_ID, "#unit", UNIT_ID, "#pending", TIMEOUT_CONSEQUENCES_PENDING,
            "#deadlineKey", QUERY_DEADLINE_KEY, "#deadlineSort", QUERY_DEADLINE_SORT));
        var values = new HashMap<String, AttributeValue>(Map.of(":version", avN(interaction.version()),
            ":status", avS(AwaitInteractionStatus.TIMED_OUT.name()), ":execution", avS(interaction.executionId())));
        var unit = Optional.ofNullable(interaction.unitId()).filter(id -> !id.isBlank());
        unit.ifPresent(id -> values.put(":unit", avS(id)));
        String condition = "#version = :version AND #status = :status AND #execution = :execution AND "
            + (unit.isPresent() ? "#unit = :unit" : "attribute_not_exists(#unit)");
        if (expiredAt.isPresent()) {
            names.put("#ttl", TTL_EPOCH_S);
            values.put(":expired", avN(Instant.ofEpochMilli(expiredAt.orElseThrow()).getEpochSecond()));
            condition += " AND #ttl > :zero AND #ttl <= :expired";
            values.put(":zero", avN(0));
        }
        try {
            dynamoClient().updateItem(UpdateItemRequest.builder().tableName(interactionTable())
                .key(primaryKey(interaction.tenantId(), interaction.interactionId()))
                .conditionExpression(condition).updateExpression("REMOVE #pending, #deadlineKey, #deadlineSort")
                .expressionAttributeNames(names).expressionAttributeValues(values).build());
        } catch (ConditionalCheckFailedException mismatch) {
            if (expiredAt.isPresent()) return; // Expiry cleanup may race native TTL deletion.
            throw new IllegalStateException("Timeout acknowledgement does not match the retained interaction", mismatch);
        }
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> queryPending(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit) {
        return blocking(() -> {
            if (limit <= 0) {
                return List.of();
            }
            String normalizedAssignee = normalizeFilter(assignee);
            String normalizedGroup = normalizeFilter(group);
            String normalizedStepId = normalizeFilter(stepId);
            PendingIndexSelection indexSelection = pendingIndexSelection(
                tenantId,
                normalizedAssignee,
                normalizedGroup,
                normalizedStepId);
            Map<String, String> names = new HashMap<>(Map.of(
                "#pendingKey", indexSelection.keyAttribute(),
                "#status", STATUS,
                "#ttl", TTL_EPOCH_S));
            Map<String, AttributeValue> values = new HashMap<>(Map.of(
                ":pendingKey", avS(indexSelection.keyValue()),
                ":completed", avS(AwaitInteractionStatus.COMPLETED.name()),
                ":failed", avS(AwaitInteractionStatus.FAILED.name()),
                ":timedOut", avS(AwaitInteractionStatus.TIMED_OUT.name()),
                ":cancelled", avS(AwaitInteractionStatus.CANCELLED.name()),
                ":expired", avS(AwaitInteractionStatus.EXPIRED.name()),
                ":nowSec", avN(Instant.now().getEpochSecond()),
                ":noExpiry", avN(0)));
            StringBuilder filter = new StringBuilder("#status <> :completed AND #status <> :failed "
                + "AND #status <> :timedOut AND #status <> :cancelled AND #status <> :expired "
                + "AND (attribute_not_exists(#ttl) OR #ttl = :noExpiry OR #ttl > :nowSec)");
            if (normalizedAssignee != null && indexSelection.filterAssignee()) {
                names.put("#assignee", ASSIGNEE);
                values.put(":assignee", avS(normalizedAssignee));
                filter.append(" AND #assignee = :assignee");
            }
            if (normalizedGroup != null && indexSelection.filterGroup()) {
                names.put("#group", GROUP);
                values.put(":group", avS(normalizedGroup));
                filter.append(" AND #group = :group");
            }
            if (normalizedStepId != null && indexSelection.filterStepId()) {
                names.put("#stepId", STEP_ID);
                values.put(":stepId", avS(normalizedStepId));
                filter.append(" AND #stepId = :stepId");
            }
            List<AwaitInteractionRecord> records = new ArrayList<>();
            Map<String, AttributeValue> lastEvaluatedKey = null;
            do {
                QueryRequest.Builder request = QueryRequest.builder()
                    .tableName(interactionTable())
                    .indexName(indexSelection.indexName())
                    .keyConditionExpression("#pendingKey = :pendingKey")
                    .filterExpression(filter.toString())
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values)
                    .limit(queryPageLimit(limit));
                if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
                    request.exclusiveStartKey(lastEvaluatedKey);
                }
                var response = dynamoClient().query(request.build());
                if (response.items() != null) {
                    for (Map<String, AttributeValue> item : response.items()) {
                        AwaitInteractionRecord record = toRecord(item);
                        if (matchesPendingQuery(
                            record,
                            tenantId,
                            normalizedAssignee,
                            normalizedGroup,
                            normalizedStepId,
                            System.currentTimeMillis())) {
                            records.add(record);
                        }
                        if (records.size() >= limit) {
                            break;
                        }
                    }
                }
                lastEvaluatedKey = response.lastEvaluatedKey();
            } while (records.size() < limit && lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
            records.sort(java.util.Comparator.comparingLong(AwaitInteractionRecord::deadlineEpochMs));
            return List.copyOf(records.subList(0, Math.min(records.size(), limit)));
        });
    }

    @PreDestroy
    void closeClient() {
        DynamoDbClient active = client;
        if (active == null) {
            return;
        }
        synchronized (this) {
            active = client;
            if (active == null) {
                return;
            }
            try {
                active.close();
            } catch (Exception e) {
                LOG.debug("Failed closing DynamoDB client during shutdown.", e);
            } finally {
                client = null;
            }
        }
    }

    private AwaitCreateResult createOrGetBlocking(AwaitCreateCommand command) {
        String interactionId = UUID.randomUUID().toString();
        AwaitInteractionRecord created = new AwaitInteractionRecord(
            command.tenantId(),
            command.executionId(),
            command.stepId(),
            command.stepIndex(),
            command.outputType(),
            interactionId,
            command.correlationId(),
            command.causationId(),
            command.idempotencyKey(),
            0L,
            AwaitInteractionStatus.WAITING,
            command.requestPayload(),
            null,
            command.unitId(),
            command.itemIndex(),
            null,
            command.assignee(),
            command.group(),
            command.transportType(),
            command.transportMetadata(),
            command.deadlineEpochMs(),
            command.nowEpochMs(),
            command.nowEpochMs(),
            command.ttlEpochS(),
            command.transportOutputType()).withInvocationPath(command.invocationPath());

        try {
            dynamoClient().transactWriteItems(TransactWriteItemsRequest.builder()
                .transactItems(
                    TransactWriteItem.builder().put(Put.builder()
                        .tableName(interactionTable())
                        .item(toItem(created))
                        .conditionExpression("attribute_not_exists(#tenant) AND attribute_not_exists(#interaction)")
                        .expressionAttributeNames(Map.of("#tenant", TENANT_ID, "#interaction", INTERACTION_ID))
                        .build()).build(),
                    TransactWriteItem.builder().put(lookupPut(
                        "idempotency",
                        command.tenantId(),
                        command.stepId() + ":" + command.idempotencyKey(),
                        interactionId,
                        command.ttlEpochS())).build(),
                    TransactWriteItem.builder().put(lookupPut(
                        "correlation",
                        command.tenantId(),
                        command.correlationId(),
                        interactionId,
                        command.ttlEpochS())).build())
                .build());
            return new AwaitCreateResult(created, false);
        } catch (TransactionCanceledException | ConditionalCheckFailedException ignored) {
            return findByLookup(
                lookupKey("idempotency", command.tenantId(), command.stepId() + ":" + command.idempotencyKey()),
                command.tenantId(),
                command.nowEpochMs())
                .map(record -> {
                    if (org.pipelineframework.awaitable.AwaitUnitOccurrence.fromUnitId(record.unitId()).isPresent()
                        || org.pipelineframework.awaitable.AwaitUnitOccurrence.fromUnitId(created.unitId()).isPresent()) {
                        requireSameOwner(record, created);
                    }
                    return new AwaitCreateResult(record, true);
                })
                .orElseThrow(() -> ignored);
        }
    }

    private void requireSameOwner(AwaitInteractionRecord existing, AwaitInteractionRecord incoming) {
        if (!existing.tenantId().equals(incoming.tenantId()) || !Objects.equals(existing.unitId(), incoming.unitId())
            || !existing.executionId().equals(incoming.executionId()) || !existing.stepId().equals(incoming.stepId())
            || existing.stepIndex() != incoming.stepIndex() || !Objects.equals(existing.itemIndex(), incoming.itemIndex())
            || !Objects.equals(existing.idempotencyKey(), incoming.idempotencyKey())
            || !Objects.equals(existing.correlationId(), incoming.correlationId())) {
            throw new IllegalStateException("Await interaction identity conflicts with its immutable owner");
        }
    }

    private void requireLookupOwner(String kind, String key, AwaitInteractionRecord record) {
        var retained = dynamoClient().getItem(GetItemRequest.builder().tableName(lookupTable())
            .key(Map.of(LOOKUP_KEY, avS(lookupKey(kind, record.tenantId(), key)))).consistentRead(true).build()).item();
        if (!record.interactionId().equals(readString(retained, INTERACTION_ID))
            || !record.tenantId().equals(readString(retained, TENANT_ID))) {
            throw new IllegalStateException("Qualified Await lookup does not retain its exact interaction owner");
        }
    }

    private AwaitCompletionResult completeBlocking(AwaitCompletionCommand command) {
        AwaitInteractionRecord current = resolveForCompletion(command)
            .orElseThrow(() -> new AwaitInteractionNotFoundException("No await interaction matches completion"));
        return completeResolvedBlocking(current, command);
    }

    private AwaitCompletionResult completeResolvedBlocking(
        AwaitInteractionRecord current,
        AwaitCompletionCommand command
    ) {
        return completeResolvedBlocking(current, command, 8);
    }

    private AwaitCompletionResult completeResolvedBlocking(AwaitInteractionRecord current,
        AwaitCompletionCommand command, int remainingRaces) {
        if (current.status() == AwaitInteractionStatus.COMPLETED) {
            verifyRetainedCompletionAdmission(current);
            ensureItemContinuationProjection(current, command.nowEpochMs());
            return new AwaitCompletionResult(current, true);
        }
        if (current.status() == AwaitInteractionStatus.COMPLETION_OBSERVED) {
            verifyRetainedCompletionAdmission(current);
            return new AwaitCompletionResult(current, true);
        }
        if (current.status().terminal()) {
            throw new AwaitInteractionTerminalException("Await interaction is terminal: " + current.status());
        }
        if (current.deadlineEpochMs() <= command.nowEpochMs()) {
            Optional<AwaitInteractionRecord> timedOut = transitionStatus(
                current.tenantId(),
                current.interactionId(),
                current.version(),
                AwaitInteractionStatus.TIMED_OUT,
                null,
                null,
                null,
                command.nowEpochMs());
            if (timedOut.isEmpty()) {
                Optional<AwaitInteractionRecord> refreshed = getBlocking(
                    current.tenantId(),
                    current.interactionId(),
                    command.nowEpochMs());
                if (refreshed.isPresent() && (refreshed.get().status() == AwaitInteractionStatus.COMPLETED
                    || refreshed.get().status() == AwaitInteractionStatus.COMPLETION_OBSERVED)) {
                    verifyRetainedCompletionAdmission(refreshed.get());
                    return new AwaitCompletionResult(refreshed.get(), true);
                }
            }
            throw new AwaitInteractionTerminalException("Await interaction timed out before completion");
        }
        Optional<AwaitInteractionRecord> completed = transitionCompletionWithAdmission(current, command);
        if (completed.isPresent()) {
            return new AwaitCompletionResult(completed.get(), false);
        }
        AwaitInteractionRecord refreshed = getBlocking(current.tenantId(), current.interactionId(), command.nowEpochMs())
            .orElseThrow(() -> new IllegalStateException("Await completion transition lost OCC race and interaction disappeared"));
        if (refreshed.status() == AwaitInteractionStatus.COMPLETED) {
            verifyRetainedCompletionAdmission(refreshed);
            ensureItemContinuationProjection(refreshed, command.nowEpochMs());
            return new AwaitCompletionResult(refreshed, true);
        }
        if (refreshed.status() == AwaitInteractionStatus.COMPLETION_OBSERVED) {
            verifyRetainedCompletionAdmission(refreshed);
            return new AwaitCompletionResult(refreshed, true);
        }
        if (refreshed.status().terminal()) {
            throw new AwaitInteractionTerminalException("Await interaction became terminal during completion: " + refreshed.status());
        }
        if (current.commandCallback() && remainingRaces > 0) {
            return completeResolvedBlocking(refreshed, command, remainingRaces - 1);
        }
        throw new IllegalStateException("Await completion transition lost OCC race");
    }

    private Optional<AwaitInteractionRecord> transitionCompletionWithAdmission(
        AwaitInteractionRecord current, AwaitCompletionCommand command
    ) {
        AwaitCompletionAdmission admission = completionAdmission(current, command.nowEpochMs());
        String status = current.observedCompletionStatus().name();
        Map<String, String> names = new HashMap<>(Map.of(
            "#version", VERSION, "#status", STATUS, "#updated", UPDATED_AT_EPOCH_MS,
            "#ttl", TTL_EPOCH_S));
        Map<String, AttributeValue> values = new HashMap<>(Map.of(
            ":expected", avN(current.version()), ":expectedStatus", avS(current.status().name()),
            ":status", avS(status), ":one", avN(1), ":now", avN(command.nowEpochMs()),
                ":nowSec", avN(Instant.ofEpochMilli(command.nowEpochMs()).getEpochSecond()),
                ":noExpiry", avN(0)));
        StringBuilder update = new StringBuilder("SET #status = :status, #version = #version + :one, #updated = :now");
        String responsePayload = serializePayload(
            current, AwaitDurablePayloadResolver.Slot.RESPONSE, command.responsePayload());
        if (responsePayload != null) {
            names.put("#response", RESPONSE_PAYLOAD_JSON);
            values.put(":response", avS(responsePayload));
            update.append(", #response = :response");
        }
        if (command.actor() != null && !command.actor().isBlank()) {
            names.put("#actor", ACTOR);
            values.put(":actor", avS(command.actor()));
            update.append(", #actor = :actor");
        }
        if (current.itemInteraction()) {
            names.put("#continuationKey", QUERY_CONTINUATION_KEY);
            names.put("#continuationDue", QUERY_CONTINUATION_DUE);
            names.put("#continuationAttempt", CONTINUATION_ATTEMPT);
            values.put(":continuationKey", avS(READY_CONTINUATION_PARTITION));
            values.put(":continuationDue", avN(command.nowEpochMs()));
            values.put(":continuationAttempt", avN(1));
            update.append(", #continuationKey = :continuationKey")
                .append(", #continuationDue = :continuationDue")
                .append(", #continuationAttempt = :continuationAttempt");
        }
        Update parentUpdate = Update.builder()
            .tableName(interactionTable())
            .key(primaryKey(current.tenantId(), current.interactionId()))
            .conditionExpression("#version = :expected AND #status = :expectedStatus "
                + "AND (attribute_not_exists(#ttl) OR #ttl = :noExpiry OR #ttl > :nowSec)")
            .updateExpression(update.toString())
            .expressionAttributeNames(names)
            .expressionAttributeValues(values)
            .build();
        List<TransactWriteItem> writes = new ArrayList<>();
        writes.add(TransactWriteItem.builder().update(parentUpdate).build());
        writes.addAll(completionLifecycleRepository().admissionWrites(admission));
        try {
            dynamoClient().transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writes).build());
        } catch (ConditionalCheckFailedException conditionalRace) {
            return Optional.empty();
        } catch (TransactionCanceledException cancelled) {
            if (conditionalCompletionCancellation(cancelled)) return Optional.empty();
            throw cancelled;
        }
        Map<String, AttributeValue> stored = dynamoClient().getItem(GetItemRequest.builder()
            .tableName(interactionTable())
            .key(primaryKey(current.tenantId(), current.interactionId()))
            .consistentRead(true)
            .build()).item();
        if (stored == null || stored.isEmpty()) {
            throw new IllegalStateException("Await completion committed but its interaction could not be read");
        }
        return Optional.of(toRecord(stored));
    }

    private AwaitCompletionAdmission completionAdmission(AwaitInteractionRecord current, long admittedAtEpochMs) {
        return new AwaitCompletionAdmission(
            AwaitCompletionAdmission.CURRENT_SCHEMA_VERSION,
            current.tenantId(),
            current.executionId(),
            current.interactionId(),
            Optional.ofNullable(current.unitId()).filter(unit -> !unit.isBlank()),
            current.stepId(),
            current.stepIndex(),
            current.itemIndex() == null ? OptionalInt.empty() : OptionalInt.of(current.itemIndex()),
            Optional.ofNullable(current.causationId()).filter(causation -> !causation.isBlank()),
            admittedAtEpochMs,
            current.ttlEpochS());
    }

    private void verifyRetainedCompletionAdmission(AwaitInteractionRecord completed) {
        completionLifecycleRepository().readAdmission(completed.tenantId(), completed.interactionId())
            .ifPresent(admission -> {
                if (!completionAdmission(completed, admission.firstAdmittedAtEpochMs()).equals(admission)) {
                    throw new IllegalStateException("Retained Await completion admission conflicts with its interaction");
                }
            });
    }

    private boolean conditionalCompletionCancellation(TransactionCanceledException failure) {
        var reasons = failure.cancellationReasons();
        if (reasons == null || reasons.isEmpty()) return false;
        boolean conditionalFailure = false;
        for (var reason : reasons) {
            if (reason == null) return false;
            String code = reason.code();
            if ("ConditionalCheckFailed".equals(code)) {
                conditionalFailure = true;
            } else if (!"None".equals(code)) {
                return false;
            }
        }
        return conditionalFailure;
    }

    private Optional<AwaitInteractionRecord> resolveForCompletion(AwaitCompletionCommand command) {
        if (command.interactionId() != null && !command.interactionId().isBlank()) {
            return getBlocking(command.tenantId(), command.interactionId(), command.nowEpochMs());
        }
        if (command.correlationId() != null && !command.correlationId().isBlank()) {
            return findByLookup(
                lookupKey("correlation", command.tenantId(), command.correlationId()),
                command.tenantId(),
                command.nowEpochMs());
        }
        return Optional.empty();
    }

    private Optional<AwaitInteractionRecord> transitionStatus(
        String tenantId,
        String interactionId,
        long expectedVersion,
        AwaitInteractionStatus status,
        String responsePayloadJson,
        String actor,
        Map<String, Object> transportMetadata,
        long nowEpochMs) {
        return transitionStatus(
            tenantId,
            interactionId,
            expectedVersion,
            null,
            status,
            responsePayloadJson,
            actor,
            transportMetadata,
            nowEpochMs,
            false);
    }

    private Optional<AwaitInteractionRecord> transitionStatus(
        String tenantId,
        String interactionId,
        long expectedVersion,
        AwaitInteractionStatus requiredStatus,
        AwaitInteractionStatus status,
        String responsePayloadJson,
        String actor,
        Map<String, Object> transportMetadata,
        long nowEpochMs) {
        return transitionStatus(
            tenantId,
            interactionId,
            expectedVersion,
            requiredStatus,
            status,
            responsePayloadJson,
            actor,
            transportMetadata,
            nowEpochMs,
            false);
    }

    private Optional<AwaitInteractionRecord> transitionStatus(
        String tenantId,
        String interactionId,
        long expectedVersion,
        AwaitInteractionStatus requiredStatus,
        AwaitInteractionStatus status,
        String responsePayloadJson,
        String actor,
        Map<String, Object> transportMetadata,
        long nowEpochMs,
        boolean projectItemContinuation) {
        return transitionStatus(tenantId, interactionId, expectedVersion, requiredStatus, status,
            responsePayloadJson, actor, transportMetadata, nowEpochMs, projectItemContinuation, null, null, false, Optional.empty());
    }

    private Optional<AwaitInteractionRecord> transitionStatus(
        String tenantId,
        String interactionId,
        long expectedVersion,
        AwaitInteractionStatus requiredStatus,
        AwaitInteractionStatus status,
        String responsePayloadJson,
        String actor,
        Map<String, Object> transportMetadata,
        long nowEpochMs,
        boolean projectItemContinuation,
        String acknowledgementJson,
        String stagedCompletionJson,
        boolean clearStagedCompletion,
        Optional<AwaitCompletionAdmission> admission) {
        Map<String, String> names = new HashMap<>();
        names.put("#version", VERSION);
        names.put("#status", STATUS);
        names.put("#updated", UPDATED_AT_EPOCH_MS);
        names.put("#ttl", TTL_EPOCH_S);
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":expected", avN(expectedVersion));
        values.put(":status", avS(status.name()));
        values.put(":one", avN(1));
        values.put(":now", avN(nowEpochMs));
        values.put(":nowSec", avN(Instant.ofEpochMilli(nowEpochMs).getEpochSecond()));
        values.put(":noExpiry", avN(0));
        if (requiredStatus != null) {
            values.put(":requiredStatus", avS(requiredStatus.name()));
        }
        if (responsePayloadJson != null) {
            names.put("#response", RESPONSE_PAYLOAD_JSON);
            values.put(":response", avS(responsePayloadJson));
        }
        if (acknowledgementJson != null) {
            names.put("#acknowledgement", COMMAND_ACKNOWLEDGEMENT_JSON);
            values.put(":acknowledgement", avS(acknowledgementJson));
        }
        if (stagedCompletionJson != null || clearStagedCompletion) {
            names.put("#stagedCompletion", STAGED_COMPLETION_JSON);
        }
        if (stagedCompletionJson != null) {
            values.put(":stagedCompletion", avS(stagedCompletionJson));
        }
        if (actor != null && !actor.isBlank()) {
            names.put("#actor", ACTOR);
            values.put(":actor", avS(actor));
        }
        if (transportMetadata != null) {
            names.put("#metadata", TRANSPORT_METADATA_JSON);
            values.put(":metadata", avS(toJson(transportMetadata)));
        }
        if (projectItemContinuation) {
            names.put("#continuationKey", QUERY_CONTINUATION_KEY);
            names.put("#continuationDue", QUERY_CONTINUATION_DUE);
            names.put("#continuationAttempt", CONTINUATION_ATTEMPT);
            values.put(":continuationKey", avS(READY_CONTINUATION_PARTITION));
            values.put(":continuationDue", avN(nowEpochMs));
            values.put(":continuationAttempt", avN(1));
        }
        if (status.terminal()) {
            names.put("#pendingTenantKey", QUERY_PENDING_TENANT_KEY);
            names.put("#pendingAssigneeKey", QUERY_PENDING_ASSIGNEE_KEY);
            names.put("#pendingGroupKey", QUERY_PENDING_GROUP_KEY);
            names.put("#pendingStepKey", QUERY_PENDING_STEP_KEY);
            names.put("#pendingDeadlineSort", QUERY_PENDING_DEADLINE_SORT);
            names.put("#deadlineKey", QUERY_DEADLINE_KEY);
            names.put("#deadlineSort", QUERY_DEADLINE_SORT);
        }
        if (status == AwaitInteractionStatus.TIMED_OUT) {
            names.put("#timeoutPending", TIMEOUT_CONSEQUENCES_PENDING);
            values.put(":timeoutPending", AttributeValue.builder().bool(true).build());
            values.put(":timeoutWork", avS(TIMEOUT_CONSEQUENCES_PARTITION));
            values.put(":timeoutSort", avS(deadlineSortKey(nowEpochMs, tenantId, interactionId)));
        }
        StringBuilder update = new StringBuilder("SET #status = :status, #version = #version + :one, #updated = :now");
        if (responsePayloadJson != null) {
            update.append(", #response = :response");
        }
        if (acknowledgementJson != null) {
            update.append(", #acknowledgement = :acknowledgement");
        }
        if (stagedCompletionJson != null) {
            update.append(", #stagedCompletion = :stagedCompletion");
        }
        if (actor != null && !actor.isBlank()) {
            update.append(", #actor = :actor");
        }
        if (transportMetadata != null) {
            update.append(", #metadata = :metadata");
        }
        if (projectItemContinuation) {
            update.append(", #continuationKey = :continuationKey")
                .append(", #continuationDue = :continuationDue")
                .append(", #continuationAttempt = :continuationAttempt");
        }
        if (status == AwaitInteractionStatus.TIMED_OUT) {
            update.append(", #timeoutPending = :timeoutPending, #deadlineKey = :timeoutWork, #deadlineSort = :timeoutSort");
        }
        if (status.terminal()) {
            update.append(" REMOVE #pendingTenantKey, #pendingAssigneeKey, #pendingGroupKey, #pendingStepKey, ")
                .append("#pendingDeadlineSort");
            if (status != AwaitInteractionStatus.TIMED_OUT) update.append(", #deadlineKey, #deadlineSort");
        }
        if (clearStagedCompletion) {
            update.append(status.terminal() ? ", #stagedCompletion" : " REMOVE #stagedCompletion");
        }
        String condition = "#version = :expected AND (attribute_not_exists(#ttl) OR #ttl = :noExpiry OR #ttl > :nowSec)";
        if (requiredStatus != null) {
            condition = "#version = :expected AND #status = :requiredStatus "
                + "AND (attribute_not_exists(#ttl) OR #ttl = :noExpiry OR #ttl > :nowSec)";
        }
        if (status == AwaitInteractionStatus.TIMED_OUT) {
            for (var terminal : List.of(AwaitInteractionStatus.COMPLETED, AwaitInteractionStatus.FAILED,
                AwaitInteractionStatus.TIMED_OUT, AwaitInteractionStatus.CANCELLED, AwaitInteractionStatus.EXPIRED)) {
                String valueKey = ":terminal" + terminal.name();
                values.put(valueKey, avS(terminal.name()));
                condition += " AND #status <> " + valueKey;
            }
        }
        try {
            if (admission.isPresent()) {
                var interactionUpdate = Update.builder().tableName(interactionTable())
                    .key(primaryKey(tenantId, interactionId)).conditionExpression(condition)
                    .updateExpression(update.toString()).expressionAttributeNames(names)
                    .expressionAttributeValues(values).build();
                List<TransactWriteItem> writes = new ArrayList<>();
                writes.add(TransactWriteItem.builder().update(interactionUpdate).build());
                writes.addAll(completionLifecycleRepository().admissionWrites(admission.orElseThrow()));
                dynamoClient().transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writes).build());
                var stored = dynamoClient().getItem(GetItemRequest.builder().tableName(interactionTable())
                    .key(primaryKey(tenantId, interactionId)).consistentRead(true).build()).item();
                if (stored == null || stored.isEmpty()) {
                    throw new IllegalStateException("Command completion committed but its interaction could not be read");
                }
                return Optional.of(toRecord(stored));
            }
            Map<String, AttributeValue> attributes = dynamoClient().updateItem(UpdateItemRequest.builder()
                .tableName(interactionTable())
                .key(primaryKey(tenantId, interactionId))
                .conditionExpression(condition)
                .updateExpression(update.toString())
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .returnValues(ReturnValue.ALL_NEW)
                .build()).attributes();
            if (attributes == null || attributes.isEmpty()) {
                return Optional.empty();
            }
            // The timeout sweep only consumes control-plane fields from this transition. Keep it
            // independent of pinned-release payload decoding while Dynamo is under recovery load.
            return Optional.of(status == AwaitInteractionStatus.TIMED_OUT
                ? toMetadataRecord(attributes)
                : toRecord(attributes));
        } catch (ConditionalCheckFailedException ignored) {
            return Optional.empty();
        } catch (TransactionCanceledException cancelled) {
            if (conditionalCompletionCancellation(cancelled)) return Optional.empty();
            throw cancelled;
        }
    }

    @Override
    public Uni<List<AwaitItemContinuationCommand>> findDueItemContinuations(long nowEpochMs, int limit) {
        return blocking(() -> {
            if (limit <= 0) {
                return List.of();
            }
            Map<String, String> names = Map.of(
                "#continuationKey", QUERY_CONTINUATION_KEY,
                "#continuationDue", QUERY_CONTINUATION_DUE,
                "#ttl", TTL_EPOCH_S);
            Map<String, AttributeValue> values = Map.of(
                ":continuationKey", avS(READY_CONTINUATION_PARTITION),
                ":now", avN(nowEpochMs),
                ":nowSec", avN(Instant.ofEpochMilli(nowEpochMs).getEpochSecond()),
                ":noExpiry", avN(0));
            List<AwaitItemContinuationCommand> commands = new ArrayList<>();
            Map<String, AttributeValue> lastEvaluatedKey = null;
            do {
                QueryRequest.Builder request = QueryRequest.builder()
                    .tableName(interactionTable())
                    .indexName(CONTINUATION_WORK_INDEX)
                    .keyConditionExpression("#continuationKey = :continuationKey AND #continuationDue <= :now")
                    .filterExpression("attribute_not_exists(#ttl) OR #ttl = :noExpiry OR #ttl > :nowSec")
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values)
                    .limit(queryPageLimit(limit));
                if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
                    request.exclusiveStartKey(lastEvaluatedKey);
                }
                var response = dynamoClient().query(request.build());
                if (response.items() != null) {
                    for (Map<String, AttributeValue> item : response.items()) {
                        AwaitInteractionRecord record = toMetadataRecord(item);
                        if (record.itemIndex() != null) {
                            commands.add(new AwaitItemContinuationCommand(
                                record.tenantId(),
                                record.executionId(),
                                record.unitId(),
                                record.interactionId(),
                                record.itemIndex(),
                                Optional.ofNullable(readInteger(item, CONTINUATION_ATTEMPT)).orElse(1),
                                nowEpochMs));
                        }
                        if (commands.size() >= limit) {
                            break;
                        }
                    }
                }
                lastEvaluatedKey = response.lastEvaluatedKey();
            } while (commands.size() < limit && lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
            return List.copyOf(commands);
        });
    }

    @Override
    public Uni<Void> rescheduleItemContinuation(
        AwaitItemContinuationCommand command,
        int nextAttempt,
        long dueEpochMs) {
        return updateContinuationProjection(command, nextAttempt, dueEpochMs, false);
    }

    @Override
    public Uni<Void> completeItemContinuation(AwaitItemContinuationCommand command) {
        return updateContinuationProjection(command, command.attempt(), command.nowEpochMs(), true);
    }

    private void ensureItemContinuationProjection(AwaitInteractionRecord record, long nowEpochMs) {
        if (!record.itemInteraction()) {
            return;
        }
        try {
            dynamoClient().updateItem(UpdateItemRequest.builder()
                .tableName(interactionTable())
                .key(primaryKey(record.tenantId(), record.interactionId()))
                .conditionExpression("#status = :completed AND attribute_not_exists(#continuationKey) "
                    + "AND attribute_not_exists(#continuationCompleted)")
                .updateExpression("SET #continuationKey = :ready, #continuationDue = :due, "
                    + "#continuationAttempt = :attempt")
                .expressionAttributeNames(Map.of(
                    "#status", STATUS,
                    "#continuationKey", QUERY_CONTINUATION_KEY,
                    "#continuationDue", QUERY_CONTINUATION_DUE,
                    "#continuationAttempt", CONTINUATION_ATTEMPT,
                    "#continuationCompleted", CONTINUATION_COMPLETED))
                .expressionAttributeValues(Map.of(
                    ":completed", avS(AwaitInteractionStatus.COMPLETED.name()),
                    ":ready", avS(READY_CONTINUATION_PARTITION),
                    ":due", avN(nowEpochMs),
                    ":attempt", avN(1)))
                .build());
        } catch (ConditionalCheckFailedException ignored) {
            // Existing work or a durable completion marker already owns this item.
        }
    }

    private Uni<Void> updateContinuationProjection(
        AwaitItemContinuationCommand command,
        int nextAttempt,
        long dueEpochMs,
        boolean complete) {
        return blocking(() -> {
            Map<String, String> names = Map.of(
                "#continuationKey", QUERY_CONTINUATION_KEY,
                "#continuationDue", QUERY_CONTINUATION_DUE,
                "#continuationAttempt", CONTINUATION_ATTEMPT,
                "#continuationCompleted", CONTINUATION_COMPLETED,
                "#execution", EXECUTION_ID,
                "#unit", UNIT_ID,
                "#item", ITEM_INDEX);
            Map<String, AttributeValue> values = new HashMap<>(Map.of(
                ":ready", avS(READY_CONTINUATION_PARTITION),
                ":execution", avS(command.executionId()),
                ":unit", avS(command.unitId()),
                ":item", avN(command.itemIndex())));
            String update;
            if (complete) {
                values.put(":completed", AttributeValue.builder().bool(true).build());
                update = "SET #continuationCompleted = :completed "
                    + "REMOVE #continuationKey, #continuationDue, #continuationAttempt";
            } else {
                values.put(":due", avN(dueEpochMs));
                values.put(":attempt", avN(nextAttempt));
                update = "SET #continuationDue = :due, #continuationAttempt = :attempt "
                    + "REMOVE #continuationCompleted";
            }
            try {
                String projectionCondition = complete
                    ? "(#continuationKey = :ready OR attribute_not_exists(#continuationKey)) "
                        + "AND #execution = :execution AND #unit = :unit AND #item = :item"
                    : "#continuationKey = :ready AND #execution = :execution "
                        + "AND #unit = :unit AND #item = :item";
                dynamoClient().updateItem(UpdateItemRequest.builder()
                    .tableName(interactionTable())
                    .key(primaryKey(command.tenantId(), command.interactionId()))
                    .conditionExpression(projectionCondition)
                    .updateExpression(update)
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values)
                    .build());
            } catch (ConditionalCheckFailedException ignored) {
                // A duplicate host may have already advanced or completed this projection.
            }
            return Boolean.TRUE;
        }).replaceWithVoid();
    }

    private Optional<AwaitInteractionRecord> getBlocking(String tenantId, String interactionId, long nowEpochMs) {
        Map<String, AttributeValue> item = dynamoClient().getItem(GetItemRequest.builder()
            .tableName(interactionTable())
            .key(primaryKey(tenantId, interactionId))
            .consistentRead(true)
            .build()).item();
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        AwaitInteractionRecord record = toRecord(item);
        return record.ttlEpochS() > 0 && record.ttlEpochS() <= Instant.ofEpochMilli(nowEpochMs).getEpochSecond()
            ? Optional.empty()
            : Optional.of(record);
    }

    private Optional<AwaitInteractionRecord> findByLookup(String lookupKey, String tenantId, long nowEpochMs) {
        Map<String, AttributeValue> item = dynamoClient().getItem(GetItemRequest.builder()
            .tableName(lookupTable())
            .key(Map.of(LOOKUP_KEY, avS(lookupKey)))
            .consistentRead(true)
            .build()).item();
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        String interactionId = readString(item, INTERACTION_ID);
        return interactionId == null || interactionId.isBlank()
            ? Optional.empty()
            : getBlocking(tenantId, interactionId, nowEpochMs);
    }

    private Put lookupPut(String kind, String tenantId, String key, String interactionId, long ttlEpochS) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(LOOKUP_KEY, avS(lookupKey(kind, tenantId, key)));
        item.put(LOOKUP_KIND, avS(kind));
        item.put(TENANT_ID, avS(tenantId));
        item.put(INTERACTION_ID, avS(interactionId));
        if (ttlEpochS > 0) item.put(TTL_EPOCH_S, avN(ttlEpochS));
        return Put.builder()
            .tableName(lookupTable())
            .item(item)
            .conditionExpression("attribute_not_exists(#lookup)")
            .expressionAttributeNames(Map.of("#lookup", LOOKUP_KEY))
            .build();
    }

    private Map<String, AttributeValue> toItem(AwaitInteractionRecord record) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TENANT_ID, avS(record.tenantId()));
        item.put(INTERACTION_ID, avS(record.interactionId()));
        item.put(EXECUTION_ID, avS(record.executionId()));
        item.put(STEP_ID, avS(record.stepId()));
        item.put(STEP_INDEX, avN(record.stepIndex()));
        record.invocationPath().ifPresent(path -> item.put("invocationPathJson", avS(toJson(path))));
        item.put(OUTPUT_TYPE, avS(record.outputType()));
        item.put(TRANSPORT_OUTPUT_TYPE, avS(record.transportOutputType()));
        item.put(CORRELATION_ID, avS(record.correlationId()));
        item.put(IDEMPOTENCY_KEY, avS(record.idempotencyKey()));
        item.put(VERSION, avN(record.version()));
        item.put(STATUS, avS(record.status().name()));
        putIfPresent(item, REQUEST_PAYLOAD_JSON,
            serializePayload(record, AwaitDurablePayloadResolver.Slot.REQUEST, record.requestPayload()));
        item.put(TRANSPORT_TYPE, avS(record.transportType()));
        putIfPresent(item, TRANSPORT_METADATA_JSON, toJson(record.transportMetadata()));
        item.put(DEADLINE_EPOCH_MS, avN(record.deadlineEpochMs()));
        item.put(CREATED_AT_EPOCH_MS, avN(record.createdAtEpochMs()));
        item.put(UPDATED_AT_EPOCH_MS, avN(record.updatedAtEpochMs()));
        if (record.ttlEpochS() > 0) item.put(TTL_EPOCH_S, avN(record.ttlEpochS()));
        putIfPresent(item, CAUSATION_ID, record.causationId());
        putIfPresent(item, RESPONSE_PAYLOAD_JSON,
            serializePayload(record, AwaitDurablePayloadResolver.Slot.RESPONSE, record.responsePayload()));
        putIfPresent(item, COMMAND_ACKNOWLEDGEMENT_JSON,
            serializeOptionalPayload(record, AwaitDurablePayloadResolver.Slot.COMMAND_ACKNOWLEDGEMENT,
                record.commandAcknowledgement()));
        putIfPresent(item, STAGED_COMPLETION_JSON,
            serializeOptionalPayload(record, AwaitDurablePayloadResolver.Slot.STAGED_COMPLETION,
                record.stagedCompletionPayload()));
        putIfPresent(item, UNIT_ID, record.unitId());
        if (record.itemIndex() != null) {
            item.put(ITEM_INDEX, avN(record.itemIndex()));
        }
        putIfPresent(item, ACTOR, record.actor());
        putIfPresent(item, ASSIGNEE, record.assignee());
        putIfPresent(item, GROUP, record.group());
        putQueryKeys(item, record);
        return item;
    }

    @SuppressWarnings("unchecked")
    private AwaitInteractionRecord toRecord(Map<String, AttributeValue> item) {
        AwaitInteractionRecord stored = toMetadataRecord(item);
        return withPayloads(
            stored,
            deserializePayload(stored, AwaitDurablePayloadResolver.Slot.REQUEST, readString(item, REQUEST_PAYLOAD_JSON)),
            deserializePayload(stored, AwaitDurablePayloadResolver.Slot.RESPONSE, readString(item, RESPONSE_PAYLOAD_JSON)),
            deserializeOptionalPayload(stored, AwaitDurablePayloadResolver.Slot.COMMAND_ACKNOWLEDGEMENT,
                readString(item, COMMAND_ACKNOWLEDGEMENT_JSON)),
            deserializeOptionalPayload(stored, AwaitDurablePayloadResolver.Slot.STAGED_COMPLETION,
                readString(item, STAGED_COMPLETION_JSON)));
    }

    /**
     * Maps the durable control-plane fields without resolving typed payloads.
     *
     * <p>Due/timeout discovery must remain available when release metadata is temporarily unavailable;
     * those paths never consume request or response payloads.
     */
    @SuppressWarnings("unchecked")
    private AwaitInteractionRecord toMetadataRecord(Map<String, AttributeValue> item) {
        return new AwaitInteractionRecord(
            readString(item, TENANT_ID),
            readString(item, EXECUTION_ID),
            readString(item, STEP_ID),
            Math.toIntExact(readLong(item, STEP_INDEX)),
            readString(item, OUTPUT_TYPE),
            readString(item, INTERACTION_ID),
            readString(item, CORRELATION_ID),
            readString(item, CAUSATION_ID),
            readString(item, IDEMPOTENCY_KEY),
            readLong(item, VERSION),
            AwaitInteractionStatus.valueOf(readString(item, STATUS)),
            null,
            null,
            readString(item, UNIT_ID),
            readInteger(item, ITEM_INDEX),
            readString(item, ACTOR),
            readString(item, ASSIGNEE),
            readString(item, GROUP),
            readString(item, TRANSPORT_TYPE),
            (Map<String, Object>) Optional.ofNullable(fromJson(readString(item, TRANSPORT_METADATA_JSON))).orElse(Map.of()),
            readLong(item, DEADLINE_EPOCH_MS),
            readLong(item, CREATED_AT_EPOCH_MS),
            readLong(item, UPDATED_AT_EPOCH_MS),
            readLong(item, TTL_EPOCH_S),
            readString(item, TRANSPORT_OUTPUT_TYPE)).withInvocationPath(Optional.ofNullable(readString(item, "invocationPathJson"))
                .map(value -> PipelineJson.mapper().convertValue(fromJson(value),
                    org.pipelineframework.invocation.PipelineInvocationPath.class)));
    }

    private AwaitInteractionRecord withPayloads(AwaitInteractionRecord stored, Object requestPayload,
        Object responsePayload, Optional<Object> commandAcknowledgement,
        Optional<Object> stagedCompletionPayload) {
        return new AwaitInteractionRecord(
            stored.tenantId(), stored.executionId(), stored.stepId(), stored.stepIndex(), stored.outputType(),
            stored.interactionId(), stored.correlationId(), stored.causationId(), stored.idempotencyKey(),
            stored.version(), stored.status(), requestPayload, responsePayload, stored.unitId(), stored.itemIndex(),
            stored.actor(), stored.assignee(), stored.group(), stored.transportType(), stored.transportMetadata(),
            stored.deadlineEpochMs(), stored.createdAtEpochMs(), stored.updatedAtEpochMs(), stored.ttlEpochS(),
            stored.transportOutputType(), commandAcknowledgement, stagedCompletionPayload).withInvocationPath(stored.invocationPath());
    }

    private String serializePayload(AwaitInteractionRecord interaction, AwaitDurablePayloadResolver.Slot slot, Object payload) {
        if (payload == null) {
            return null;
        }
        if (durablePayloadResolver == null || !durablePayloadResolver.supportsTypedPayloads(interaction)) {
            return toJson(payload);
        }
        return durablePayloadResolver.encode(interaction, slot, restoreTransitionTypedPayload(interaction, slot, payload));
    }

    private String serializeOptionalPayload(AwaitInteractionRecord interaction,
        AwaitDurablePayloadResolver.Slot slot, Optional<Object> payload) {
        return payload.map(value -> serializePayload(interaction, slot, value)).orElse(null);
    }

    private Optional<Object> deserializeOptionalPayload(AwaitInteractionRecord interaction,
        AwaitDurablePayloadResolver.Slot slot, String payload) {
        return Optional.ofNullable(deserializePayload(interaction, slot, payload));
    }

    private Object restoreTransitionTypedPayload(
        AwaitInteractionRecord interaction,
        AwaitDurablePayloadResolver.Slot slot,
        Object payload
    ) {
        if (!(payload instanceof TypedDurablePayload) && !(payload instanceof Map<?, ?>)) {
            return payload;
        }
        try {
            // In-process suspension snapshots retain the envelope object; JSON transitions
            // materialize it as a map. Both must pass the same pinned-binding checks.
            var envelope = TypedDurablePayload.fromDurableValue(payload);
            if (envelope.isPresent()) {
                return durablePayloadResolver.decodeEnvelope(interaction, slot, envelope.get());
            }
            // Transition records written before their payload field carried the typed envelope may
            // still materialize as a JSON object. Bind that known legacy shape to the descriptor,
            // never pass the map through to another durable write.
            return durablePayloadResolver.decodeLegacy(interaction, slot,
                PipelineJson.mapper().writeValueAsString(payload));
        } catch (Exception e) {
            throw new IllegalStateException("Await transition carried an invalid typed durable payload: interactionId="
                + interaction.interactionId() + ", slot=" + slot, e);
        }
    }

    private Object deserializePayload(AwaitInteractionRecord interaction, AwaitDurablePayloadResolver.Slot slot, String payload) {
        if (payload == null) {
            return null;
        }
        if (!isTypedDurablePayload(payload)) {
            return durablePayloadResolver == null || !durablePayloadResolver.supportsTypedPayloads(interaction)
                ? fromJson(payload)
                : durablePayloadResolver.decodeLegacy(interaction, slot, payload);
        }
        if (durablePayloadResolver == null) {
            throw new IllegalStateException("Typed await payload requires the pinned-release durable payload resolver: interactionId="
                + interaction.interactionId() + ", slot=" + slot);
        }
        return durablePayloadResolver.decode(interaction, slot, payload);
    }

    private static boolean isTypedDurablePayload(String payload) {
        return TypedDurablePayload.fromSerializedBytes(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8)).isPresent();
    }

    private <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }

    private DynamoDbClient dynamoClient() {
        DynamoDbClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            active = client;
            if (active != null) {
                return active;
            }
            var builder = DynamoDbClient.builder();
            builder.httpClientBuilder(UrlConnectionHttpClient.builder()
                .connectionTimeout(java.time.Duration.ofSeconds(10))
                .socketTimeout(java.time.Duration.ofSeconds(30)));
            orchestratorConfig.dynamo().region().filter(region -> !region.isBlank())
                .ifPresent(region -> builder.region(Region.of(region)));
            orchestratorConfig.dynamo().endpointOverride()
                .filter(endpoint -> !endpoint.isBlank())
                .ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
            client = builder.build();
            return client;
        }
    }

    private String interactionTable() {
        return orchestratorConfig.dynamo().awaitInteractionTable();
    }

    private DynamoAwaitCompletionLifecycleRepository completionLifecycleRepository() {
        return new DynamoAwaitCompletionLifecycleRepository(
            dynamoClient(), orchestratorConfig.dynamo().awaitCompletionLifecycleTable(), Clock.systemUTC());
    }

    private String lookupTable() {
        return orchestratorConfig.dynamo().awaitInteractionKeyTable();
    }

    private String toJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return PipelineJson.mapper().writeValueAsString(encode(value));
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing await payload to JSON.", e);
        }
    }

    private Object fromJson(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            Object decoded = PipelineJson.mapper().readValue(value, Object.class);
            if (decoded instanceof Map<?, ?> map && map.containsKey(ENCODED_JAVA_CLASS)) {
                String className = String.valueOf(map.get(ENCODED_JAVA_CLASS));
                Object payload = map.get(ENCODED_PAYLOAD);
                return org.pipelineframework.awaitable.AwaitPayloadSupport.coercePayload(
                    payload,
                    org.pipelineframework.awaitable.AwaitPayloadSupport.resolvePayloadClass(
                        className,
                        Thread.currentThread().getContextClassLoader()));
            }
            return decoded;
        } catch (Exception e) {
            throw new IllegalStateException("Failed deserializing await payload JSON.", e);
        }
    }

    private Object encode(Object value) {
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean
            || value instanceof Map<?, ?> || value instanceof Iterable<?>) {
            return value;
        }
        return Map.of(
            ENCODED_JAVA_CLASS, value.getClass().getName(),
            ENCODED_PAYLOAD, PipelineJson.mapper().convertValue(value, Object.class));
    }

    private static String lookupKey(String kind, String tenantId, String key) {
        return kind + ":" + tenantId.length() + ":" + tenantId + ":" + key.length() + ":" + key;
    }

    private static Map<String, AttributeValue> primaryKey(String tenantId, String interactionId) {
        return Map.of(TENANT_ID, avS(tenantId), INTERACTION_ID, avS(interactionId));
    }

    private static AttributeValue avS(String value) {
        return value == null ? null : AttributeValue.builder().s(value).build();
    }

    private static int queryPageLimit(int limit) {
        return (int) Math.min(Math.max((long) limit * 3L, (long) limit), 1_000L);
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static AttributeValue avN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static void putQueryKeys(Map<String, AttributeValue> item, AwaitInteractionRecord record) {
        item.put(QUERY_EXECUTION_KEY, avS(scopedKey(record.tenantId(), record.executionId())));
        item.put(QUERY_EXECUTION_SORT, avS(deadlineSortKey(record.createdAtEpochMs(), record.interactionId())));
        if (record.itemInteraction() && record.status() == AwaitInteractionStatus.COMPLETED) {
            item.put(QUERY_CONTINUATION_KEY, avS(READY_CONTINUATION_PARTITION));
            item.put(QUERY_CONTINUATION_DUE, avN(record.updatedAtEpochMs()));
            item.put(CONTINUATION_ATTEMPT, avN(1));
        }
        if (record.unitId() != null && !record.unitId().isBlank()) {
            item.put(QUERY_UNIT_KEY, avS(scopedKey(record.tenantId(), record.unitId())));
            item.put(QUERY_UNIT_SORT, avS(itemSortKey(record.itemIndex(), record.causationId(), record.interactionId())));
        }
        if (record.status() == AwaitInteractionStatus.TIMED_OUT) {
            item.put(TIMEOUT_CONSEQUENCES_PENDING, AttributeValue.builder().bool(true).build());
            item.put(QUERY_DEADLINE_KEY, avS(TIMEOUT_CONSEQUENCES_PARTITION));
            item.put(QUERY_DEADLINE_SORT, avS(deadlineSortKey(record.updatedAtEpochMs(), record.tenantId(), record.interactionId())));
        }
        if (record.status().terminal()) {
            return;
        }
        item.put(QUERY_PENDING_TENANT_KEY, avS(record.tenantId()));
        putIfPresent(item, QUERY_PENDING_ASSIGNEE_KEY, scopedOptionalKey(record.tenantId(), record.assignee()));
        putIfPresent(item, QUERY_PENDING_GROUP_KEY, scopedOptionalKey(record.tenantId(), record.group()));
        putIfPresent(item, QUERY_PENDING_STEP_KEY, scopedOptionalKey(record.tenantId(), record.stepId()));
        item.put(QUERY_PENDING_DEADLINE_SORT, avS(deadlineSortKey(record.deadlineEpochMs(), record.interactionId())));
        item.put(QUERY_DEADLINE_KEY, avS(ACTIVE_DEADLINE_PARTITION));
        item.put(QUERY_DEADLINE_SORT, avS(deadlineSortKey(record.deadlineEpochMs(), record.tenantId(), record.interactionId())));
    }

    private static PendingIndexSelection pendingIndexSelection(String tenantId, String assignee, String group, String stepId) {
        if (assignee != null && !assignee.isBlank()) {
            return new PendingIndexSelection(
                PENDING_ASSIGNEE_INDEX,
                QUERY_PENDING_ASSIGNEE_KEY,
                scopedKey(tenantId, assignee),
                false,
                true,
                true);
        }
        if (group != null && !group.isBlank()) {
            return new PendingIndexSelection(
                PENDING_GROUP_INDEX,
                QUERY_PENDING_GROUP_KEY,
                scopedKey(tenantId, group),
                true,
                false,
                true);
        }
        if (stepId != null && !stepId.isBlank()) {
            return new PendingIndexSelection(
                PENDING_STEP_INDEX,
                QUERY_PENDING_STEP_KEY,
                scopedKey(tenantId, stepId),
                true,
                true,
                false);
        }
        return new PendingIndexSelection(
            PENDING_TENANT_INDEX,
            QUERY_PENDING_TENANT_KEY,
            tenantId,
            true,
            true,
            true);
    }

    private static boolean matchesPendingQuery(
        AwaitInteractionRecord record,
        String tenantId,
        String assignee,
        String group,
        String stepId,
        long nowEpochMs) {
        String normalizedAssignee = normalizeFilter(assignee);
        String normalizedGroup = normalizeFilter(group);
        String normalizedStepId = normalizeFilter(stepId);
        return !record.status().terminal()
            && Objects.equals(record.tenantId(), tenantId)
            && (normalizedAssignee == null || Objects.equals(record.assignee(), normalizedAssignee))
            && (normalizedGroup == null || Objects.equals(record.group(), normalizedGroup))
            && (normalizedStepId == null || Objects.equals(record.stepId(), normalizedStepId))
            && !ttlExpired(record, nowEpochMs);
    }

    private static String normalizeFilter(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static boolean ttlExpired(AwaitInteractionRecord record, long nowEpochMs) {
        return record.ttlEpochS() > 0 && record.ttlEpochS() <= Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
    }

    private static String scopedOptionalKey(String tenantId, String value) {
        return value == null || value.isBlank() ? null : scopedKey(tenantId, value);
    }

    private static String scopedKey(String left, String right) {
        return left.length() + ":" + left + ":" + right.length() + ":" + right;
    }

    private static String itemSortKey(Integer itemIndex, String causationId, String interactionId) {
        int safeItemIndex = itemIndex == null ? Integer.MAX_VALUE : itemIndex;
        return String.format("%010d#%s#%s", safeItemIndex, nullToEmpty(causationId), interactionId);
    }

    private static String deadlineSortKey(long deadlineEpochMs, String... suffixParts) {
        return String.format("%019d#%s", deadlineEpochMs, String.join("#", suffixParts));
    }

    private static String deadlineUpperBound(long deadlineEpochMs) {
        return String.format("%019d~", deadlineEpochMs);
    }

    private record PendingIndexSelection(
        String indexName,
        String keyAttribute,
        String keyValue,
        boolean filterAssignee,
        boolean filterGroup,
        boolean filterStepId) {
    }

    private static String readString(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        return value == null ? null : value.s();
    }

    private static long readLong(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        if (value == null || value.n() == null || value.n().isBlank()) {
            return 0L;
        }
        return Long.parseLong(value.n());
    }

    private static Integer readInteger(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        if (value == null || value.n() == null || value.n().isBlank()) {
            return null;
        }
        return Integer.valueOf(value.n());
    }

    private static void putIfPresent(Map<String, AttributeValue> item, String key, String value) {
        if (value != null && !value.isBlank()) {
            item.put(key, avS(value));
        }
    }
}

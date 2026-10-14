package org.pipelineframework.awaitable.store;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.awaitable.AwaitCompletionAdmission;
import org.pipelineframework.awaitable.AwaitCompletionCursor;
import org.pipelineframework.awaitable.AwaitCompletionPage;
import org.pipelineframework.awaitable.AwaitReleaseDisposition;
import org.pipelineframework.awaitable.AwaitUnitReleaseAcknowledgement;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;

/**
 * DynamoDB encoding and strongly consistent reads for retained Await lifecycle facts.
 *
 * <p>This helper does not perform lifecycle admission or release. Callers include its
 * conditional writes in the owning store's existing DynamoDB transaction, alongside the
 * authoritative interaction or execution-state compare-and-set.</p>
 */
public final class DynamoAwaitCompletionLifecycleRepository {
    private static final String PK = "pk";
    private static final String SK = "sk";
    private static final String RECORD_TYPE = "record_type";
    private static final String ADMISSION_TYPE = "AWAIT_COMPLETION_ADMISSION";
    private static final String OPEN_TYPE = "AWAIT_COMPLETION_OPEN";
    private static final String RELEASE_TYPE = "AWAIT_UNIT_RELEASE_ACKNOWLEDGEMENT";
    private static final String SCHEMA_VERSION = "schema_version";
    private static final String TENANT_ID = "tenant_id";
    private static final String EXECUTION_ID = "execution_id";
    private static final String INTERACTION_ID = "interaction_id";
    private static final String UNIT_ID = "unit_id";
    private static final String STEP_ID = "step_id";
    private static final String STEP_INDEX = "step_index";
    private static final String ITEM_INDEX = "item_index";
    private static final String CAUSATION_ID = "causation_id";
    private static final String ADMITTED_AT = "first_admitted_at_epoch_ms";
    private static final String PIPELINE_ID = "pipeline_id";
    private static final String CONTRACT_VERSION = "contract_version";
    private static final String RELEASE_VERSION = "release_version";
    private static final String PARENT_VERSION = "parent_version";
    private static final String DISPOSITION = "disposition";
    private static final String ACKNOWLEDGED_AT = "acknowledged_at_epoch_ms";
    private static final String TTL = "ttl_epoch_s";
    private static final int MAX_PAGE_SIZE = 100;

    private final DynamoDbClient client;
    private final String tableName;
    private final Clock clock;

    public DynamoAwaitCompletionLifecycleRepository(DynamoDbClient client, String tableName, Clock clock) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.tableName = requiredText(tableName, "tableName");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * Returns the immutable admission fact and open-work projection writes. Include both
     * writes in the same transaction as the winning interaction completion CAS.
     */
    public List<TransactWriteItem> admissionWrites(AwaitCompletionAdmission admission) {
        Objects.requireNonNull(admission, "admission must not be null");
        return List.of(
            conditionalPut(admissionItem(admission)),
            conditionalPut(openItem(admission)));
    }

    /** Include this immutable acknowledgement write in the parent release CAS transaction. */
    public TransactWriteItem releaseWrite(AwaitUnitReleaseAcknowledgement acknowledgement) {
        Objects.requireNonNull(acknowledgement, "acknowledgement must not be null");
        return conditionalPut(releaseItem(acknowledgement));
    }

    /**
     * Adds an exact immutable-acknowledgement observation to a parent transaction. This is for
     * an authorized same-unit redrive that must preserve the first acknowledgement; it does not
     * create or change lifecycle authority.
     */
    public TransactWriteItem releaseObservationCondition(AwaitUnitReleaseAcknowledgement acknowledgement) {
        Objects.requireNonNull(acknowledgement, "acknowledgement must not be null");
        Map<String, String> names = Map.ofEntries(
            Map.entry("#type", RECORD_TYPE), Map.entry("#schema", SCHEMA_VERSION),
            Map.entry("#tenant", TENANT_ID), Map.entry("#execution", EXECUTION_ID),
            Map.entry("#unit", UNIT_ID), Map.entry("#pipeline", PIPELINE_ID),
            Map.entry("#contract", CONTRACT_VERSION), Map.entry("#release", RELEASE_VERSION),
            Map.entry("#step", STEP_INDEX), Map.entry("#parent", PARENT_VERSION),
            Map.entry("#disposition", DISPOSITION), Map.entry("#acknowledged", ACKNOWLEDGED_AT),
            Map.entry("#ttl", TTL));
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":type", string(RELEASE_TYPE));
        values.put(":schema", number(acknowledgement.schemaVersion()));
        values.put(":tenant", string(acknowledgement.tenantId()));
        values.put(":execution", string(acknowledgement.executionId()));
        values.put(":unit", string(acknowledgement.unitId()));
        values.put(":pipeline", string(acknowledgement.pipelineId()));
        values.put(":contract", string(acknowledgement.contractVersion()));
        values.put(":release", string(acknowledgement.releaseVersion()));
        values.put(":step", number(acknowledgement.stepIndex()));
        values.put(":parent", number(acknowledgement.parentVersion()));
        values.put(":disposition", string(acknowledgement.disposition().name()));
        values.put(":acknowledged", number(acknowledgement.acknowledgedAtEpochMs()));
        String ttlCondition = acknowledgement.ttlEpochS() == 0
            ? "attribute_not_exists(#ttl)" : "#ttl = :ttl";
        if (acknowledgement.ttlEpochS() > 0) values.put(":ttl", number(acknowledgement.ttlEpochS()));
        String condition = "#type = :type AND #schema = :schema AND #tenant = :tenant AND "
            + "#execution = :execution AND #unit = :unit AND #pipeline = :pipeline AND "
            + "#contract = :contract AND #release = :release AND #step = :step AND "
            + "#parent = :parent AND #disposition = :disposition AND #acknowledged = :acknowledged AND "
            + ttlCondition;
        return TransactWriteItem.builder().conditionCheck(ConditionCheck.builder()
            .tableName(tableName)
            .key(releaseKey(acknowledgement.tenantId(), acknowledgement.executionId(), acknowledgement.unitId()))
            .conditionExpression(condition)
            .expressionAttributeNames(names)
            .expressionAttributeValues(Map.copyOf(values))
            .build()).build();
    }

    /** Strongly reads the immutable first-admission fact, treating logical expiry as absent. */
    public Optional<AwaitCompletionAdmission> readAdmission(String tenantId, String interactionId) {
        return readAdmissionFact(tenantId, interactionId)
            .filter(admission -> !expired(admission.ttlEpochS()));
    }

    private Optional<AwaitCompletionAdmission> readAdmissionFact(String tenantId, String interactionId) {
        Map<String, AttributeValue> item = strongGet(admissionKey(
            requiredText(tenantId, "tenantId"), requiredText(interactionId, "interactionId")));
        if (item.isEmpty()) return Optional.empty();
        AwaitCompletionAdmission admission = decodeAdmission(item);
        if (!admission.tenantId().equals(tenantId) || !admission.interactionId().equals(interactionId)) {
            throw corrupt("Admission fact identity does not match its key");
        }
        return Optional.of(admission);
    }

    /** Strongly reads the immutable parent-release acknowledgement, treating logical expiry as absent. */
    public Optional<AwaitUnitReleaseAcknowledgement> readRelease(
        String tenantId, String executionId, String unitId
    ) {
        String tenant = requiredText(tenantId, "tenantId");
        String execution = requiredText(executionId, "executionId");
        String unit = requiredText(unitId, "unitId");
        Map<String, AttributeValue> item = strongGet(releaseKey(tenant, execution, unit));
        if (item.isEmpty()) return Optional.empty();
        AwaitUnitReleaseAcknowledgement acknowledgement = decodeRelease(item);
        if (!acknowledgement.tenantId().equals(tenant)
            || !acknowledgement.executionId().equals(execution)
            || !acknowledgement.unitId().equals(unit)) {
            throw corrupt("Release acknowledgement identity does not match its key");
        }
        return expired(acknowledgement.ttlEpochS()) ? Optional.empty() : Optional.of(acknowledgement);
    }

    /**
     * Reads at most one strongly consistent page from the tenant's primary-key open-work
     * partition. No page-level snapshot is promised. The cursor carries the complete DynamoDB
     * last-evaluated primary key and is scoped to this tenant.
     */
    public AwaitCompletionPage findOpen(
        String tenantId, int limit, Optional<AwaitCompletionCursor> cursor
    ) {
        String tenant = requiredText(tenantId, "tenantId");
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("limit must be between 1 and " + MAX_PAGE_SIZE);
        }
        Objects.requireNonNull(cursor, "cursor must not be null");
        String partition = openPartition(tenant);
        Map<String, AttributeValue> exclusiveStart = cursor.map(value -> {
            value.requireScope(tenant);
            Map<String, AttributeValue> decoded = decodeCursor(value.position());
            if (!partition.equals(text(decoded.get(PK)))) {
                throw new IllegalArgumentException("Await completion cursor partition does not match tenant");
            }
            return decoded;
        }).orElseGet(Map::of);

        QueryRequest.Builder request = QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression("#pk = :pk")
            .expressionAttributeNames(Map.of("#pk", PK))
            .expressionAttributeValues(Map.of(":pk", string(partition)))
            .limit(limit)
            .consistentRead(true);
        if (!exclusiveStart.isEmpty()) request.exclusiveStartKey(exclusiveStart);
        QueryResponse response = client.query(request.build());
        if (response.items().size() > limit) {
            throw corrupt("DynamoDB exceeded the bounded open-completion page size");
        }

        List<AwaitCompletionAdmission> admissions = new ArrayList<>();
        for (Map<String, AttributeValue> projection : response.items()) {
            verifyOpenProjection(projection, tenant);
            String interactionId = text(projection.get(INTERACTION_ID));
            Map<String, AttributeValue> admissionItem = strongGet(admissionKey(tenant, interactionId));
            if (admissionItem.isEmpty()) {
                // An absent fact cannot authorize cleanup of this projection.
                throw corrupt("Open completion projection has no readable admission fact");
            }
            AwaitCompletionAdmission admission = decodeAdmission(admissionItem);
            if (!admission.tenantId().equals(tenant) || !admission.interactionId().equals(interactionId)) {
                throw corrupt("Admission fact identity does not match its key");
            }
            verifyProjectionMatchesFact(projection, admission);
            if (expired(admission.ttlEpochS())) {
                retireExpiredProjection(projection, admission);
            } else {
                admissions.add(admission);
            }
        }

        Map<String, AttributeValue> lastEvaluatedKey = response.lastEvaluatedKey();
        Optional<AwaitCompletionCursor> nextCursor = Optional.empty();
        if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
            if (lastEvaluatedKey.size() != 2 || !lastEvaluatedKey.containsKey(PK) || !lastEvaluatedKey.containsKey(SK)) {
                throw corrupt("DynamoDB returned an incomplete open-completion continuation key");
            }
            if (!partition.equals(text(lastEvaluatedKey.get(PK)))) {
                throw corrupt("DynamoDB returned an open-completion continuation for another tenant");
            }
            if (!exclusiveStart.isEmpty()
                && text(lastEvaluatedKey.get(SK)).compareTo(text(exclusiveStart.get(SK))) <= 0) {
                throw corrupt("DynamoDB did not advance the open-completion continuation position");
            }
            String position = encodeCursor(lastEvaluatedKey);
            if (cursor.map(AwaitCompletionCursor::position).filter(position::equals).isPresent()) {
                throw corrupt("DynamoDB repeated the open-completion continuation position");
            }
            nextCursor = Optional.of(new AwaitCompletionCursor(tenant, position));
        }
        return new AwaitCompletionPage(admissions, nextCursor, response.items().size());
    }

    /**
     * Conditionally removes only the open projection for this exact immutable fact. Missing
     * projections are an idempotent success; admission facts and release acknowledgements remain.
     */
    public void retire(AwaitCompletionAdmission admission) {
        Objects.requireNonNull(admission, "admission must not be null");
        AwaitCompletionAdmission retained = readAdmissionFact(admission.tenantId(), admission.interactionId())
            .orElseThrow(() -> corrupt("Cannot retire an open projection without its retained admission fact"));
        if (!retained.equals(admission)) {
            throw corrupt("Retirement admission does not match the retained immutable fact");
        }
        Map<String, AttributeValue> key = openKey(admission.tenantId(), admission);
        try {
            client.deleteItem(DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression(retirementCondition(admission))
                .expressionAttributeNames(Map.of(
                    "#type", RECORD_TYPE, "#tenant", TENANT_ID, "#execution", EXECUTION_ID,
                    "#interaction", INTERACTION_ID, "#admitted", ADMITTED_AT, "#ttl", TTL))
                .expressionAttributeValues(retirementValues(admission))
                .build());
        } catch (ConditionalCheckFailedException ignored) {
            Map<String, AttributeValue> current = strongGet(key);
            if (!current.isEmpty()) {
                verifyOpenProjection(current, admission.tenantId());
                verifyProjectionMatchesFact(current, admission);
                throw corrupt("Open completion projection changed during retirement");
            }
        }
    }

    private TransactWriteItem conditionalPut(Map<String, AttributeValue> item) {
        return TransactWriteItem.builder().put(Put.builder()
            .tableName(tableName)
            .item(item)
            .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
            .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK))
            .build()).build();
    }

    private Map<String, AttributeValue> strongGet(Map<String, AttributeValue> key) {
        return client.getItem(GetItemRequest.builder().tableName(tableName).key(key).consistentRead(true).build()).item();
    }

    private Map<String, AttributeValue> admissionItem(AwaitCompletionAdmission admission) {
        Map<String, AttributeValue> item = new HashMap<>(admissionKey(admission.tenantId(), admission.interactionId()));
        item.put(RECORD_TYPE, string(ADMISSION_TYPE));
        item.put(SCHEMA_VERSION, number(admission.schemaVersion()));
        item.put(TENANT_ID, string(admission.tenantId()));
        item.put(EXECUTION_ID, string(admission.executionId()));
        item.put(INTERACTION_ID, string(admission.interactionId()));
        admission.unitId().ifPresent(value -> item.put(UNIT_ID, string(value)));
        item.put(STEP_ID, string(admission.stepId()));
        item.put(STEP_INDEX, number(admission.stepIndex()));
        if (admission.itemIndex().isPresent()) item.put(ITEM_INDEX, number(admission.itemIndex().getAsInt()));
        admission.causationId().ifPresent(value -> item.put(CAUSATION_ID, string(value)));
        item.put(ADMITTED_AT, number(admission.firstAdmittedAtEpochMs()));
        addTtl(item, admission.ttlEpochS());
        return Map.copyOf(item);
    }

    private Map<String, AttributeValue> openItem(AwaitCompletionAdmission admission) {
        Map<String, AttributeValue> item = new HashMap<>(openKey(admission.tenantId(), admission));
        item.put(RECORD_TYPE, string(OPEN_TYPE));
        item.put(TENANT_ID, string(admission.tenantId()));
        item.put(EXECUTION_ID, string(admission.executionId()));
        item.put(INTERACTION_ID, string(admission.interactionId()));
        item.put(ADMITTED_AT, number(admission.firstAdmittedAtEpochMs()));
        addTtl(item, admission.ttlEpochS());
        return Map.copyOf(item);
    }

    private Map<String, AttributeValue> releaseItem(AwaitUnitReleaseAcknowledgement acknowledgement) {
        Map<String, AttributeValue> item = new HashMap<>(releaseKey(acknowledgement.tenantId(),
            acknowledgement.executionId(), acknowledgement.unitId()));
        item.put(RECORD_TYPE, string(RELEASE_TYPE));
        item.put(SCHEMA_VERSION, number(acknowledgement.schemaVersion()));
        item.put(TENANT_ID, string(acknowledgement.tenantId()));
        item.put(EXECUTION_ID, string(acknowledgement.executionId()));
        item.put(UNIT_ID, string(acknowledgement.unitId()));
        item.put(PIPELINE_ID, string(acknowledgement.pipelineId()));
        item.put(CONTRACT_VERSION, string(acknowledgement.contractVersion()));
        item.put(RELEASE_VERSION, string(acknowledgement.releaseVersion()));
        item.put(STEP_INDEX, number(acknowledgement.stepIndex()));
        item.put(PARENT_VERSION, number(acknowledgement.parentVersion()));
        item.put(DISPOSITION, string(acknowledgement.disposition().name()));
        item.put(ACKNOWLEDGED_AT, number(acknowledgement.acknowledgedAtEpochMs()));
        addTtl(item, acknowledgement.ttlEpochS());
        return Map.copyOf(item);
    }

    private AwaitCompletionAdmission decodeAdmission(Map<String, AttributeValue> item) {
        requireType(item, ADMISSION_TYPE);
        return new AwaitCompletionAdmission(
            integer(item, SCHEMA_VERSION), text(item.get(TENANT_ID)), text(item.get(EXECUTION_ID)),
            text(item.get(INTERACTION_ID)), optionalText(item.get(UNIT_ID)),
            text(item.get(STEP_ID)), integer(item, STEP_INDEX), optionalInteger(item.get(ITEM_INDEX)),
            optionalText(item.get(CAUSATION_ID)), longValue(item, ADMITTED_AT), ttlValue(item));
    }

    private AwaitUnitReleaseAcknowledgement decodeRelease(Map<String, AttributeValue> item) {
        requireType(item, RELEASE_TYPE);
        return new AwaitUnitReleaseAcknowledgement(
            integer(item, SCHEMA_VERSION), text(item.get(TENANT_ID)), text(item.get(EXECUTION_ID)),
            text(item.get(UNIT_ID)), text(item.get(PIPELINE_ID)), text(item.get(CONTRACT_VERSION)),
            text(item.get(RELEASE_VERSION)), integer(item, STEP_INDEX), longValue(item, PARENT_VERSION),
            disposition(item.get(DISPOSITION)), longValue(item, ACKNOWLEDGED_AT), ttlValue(item));
    }

    private void verifyOpenProjection(Map<String, AttributeValue> item, String tenantId) {
        requireType(item, OPEN_TYPE);
        if (!tenantId.equals(text(item.get(TENANT_ID)))
            || !openPartition(tenantId).equals(text(item.get(PK)))) {
            throw corrupt("Open completion projection tenant does not match its partition");
        }
        requiredText(text(item.get(EXECUTION_ID)), EXECUTION_ID);
        requiredText(text(item.get(INTERACTION_ID)), INTERACTION_ID);
        longValue(item, ADMITTED_AT);
        ttlValue(item);
    }

    private void verifyProjectionMatchesFact(Map<String, AttributeValue> item, AwaitCompletionAdmission admission) {
        if (!text(item.get(TENANT_ID)).equals(admission.tenantId())
            || !text(item.get(EXECUTION_ID)).equals(admission.executionId())
            || !text(item.get(INTERACTION_ID)).equals(admission.interactionId())
            || longValue(item, ADMITTED_AT) != admission.firstAdmittedAtEpochMs()
            || ttlValue(item) != admission.ttlEpochS()
            || !openKey(admission.tenantId(), admission).equals(Map.of(PK, item.get(PK), SK, item.get(SK)))) {
            throw corrupt("Open completion projection does not match its admission fact");
        }
    }

    private void retireExpiredProjection(Map<String, AttributeValue> projection,
        AwaitCompletionAdmission admission) {
        verifyProjectionMatchesFact(projection, admission);
        if (admission.ttlEpochS() <= 0 || !expired(admission.ttlEpochS())) return;
        retire(admission);
    }

    private String retirementCondition(AwaitCompletionAdmission admission) {
        String identity = "#type = :type AND #tenant = :tenant AND #execution = :execution "
            + "AND #interaction = :interaction AND #admitted = :admitted AND ";
        return identity + (admission.ttlEpochS() == 0 ? "attribute_not_exists(#ttl)" : "#ttl = :ttl");
    }

    private Map<String, AttributeValue> retirementValues(AwaitCompletionAdmission admission) {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":type", string(OPEN_TYPE));
        values.put(":tenant", string(admission.tenantId()));
        values.put(":execution", string(admission.executionId()));
        values.put(":interaction", string(admission.interactionId()));
        values.put(":admitted", number(admission.firstAdmittedAtEpochMs()));
        if (admission.ttlEpochS() > 0) values.put(":ttl", number(admission.ttlEpochS()));
        return Map.copyOf(values);
    }

    private Map<String, AttributeValue> admissionKey(String tenantId, String interactionId) {
        return Map.of(PK, string("ADMISSION#" + component(tenantId) + "#" + component(interactionId)), SK, string("FACT"));
    }

    private Map<String, AttributeValue> openKey(String tenantId, AwaitCompletionAdmission admission) {
        String sort = "OPEN#" + String.format(java.util.Locale.ROOT, "%019d", admission.firstAdmittedAtEpochMs())
            + "#" + component(admission.interactionId());
        return Map.of(PK, string(openPartition(tenantId)), SK, string(sort));
    }

    private Map<String, AttributeValue> releaseKey(String tenantId, String executionId, String unitId) {
        return Map.of(PK, string("RELEASE#" + component(tenantId) + "#" + component(executionId)),
            SK, string("UNIT#" + component(unitId)));
    }

    private String openPartition(String tenantId) {
        return "OPEN#" + component(tenantId);
    }

    private boolean expired(long ttlEpochS) {
        return ttlEpochS > 0 && ttlEpochS <= clock.instant().getEpochSecond();
    }

    private String encodeCursor(Map<String, AttributeValue> key) {
        return "v1." + component(text(key.get(PK))) + "." + component(text(key.get(SK)));
    }

    private Map<String, AttributeValue> decodeCursor(String position) {
        String[] parts = position.split("\\.", -1);
        if (parts.length != 3 || !"v1".equals(parts[0])) {
            throw new IllegalArgumentException("Invalid Await completion cursor");
        }
        String partition = decodeComponent(parts[1]);
        String sort = decodeComponent(parts[2]);
        if (partition.isBlank() || !sort.matches("OPEN#[0-9]{19}#[A-Za-z0-9_-]+")) {
            throw new IllegalArgumentException("Invalid Await completion cursor");
        }
        try {
            Long.parseLong(sort.substring(5, 24));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid Await completion cursor", exception);
        }
        String interactionComponent = sort.substring(sort.lastIndexOf('#') + 1);
        if (decodeComponent(interactionComponent).isBlank()) {
            throw new IllegalArgumentException("Invalid Await completion cursor");
        }
        return Map.of(PK, string(partition), SK, string(sort));
    }

    private static String component(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeComponent(String value) {
        try {
            return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Invalid Await completion cursor", exception);
        }
    }

    private static void requireType(Map<String, AttributeValue> item, String expected) {
        if (!expected.equals(text(item.get(RECORD_TYPE)))) throw corrupt("Unexpected lifecycle record type");
    }

    private static String text(AttributeValue value) {
        if (value == null || value.s() == null) throw corrupt("Missing lifecycle string attribute");
        return value.s();
    }

    private static Optional<String> optionalText(AttributeValue value) {
        return value == null ? Optional.empty() : Optional.of(requiredText(text(value), "optional attribute"));
    }

    private static long longValue(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.n() == null) throw corrupt("Missing lifecycle numeric attribute: " + name);
        try {
            return Long.parseLong(value.n());
        } catch (NumberFormatException exception) {
            throw corrupt("Invalid lifecycle numeric attribute: " + name, exception);
        }
    }

    private static int integer(Map<String, AttributeValue> item, String name) {
        long value = longValue(item, name);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) throw corrupt("Lifecycle integer out of range: " + name);
        return (int) value;
    }

    private static long ttlValue(Map<String, AttributeValue> item) {
        return item.containsKey(TTL) ? longValue(item, TTL) : 0L;
    }

    private static void addTtl(Map<String, AttributeValue> item, long ttlEpochS) {
        if (ttlEpochS > 0) item.put(TTL, number(ttlEpochS));
    }

    private static java.util.OptionalInt optionalInteger(AttributeValue value) {
        if (value == null) return java.util.OptionalInt.empty();
        try {
            return java.util.OptionalInt.of(Integer.parseInt(value.n()));
        } catch (RuntimeException exception) {
            throw corrupt("Invalid lifecycle optional integer", exception);
        }
    }

    private static AwaitReleaseDisposition disposition(AttributeValue value) {
        try {
            return AwaitReleaseDisposition.valueOf(text(value));
        } catch (IllegalArgumentException exception) {
            throw corrupt("Invalid Await release disposition", exception);
        }
    }

    private static AttributeValue string(String value) {
        return AttributeValue.builder().s(requiredText(value, "value")).build();
    }

    private static AttributeValue number(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String requiredText(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
        return value;
    }

    private static IllegalStateException corrupt(String message) {
        return new IllegalStateException(message);
    }

    private static IllegalStateException corrupt(String message, Throwable cause) {
        return new IllegalStateException(message, cause);
    }
}

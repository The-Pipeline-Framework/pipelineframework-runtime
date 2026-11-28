package org.pipelineframework.orchestrator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/**
 * Immutable, append-only DynamoDB {@link ForeignDispatchIntentStore}.
 *
 * <p>Uses strictly conditional {@code PutItem} writes ({@code attribute_not_exists(pk) AND
 * attribute_not_exists(sk)}) with distinct sort keys ({@code RESERVE} and {@code SETTLE}) —
 * never {@code UpdateItem} or blind upserts.</p>
 */
public final class DynamoForeignDispatchIntentStore implements ForeignDispatchIntentStore {

    public static final String PK_ATTR = "pk";
    public static final String SK_ATTR = "sk";
    public static final String SK_RESERVE = "RESERVE";
    public static final String SK_SETTLE = "SETTLE";

    private static final String IMMUTABLE_CONDITION = "attribute_not_exists(#pk) AND attribute_not_exists(#sk)";
    private static final Map<String, String> KEY_NAMES = Map.of("#pk", PK_ATTR, "#sk", SK_ATTR);

    private final DynamoDbClient dynamoDb;
    private final String tableName;

    public DynamoForeignDispatchIntentStore(DynamoDbClient dynamoDb, String tableName) {
        this.dynamoDb = Objects.requireNonNull(dynamoDb, "dynamoDb must not be null");
        if (Objects.requireNonNull(tableName, "tableName must not be null").isBlank()) {
            throw new IllegalArgumentException("tableName must not be blank");
        }
        this.tableName = tableName;
    }

    @Override
    public ReservationOutcome reserve(ForeignDispatchIntent intent) {
        Objects.requireNonNull(intent, "intent must not be null");
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put(PK_ATTR, s(intent.storageKey()));
        item.put(SK_ATTR, s(SK_RESERVE));
        item.put("tenantId", s(intent.tenantId()));
        item.put("executionId", s(intent.executionId()));
        item.put("executionVersion", n(intent.executionVersion()));
        item.put("attempt", n(intent.attempt()));
        item.put("transitionKey", s(intent.transitionKey()));
        item.put("stepIndex", n(intent.stepIndex()));
        item.put("stepId", s(intent.stepId()));
        item.put("targetUri", s(intent.targetUri()));
        item.put("requestDigest", s(intent.requestDigest()));
        item.put("createdAtEpochMs", n(intent.createdAtEpochMs()));
        try {
            dynamoDb.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression(IMMUTABLE_CONDITION)
                .expressionAttributeNames(KEY_NAMES)
                .build());
            return ReservationOutcome.RESERVED;
        } catch (ConditionalCheckFailedException alreadyExists) {
            return ReservationOutcome.ALREADY_DISPATCHED_UNCERTAIN;
        }
    }

    @Override
    public void recordSettlement(ForeignDispatchIntent intent, SettlementStatus status, long settledAtEpochMs) {
        Objects.requireNonNull(intent, "intent must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put(PK_ATTR, s(intent.storageKey()));
        item.put(SK_ATTR, s(SK_SETTLE));
        item.put("tenantId", s(intent.tenantId()));
        item.put("executionId", s(intent.executionId()));
        item.put("transitionKey", s(intent.transitionKey()));
        item.put("stepIndex", n(intent.stepIndex()));
        item.put("stepId", s(intent.stepId()));
        item.put("settlementStatus", s(status.name()));
        item.put("settledAtEpochMs", n(settledAtEpochMs));
        try {
            dynamoDb.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression(IMMUTABLE_CONDITION)
                .expressionAttributeNames(KEY_NAMES)
                .build());
        } catch (ConditionalCheckFailedException ignoredAlreadySettled) {
            // Append-only: first recorded settlement wins and is never overwritten.
        }
    }

    private static AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue n(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }
}

package org.pipelineframework.awsproof;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/** Proof-only immutable one-shot fault controls. */
@ApplicationScoped
final class ProofFaultInjector {
    @Inject
    DynamoDbClient dynamo;

    void failIfArmed(String point, String correlation) {
        String table = Optional.ofNullable(System.getenv("TPF_PROOF_FAULT_TABLE")).orElse("");
        if (table.isBlank()) {
            return;
        }
        String exact = required(point, "point") + "#" + required(correlation, "correlation");
        String wildcard = point + "#*";
        if (isPersistent(table, exact) || isPersistent(table, wildcard)) {
            throw new ProofInjectedFaultException(point);
        }
        Optional<String> armed = isArmed(table, exact)
            ? Optional.of(exact)
            : isArmed(table, wildcard) ? Optional.of(wildcard) : Optional.empty();
        if (armed.isEmpty()) {
            return;
        }
        String pk = armed.orElseThrow();
        try {
            dynamo.putItem(PutItemRequest.builder()
                .tableName(table)
                .item(Map.of(
                    "pk", string(pk),
                    "sk", string("CONSUMED"),
                    "consumed_at_epoch_ms", number(Instant.now().toEpochMilli())))
                .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                .expressionAttributeNames(Map.of("#pk", "pk", "#sk", "sk"))
                .build());
        } catch (ConditionalCheckFailedException alreadyConsumed) {
            return;
        }
        throw new ProofInjectedFaultException(point);
    }

    private boolean isArmed(String table, String pk) {
        Map<String, AttributeValue> key = Map.of(
            "pk", string(pk),
            "sk", string("ARMED"));
        return !dynamo.getItem(GetItemRequest.builder().tableName(table).consistentRead(true).key(key).build())
            .item().isEmpty();
    }

    private boolean isPersistent(String table, String pk) {
        Map<String, AttributeValue> key = Map.of(
            "pk", string(pk),
            "sk", string("PERSISTENT"));
        return !dynamo.getItem(GetItemRequest.builder().tableName(table).consistentRead(true).key(key).build())
            .item().isEmpty();
    }

    private static AttributeValue string(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue number(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

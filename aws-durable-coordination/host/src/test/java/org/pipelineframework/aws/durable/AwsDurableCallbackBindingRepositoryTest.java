package org.pipelineframework.aws.durable;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableBindingStatus;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

class AwsDurableCallbackBindingRepositoryTest {
    private static final AwsDurableAwaitIdentity AWAIT = new AwsDurableAwaitIdentity(
        "tenant-a", "execution-a", "interaction-a", "correlation-a", 1);

    @Test
    void replayedAuthorityIgnoresReconstructedHousekeepingTimestamps() {
        AwsDurableCallbackBinding original = binding("callback-a", 100, 1_000);
        AwsDurableCallbackBinding reconstructed = binding("callback-a", 200, 2_000);

        assertThat(AwsDurableCallbackBindingRepository.sameBindingAuthority(original, reconstructed)).isTrue();
    }

    @Test
    void generationCannotBeReboundToAnotherProviderCallback() {
        assertThat(AwsDurableCallbackBindingRepository.sameBindingAuthority(
            binding("callback-a", 100, 1_000),
            binding("callback-b", 100, 1_000))).isFalse();
    }

    @Test
    void replayedRegistrationAuthorityIgnoresHousekeepingTimestamps() {
        AwsDurableCallbackRegistration original = registration("callback-a", 100, 1_000);
        AwsDurableCallbackRegistration replayed = registration("callback-a", 200, 2_000);

        assertThat(AwsDurableCallbackBindingRepository.sameRegistrationAuthority(original, replayed)).isTrue();
    }

    @Test
    void generationCannotBeRegisteredToAnotherProviderCallback() {
        assertThat(AwsDurableCallbackBindingRepository.sameRegistrationAuthority(
            registration("callback-a", 100, 1_000),
            registration("callback-b", 100, 1_000))).isFalse();
    }

    @ParameterizedTest
    @CsvSource({
        "other-pipeline, 1, release-a",
        "pipeline-a, other-contract, release-a",
        "pipeline-a, 1, other-release"
    })
    void conflictingSemanticCheckpointCannotReplayRegistration(
        String pipelineId, String contractVersion, String releaseVersion
    ) {
        DynamoDbClient dynamo = mock(DynamoDbClient.class);
        var repository = new AwsDurableCallbackBindingRepository(dynamo, "bindings");
        var original = registration("callback-a", 100, 1_000);
        persistThenConflict(dynamo, repository, original);
        var candidate = new AwsDurableCallbackRegistration(
            new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint(
                "tenant-a", "execution-a", pipelineId, contractVersion, releaseVersion), 1),
            original.providerExecutionName(), original.providerExecutionArn(), original.providerCallbackId(),
            200, 2_000);

        assertThatThrownBy(() -> repository.register(candidate))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already owned");
    }

    @Test
    void actualRegistrationReplayAcceptsChangedHousekeepingTimes() {
        DynamoDbClient dynamo = mock(DynamoDbClient.class);
        var repository = new AwsDurableCallbackBindingRepository(dynamo, "bindings");
        persistThenConflict(dynamo, repository, registration("callback-a", 100, 1_000));

        assertThat(repository.register(registration("callback-a", 200, 2_000))).isFalse();
    }

    private static void persistThenConflict(
        DynamoDbClient dynamo, AwsDurableCallbackBindingRepository repository,
        AwsDurableCallbackRegistration original
    ) {
        var rows = new java.util.HashMap<String, Map<String, AttributeValue>>();
        when(dynamo.transactWriteItems(any(TransactWriteItemsRequest.class))).thenAnswer(call -> {
            TransactWriteItemsRequest request = call.getArgument(0);
            request.transactItems().stream().filter(item -> item.put() != null)
                .forEach(item -> rows.put(item.put().item().get("sk").s(), item.put().item()));
            return TransactWriteItemsResponse.builder().build();
        });
        assertThat(repository.register(original)).isTrue();
        when(dynamo.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            GetItemRequest request = call.getArgument(0);
            return GetItemResponse.builder().item(rows.getOrDefault(request.key().get("sk").s(), Map.of())).build();
        });
        when(dynamo.transactWriteItems(any(TransactWriteItemsRequest.class)))
            .thenThrow(TransactionCanceledException.builder().message("existing authority").build());
    }


    @Test
    void successorClaimAtomicallyFencesThePredecessorWithoutExpiringEitherRecord() {
        DynamoDbClient dynamo = mock(DynamoDbClient.class);
        var repository = new AwsDurableCallbackBindingRepository(dynamo, "bindings");
        var checkpoint = new AwsDurableDriverCheckpoint(registration("callback-a", 100, 1000).checkpoint().execution(), 2);

        assertThat(repository.claimGeneration(checkpoint)).isEqualTo(AwsDurableGenerationAdmission.CREATED);

        var captured = org.mockito.ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamo).transactWriteItems(captured.capture());
        var operations = captured.getValue().transactItems();
        assertThat(operations).hasSize(3);
        assertThat(operations.get(0).put().item().get("sk").s()).isEqualTo("CLAIM#2");
        assertThat(operations.get(1).conditionCheck().key().get("sk").s()).isEqualTo("CLAIM#1");
        assertThat(operations.get(1).conditionCheck().expressionAttributeValues())
            .containsEntry(":release", AttributeValue.fromS("release-a"));
        assertThat(operations.get(2).put().item().get("sk").s()).isEqualTo("SUCCESSOR#1");
        assertThat(operations.get(0).put().item()).doesNotContainKey("expires_at_epoch_s");
        assertThat(operations.get(2).put().item()).doesNotContainKey("expires_at_epoch_s");
    }

    @Test
    void bindingAdmissionChecksTheGenerationFenceAndRegisteredCallbackInOneTransaction() {
        DynamoDbClient dynamo = mock(DynamoDbClient.class);
        var repository = new AwsDurableCallbackBindingRepository(dynamo, "bindings");

        assertThat(repository.bind(binding("callback-a", 100, 1000))).isTrue();

        var captured = org.mockito.ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamo).transactWriteItems(captured.capture());
        var checks = captured.getValue().transactItems();
        assertThat(checks).hasSize(4);
        assertThat(checks.get(0).conditionCheck().key().get("sk").s()).isEqualTo("CLAIM#1");
        assertThat(checks.get(1).conditionCheck().key().get("sk").s()).isEqualTo("SUCCESSOR#1");
        assertThat(checks.get(1).conditionCheck().conditionExpression()).isEqualTo("attribute_not_exists(#pk)");
        assertThat(checks.get(2).conditionCheck().expressionAttributeValues())
            .containsEntry(":callback", AttributeValue.fromS("callback-a"));
    }

    @Test
    void successorFenceRejectsStaleBindingEvenWhenCallbackRowsHaveExpired() {
        DynamoDbClient dynamo = mock(DynamoDbClient.class);
        when(dynamo.transactWriteItems(any(TransactWriteItemsRequest.class)))
            .thenThrow(TransactionCanceledException.builder().build());
        when(dynamo.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            GetItemRequest request = call.getArgument(0);
            assertThat(request.consistentRead()).isTrue();
            return GetItemResponse.builder().item(request.key().get("sk").s().equals("SUCCESSOR#1")
                ? Map.of("generation", AttributeValue.fromN("2")) : Map.of()).build();
        });

        assertThatThrownBy(() -> new AwsDurableCallbackBindingRepository(dynamo, "bindings")
            .bind(binding("callback-a", 100, 1000))).isInstanceOf(AwsDurableStaleGenerationException.class);
    }

    @Test
    void missingPredecessorKeepsGenerationAdmissionRetryable() {
        DynamoDbClient dynamo = mock(DynamoDbClient.class);
        var cancellation = TransactionCanceledException.builder().message("missing predecessor").build();
        when(dynamo.transactWriteItems(any(TransactWriteItemsRequest.class))).thenThrow(cancellation);
        when(dynamo.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        var checkpoint = new AwsDurableDriverCheckpoint(registration("callback-a", 100, 1000).checkpoint().execution(), 2);

        assertThatThrownBy(() -> new AwsDurableCallbackBindingRepository(dynamo, "bindings")
            .claimGeneration(checkpoint)).isSameAs(cancellation);
    }

    @Test
    void malformedBindingIsIsolatedFromTheReconciliationBatch() {
        Map<String, AttributeValue> malformed = Map.of(
            "pk", AttributeValue.fromS("malformed"),
            "sk", AttributeValue.fromS("GEN#00000000000000000001"),
            "record_type", AttributeValue.fromS("BINDING"));

        assertThat(AwsDurableCallbackBindingRepository.decodeBinding(malformed)).isEmpty();
    }

    @Test
    void providerExecutionLookupUsesTheDedicatedIndex() {
        DynamoDbClient dynamo = mock(DynamoDbClient.class);
        when(dynamo.query(any(QueryRequest.class))).thenReturn(registrationResponse());
        var repository = new AwsDurableCallbackBindingRepository(dynamo, "bindings");

        Optional<AwsDurableCallbackRegistration> result =
            repository.findRegistrationByProviderExecutionArn("arn:closed");

        assertThat(result).hasValueSatisfying(registration -> {
            assertThat(registration.providerExecutionArn()).isEqualTo("arn:closed");
            assertThat(registration.checkpoint().executionId()).isEqualTo("execution-a");
            assertThat(registration.checkpoint().generation()).isEqualTo(1);
        });
        verify(dynamo).query(org.mockito.ArgumentMatchers.<QueryRequest>argThat(request ->
            AwsDurableCallbackBindingRepository.PROVIDER_REGISTRATION_INDEX.equals(request.indexName())
                && request.limit() == 1
                && request.filterExpression() == null
                && request.keyConditionExpression().equals(
                    "#providerArn = :providerArn AND begins_with(#sk, :registration)")
                && request.expressionAttributeNames().get("#sk").equals("sk")
                && request.expressionAttributeValues().get(":registration").s().equals("REGISTRATION#")));
    }

    private static QueryResponse registrationResponse() {
        return QueryResponse.builder()
            .items(Map.ofEntries(
                Map.entry("pk", AttributeValue.fromS("tenant-a#execution-a")),
                Map.entry("sk", AttributeValue.fromS("REGISTRATION#00000000000000000001")),
                Map.entry("record_type", AttributeValue.fromS("REGISTRATION")),
                Map.entry("tenant_id", AttributeValue.fromS("tenant-a")),
                Map.entry("execution_id", AttributeValue.fromS("execution-a")),
                Map.entry("pipeline_id", AttributeValue.fromS("pipeline-a")),
                Map.entry("contract_version", AttributeValue.fromS("contract-a")),
                Map.entry("release_version", AttributeValue.fromS("release-a")),
                Map.entry("generation", AttributeValue.fromN("1")),
                Map.entry("provider_execution_name", AttributeValue.fromS("provider-a")),
                Map.entry("provider_execution_arn", AttributeValue.fromS("arn:closed")),
                Map.entry("provider_callback_id", AttributeValue.fromS("callback-a")),
                Map.entry("created_at_epoch_ms", AttributeValue.fromN("100")),
                Map.entry("expires_at_epoch_s", AttributeValue.fromN("1000"))))
            .build();
    }

    @Test
    void exhaustedRegistrationIndexReturnsEmptyForRetryableRepair() {
        DynamoDbClient dynamo = mock(DynamoDbClient.class);
        when(dynamo.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());

        assertThat(new AwsDurableCallbackBindingRepository(dynamo, "bindings")
            .findRegistrationByProviderExecutionArn("arn:closed")).isEmpty();
        verify(dynamo).query(any(QueryRequest.class));
    }

    private static AwsDurableCallbackBinding binding(String callbackId, long createdAt, long expiresAt) {
        return new AwsDurableCallbackBinding(
            AWAIT,
            "provider-execution-a",
            "arn:aws:lambda:us-east-2:123456789012:function:proof:1/durable-execution/execution-a",
            callbackId,
            AwsDurableBindingStatus.OPEN,
            createdAt,
            expiresAt);
    }

    private static AwsDurableCallbackRegistration registration(String callbackId, long createdAt, long expiresAt) {
        return new AwsDurableCallbackRegistration(
            new AwsDurableDriverCheckpoint(
                new AwsDurableExecutionCheckpoint(
                    "tenant-a", "execution-a", "pipeline-a", "1", "release-a"),
                1),
            "provider-execution-a",
            "arn:aws:lambda:us-east-2:123456789012:function:proof:1/durable-execution/execution-a",
            callbackId,
            createdAt,
            expiresAt);
    }
}

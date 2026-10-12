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
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
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
        when(dynamo.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());
        assertThat(repository.register(original)).isTrue();
        var stored = org.mockito.ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamo).putItem(stored.capture());
        when(dynamo.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(stored.getValue().item()).build());
        when(dynamo.putItem(any(PutItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("existing authority").build());
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

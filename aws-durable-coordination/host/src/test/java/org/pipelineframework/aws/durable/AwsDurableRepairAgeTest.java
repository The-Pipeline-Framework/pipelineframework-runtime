package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableRecoveryLocator;
import org.pipelineframework.aws.durable.model.AwsDurableStartResponse;
import org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

class AwsDurableRepairAgeTest {
    private static final String ARN = "arn:aws:lambda:eu-west-1:123456789012:function:durable:7/durable-execution/private-id";
    private static final String OTHER_ARN = "arn:aws:lambda:eu-west-1:123456789012:function:durable:7/durable-execution/other-id";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void firstObservationSurvivesRepositoryRestartAndIsScopedToExactProviderArn() {
        var rows = new HashMap<Map<String, AttributeValue>, Map<String, AttributeValue>>();
        var dynamo = rowStore(rows);
        var first = repository(dynamo, 1_000);
        var restarted = repository(dynamo, 5_000);

        assertThat(first.recordRepairFirstObservedAt(ARN)).hasValue(1_000L);
        assertThat(restarted.recordRepairFirstObservedAt(ARN)).hasValue(1_000L);
        assertThat(restarted.recordRepairFirstObservedAt(OTHER_ARN)).hasValue(5_000L);
        assertThat(rows).hasSize(2);
        var stored = rows.values().stream().filter(row -> ARN.equals(row.get("repair_provider_execution_arn").s())).findFirst().orElseThrow();
        assertThat(stored.get("record_type").s()).isEqualTo(AwsDurableCallbackBindingRepository.REPAIR_FIRST_OBSERVED);
        assertThat(stored.get("first_observed_at_epoch_ms").n()).isEqualTo("1000");
        assertThat(stored.get("timestamp_source").s()).isEqualTo("HOST_FIRST_OBSERVED");
        assertThat(stored).doesNotContainKey("expires_at_epoch_s");
        assertThat(stored).doesNotContainKey("provider_execution_arn");
    }

    @Test
    void aConflictingOrInvalidFirstObservationFailsClosedAndNegativeClockDoesNotPersist() {
        var collision = mock(DynamoDbClient.class);
        when(collision.putItem(any(PutItemRequest.class))).thenThrow(ConditionalCheckFailedException.builder().build());
        when(collision.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(Map.of(
            "record_type", AttributeValue.fromS(AwsDurableCallbackBindingRepository.REPAIR_FIRST_OBSERVED),
            "repair_provider_execution_arn", AttributeValue.fromS(OTHER_ARN),
            "timestamp_source", AttributeValue.fromS("HOST_FIRST_OBSERVED"),
            "first_observed_at_epoch_ms", AttributeValue.fromN("1000"))).build());
        assertThatThrownBy(() -> repository(collision, 2_000).recordRepairFirstObservedAt(ARN))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("another provider execution");

        when(collision.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(Map.of(
            "record_type", AttributeValue.fromS(AwsDurableCallbackBindingRepository.REPAIR_FIRST_OBSERVED),
            "repair_provider_execution_arn", AttributeValue.fromS(ARN),
            "timestamp_source", AttributeValue.fromS("HOST_FIRST_OBSERVED"),
            "first_observed_at_epoch_ms", AttributeValue.fromN("-1"))).build());
        assertThatThrownBy(() -> repository(collision, 2_000).recordRepairFirstObservedAt(ARN))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("missing or invalid");

        var negative = mock(DynamoDbClient.class);
        assertThat(repository(negative, -1).recordRepairFirstObservedAt(ARN)).isEmpty();
        verify(negative, never()).putItem(any(PutItemRequest.class));
    }

    @Test
    void sampledAgeUsesTheRetainedTimestampAndNeverEmitsPrivateArnOrInvalidDuration() throws Exception {
        var output = new ArrayList<String>();
        var telemetry = new AwsDurableHostTelemetry(fixed(1_250), output::add);
        telemetry.recordRepairAge(ARN, OptionalLong.of(1_000));
        telemetry.recordRepairAge(ARN, OptionalLong.empty());
        telemetry.recordRepairAge(OTHER_ARN, OptionalLong.of(1_300));
        telemetry.recordRepairAge(OTHER_ARN, OptionalLong.of(-1));

        JsonNode sampled = MAPPER.readTree(output.get(0));
        assertThat(sampled.path("Outcome").asText()).isEqualTo("REPAIR_AGE_SAMPLED");
        assertThat(sampled.path("RepairAgeMilliseconds").asLong()).isEqualTo(250);
        assertThat(sampled.path("RepairAgeCoverage").asText())
            .isEqualTo("SAMPLED_PER_TARGETED_REPAIR_ATTEMPT_NOT_GLOBAL_INVENTORY");
        JsonNode missing = MAPPER.readTree(output.get(1));
        assertThat(missing.path("Outcome").asText()).isEqualTo("REPAIR_AGE_UNKNOWN");
        assertThat(missing.has("RepairAgeMilliseconds")).isFalse();
        JsonNode future = MAPPER.readTree(output.get(2));
        assertThat(future.has("RepairAgeMilliseconds")).isFalse();
        JsonNode negative = MAPPER.readTree(output.get(3));
        assertThat(negative.path("Outcome").asText()).isEqualTo("REPAIR_AGE_UNKNOWN");
        assertThat(negative.has("RepairAgeMilliseconds")).isFalse();
        assertThat(output).allSatisfy(message -> assertThat(message).doesNotContain("private-id", "other-id"));
    }

    @Test
    void failedObservationStorageAndReplacementStartDoNotBlockRepairOrRetireTheFact() throws Exception {
        var messages = new ArrayList<String>();
        var telemetry = new AwsDurableHostTelemetry(fixed(5_000), messages::add);
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        when(bindings.recordRepairFirstObservedAt(ARN)).thenThrow(new IllegalStateException("DynamoDB unavailable"));
        var controlPlane = mock(PipelineControlPlane.class);
        var starter = mock(AwsDurableExecutionStarter.class);
        var pinned = mock(AwsDurableExecutionStarter.class);
        var provider = mock(AwsDurableProviderExecutionInspector.class);
        var checkpoint = new AwsDurableExecutionCheckpoint("tenant", "execution", "pipeline", "contract", "release");
        when(provider.inspect(ARN)).thenReturn(AwsDurableProviderExecutionState.REPLACEABLE);
        when(bindings.findRecoveryByProviderExecutionArn(ARN)).thenReturn(Optional.of(
            new AwsDurableRecoveryLocator(new AwsDurableDriverCheckpoint(checkpoint, 1), "execution", ARN)));
        when(controlPlane.getExecutionSemanticCheckpoint("tenant", "execution")).thenReturn(Uni.createFrom().item(
            new ExecutionSemanticCheckpoint("tenant", "execution", "pipeline", "contract", "release", ExecutionStatus.RUNNING)));
        when(starter.pinnedToExecution(ARN)).thenReturn(pinned);
        when(bindings.claimGeneration(new AwsDurableDriverCheckpoint(checkpoint, 2))).thenReturn(AwsDurableGenerationAdmission.CREATED);
        when(pinned.start(any(AwsDurableExecutionInput.class))).thenReturn(new AwsDurableStartResponse("replacement", 202));
        var reconciler = new AwsDurableTerminalReconciler(bindings, controlPlane, starter, provider, Duration.ofSeconds(1), telemetry);

        assertThat(reconciler.reconcile(ARN)).isTrue();
        JsonNode unknown = MAPPER.readTree(messages.get(0));
        assertThat(unknown.path("Outcome").asText()).isEqualTo("REPAIR_AGE_UNKNOWN");
        assertThat(unknown.has("RepairAgeMilliseconds")).isFalse();
        assertThat(messages).allSatisfy(message -> assertThat(message).doesNotContain("private-id"));
        verify(bindings).recordRepairFirstObservedAt(ARN);
    }

    @Test
    void providerOnlyObservationAndAcceptedReplacementKeepTheFirstFact() {
        var rows = new HashMap<Map<String, AttributeValue>, Map<String, AttributeValue>>();
        var dynamo = rowStore(rows);
        var bindings = spy(repository(dynamo, 1_000));
        var checkpoint = new AwsDurableExecutionCheckpoint("tenant", "execution", "pipeline", "contract", "release");
        doReturn(Optional.of(new AwsDurableRecoveryLocator(new AwsDurableDriverCheckpoint(checkpoint, 1), "execution", ARN)))
            .when(bindings).findRecoveryByProviderExecutionArn(ARN);
        doReturn(AwsDurableGenerationAdmission.CREATED).when(bindings).claimGeneration(any(AwsDurableDriverCheckpoint.class));

        var controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getExecutionSemanticCheckpoint("tenant", "execution")).thenReturn(Uni.createFrom().item(
            new ExecutionSemanticCheckpoint("tenant", "execution", "pipeline", "contract", "release", ExecutionStatus.RUNNING)));
        var starter = mock(AwsDurableExecutionStarter.class);
        var pinned = mock(AwsDurableExecutionStarter.class);
        when(starter.pinnedToExecution(ARN)).thenReturn(pinned);
        when(pinned.start(any(AwsDurableExecutionInput.class))).thenReturn(new AwsDurableStartResponse("replacement", 202));
        var provider = mock(AwsDurableProviderExecutionInspector.class);
        when(provider.inspect(ARN)).thenReturn(AwsDurableProviderExecutionState.ACTIVE, AwsDurableProviderExecutionState.REPLACEABLE);
        var telemetry = new AwsDurableHostTelemetry(fixed(5_000), ignored -> { });
        var reconciler = new AwsDurableTerminalReconciler(bindings, controlPlane, starter, provider,
            Duration.ofSeconds(1), telemetry);

        assertThat(reconciler.reconcile(ARN)).isFalse();
        assertThat(reconciler.reconcile(ARN)).isTrue();
        assertThat(repository(dynamo, 9_000).recordRepairFirstObservedAt(ARN)).hasValue(1_000L);
        verify(dynamo, never()).deleteItem(any(DeleteItemRequest.class));
    }

    private static DynamoDbClient rowStore(Map<Map<String, AttributeValue>, Map<String, AttributeValue>> rows) {
        DynamoDbClient dynamo = mock(DynamoDbClient.class);
        when(dynamo.putItem(any(PutItemRequest.class))).thenAnswer(call -> {
            PutItemRequest request = call.getArgument(0);
            Map<String, AttributeValue> item = request.item();
            Map<String, AttributeValue> key = Map.of("pk", item.get("pk"), "sk", item.get("sk"));
            if (rows.putIfAbsent(key, item) != null) {
                throw ConditionalCheckFailedException.builder().message("already exists").build();
            }
            return PutItemResponse.builder().build();
        });
        when(dynamo.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            GetItemRequest request = call.getArgument(0);
            return GetItemResponse.builder().item(rows.getOrDefault(request.key(), Map.of())).build();
        });
        return dynamo;
    }

    private static AwsDurableCallbackBindingRepository repository(DynamoDbClient dynamo, long now) {
        Clock clock = fixed(now);
        return new AwsDurableCallbackBindingRepository(dynamo, "bindings",
            new AwsDurableHostTelemetry(clock, ignored -> { }), clock);
    }

    private static Clock fixed(long millis) {
        return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }
}

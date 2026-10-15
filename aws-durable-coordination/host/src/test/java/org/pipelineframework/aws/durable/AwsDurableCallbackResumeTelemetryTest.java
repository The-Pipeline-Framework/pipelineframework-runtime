package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableOperation;
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
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

class AwsDurableCallbackResumeTelemetryTest {
    private static final String TABLE = "bindings";

    @Test
    void emitsOneMeasuredSampleFromApiAcknowledgementObservationToFirstVerifiedResume() {
        AwsDurableAwaitIdentity identity = identity(1, 2);
        AwsDurableCallbackBinding binding = binding(identity);
        InMemoryDynamo dynamo = new InMemoryDynamo(binding, registration(identity));
        List<String> events = new ArrayList<>();
        AwsDurableHostTelemetry telemetry = new AwsDurableHostTelemetry(
            Clock.fixed(Instant.ofEpochMilli(4_000), ZoneOffset.UTC), events::add);
        AwsDurableCallbackBindingRepository repository = new AwsDurableCallbackBindingRepository(
            dynamo.client, TABLE, telemetry, Clock.fixed(Instant.ofEpochMilli(5_500), ZoneOffset.UTC));

        repository.recordDriverResumed(identity(1, 1)); // A different occurrence cannot borrow this callback's fact.
        repository.recordDriverResumed(identity(2, 2)); // A different generation cannot borrow this callback's fact.
        assertThat(dynamo.rows.values()).noneMatch(row -> "DRIVER_RESUMED".equals(text(row, "record_type")));

        repository.recordCallbackAccepted(binding, 4_000);
        repository.recordDriverResumed(identity);
        repository.recordDriverResumed(identity); // Durable action replay/lost response.

        assertThat(dynamo.rows.values()).filteredOn(row -> "CALLBACK_ACCEPTED".equals(text(row, "record_type")))
            .singleElement().satisfies(row -> {
                assertThat(text(row, "timestamp_source")).isEqualTo("HOST_API_ACK_OBSERVATION");
                assertThat(text(row, "provider_execution_arn")).isEqualTo("arn:secret-provider-execution");
                assertThat(text(row, "release_version")).isEqualTo("release-a");
                assertThat(text(row, "callback_success_observed_at_epoch_ms")).isEqualTo("4000");
            });
        assertThat(dynamo.rows.values()).filteredOn(row -> "DRIVER_RESUMED".equals(text(row, "record_type")))
            .singleElement().satisfies(row -> assertThat(text(row, "resumed_at_epoch_ms")).isEqualTo("5500"));
        assertThat(dynamo.rows.values()).filteredOn(row -> "CALLBACK_LATENCY".equals(text(row, "record_type")))
            .singleElement().satisfies(row -> {
                assertThat(text(row, "latency_outcome")).isEqualTo("MEASURED");
                assertThat(text(row, "latency_ms")).isEqualTo("1500");
            });
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).contains("CallbackToFirstResumedActionMilliseconds", "1500", "CALLBACK_RESUME", "MEASURED");
        assertThat(events.get(0)).doesNotContain(identity.tenantId(), identity.executionId(), identity.interactionId(),
            binding.providerExecutionArn(), binding.providerCallbackId(), "release-a");
    }

    @Test
    void lateHostAcknowledgementIsUnknownRatherThanZeroOrNegativeLatency() {
        AwsDurableAwaitIdentity identity = identity(2, 1);
        AwsDurableCallbackBinding binding = binding(identity);
        InMemoryDynamo dynamo = new InMemoryDynamo(binding, registration(identity));
        List<String> events = new ArrayList<>();
        AwsDurableHostTelemetry telemetry = new AwsDurableHostTelemetry(
            Clock.fixed(Instant.ofEpochMilli(6_000), ZoneOffset.UTC), events::add);
        AwsDurableCallbackBindingRepository repository = new AwsDurableCallbackBindingRepository(
            dynamo.client, TABLE, telemetry, Clock.fixed(Instant.ofEpochMilli(5_000), ZoneOffset.UTC));

        repository.recordDriverResumed(identity);
        repository.recordCallbackAccepted(binding, 5_500); // API response followed the resumed action.

        assertThat(dynamo.rows.values()).filteredOn(row -> "CALLBACK_LATENCY".equals(text(row, "record_type")))
            .singleElement().satisfies(row -> {
                assertThat(text(row, "latency_outcome")).isEqualTo("UNKNOWN");
                assertThat(row).doesNotContainKey("latency_ms");
            });
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).contains("CALLBACK_RESUME", "UNKNOWN", "CallbackResumeUnknownCount");
        assertThat(events.get(0)).doesNotContain("CallbackToFirstResumedActionMilliseconds");
    }

    @Test
    void normalApiReturnProvidesOnlyTheHostObservationTimestamp() {
        var lambda = mock(software.amazon.awssdk.services.lambda.LambdaClient.class);
        var client = new AwsDurableCallbackClient(lambda, new com.fasterxml.jackson.databind.ObjectMapper(),
            Clock.fixed(Instant.ofEpochMilli(8_123), ZoneOffset.UTC));

        long observedAt = client.sendSuccessObserved(binding(identity(1, 1)), new org.pipelineframework.aws.durable.model.AwsDurableCallbackSignal(
            "tenant-a", "execution-a", "interaction-a", "correlation-a", 1, "COMPLETED", 1));

        assertThat(observedAt).isEqualTo(8_123);
    }

    @Test
    void genuineRetainedDriverContextIsHistoricalEvidenceAfterSuccessorGenerationFence() {
        var fixture = new AttributionFixture();
        var generationRows = fixture.nonTimingRows();

        fixture.actions.invoke(resumedRequest(Optional.of("pipeline-a"), Optional.of("contract-a"),
            Optional.of("release-a"), Optional.of("arn:secret-provider-execution")));
        fixture.actions.invoke(resumedRequest(Optional.of("pipeline-a"), Optional.of("contract-a"),
            Optional.of("release-a"), Optional.of("arn:secret-provider-execution")));

        assertThat(fixture.rowsOf("DRIVER_RESUMED")).singleElement().satisfies(row -> {
            assertThat(text(row, "generation")).isEqualTo("1");
            assertThat(text(row, "occurrence")).isEqualTo("2");
            assertThat(text(row, "provider_execution_arn")).isEqualTo("arn:secret-provider-execution");
            assertThat(text(row, "release_version")).isEqualTo("release-a");
            assertThat(text(row, "resumed_at_epoch_ms")).isEqualTo("5500");
        });
        assertThat(fixture.rowsOf("CALLBACK_LATENCY")).singleElement().satisfies(row -> {
            assertThat(text(row, "generation")).isEqualTo("1");
            assertThat(text(row, "latency_ms")).isEqualTo("1500");
        });
        assertThat(fixture.events).hasSize(1);
        assertThat(fixture.nonTimingRows()).isEqualTo(generationRows);
        assertThat(fixture.repository.isCurrentGeneration("tenant-a", "execution-a", 2)).isTrue();
        org.mockito.Mockito.verifyNoInteractions(fixture.controlPlane);
    }

    @ParameterizedTest
    @ValueSource(strings = {"provider", "pipeline", "contract", "release", "missingProvider", "missingRelease", "missingContext"})
    void mismatchedOrMissingDriverContextCannotBorrowRetainedAttribution(String mismatch) {
        var fixture = new AttributionFixture();
        Optional<String> pipeline = Optional.of("pipeline-a");
        Optional<String> contract = Optional.of("contract-a");
        Optional<String> release = Optional.of("release-a");
        Optional<String> provider = Optional.of("arn:secret-provider-execution");
        switch (mismatch) {
            case "provider" -> provider = Optional.of("arn:other-provider-execution");
            case "pipeline" -> pipeline = Optional.of("other-pipeline");
            case "contract" -> contract = Optional.of("other-contract");
            case "release" -> release = Optional.of("other-release");
            case "missingProvider" -> provider = Optional.empty();
            case "missingRelease" -> release = Optional.empty();
            case "missingContext" -> {
                pipeline = Optional.empty();
                contract = Optional.empty();
                release = Optional.empty();
                provider = Optional.empty();
            }
            default -> throw new AssertionError("Unknown attribution challenge: " + mismatch);
        }
        try {
            fixture.actions.invoke(resumedRequest(pipeline, contract, release, provider));
        } catch (IllegalArgumentException | IllegalStateException rejected) {
            // Explicit rejection and an ignored observation are both safe attribution outcomes.
        }
        var improperlyAttributedResumes = fixture.rowsOf("DRIVER_RESUMED");
        var improperlyAttributedSamples = fixture.rowsOf("CALLBACK_LATENCY");
        var improperlyEmittedSamples = List.copyOf(fixture.events);

        // A rejected observation must not consume the immutable first slot for genuine context.
        fixture.actions.invoke(resumedRequest(Optional.of("pipeline-a"), Optional.of("contract-a"),
            Optional.of("release-a"), Optional.of("arn:secret-provider-execution")));
        org.junit.jupiter.api.Assertions.assertAll(
            () -> assertThat(improperlyAttributedResumes).as("wrong/missing context cannot claim first resume").isEmpty(),
            () -> assertThat(improperlyAttributedSamples).as("wrong/missing context cannot claim measurement").isEmpty(),
            () -> assertThat(improperlyEmittedSamples).as("wrong/missing context cannot publish measurement").isEmpty(),
            () -> assertThat(fixture.rowsOf("DRIVER_RESUMED")).hasSize(1),
            () -> assertThat(fixture.rowsOf("CALLBACK_LATENCY")).hasSize(1),
            () -> assertThat(fixture.events).hasSize(1));
        org.mockito.Mockito.verifyNoInteractions(fixture.controlPlane);
    }

    private static AwsDurableActionRequest resumedRequest(Optional<String> pipeline, Optional<String> contract,
        Optional<String> release, Optional<String> provider) {
        var identity = identity(1, 2);
        return new AwsDurableActionRequest(AwsDurableOperation.RECORD_AWAIT_RESUMED,
            identity.tenantId(), Optional.of(identity.executionId()), Optional.empty(), pipeline, contract, release,
            Optional.empty(), Optional.of(identity), Optional.empty(), provider, Optional.empty(), Optional.empty(),
            Optional.empty(), identity.generation());
    }

    private static final class AttributionFixture {
        private final InMemoryDynamo dynamo = new InMemoryDynamo(binding(identity(1, 2)), registration(identity(1, 2)));
        private final List<String> events = new ArrayList<>();
        private final org.pipelineframework.orchestrator.PipelineControlPlane controlPlane =
            mock(org.pipelineframework.orchestrator.PipelineControlPlane.class);
        private final AwsDurableHostTelemetry telemetry = new AwsDurableHostTelemetry(
            Clock.fixed(Instant.ofEpochMilli(4_000), ZoneOffset.UTC), events::add);
        private final AwsDurableCallbackBindingRepository repository = new AwsDurableCallbackBindingRepository(
            dynamo.client, TABLE, telemetry, Clock.fixed(Instant.ofEpochMilli(5_500), ZoneOffset.UTC));
        private final AwsDurableControlPlaneActions actions = new AwsDurableControlPlaneActions(controlPlane,
            repository, mock(AwsDurableInputDecoder.class), new com.fasterxml.jackson.databind.ObjectMapper(),
            Duration.ofSeconds(1), Duration.ofHours(1), telemetry);

        private AttributionFixture() {
            var original = registration(identity(1, 2)).checkpoint();
            assertThat(repository.claimGeneration(original)).isEqualTo(AwsDurableGenerationAdmission.CREATED);
            assertThat(repository.claimGeneration(new AwsDurableDriverCheckpoint(original.execution(), 2)))
                .isEqualTo(AwsDurableGenerationAdmission.CREATED);
            assertThat(repository.isCurrentGeneration("tenant-a", "execution-a", 1)).isFalse();
            assertThat(repository.isCurrentGeneration("tenant-a", "execution-a", 2)).isTrue();
            repository.recordCallbackAccepted(binding(identity(1, 2)), 4_000);
            events.clear();
        }

        private List<Map<String, AttributeValue>> rowsOf(String type) {
            return dynamo.rows.values().stream().filter(row -> type.equals(text(row, "record_type"))).toList();
        }

        private Map<String, Map<String, AttributeValue>> nonTimingRows() {
            var snapshot = new HashMap<String, Map<String, AttributeValue>>();
            dynamo.rows.forEach((key, row) -> {
                if (!List.of("CALLBACK_ACCEPTED", "DRIVER_RESUMED", "CALLBACK_LATENCY").contains(text(row, "record_type"))) {
                    snapshot.put(key, row);
                }
            });
            return Map.copyOf(snapshot);
        }
    }

    private static AwsDurableAwaitIdentity identity(long generation, long occurrence) {
        return new AwsDurableAwaitIdentity("tenant-a", "execution-a", "interaction-a", "correlation-a", generation, occurrence);
    }

    private static AwsDurableCallbackBinding binding(AwsDurableAwaitIdentity identity) {
        return new AwsDurableCallbackBinding(identity, "execution-name", "arn:secret-provider-execution", "callback-secret",
            AwsDurableBindingStatus.OPEN, 1_000, 100_000);
    }

    private static AwsDurableCallbackRegistration registration(AwsDurableAwaitIdentity identity) {
        return new AwsDurableCallbackRegistration(new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint(
            identity.tenantId(), identity.executionId(), "pipeline-a", "contract-a", "release-a"), identity.generation()),
            "execution-name", "arn:secret-provider-execution", "callback-secret", 1_000, 100_000,
            java.util.Optional.of(identity));
    }

    private static Map<String, AttributeValue> bindingItem(AwsDurableCallbackBinding binding) {
        var identity = binding.awaitIdentity();
        return Map.ofEntries(
            Map.entry("pk", s(binding.partitionKey())), Map.entry("sk", s(binding.sortKey())),
            Map.entry("record_type", s("BINDING")), Map.entry("tenant_id", s(identity.tenantId())),
            Map.entry("execution_id", s(identity.executionId())), Map.entry("interaction_id", s(identity.interactionId())),
            Map.entry("correlation_id", s(identity.correlationId())), Map.entry("generation", n(identity.generation())),
            Map.entry("occurrence", n(identity.occurrence())), Map.entry("provider_execution_name", s(binding.providerExecutionName())),
            Map.entry("provider_execution_arn", s(binding.providerExecutionArn())), Map.entry("provider_callback_id", s(binding.providerCallbackId())),
            Map.entry("binding_status", s(binding.status().name())), Map.entry("created_at_epoch_ms", n(binding.createdAtEpochMs())),
            Map.entry("expires_at_epoch_s", n(binding.expiresAtEpochS())));
    }

    private static Map<String, AttributeValue> registrationItem(AwsDurableCallbackRegistration registration) {
        var identity = registration.awaitIdentity().orElseThrow();
        return Map.ofEntries(
            Map.entry("pk", s(registration.partitionKey())), Map.entry("sk", s(registration.sortKey())),
            Map.entry("record_type", s("REGISTRATION")), Map.entry("tenant_id", s(identity.tenantId())),
            Map.entry("execution_id", s(identity.executionId())), Map.entry("pipeline_id", s("pipeline-a")),
            Map.entry("contract_version", s("contract-a")), Map.entry("release_version", s("release-a")),
            Map.entry("generation", n(identity.generation())), Map.entry("provider_execution_name", s(registration.providerExecutionName())),
            Map.entry("provider_execution_arn", s(registration.providerExecutionArn())), Map.entry("provider_callback_id", s(registration.providerCallbackId())),
            Map.entry("created_at_epoch_ms", n(registration.createdAtEpochMs())), Map.entry("expires_at_epoch_s", n(registration.expiresAtEpochS())),
            Map.entry("interaction_id", s(identity.interactionId())), Map.entry("correlation_id", s(identity.correlationId())),
            Map.entry("occurrence", n(identity.occurrence())), Map.entry("schema_version", n(3)));
    }

    private static String text(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        return value == null ? "" : value.s() != null ? value.s() : value.n();
    }

    private static AttributeValue s(String value) { return AttributeValue.builder().s(value).build(); }
    private static AttributeValue n(long value) { return AttributeValue.builder().n(Long.toString(value)).build(); }

    private static final class InMemoryDynamo {
        private final DynamoDbClient client = mock(DynamoDbClient.class);
        private final Map<String, Map<String, AttributeValue>> rows = new ConcurrentHashMap<>();

        private InMemoryDynamo(AwsDurableCallbackBinding binding, AwsDurableCallbackRegistration registration) {
            putSeed(bindingItem(binding));
            putSeed(registrationItem(registration));
            when(client.getItem(any(GetItemRequest.class))).thenAnswer(invocation -> {
                GetItemRequest request = invocation.getArgument(0);
                return GetItemResponse.builder().item(rows.getOrDefault(key(request.key()), Map.of())).build();
            });
            when(client.putItem(any(PutItemRequest.class))).thenAnswer(invocation -> {
                PutItemRequest request = invocation.getArgument(0);
                String key = key(request.item());
                if (rows.putIfAbsent(key, Map.copyOf(request.item())) != null) {
                    throw ConditionalCheckFailedException.builder().message("immutable row exists").build();
                }
                return PutItemResponse.builder().build();
            });
            when(client.transactWriteItems(any(TransactWriteItemsRequest.class))).thenAnswer(invocation -> {
                TransactWriteItemsRequest request = invocation.getArgument(0);
                synchronized (rows) {
                    for (var operation : request.transactItems()) {
                        if (operation.put() != null && rows.containsKey(key(operation.put().item()))) {
                            throw TransactionCanceledException.builder().message("immutable generation row exists").build();
                        }
                        if (operation.conditionCheck() != null) {
                            var check = operation.conditionCheck();
                            var existing = rows.getOrDefault(key(check.key()), Map.of());
                            if (!text(existing, "pipeline_id").equals(check.expressionAttributeValues().get(":pipeline").s())
                                || !text(existing, "contract_version").equals(check.expressionAttributeValues().get(":contract").s())
                                || !text(existing, "release_version").equals(check.expressionAttributeValues().get(":release").s())) {
                                throw TransactionCanceledException.builder().message("predecessor identity absent/conflicting").build();
                            }
                        }
                    }
                    request.transactItems().stream().filter(operation -> operation.put() != null)
                        .forEach(operation -> putSeed(operation.put().item()));
                }
                return TransactWriteItemsResponse.builder().build();
            });
        }

        private void putSeed(Map<String, AttributeValue> item) { rows.put(key(item), item); }

        private static String key(Map<String, AttributeValue> item) {
            return text(item, "pk") + "\u0000" + text(item, "sk");
        }
    }
}

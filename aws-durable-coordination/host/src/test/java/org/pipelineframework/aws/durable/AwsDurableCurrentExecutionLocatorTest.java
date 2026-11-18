package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableRecoveryLocator;
import org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

class AwsDurableCurrentExecutionLocatorTest {
    private static final ObjectMapper JSON = new ObjectMapper().registerModule(new Jdk8Module());
    private static final AwsDurableExecutionCheckpoint PIN = new AwsDurableExecutionCheckpoint(
        "tenant", "execution", "pipeline", "contract", "release");

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 10})
    void noClaimOrUnregisteredCurrentLocatorRemainsAbsent(long latest) {
        var fixture = new Fixture(latest);
        if (latest > 1) fixture.locators.put(latest - 1, recoveryItem(latest - 1));

        assertThat(fixture.repository.findCurrentRecovery(PIN)).isEmpty();

        if (latest > 1) assertThat(fixture.reads).noneMatch(request -> request.key().get("sk").s()
            .equals(AwsDurableRecoveryLocator.sortKey(latest - 1)));
        if (latest == 0) assertThat(fixture.reads).hasSize(1);
        fixture.verifyReadOnly();
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 9, 10, 11, 99, 100, Long.MAX_VALUE - 1, Long.MAX_VALUE})
    void observesNumericLatestAcrossDecimalBoundariesAndTheEntireLongDomain(long latest) {
        var fixture = new Fixture(latest);
        fixture.locators.put(latest, recoveryItem(latest));

        assertThat(fixture.repository.findCurrentRecovery(PIN)).contains(locator(latest));

        assertThat(fixture.reads.size()).isLessThanOrEqualTo(129);
        fixture.verifyReadOnly();
    }

    @Test
    void missingIntermediateClaimCannotReauthoriseAnOlderCompleteLocator() {
        var fixture = new Fixture(10);
        fixture.claimOverrides.put(4L, Map.of());
        fixture.locators.put(3L, recoveryItem(3));
        fixture.locators.put(10L, recoveryItem(10));

        assertThat(fixture.repository.findCurrentRecovery(PIN)).isEmpty();

        fixture.verifyReadOnly();
    }

    @Test
    void successorWinningAfterCandidateSelectionReturnsPending() {
        var fixture = new Fixture(9);
        fixture.locators.put(9L, recoveryItem(9));
        fixture.onRecoveryRead = () -> fixture.latest = 10;

        assertThat(fixture.repository.findCurrentRecovery(PIN)).isEmpty();

        assertThat(fixture.reads).anyMatch(request -> request.key().get("sk").s().equals("SUCCESSOR#9"));
        fixture.verifyReadOnly();
    }

    @ParameterizedTest
    @ValueSource(strings = {"pk", "sk", "record_type", "schema_version", "pipeline_id", "contract_version", "release_version"})
    void deniesForeignOrMalformedObservedClaims(String field) {
        var fixture = new Fixture(4);
        var malformed = new HashMap<>(claimItem(2));
        malformed.put(field, field.equals("schema_version") ? AttributeValue.fromN("1") : AttributeValue.fromS("foreign"));
        fixture.claimOverrides.put(2L, malformed);

        assertThatThrownBy(() -> fixture.repository.findCurrentRecovery(PIN)).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Generation claim differs");

        fixture.verifyReadOnly();
    }

    @ParameterizedTest
    @ValueSource(strings = {"pk", "sk", "record_type", "tenant_id", "execution_id", "pipeline_id", "contract_version", "release_version", "generation", "provider_execution_arn"})
    void deniesForeignOrMalformedRetainedLocators(String field) {
        var fixture = new Fixture(2);
        var malformed = new HashMap<>(recoveryItem(2));
        if (field.equals("provider_execution_arn")) malformed.remove(field);
        else malformed.put(field, field.equals("generation") ? AttributeValue.fromN("1") : AttributeValue.fromS("foreign"));
        fixture.locators.put(2L, malformed);

        assertThatThrownBy(() -> fixture.repository.findCurrentRecovery(PIN)).isInstanceOf(IllegalStateException.class);

        fixture.verifyReadOnly();
    }

    @ParameterizedTest
    @ValueSource(strings = {"tenant", "execution", "pipeline", "contract", "release"})
    void semanticAuthorityDeniesForeignPinsBeforeProviderStorageIsRead(String field) {
        var fixture = new Fixture(2);
        var controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getExecutionSemanticCheckpoint("tenant", "execution")).thenReturn(Uni.createFrom().item(
            new ExecutionSemanticCheckpoint(field.equals("tenant") ? "foreign" : "tenant",
                field.equals("execution") ? "foreign" : "execution", field.equals("pipeline") ? "foreign" : "pipeline",
                field.equals("contract") ? "foreign" : "contract", field.equals("release") ? "foreign" : "release",
                ExecutionStatus.SUCCEEDED)));

        assertThatThrownBy(() -> fixture.actions(controlPlane).invoke(AwsDurableActionRequest.currentExecutionLocator(PIN)))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("requested Release pin");

        verify(controlPlane).getExecutionSemanticCheckpoint("tenant", "execution");
        verifyNoMoreInteractions(controlPlane);
        verifyNoInteractions(fixture.dynamo);
    }

    @ParameterizedTest
    @ValueSource(strings = {"tenantId", "executionId", "pipelineId", "contractVersion", "releaseVersion"})
    void missingRequiredWirePinsAreRejectedBeforeAnyRead(String missing) {
        var fixture = new Fixture(2);
        var controlPlane = mock(PipelineControlPlane.class);
        ObjectNode wire = JSON.valueToTree(AwsDurableActionRequest.currentExecutionLocator(PIN));
        wire.remove(missing);

        assertThatThrownBy(() -> fixture.actions(controlPlane).invoke(JSON.treeToValue(wire, AwsDurableActionRequest.class)))
            .hasStackTraceContaining(missing);

        verifyNoInteractions(controlPlane, fixture.dynamo);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void existingLambdaWireReturnsCurrentLocatorOrPendingWithoutAClientGeneration(boolean registered) throws Exception {
        var fixture = new Fixture(10);
        fixture.locators.put(9L, recoveryItem(9));
        if (registered) fixture.locators.put(10L, recoveryItem(10));
        var controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getExecutionSemanticCheckpoint("tenant", "execution")).thenReturn(Uni.createFrom().item(
            new ExecutionSemanticCheckpoint("tenant", "execution", "pipeline", "contract", "release", ExecutionStatus.SUCCEEDED)));
        var actions = fixture.actions(controlPlane);
        var lambda = mock(LambdaClient.class);
        when(lambda.invoke(any(InvokeRequest.class))).thenAnswer(call -> {
            InvokeRequest invocation = call.getArgument(0);
            ObjectNode wire = (ObjectNode) JSON.readTree(invocation.payload().asUtf8String());
            assertThat(wire.path("operation").asText()).isEqualTo("READ_CURRENT_EXECUTION_LOCATOR");
            wire.put("generation", 777); // This operation derives generation from retained authority.
            var decoded = JSON.treeToValue(wire, AwsDurableActionRequest.class);
            return InvokeResponse.builder().statusCode(200)
                .payload(SdkBytes.fromUtf8String(JSON.writeValueAsString(actions.invoke(decoded)))).build();
        });

        var response = new LambdaAwsDurableActionInvoker(lambda, "action:7")
            .invoke(AwsDurableActionRequest.currentExecutionLocator(PIN));

        if (registered) assertThat(JSON.readValue(response.resultJson().orElseThrow(), AwsDurableRecoveryLocator.class))
            .isEqualTo(locator(10));
        else assertThat(response.resultJson()).contains("null");
        verify(controlPlane).getExecutionSemanticCheckpoint("tenant", "execution");
        verifyNoMoreInteractions(controlPlane);
        fixture.verifyReadOnly();
    }

    private static final class Fixture {
        final DynamoDbClient dynamo = mock(DynamoDbClient.class);
        final AwsDurableCallbackBindingRepository repository = new AwsDurableCallbackBindingRepository(dynamo, "bindings");
        final List<GetItemRequest> reads = new ArrayList<>();
        final Map<Long, Map<String, AttributeValue>> locators = new HashMap<>();
        final Map<Long, Map<String, AttributeValue>> claimOverrides = new HashMap<>();
        long latest;
        Runnable onRecoveryRead = () -> {};

        Fixture(long latest) {
            this.latest = latest;
            when(dynamo.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
                GetItemRequest request = call.getArgument(0);
                reads.add(request);
                assertThat(request.tableName()).isEqualTo("bindings");
                assertThat(request.consistentRead()).isTrue();
                assertThat(request.key()).containsEntry("pk", AttributeValue.fromS("tenant#execution"));
                String sort = request.key().get("sk").s();
                Map<String, AttributeValue> item;
                if (sort.startsWith("CLAIM#")) {
                    long generation = Long.parseLong(sort.substring("CLAIM#".length()));
                    assertThat(generation).isPositive();
                    item = claimOverrides.getOrDefault(generation, generation <= this.latest ? claimItem(generation) : Map.of());
                } else if (sort.startsWith("SUCCESSOR#")) {
                    long generation = Long.parseLong(sort.substring("SUCCESSOR#".length()));
                    item = generation < this.latest ? Map.of("generation", AttributeValue.fromN(Long.toString(generation + 1))) : Map.of();
                } else {
                    assertThat(sort).startsWith("RECOVERY#");
                    long generation = Long.parseLong(sort.substring("RECOVERY#".length()));
                    item = locators.getOrDefault(generation, Map.of());
                    onRecoveryRead.run();
                }
                return GetItemResponse.builder().item(item).build();
            });
        }

        AwsDurableControlPlaneActions actions(PipelineControlPlane controlPlane) {
            return new AwsDurableControlPlaneActions(controlPlane, repository, mock(AwsDurableInputDecoder.class), JSON,
                Duration.ofSeconds(5), Duration.ofDays(1));
        }

        void verifyReadOnly() {
            verify(dynamo, times(reads.size())).getItem(any(GetItemRequest.class));
            verifyNoMoreInteractions(dynamo);
        }
    }

    private static Map<String, AttributeValue> claimItem(long generation) {
        return Map.of("pk", AttributeValue.fromS("tenant#execution"), "sk", AttributeValue.fromS("CLAIM#" + generation),
            "record_type", AttributeValue.fromS("GENERATION_CLAIM"), "schema_version", AttributeValue.fromN("2"),
            "pipeline_id", AttributeValue.fromS("pipeline"), "contract_version", AttributeValue.fromS("contract"),
            "release_version", AttributeValue.fromS("release"));
    }

    private static Map<String, AttributeValue> recoveryItem(long generation) {
        return Map.ofEntries(Map.entry("pk", AttributeValue.fromS("tenant#execution")),
            Map.entry("sk", AttributeValue.fromS(AwsDurableRecoveryLocator.sortKey(generation))),
            Map.entry("record_type", AttributeValue.fromS("RECOVERY")), Map.entry("tenant_id", AttributeValue.fromS("tenant")),
            Map.entry("execution_id", AttributeValue.fromS("execution")), Map.entry("pipeline_id", AttributeValue.fromS("pipeline")),
            Map.entry("contract_version", AttributeValue.fromS("contract")), Map.entry("release_version", AttributeValue.fromS("release")),
            Map.entry("generation", AttributeValue.fromN(Long.toString(generation))),
            Map.entry("provider_execution_name", AttributeValue.fromS("retained-name-" + generation)),
            Map.entry("provider_execution_arn", AttributeValue.fromS("arn:retained:" + generation)));
    }

    private static AwsDurableRecoveryLocator locator(long generation) {
        return new AwsDurableRecoveryLocator(new AwsDurableDriverCheckpoint(PIN, generation),
            "retained-name-" + generation, "arn:retained:" + generation);
    }
}

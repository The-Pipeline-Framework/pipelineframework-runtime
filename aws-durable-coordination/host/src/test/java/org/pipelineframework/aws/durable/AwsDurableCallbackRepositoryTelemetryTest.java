package org.pipelineframework.aws.durable;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.aws.durable.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Scripted SDK results exercise repository classification, not DynamoDB transaction enforcement. */
class AwsDurableCallbackRepositoryTelemetryTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void bindingReplayAndConflictingCallbackStayDistinctEvenWhenTelemetryFails(boolean sinkFails) throws Exception {
        var messages = new ArrayList<String>();
        var dynamo = scriptedWritesAndReads();
        var repository = repository(dynamo, messages, sinkFails);
        assertEquals(AwsDurableGenerationAdmission.CREATED, repository.claimGeneration(checkpoint()));
        assertTrue(repository.bind(binding("private-callback", 100)));
        var cancellation = cancelFutureWrites(dynamo);
        assertFalse(repository.bind(binding("private-callback", 200)));
        var conflict = assertThrows(IllegalStateException.class, () -> repository.bind(binding("different-private-callback", 100)));
        assertSame(cancellation, conflict.getCause());
        if (!sinkFails) assertEquals(List.of("CREATED", "REPLAYED", "CONFLICT"), outcomes(messages, "BINDING"));
        assertPrivate(messages);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void registrationReplayAndConflictingCallbackStayDistinctEvenWhenTelemetryFails(boolean sinkFails) throws Exception {
        var messages = new ArrayList<String>();
        var dynamo = scriptedWritesAndReads();
        var repository = repository(dynamo, messages, sinkFails);
        assertTrue(repository.register(registration("private-callback", 100)));
        var cancellation = cancelFutureWrites(dynamo);
        assertFalse(repository.register(registration("private-callback", 200)));
        var conflict = assertThrows(IllegalStateException.class, () -> repository.register(registration("different-private-callback", 100)));
        assertSame(cancellation, conflict.getCause());
        if (!sinkFails) assertEquals(List.of("CREATED", "REPLAYED", "CONFLICT"), outcomes(messages, "REGISTRATION"));
        assertPrivate(messages);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void staleFenceRejectionStaysAnExceptionEvenWhenTelemetryFails(boolean sinkFails) throws Exception {
        var messages = new ArrayList<String>();
        var dynamo = mock(DynamoDbClient.class);
        cancelFutureWrites(dynamo);
        when(dynamo.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            GetItemRequest request = call.getArgument(0);
            return GetItemResponse.builder().item("SUCCESSOR#1".equals(request.key().get("sk").s())
                ? Map.of("generation", AttributeValue.fromN("2")) : Map.of()).build();
        });
        assertThrows(AwsDurableStaleGenerationException.class,
            () -> repository(dynamo, messages, sinkFails).bind(binding("private-callback", 100)));
        if (!sinkFails) assertEquals(List.of("STALE_REJECTED"), outcomes(messages, "GENERATION"));
        assertPrivate(messages);
    }

    private AwsDurableCallbackBindingRepository repository(DynamoDbClient dynamo, List<String> messages, boolean sinkFails) {
        return new AwsDurableCallbackBindingRepository(dynamo, "bindings", new AwsDurableHostTelemetry(Clock.systemUTC(), message -> {
            if (sinkFails) throw new IllegalStateException("telemetry unavailable");
            messages.add(message);
        }));
    }

    private DynamoDbClient scriptedWritesAndReads() {
        var dynamo = mock(DynamoDbClient.class);
        var rows = new HashMap<Map<String, AttributeValue>, Map<String, AttributeValue>>();
        when(dynamo.transactWriteItems(any(TransactWriteItemsRequest.class))).thenAnswer(call -> {
            TransactWriteItemsRequest request = call.getArgument(0);
            request.transactItems().stream().filter(item -> item.put() != null).forEach(item -> {
                var row = item.put().item(); rows.put(Map.of("pk", row.get("pk"), "sk", row.get("sk")), row);
            });
            return TransactWriteItemsResponse.builder().build();
        });
        when(dynamo.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            GetItemRequest request = call.getArgument(0);
            return GetItemResponse.builder().item(rows.getOrDefault(request.key(), Map.of())).build();
        });
        return dynamo;
    }

    private TransactionCanceledException cancelFutureWrites(DynamoDbClient dynamo) {
        var cancellation = TransactionCanceledException.builder().message("scripted cancellation").build();
        when(dynamo.transactWriteItems(any(TransactWriteItemsRequest.class))).thenThrow(cancellation);
        return cancellation;
    }

    private List<String> outcomes(List<String> messages, String stage) throws Exception {
        var outcomes = new ArrayList<String>(); var mapper = new ObjectMapper();
        for (String message : messages) {
            var event = mapper.readTree(message);
            if (stage.equals(event.path("Stage").asText())) outcomes.add(event.path("Outcome").asText());
        }
        return outcomes;
    }

    private void assertPrivate(List<String> messages) {
        for (String message : messages) {
            assertFalse(message.contains("private-"));
            assertFalse(message.contains("callback"));
        }
    }

    private AwsDurableDriverCheckpoint checkpoint() {
        return new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint("private-tenant", "private-execution", "pipeline", "contract", "release"), 1);
    }

    private AwsDurableCallbackRegistration registration(String callback, long created) {
        return new AwsDurableCallbackRegistration(checkpoint(), "private-provider", "private-provider-arn", callback, created, 1000);
    }

    private AwsDurableCallbackBinding binding(String callback, long created) {
        return new AwsDurableCallbackBinding(new AwsDurableAwaitIdentity("private-tenant", "private-execution", "private-interaction", "private-correlation", 1),
            "private-provider", "private-provider-arn", callback, AwsDurableBindingStatus.OPEN, created, 1000);
    }
}

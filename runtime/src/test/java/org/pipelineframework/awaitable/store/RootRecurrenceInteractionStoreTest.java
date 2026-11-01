package org.pipelineframework.awaitable.store;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.invocation.PipelineInvocationPath;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RootRecurrenceInteractionStoreTest {
    @Test
    void dynamoStoresAndReloadsInvocationOccurrencesWithoutProcessState() {
        var client = mock(DynamoDbClient.class);
        var config = mock(PipelineOrchestratorConfig.class);
        var dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.awaitInteractionTable()).thenReturn("interactions");
        when(dynamo.awaitInteractionKeyTable()).thenReturn("keys");
        when(dynamo.awaitCompletionLifecycleTable()).thenReturn("lifecycle");
        when(dynamo.region()).thenReturn(Optional.empty());
        when(dynamo.endpointOverride()).thenReturn(Optional.empty());
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
            .thenReturn(TransactWriteItemsResponse.builder().build());
        var path = new PipelineInvocationPath(List.of(new PipelineInvocationPath.Frame("$root", "$recur", 2)));
        long now = System.currentTimeMillis();
        var command = new AwaitCreateCommand("tenant", "execution", "Review", 4,
            String.class.getName(), "cause", "idempotency", "correlation", "approved prompt", "owner", "owners",
            "interaction-api", "legacy-unit", 0, now, now + 60_000, now / 1000 + 3600)
            .withInvocationPath(Optional.of(path));
        var first = new DynamoAwaitInteractionStore(client, config).createOrGet(command).await().indefinitely().record();
        var writes = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client, atLeastOnce()).transactWriteItems(writes.capture());
        Map<String, AttributeValue> item = writes.getAllValues().stream().flatMap(write -> write.transactItems().stream())
            .filter(write -> write.put() != null && "interactions".equals(write.put().tableName()))
            .map(write -> write.put().item()).findFirst().orElseThrow();
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());
        var restarted = new DynamoAwaitInteractionStore(client, config);
        var restored = restarted.get("tenant", first.interactionId()).await().indefinitely().orElseThrow();
        assertEquals(Optional.of(path), restored.invocationPath());
        assertEquals("approved prompt", restored.requestPayload());
    }
}

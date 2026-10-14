package org.pipelineframework.awaitable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitDurablePayloadResolver;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionCursor;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

class DynamoAwaitInteractionStoreTest {

    @Test
    void boundedUnitPagePreservesEmptyFilteredContinuationWithoutFetchingNextPage() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        Map<String, AttributeValue> key = Map.of(
            "tenant_id", avS("tenant-a"), "interaction_id", avS("interaction-100"),
            "query_unit_key", avS(scoped("tenant-a", "unit-1")), "query_unit_sort", avS("0000000100#cause#interaction-100"));
        when(client.query(any(QueryRequest.class))).thenReturn(
            QueryResponse.builder().lastEvaluatedKey(key).build(),
            QueryResponse.builder().items(item("tenant-a", "interaction-101", "unit-1", 101,
                AwaitInteractionStatus.DISPATCHED, 20_000L, "alice", "finance")).build());

        var first = store.findByUnitPage("tenant-a", "unit-1", 100, Optional.empty()).await().indefinitely();
        assertTrue(first.interactions().isEmpty());
        assertTrue(first.nextCursor().isPresent());
        verify(client, times(1)).query(any(QueryRequest.class));
        var second = store.findByUnitPage("tenant-a", "unit-1", 100, first.nextCursor()).await().indefinitely();
        assertEquals("interaction-101", second.interactions().getFirst().interactionId());
        assertTrue(second.nextCursor().isEmpty());
        ArgumentCaptor<QueryRequest> queries = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client, times(2)).query(queries.capture());
        assertEquals(100, queries.getAllValues().getFirst().limit());
        assertEquals(key, queries.getAllValues().getLast().exclusiveStartKey());
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void unitPageRejectsForeignCursorAndUnboundedLimitsBeforeStorage() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> store.findByUnitPage(
            "tenant-a", "unit-1", 100, Optional.of(new AwaitInteractionCursor("tenant-b", "unit-1", "opaque")))
            .await().indefinitely());
        for (int limit : new int[] {0, -1, 101, Integer.MAX_VALUE}) {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> store.findByUnitPage(
                "tenant-a", "unit-1", limit, Optional.empty()).await().indefinitely());
        }
        verifyNoInteractions(client);
    }

    @Test
    void reconstructsAwaitInteractionsThroughExecutionIndex() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        Map<String, AttributeValue> interaction = item(
            "tenant-a", "interaction-1", "unit-1", 0,
            AwaitInteractionStatus.COMPLETED, 20_000L, "alice", "finance");
        when(client.query(any(QueryRequest.class)))
            .thenReturn(QueryResponse.builder().items(interaction).build());

        var records = store.findByExecution("tenant-a", "execution-interaction-1", 10).await().indefinitely();

        assertEquals(1, records.size());
        ArgumentCaptor<QueryRequest> query = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client).query(query.capture());
        assertEquals("await-interaction-by-execution", query.getValue().indexName());
        assertEquals(scoped("tenant-a", "execution-interaction-1"), query.getValue().expressionAttributeValues()
            .get(":executionKey").s());
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void reconstructsAwaitInteractionsAcrossFilteredQueryPages() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        Map<String, AttributeValue> pageKey = Map.of(
            "tenant_id", avS("tenant-a"),
            "interaction_id", avS("page-1"));
        when(client.query(any(QueryRequest.class))).thenReturn(
            QueryResponse.builder()
                .items(item("tenant-b", "interaction-other", "unit-1", 0,
                    AwaitInteractionStatus.COMPLETED, 20_000L, "alice", "finance"))
                .lastEvaluatedKey(pageKey)
                .build(),
            QueryResponse.builder()
                .items(item("tenant-a", "interaction-1", "unit-1", 0,
                    AwaitInteractionStatus.COMPLETED, 20_000L, "alice", "finance"))
                .build());

        var records = store.findByExecution("tenant-a", "execution-interaction-1", 1)
            .await().indefinitely();

        assertEquals(1, records.size());
        ArgumentCaptor<QueryRequest> queries = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client, times(2)).query(queries.capture());
        assertEquals(pageKey, queries.getAllValues().get(1).exclusiveStartKey());
    }

    @Test
    void queriesContinuationProjectionWithoutScanningInteractions() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        Map<String, AttributeValue> work = new java.util.HashMap<>(item(
            "tenant-a", "interaction-1", "unit-1", 0,
            AwaitInteractionStatus.COMPLETED, 20_000L, "alice", "finance"));
        work.put("query_continuation_key", avS("ready"));
        work.put("query_continuation_due_epoch_ms", AttributeValue.builder().n("1000").build());
        work.put("continuation_attempt", AttributeValue.builder().n("2").build());
        when(client.query(any(QueryRequest.class)))
            .thenReturn(QueryResponse.builder().items(work).build());

        var commands = store.findDueItemContinuations(2_000L, 10).await().indefinitely();

        assertEquals(1, commands.size());
        assertEquals(2, commands.getFirst().attempt());
        ArgumentCaptor<QueryRequest> query = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client).query(query.capture());
        assertEquals("await-interaction-continuation-work", query.getValue().indexName());
        assertTrue(query.getValue().keyConditionExpression().contains("#continuationDue <= :now"));
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void callbackObservationRetainsDeadlineIndexesUntilCommandSettlement() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        var dispatching = new java.util.HashMap<>(item("tenant-a", "interaction-1", "unit-1", 0,
            AwaitInteractionStatus.DISPATCHING, 20_000L, "alice", "finance"));
        dispatching.put("ttl_epoch_s", avN(0));
        dispatching.put("transport_metadata_json", avS("{\"completionMode\":\"CONNECTOR_CALLBACK\"}"));
        var observed = new java.util.HashMap<>(dispatching);
        observed.put("status", avS("COMPLETION_OBSERVED"));
        observed.put("response_payload_json", avS("\"done\""));
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(dispatching).build(),
                GetItemResponse.builder().item(observed).build());
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
            .thenReturn(TransactWriteItemsResponse.builder().build());

        var completion = store.complete(new AwaitCompletionCommand("tenant-a", "interaction-1", "",
            "completion-1", "done", "provider", 2_000L)).await().indefinitely();
        assertEquals(AwaitInteractionStatus.COMPLETION_OBSERVED, completion.record().status());
        ArgumentCaptor<TransactWriteItemsRequest> completionWrites = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(completionWrites.capture());
        assertEquals(3, completionWrites.getValue().transactItems().size());
        var parentMutation = completionWrites.getValue().transactItems().getFirst().update();
        assertFalse(parentMutation.updateExpression().contains("REMOVE"));
        assertTrue(parentMutation.conditionExpression().contains("#version = :expected"));
        assertTrue(parentMutation.conditionExpression().contains("#ttl = :noExpiry"));
        assertEquals("0", parentMutation.expressionAttributeValues().get(":noExpiry").n());
        assertEquals("COMPLETION_OBSERVED", parentMutation.expressionAttributeValues().get(":status").s());
        assertTrue(completionWrites.getValue().transactItems().subList(1, 3).stream()
            .allMatch(write -> write.put().conditionExpression().contains("attribute_not_exists")));
        ArgumentCaptor<GetItemRequest> completionReads = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(client, times(2)).getItem(completionReads.capture());
        assertTrue(completionReads.getAllValues().stream().allMatch(GetItemRequest::consistentRead));
        assertTrue(completionReads.getAllValues().stream().allMatch(request ->
            "interaction-1".equals(request.key().get("interaction_id").s())));

        var completed = new java.util.HashMap<>(observed);
        completed.put("status", avS("COMPLETED"));
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(completed).build());
        var settled = store.settleCommandDispatch(completion.record(),
            org.pipelineframework.awaitable.CommandDispatchSettlement.AMBIGUOUS, 3_000L).await().indefinitely();
        assertEquals(AwaitInteractionStatus.COMPLETED, settled.orElseThrow().status());
        ArgumentCaptor<UpdateItemRequest> updates = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(updates.capture());
        var settlement = updates.getValue();
        assertTrue(settlement.updateExpression().contains("REMOVE"));
        assertTrue(settlement.conditionExpression().contains("#ttl = :noExpiry"));
        assertEquals("0", settlement.expressionAttributeValues().get(":noExpiry").n());
        assertEquals("COMPLETION_OBSERVED", settlement.expressionAttributeValues().get(":requiredStatus").s());
        assertEquals("\"done\"", settlement.expressionAttributeValues().get(":response").s());
    }

    @Test
    void firstCreateUsesItsConditionalTransactionWithoutAnIdempotencyLookupRead() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
            .thenReturn(TransactWriteItemsResponse.builder().build());

        store.createOrGet(command("tenant-a", "execution-1", "review", "idem-1", "corr-1", "unit-1", 0,
            "alice", "finance", 20_000L)).await().indefinitely();

        verify(client).transactWriteItems(any(TransactWriteItemsRequest.class));
        verify(client, never()).getItem(any(GetItemRequest.class));
    }

    @Test
    void importWithoutExpiryOmitsTtlFromInteractionAndLookupRecordsAndReadsAsZero() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        AwaitInteractionRecord imported = new AwaitInteractionRecord("tenant-a", "execution-a", "review", 0,
            String.class.getName(), "interaction-imported", "correlation-imported", null, "idempotency-imported", 0,
            AwaitInteractionStatus.WAITING, Map.of("request", "value"), null, "unit-a", null, null,
            "alice", "finance", "local", Map.of(), 20_000L, 1_000L, 1_000L, 0L, String.class.getName());
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
            .thenReturn(TransactWriteItemsResponse.builder().build());

        store.importRecord(imported).await().indefinitely();

        ArgumentCaptor<TransactWriteItemsRequest> writes = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(writes.capture());
        assertEquals(3, writes.getValue().transactItems().size());
        assertTrue(writes.getValue().transactItems().stream().allMatch(write ->
            !write.put().item().containsKey("ttl_epoch_s")));

        Map<String, AttributeValue> readable = item("tenant-a", "interaction-imported", "unit-a", null,
            AwaitInteractionStatus.WAITING, 20_000L, "alice", "finance");
        readable.remove("ttl_epoch_s");
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(readable).build());
        assertEquals(0L, store.get("tenant-a", "interaction-imported").await().indefinitely().orElseThrow().ttlEpochS());
    }

    @Test
    void writesAndRestoresTypedDurableAwaitPayloads() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        AwaitDurablePayloadResolver payloads = mock(AwaitDurablePayloadResolver.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig(), payloads);
        String typedRequest = "{\"canonicalTypeId\":\"Request\",\"typeExpressionFingerprint\":\"request\",\"catalogFingerprint\":\"catalog\",\"encoding\":\"json\",\"encodingVersion\":1,\"payload\":\"e30=\"}";
        String typedResponse = "{\"canonicalTypeId\":\"Decision\",\"typeExpressionFingerprint\":\"response\",\"catalogFingerprint\":\"catalog\",\"encoding\":\"json\",\"encodingVersion\":1,\"payload\":\"e30=\"}";
        Decision decision = new Decision("approved");
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        when(payloads.supportsTypedPayloads(any())).thenReturn(true);
        when(payloads.encode(any(), eq(AwaitDurablePayloadResolver.Slot.REQUEST), any())).thenReturn(typedRequest);

        store.createOrGet(command("tenant-a", "execution-1", "review", "idem-1", "corr-1", "unit-1", 0, "alice", "finance", 20_000L))
            .await().indefinitely();

        ArgumentCaptor<TransactWriteItemsRequest> create = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(create.capture());
        assertEquals(typedRequest, create.getValue().transactItems().getFirst().put().item().get("request_payload_json").s());

        Map<String, AttributeValue> waiting = item("tenant-a", "interaction-1", "unit-1", 0,
            AwaitInteractionStatus.WAITING, 20_000L, "alice", "finance");
        Map<String, AttributeValue> completed = new java.util.HashMap<>(waiting);
        completed.put("status", avS(AwaitInteractionStatus.COMPLETED.name()));
        completed.put("response_payload_json", avS(typedResponse));
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(waiting).build(),
            GetItemResponse.builder().item(completed).build());
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
            .thenReturn(TransactWriteItemsResponse.builder().build());
        when(payloads.encode(any(), eq(AwaitDurablePayloadResolver.Slot.RESPONSE), any())).thenReturn(typedResponse);
        when(payloads.decode(any(), eq(AwaitDurablePayloadResolver.Slot.RESPONSE), eq(typedResponse))).thenReturn(decision);

        var completion = store.complete(new AwaitCompletionCommand(
            "tenant-a", "interaction-1", null, "completion-1", decision, "provider", 2_000L)).await().indefinitely();

        ArgumentCaptor<TransactWriteItemsRequest> updates = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client, times(2)).transactWriteItems(updates.capture());
        var completionWrite = updates.getAllValues().getLast();
        assertEquals(3, completionWrite.transactItems().size());
        var parentMutation = completionWrite.transactItems().getFirst().update();
        assertTrue(completionWrite.transactItems().get(1).put().conditionExpression().contains("attribute_not_exists"));
        assertEquals(typedResponse, parentMutation.expressionAttributeValues().get(":response").s());
        assertEquals("ready", parentMutation.expressionAttributeValues().get(":continuationKey").s());
        assertTrue(parentMutation.updateExpression().contains("#continuationDue = :continuationDue"));
        assertEquals(decision, completion.record().responsePayload());
        verify(payloads).encode(any(), eq(AwaitDurablePayloadResolver.Slot.REQUEST), any());
        verify(payloads).encode(any(), eq(AwaitDurablePayloadResolver.Slot.RESPONSE), eq(decision));
        verify(payloads).decode(any(), eq(AwaitDurablePayloadResolver.Slot.RESPONSE), eq(typedResponse));
    }

    private record Decision(String result) {
    }

    @Test
    void createOrGetWritesUnitPendingAndDeadlineQueryKeys() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());

        var result = store.createOrGet(command(
                "tenant-a",
                "execution-1",
                "review",
                "idem-1",
                "corr-1",
                "unit-1",
                2,
                "alice",
                "finance",
                20_000L))
            .await().indefinitely();

        assertFalse(result.duplicate());
        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(captor.capture());
        Map<String, AttributeValue> item = captor.getValue().transactItems().getFirst().put().item();
        assertEquals(scoped("tenant-a", "unit-1"), item.get("query_unit_key").s());
        assertTrue(item.get("query_unit_sort").s().startsWith("0000000002#cause-1#"));
        assertEquals("tenant-a", item.get("query_pending_tenant_key").s());
        assertEquals(scoped("tenant-a", "alice"), item.get("query_pending_assignee_key").s());
        assertEquals(scoped("tenant-a", "finance"), item.get("query_pending_group_key").s());
        assertEquals(scoped("tenant-a", "review"), item.get("query_pending_step_key").s());
        assertTrue(item.get("query_pending_deadline_sort").s().startsWith("0000000000000020000#"));
        assertEquals("active", item.get("query_deadline_key").s());
        assertTrue(item.get("query_deadline_sort").s().startsWith("0000000000000020000#tenant-a#"));
    }

    @Test
    void findByUnitQueriesUnitIndexAndKeepsTenantsAndUnitsIsolated() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(List.of(
                item("tenant-a", "interaction-2", "unit-1", 2, AwaitInteractionStatus.WAITING, 30_000L, "alice", "finance"),
                item("tenant-b", "interaction-other-tenant", "unit-1", 1, AwaitInteractionStatus.WAITING, 10_000L, "alice", "finance"),
                item("tenant-a", "interaction-other-unit", "unit-2", 1, AwaitInteractionStatus.WAITING, 10_000L, "alice", "finance"),
                item("tenant-a", "interaction-0", "unit-1", 0, AwaitInteractionStatus.WAITING, 10_000L, "alice", "finance")))
            .build());

        List<AwaitInteractionRecord> records = store.findByUnit("tenant-a", "unit-1").await().indefinitely();

        assertEquals(List.of(0, 2), records.stream().map(AwaitInteractionRecord::itemIndex).toList());
        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client).query(captor.capture());
        QueryRequest request = captor.getValue();
        assertEquals("await-interaction-by-unit", request.indexName());
        assertEquals("#unitKey = :unitKey", request.keyConditionExpression());
        assertEquals("query_unit_key", request.expressionAttributeNames().get("#unitKey"));
        assertEquals(scoped("tenant-a", "unit-1"), request.expressionAttributeValues().get(":unitKey").s());
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void queryPendingUsesNarrowestAssigneeIndexAndFiltersRemainingFields() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(List.of(
                item("tenant-a", "interaction-1", "unit-1", null, AwaitInteractionStatus.WAITING, 10_000L, "alice", "finance"),
                item("tenant-a", "interaction-other-group", "unit-2", null, AwaitInteractionStatus.WAITING, 11_000L, "alice", "legal"),
                item("tenant-a", "interaction-terminal", "unit-3", null, AwaitInteractionStatus.COMPLETED, 9_000L, "alice", "finance")))
            .build());

        List<AwaitInteractionRecord> records = store.queryPending("tenant-a", "alice", "finance", "review", 10)
            .await().indefinitely();

        assertEquals(1, records.size());
        assertEquals("interaction-1", records.getFirst().interactionId());
        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client).query(captor.capture());
        QueryRequest request = captor.getValue();
        assertEquals("await-interaction-pending-by-assignee", request.indexName());
        assertEquals("#pendingKey = :pendingKey", request.keyConditionExpression());
        assertEquals("query_pending_assignee_key", request.expressionAttributeNames().get("#pendingKey"));
        assertEquals(scoped("tenant-a", "alice"), request.expressionAttributeValues().get(":pendingKey").s());
        assertFalse(request.filterExpression().contains("#assignee = :assignee"));
        assertTrue(request.filterExpression().contains("#group = :group"));
        assertTrue(request.filterExpression().contains("#stepId = :stepId"));
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void queryPendingTreatsBlankFiltersAsNotProvided() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(List.of(item("tenant-a", "interaction-1", "unit-1", null, AwaitInteractionStatus.WAITING, 10_000L, "alice", "finance")))
            .build());

        List<AwaitInteractionRecord> records = store.queryPending("tenant-a", " ", "", "\t", 10)
            .await().indefinitely();

        assertEquals(1, records.size());
        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client).query(captor.capture());
        QueryRequest request = captor.getValue();
        assertEquals("await-interaction-pending-by-tenant", request.indexName());
        assertEquals("query_pending_tenant_key", request.expressionAttributeNames().get("#pendingKey"));
        assertEquals("tenant-a", request.expressionAttributeValues().get(":pendingKey").s());
        assertFalse(request.filterExpression().contains("#assignee = :assignee"));
        assertFalse(request.filterExpression().contains("#group = :group"));
        assertFalse(request.filterExpression().contains("#stepId = :stepId"));
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void findTimedOutQueriesDeadlineIndexAcrossTenantsAndRespectsLimit() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(List.of(
                item("tenant-b", "interaction-b", "unit-b", null, AwaitInteractionStatus.WAITING, 1_500L, "bob", "legal"),
                item("tenant-a", "interaction-a", "unit-a", null, AwaitInteractionStatus.WAITING, 1_000L, "alice", "finance"),
                item("tenant-c", "interaction-future", "unit-c", null, AwaitInteractionStatus.WAITING, 4_000L, "carol", "ops"),
                item("tenant-d", "interaction-terminal", "unit-d", null, AwaitInteractionStatus.CANCELLED, 500L, "dan", "ops")))
            .build());

        List<AwaitInteractionRecord> records = store.findTimedOut(2_000L, 2).await().indefinitely();

        assertEquals(List.of("interaction-a", "interaction-b"), records.stream().map(AwaitInteractionRecord::interactionId).toList());
        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client).query(captor.capture());
        QueryRequest request = captor.getValue();
        assertEquals("await-interaction-pending-by-deadline", request.indexName());
        assertEquals("#deadlineKey = :deadlineKey AND #deadlineSort <= :deadlineCutoff", request.keyConditionExpression());
        assertEquals("active", request.expressionAttributeValues().get(":deadlineKey").s());
        assertEquals("0000000000000002000~", request.expressionAttributeValues().get(":deadlineCutoff").s());
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void activeTimeoutAdmissionConsumesOneBoundedPageEvenWhenNoLiveInteractionIsReturned() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.query(any(QueryRequest.class))).thenReturn(
            QueryResponse.builder().lastEvaluatedKey(Map.of("tenant_id", avS("tenant-a"),
                "interaction_id", avS("expired-page-boundary"))).build(),
            QueryResponse.builder().items(item("tenant-a", "retained-active", "unit-a", 0,
                AwaitInteractionStatus.WAITING, 70_000L, "alice", "finance")).build());
        assertTrue(store.findTimedOut(100_000L, 100).await().indefinitely().isEmpty());
        verify(client, times(1)).query(any(QueryRequest.class));
        assertEquals("retained-active", store.findTimedOut(100_000L, 100).await().indefinitely().getFirst().interactionId());
        ArgumentCaptor<QueryRequest> queries = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client, times(2)).query(queries.capture());
        assertTrue(queries.getAllValues().stream().allMatch(query -> query.limit() <= 100));
        assertTrue(queries.getAllValues().getLast().exclusiveStartKey().isEmpty());
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void findTimedOutUsesMetadataOnlyViewWithoutTypedPayloadResolution() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        AwaitDurablePayloadResolver payloads = mock(AwaitDurablePayloadResolver.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig(), payloads);
        Map<String, AttributeValue> timedOut = item(
            "tenant-a", "interaction-a", "unit-a", 7, AwaitInteractionStatus.WAITING, 1_000L, "alice", "finance");
        timedOut.put("request_payload_json", avS("{\"canonicalTypeId\":\"Request\",\"typeExpressionFingerprint\":\"request\",\"catalogFingerprint\":\"catalog\",\"encoding\":\"json\",\"encodingVersion\":1,\"payload\":\"e30=\"}"));
        timedOut.put("response_payload_json", avS("{\"canonicalTypeId\":\"Decision\",\"typeExpressionFingerprint\":\"response\",\"catalogFingerprint\":\"catalog\",\"encoding\":\"json\",\"encodingVersion\":1,\"payload\":\"e30=\"}"));
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(List.of(timedOut)).build());

        AwaitInteractionRecord record = store.findTimedOut(2_000L, 1).await().indefinitely().getFirst();

        assertEquals("interaction-a", record.interactionId());
        assertEquals(7, record.itemIndex());
        assertEquals(AwaitInteractionStatus.WAITING, record.status());
        assertEquals(null, record.requestPayload());
        assertEquals(null, record.responsePayload());
        verifyNoInteractions(payloads);
    }

    @Test
    void terminalTransitionRemovesActivePendingAndDeadlineQueryKeys() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        Map<String, AttributeValue> timedOut = item("tenant-a", "interaction-1", "unit-1", null,
            AwaitInteractionStatus.TIMED_OUT, 10_000L, "alice", "finance");
        timedOut.put("ttl_epoch_s", avN(0));
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(timedOut).build());

        var updated = store.markTimedOut("tenant-a", "interaction-1", 0L, 11_000L).await().indefinitely();

        assertTrue(updated.isPresent());
        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(captor.capture());
        String updateExpression = captor.getValue().updateExpression();
        assertTrue(updateExpression.contains("REMOVE"));
        assertTrue(updateExpression.contains("#pendingTenantKey"));
        assertTrue(updateExpression.contains("#pendingAssigneeKey"));
        assertTrue(updateExpression.contains("#pendingGroupKey"));
        assertTrue(updateExpression.contains("#pendingStepKey"));
        assertTrue(updateExpression.contains("#pendingDeadlineSort"));
        assertTrue(updateExpression.contains("#deadlineKey"));
        assertTrue(captor.getValue().conditionExpression().contains("#ttl = :noExpiry"));
        assertEquals("0", captor.getValue().expressionAttributeValues().get(":noExpiry").n());
        assertTrue(updateExpression.substring(0, updateExpression.indexOf(" REMOVE ")).contains("#deadlineKey = :timeoutWork"));
        assertEquals("TIMEOUT_CONSEQUENCES", captor.getValue().expressionAttributeValues().get(":timeoutWork").s());
        assertFalse(updateExpression.substring(updateExpression.indexOf(" REMOVE ")).contains("#deadlineKey"));
        assertFalse(updateExpression.substring(updateExpression.indexOf(" REMOVE ")).contains("#deadlineSort"));
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void timeoutAndPendingConsequencesArePublishedByOneConditionalMutation() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        AwaitDurablePayloadResolver payloads = mock(AwaitDurablePayloadResolver.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig(), payloads);
        var retained = item("tenant-a", "atomic-timeout", "unit-a", 7, AwaitInteractionStatus.TIMED_OUT, 70_000L, "alice", "finance");
        retained.put("version", avN(1));
        when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().attributes(retained).build());
        var timedOut = store.markTimedOut("tenant-a", "atomic-timeout", 0L, 71_000L).await().indefinitely().orElseThrow();
        assertEquals(AwaitInteractionStatus.TIMED_OUT, timedOut.status());
        assertEquals(1L, timedOut.version());
        ArgumentCaptor<UpdateItemRequest> updates = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(updates.capture());
        var update = updates.getValue();
        assertTrue(update.updateExpression().contains("#status = :status"));
        assertTrue(update.updateExpression().contains("#timeoutPending = :timeoutPending"));
        assertTrue(update.updateExpression().contains("#deadlineKey = :timeoutWork"));
        assertTrue(update.updateExpression().contains("#deadlineSort = :timeoutSort"));
        assertEquals("timeout_consequences_pending", update.expressionAttributeNames().get("#timeoutPending"));
        assertEquals(Boolean.TRUE, update.expressionAttributeValues().get(":timeoutPending").bool());
        assertEquals("TIMEOUT_CONSEQUENCES", update.expressionAttributeValues().get(":timeoutWork").s());
        assertTrue(update.conditionExpression().contains("#version = :expected"));
        assertTrue(update.conditionExpression().contains("#status <> :terminalCOMPLETED"));
        assertTrue(update.conditionExpression().contains("#status <> :terminalTIMED_OUT"));
        verifyNoInteractions(payloads);
    }

    @Test
    void boundedExpiredTimeoutPageIsCleanedWithoutFetchingASecondPageOrDecodingPayloads() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        AwaitDurablePayloadResolver payloads = mock(AwaitDurablePayloadResolver.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig(), payloads);
        var expired = item("tenant-a", "expired-timeout", "unit-a", 0, AwaitInteractionStatus.TIMED_OUT, 70_000L, "alice", "finance");
        expired.put("ttl_epoch_s", avN(90L));
        var retained = item("tenant-a", "retained-timeout", "unit-a", 1, AwaitInteractionStatus.TIMED_OUT, 70_000L, "alice", "finance");
        when(client.query(any(QueryRequest.class))).thenReturn(
            QueryResponse.builder().items(expired).lastEvaluatedKey(Map.of("tenant_id", avS("tenant-a"),
                "interaction_id", avS("expired-timeout"))).build(),
            QueryResponse.builder().items(retained).build());
        when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().build());
        assertTrue(store.findDueTimeoutConsequences(100_000L, 100).await().indefinitely().isEmpty());
        verify(client, times(1)).query(any(QueryRequest.class));
        var next = store.findDueTimeoutConsequences(100_000L, 100).await().indefinitely();
        assertEquals("retained-timeout", next.getFirst().interactionId());
        ArgumentCaptor<QueryRequest> queries = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client, times(2)).query(queries.capture());
        assertTrue(queries.getAllValues().stream().allMatch(query -> query.limit() == 100
            && query.indexName().equals("await-interaction-pending-by-deadline")));
        assertTrue(queries.getAllValues().getLast().exclusiveStartKey().isEmpty());
        ArgumentCaptor<UpdateItemRequest> cleanup = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(cleanup.capture());
        assertEquals("expired-timeout", cleanup.getValue().key().get("interaction_id").s());
        assertTrue(cleanup.getValue().conditionExpression().contains("#ttl <= :expired"));
        assertEquals("REMOVE #pending, #deadlineKey, #deadlineSort", cleanup.getValue().updateExpression());
        verify(client, never()).scan(any(ScanRequest.class));
        verifyNoInteractions(payloads);
        for (int limit : new int[] {0, -1, 101, Integer.MAX_VALUE}) {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> store.findDueTimeoutConsequences(100_000L, limit));
        }
        verify(client, times(2)).query(any(QueryRequest.class));
    }

    @Test
    void acknowledgementConditionsScopeAndVersionAndRemovesOnlyTheProjection() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        var retained = item("tenant-a", "ack-timeout", "unit-a", 0, AwaitInteractionStatus.TIMED_OUT, 70_000L, "alice", "finance");
        retained.put("version", avN(3));
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(retained).build());
        when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().build());
        var timedOut = store.findDueTimeoutConsequences(100_000L, 1).await().indefinitely().getFirst();
        store.completeTimeoutConsequences(timedOut).await().indefinitely();
        store.completeTimeoutConsequences(timedOut).await().indefinitely();
        ArgumentCaptor<UpdateItemRequest> acknowledgements = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client, times(2)).updateItem(acknowledgements.capture());
        assertEquals(acknowledgements.getAllValues().getFirst(), acknowledgements.getAllValues().getLast());
        var acknowledgement = acknowledgements.getValue();
        assertEquals("tenant-a", acknowledgement.key().get("tenant_id").s());
        assertEquals("ack-timeout", acknowledgement.key().get("interaction_id").s());
        assertEquals("3", acknowledgement.expressionAttributeValues().get(":version").n());
        assertEquals("execution-ack-timeout", acknowledgement.expressionAttributeValues().get(":execution").s());
        assertEquals("unit-a", acknowledgement.expressionAttributeValues().get(":unit").s());
        assertEquals("TIMED_OUT", acknowledgement.expressionAttributeValues().get(":status").s());
        assertEquals("#version = :version AND #status = :status AND #execution = :execution AND #unit = :unit",
            acknowledgement.conditionExpression());
        assertEquals("REMOVE #pending, #deadlineKey, #deadlineSort", acknowledgement.updateExpression());
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("mismatched identity/version").build());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
            () -> store.completeTimeoutConsequences(timedOut).await().indefinitely());
    }

    @Test
    void markTimedOutUsesMetadataOnlyResultWithoutTypedPayloadResolution() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        AwaitDurablePayloadResolver payloads = mock(AwaitDurablePayloadResolver.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig(), payloads);
        Map<String, AttributeValue> timedOut = item(
            "tenant-a", "interaction-a", "unit-a", 7, AwaitInteractionStatus.TIMED_OUT, 1_000L, "alice", "finance");
        timedOut.put("request_payload_json", avS("{\"canonicalTypeId\":\"Request\",\"typeExpressionFingerprint\":\"request\",\"catalogFingerprint\":\"catalog\",\"encoding\":\"json\",\"encodingVersion\":1,\"payload\":\"e30=\"}"));
        timedOut.put("response_payload_json", avS("{\"canonicalTypeId\":\"Decision\",\"typeExpressionFingerprint\":\"response\",\"catalogFingerprint\":\"catalog\",\"encoding\":\"json\",\"encodingVersion\":1,\"payload\":\"e30=\"}"));
        when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().attributes(timedOut).build());

        AwaitInteractionRecord record = store.markTimedOut("tenant-a", "interaction-a", 0L, 2_000L)
            .await().indefinitely().orElseThrow();

        assertEquals(AwaitInteractionStatus.TIMED_OUT, record.status());
        assertEquals(null, record.requestPayload());
        assertEquals(null, record.responsePayload());
        verifyNoInteractions(payloads);
    }

    @Test
    void completeTreatsCompletedRecordAfterOccRaceAsDuplicate() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.getItem(any(GetItemRequest.class))).thenReturn(
            GetItemResponse.builder()
                .item(item("tenant-a", "interaction-1", "unit-1", null, AwaitInteractionStatus.WAITING, 10_000L, "alice", "finance"))
                .build(),
            GetItemResponse.builder()
                .item(item("tenant-a", "interaction-1", "unit-1", null, AwaitInteractionStatus.COMPLETED, 10_000L, "alice", "finance"))
                .build(),
            GetItemResponse.builder().build());
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("conditional transaction failed").build());

        var result = store.complete(new AwaitCompletionCommand(
                "tenant-a",
                "interaction-1",
                null,
                "completion-1",
                Map.of("decision", "approved"),
                "provider",
                2_000L))
            .await().indefinitely();

        assertTrue(result.duplicate());
        assertEquals(AwaitInteractionStatus.COMPLETED, result.record().status());
        verify(client).transactWriteItems(any(TransactWriteItemsRequest.class));
        verify(client, times(3)).getItem(any(GetItemRequest.class));
    }

    @Test
    void duplicateItemCompletionRepairsMissingContinuationProjection() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        Map<String, AttributeValue> completed = item(
            "tenant-a", "interaction-1", "unit-1", 0,
            AwaitInteractionStatus.COMPLETED, 10_000L, "alice", "finance");
        when(client.getItem(any(GetItemRequest.class))).thenAnswer(invocation -> {
            GetItemRequest request = invocation.getArgument(0);
            return "tpf_await_completion_lifecycle".equals(request.tableName())
                ? GetItemResponse.builder().build()
                : GetItemResponse.builder().item(completed).build();
        });
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().build());

        var result = store.complete(new AwaitCompletionCommand(
                "tenant-a", "interaction-1", null, "completion-1", "approved", "provider", 2_000L))
            .await().indefinitely();

        assertTrue(result.duplicate());
        ArgumentCaptor<UpdateItemRequest> repair = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(repair.capture());
        assertTrue(repair.getValue().conditionExpression().contains("attribute_not_exists(#continuationCompleted)"));
        assertEquals("ready", repair.getValue().expressionAttributeValues().get(":ready").s());
        assertEquals("1", repair.getValue().expressionAttributeValues().get(":attempt").n());
    }

    @Test
    void completedContinuationRecordsMarkerWhenLegacyProjectionIsMissing() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().build());
        var command = new org.pipelineframework.orchestrator.AwaitItemContinuationCommand(
            "tenant-a", "execution-1", "unit-1", "interaction-1", 0, 1, 2_000L);

        store.completeItemContinuation(command).await().indefinitely();

        ArgumentCaptor<UpdateItemRequest> update = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(update.capture());
        assertTrue(update.getValue().conditionExpression().contains("attribute_not_exists(#continuationKey)"));
        assertTrue(update.getValue().updateExpression().contains("#continuationCompleted = :completed"));
        assertTrue(update.getValue().expressionAttributeValues().get(":completed").bool());
    }

    private static AwaitCreateCommand command(
        String tenantId,
        String executionId,
        String stepId,
        String idempotencyKey,
        String correlationId,
        String unitId,
        Integer itemIndex,
        String assignee,
        String group,
        long deadlineEpochMs) {
        return new AwaitCreateCommand(
            tenantId,
            executionId,
            stepId,
            0,
            String.class.getName(),
            "cause-1",
            idempotencyKey,
            correlationId,
            Map.of("orderId", "o-1"),
            assignee,
            group,
            "interaction-api",
            unitId,
            itemIndex,
            1_000L,
            deadlineEpochMs,
            Long.MAX_VALUE);
    }

    private static Map<String, AttributeValue> item(
        String tenantId,
        String interactionId,
        String unitId,
        Integer itemIndex,
        AwaitInteractionStatus status,
        long deadlineEpochMs,
        String assignee,
        String group) {
        Map<String, AttributeValue> item = new java.util.HashMap<>();
        item.put("tenant_id", avS(tenantId));
        item.put("interaction_id", avS(interactionId));
        item.put("execution_id", avS("execution-" + interactionId));
        item.put("step_id", avS("review"));
        item.put("step_index", avN(0));
        item.put("output_type", avS(String.class.getName()));
        item.put("correlation_id", avS("correlation-" + interactionId));
        item.put("causation_id", avS("cause-" + interactionId));
        item.put("idempotency_key", avS("idem-" + interactionId));
        item.put("version", avN(0));
        item.put("status", avS(status.name()));
        item.put("unit_id", avS(unitId));
        item.put("assignee", avS(assignee));
        item.put("group_name", avS(group));
        item.put("transport_type", avS("interaction-api"));
        item.put("deadline_epoch_ms", avN(deadlineEpochMs));
        item.put("created_at_epoch_ms", avN(1_000L + deadlineEpochMs));
        item.put("updated_at_epoch_ms", avN(2_000L + deadlineEpochMs));
        item.put("ttl_epoch_s", avN(Long.MAX_VALUE));
        if (itemIndex != null) {
            item.put("item_index", avN(itemIndex));
        }
        return item;
    }

    private static PipelineOrchestratorConfig mockConfig() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.awaitCompletionLifecycleTable()).thenReturn("tpf_await_completion_lifecycle");
        when(dynamo.awaitInteractionTable()).thenReturn("tpf_await_interaction");
        when(dynamo.awaitInteractionKeyTable()).thenReturn("tpf_await_interaction_key");
        when(dynamo.region()).thenReturn(Optional.empty());
        when(dynamo.endpointOverride()).thenReturn(Optional.empty());
        return config;
    }

    private static String scoped(String left, String right) {
        return left.length() + ":" + left + ":" + right.length() + ":" + right;
    }

    private static AttributeValue avS(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue avN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }
}

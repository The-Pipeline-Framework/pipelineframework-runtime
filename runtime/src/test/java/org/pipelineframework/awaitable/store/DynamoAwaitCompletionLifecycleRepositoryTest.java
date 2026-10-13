package org.pipelineframework.awaitable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitCompletionAdmission;
import org.pipelineframework.awaitable.AwaitCompletionCursor;
import org.pipelineframework.awaitable.AwaitReleaseDisposition;
import org.pipelineframework.awaitable.AwaitUnitReleaseAcknowledgement;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;

class DynamoAwaitCompletionLifecycleRepositoryTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-12T12:00:00Z"), ZoneOffset.UTC);
    private static final String TABLE = "await-lifecycle";

    @Test
    void admissionWritesAreConditionalAndKeepFactAndProjectionDistinct() {
        DynamoAwaitCompletionLifecycleRepository repository = repository(mock(DynamoDbClient.class));
        AwaitCompletionAdmission admission = admission(100_000L, 0L);

        List<TransactWriteItem> writes = repository.admissionWrites(admission);

        assertEquals(2, writes.size());
        Map<String, AttributeValue> fact = put(writes.get(0)).item();
        Map<String, AttributeValue> open = put(writes.get(1)).item();
        assertEquals("AWAIT_COMPLETION_ADMISSION", fact.get("record_type").s());
        assertEquals("AWAIT_COMPLETION_OPEN", open.get("record_type").s());
        assertEquals("tenant-a", fact.get("tenant_id").s());
        assertEquals("execution-a", fact.get("execution_id").s());
        assertEquals("interaction-a", fact.get("interaction_id").s());
        assertEquals("100000", fact.get("first_admitted_at_epoch_ms").n());
        assertFalse(fact.containsKey("item_index"));
        assertFalse(fact.containsKey("ttl_epoch_s"));
        assertTrue(put(writes.get(0)).conditionExpression().contains("attribute_not_exists"));
        assertTrue(put(writes.get(1)).conditionExpression().contains("attribute_not_exists"));
        assertTrue(fact.get("pk").s().startsWith("ADMISSION#"));
        assertTrue(open.get("pk").s().startsWith("OPEN#"));
        assertFalse(open.containsKey("ttl_epoch_s"));
    }

    @Test
    void releaseWriteRetainsTerminalDispositionAsAnImmutableFact() {
        DynamoAwaitCompletionLifecycleRepository repository = repository(mock(DynamoDbClient.class));
        AwaitUnitReleaseAcknowledgement acknowledgement = acknowledgement(
            AwaitReleaseDisposition.TERMINAL_FAILED, 42_000L, 0L);

        Put put = put(repository.releaseWrite(acknowledgement));

        assertEquals("AWAIT_UNIT_RELEASE_ACKNOWLEDGEMENT", put.item().get("record_type").s());
        assertEquals("TERMINAL_FAILED", put.item().get("disposition").s());
        assertEquals("42000", put.item().get("acknowledged_at_epoch_ms").n());
        assertTrue(put.conditionExpression().contains("attribute_not_exists"));
    }

    @Test
    void releaseObservationConditionChecksTheWholeFactWithoutReplacingIt() {
        DynamoAwaitCompletionLifecycleRepository repository = repository(mock(DynamoDbClient.class));
        AwaitUnitReleaseAcknowledgement acknowledgement = acknowledgement(
            AwaitReleaseDisposition.CONTINUATION_QUEUED, 42_000L, 0L);

        var condition = repository.releaseObservationCondition(acknowledgement).conditionCheck();

        assertTrue(condition.conditionExpression().contains("#disposition = :disposition"));
        assertTrue(condition.conditionExpression().contains("#acknowledged = :acknowledged"));
        assertTrue(condition.conditionExpression().contains("attribute_not_exists(#ttl)"));
        assertEquals("CONTINUATION_QUEUED", condition.expressionAttributeValues().get(":disposition").s());
        assertFalse(condition.expressionAttributeValues().containsKey(":ttl"));
        assertEquals(2, condition.key().size());
    }

    @Test
    void readsAreStrongAndDecodeImmutableAcknowledgement() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitCompletionLifecycleRepository repository = repository(client);
        Map<String, AttributeValue> item = releaseItem(AwaitReleaseDisposition.CONTINUATION_QUEUED, 77_000L, 0L);
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());

        Optional<AwaitUnitReleaseAcknowledgement> found = repository.readRelease("tenant-a", "execution-a", "unit-a");

        assertTrue(found.isPresent());
        assertEquals(AwaitReleaseDisposition.CONTINUATION_QUEUED, found.orElseThrow().disposition());
        assertEquals(77_000L, found.orElseThrow().acknowledgedAtEpochMs());
        verify(client).getItem(org.mockito.ArgumentMatchers.<GetItemRequest>argThat(GetItemRequest::consistentRead));
    }

    @Test
    void expiredOpenFactIsVerifiedThenRetiredAndPageKeepsContinuation() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitCompletionLifecycleRepository repository = repository(client);
        long expiredTtl = CLOCK.instant().getEpochSecond() - 1;
        AwaitCompletionAdmission admission = admission(100_000L, expiredTtl);
        String openPartition = "OPEN#" + component("tenant-a");
        String openSort = "OPEN#" + String.format(java.util.Locale.ROOT, "%019d", admission.firstAdmittedAtEpochMs())
            + "#" + component(admission.interactionId());
        Map<String, AttributeValue> projection = Map.of(
            "pk", s(openPartition), "sk", s(openSort), "record_type", s("AWAIT_COMPLETION_OPEN"),
            "tenant_id", s("tenant-a"), "execution_id", s("execution-a"), "interaction_id", s("interaction-a"),
            "first_admitted_at_epoch_ms", n(admission.firstAdmittedAtEpochMs()), "ttl_epoch_s", n(expiredTtl));
        Map<String, AttributeValue> fact = admissionItem(admission);
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(projection).lastEvaluatedKey(Map.of("pk", s(openPartition), "sk", s(openSort))).build());
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(fact).build());

        var page = repository.findOpen("tenant-a", 10, Optional.empty());

        assertTrue(page.admissions().isEmpty());
        assertEquals(1, page.evaluatedEntryCount());
        assertTrue(page.nextCursor().isPresent());
        page.nextCursor().orElseThrow().requireScope("tenant-a");
        verify(client).query(org.mockito.ArgumentMatchers.<QueryRequest>argThat(request ->
            request.consistentRead() && request.limit() == 10 && request.indexName() == null));
        verify(client).deleteItem(any(DeleteItemRequest.class));
        verify(client, never()).scan(any(software.amazon.awssdk.services.dynamodb.model.ScanRequest.class));
    }

    @Test
    void foreignOrMalformedCursorFailsBeforeDynamoIo() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitCompletionLifecycleRepository repository = repository(client);

        assertThrows(IllegalArgumentException.class, () -> repository.findOpen(
            "tenant-a", 5, Optional.of(new AwaitCompletionCursor("tenant-b", "v1.invalid.invalid"))));
        assertThrows(IllegalArgumentException.class, () -> repository.findOpen(
            "tenant-a", 5, Optional.of(new AwaitCompletionCursor("tenant-a", "bad"))));
        verify(client, never()).query(any(QueryRequest.class));
    }

    @Test
    void repeatedNativeContinuationPositionFailsClosed() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitCompletionLifecycleRepository repository = repository(client);
        String partition = "OPEN#" + component("tenant-a");
        String sort = "OPEN#0000000000000010000#" + component("interaction-a");
        AwaitCompletionCursor cursor = new AwaitCompletionCursor("tenant-a",
            "v1." + component(partition) + "." + component(sort));
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .lastEvaluatedKey(Map.of("pk", s(partition), "sk", s(sort))).build());

        assertThrows(IllegalStateException.class, () -> repository.findOpen("tenant-a", 10, Optional.of(cursor)));
    }

    @Test
    void invalidBoundsFailBeforeDynamoIo() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitCompletionLifecycleRepository repository = repository(client);

        assertThrows(IllegalArgumentException.class, () -> repository.findOpen("tenant-a", 0, Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> repository.findOpen("tenant-a", 101, Optional.empty()));
        verify(client, never()).query(any(QueryRequest.class));
    }

    private static DynamoAwaitCompletionLifecycleRepository repository(DynamoDbClient client) {
        return new DynamoAwaitCompletionLifecycleRepository(client, TABLE, CLOCK);
    }

    private static AwaitCompletionAdmission admission(long admittedAt, long ttl) {
        return new AwaitCompletionAdmission(1, "tenant-a", "execution-a", "interaction-a", Optional.of("unit-a"),
            "await-step", 3, OptionalInt.empty(), Optional.empty(), admittedAt, ttl);
    }

    private static AwaitUnitReleaseAcknowledgement acknowledgement(
        AwaitReleaseDisposition disposition, long acknowledgedAt, long ttl
    ) {
        return new AwaitUnitReleaseAcknowledgement(1, "tenant-a", "execution-a", "unit-a", "pipeline-a",
            "contract-a", "release-a", 3, 9, disposition, acknowledgedAt, ttl);
    }

    private static Map<String, AttributeValue> admissionItem(AwaitCompletionAdmission admission) {
        Map<String, AttributeValue> item = new java.util.HashMap<>(Map.ofEntries(
            Map.entry("pk", s("ADMISSION#" + component(admission.tenantId()) + "#" + component(admission.interactionId()))),
            Map.entry("sk", s("FACT")), Map.entry("record_type", s("AWAIT_COMPLETION_ADMISSION")),
            Map.entry("schema_version", n(admission.schemaVersion())), Map.entry("tenant_id", s(admission.tenantId())),
            Map.entry("execution_id", s(admission.executionId())), Map.entry("interaction_id", s(admission.interactionId())),
            Map.entry("unit_id", s(admission.unitId().orElseThrow())), Map.entry("step_id", s(admission.stepId())),
            Map.entry("step_index", n(admission.stepIndex())),
            Map.entry("first_admitted_at_epoch_ms", n(admission.firstAdmittedAtEpochMs()))));
        if (admission.ttlEpochS() > 0) item.put("ttl_epoch_s", n(admission.ttlEpochS()));
        return Map.copyOf(item);
    }

    private static Map<String, AttributeValue> releaseItem(
        AwaitReleaseDisposition disposition, long acknowledgedAt, long ttl
    ) {
        AwaitUnitReleaseAcknowledgement acknowledgement = acknowledgement(disposition, acknowledgedAt, ttl);
        Map<String, AttributeValue> item = new java.util.HashMap<>(Map.ofEntries(
            Map.entry("pk", s("RELEASE#" + component(acknowledgement.tenantId()) + "#"
                + component(acknowledgement.executionId()))),
            Map.entry("sk", s("UNIT#" + component(acknowledgement.unitId()))),
            Map.entry("record_type", s("AWAIT_UNIT_RELEASE_ACKNOWLEDGEMENT")),
            Map.entry("schema_version", n(acknowledgement.schemaVersion())), Map.entry("tenant_id", s(acknowledgement.tenantId())),
            Map.entry("execution_id", s(acknowledgement.executionId())), Map.entry("unit_id", s(acknowledgement.unitId())),
            Map.entry("pipeline_id", s(acknowledgement.pipelineId())),
            Map.entry("contract_version", s(acknowledgement.contractVersion())),
            Map.entry("release_version", s(acknowledgement.releaseVersion())),
            Map.entry("step_index", n(acknowledgement.stepIndex())), Map.entry("parent_version", n(acknowledgement.parentVersion())),
            Map.entry("disposition", s(acknowledgement.disposition().name())),
            Map.entry("acknowledged_at_epoch_ms", n(acknowledgement.acknowledgedAtEpochMs()))));
        if (acknowledgement.ttlEpochS() > 0) item.put("ttl_epoch_s", n(acknowledgement.ttlEpochS()));
        return Map.copyOf(item);
    }

    private static Put put(TransactWriteItem item) {
        return item.put();
    }

    private static AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue n(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String component(String value) {
        return java.util.Base64.getUrlEncoder().withoutPadding()
            .encodeToString(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}

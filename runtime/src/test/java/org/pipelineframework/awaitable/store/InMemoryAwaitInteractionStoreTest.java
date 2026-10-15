package org.pipelineframework.awaitable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionNotFoundException;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitInteractionTerminalException;
import org.pipelineframework.orchestrator.AwaitItemContinuationCommand;

class InMemoryAwaitInteractionStoreTest {
    @Test
    void legacyConfiguredKeyStillDeduplicatesActiveInteractionAcrossExecutions() {
        var store = new InMemoryAwaitInteractionStore();
        var first = store.createOrGet(createCommand("review:business-key", 10_000L, 70_000L)).await().indefinitely().record();
        var duplicate = store.createOrGet(new AwaitCreateCommand("tenant", "execution-2", "review", 0,
            String.class.getName(), "cause-2", first.idempotencyKey(), "corr-execution-2", "different request",
            "alice", "finance", "local", "legacy-unit-2", null, 12_000L, 70_000L, Long.MAX_VALUE))
            .await().indefinitely();
        assertTrue(duplicate.duplicate());
        assertEquals(first, duplicate.record());
        assertTrue(store.findByCorrelation("tenant", "corr-execution-2").await().indefinitely().isEmpty());
    }

    @Test
    void qualifiedTerminalRetryReturnsExactInteractionWhileNewCauseCreatesFreshInteraction() {
        var store = new InMemoryAwaitInteractionStore();
        var command = occurrenceCommand("initial", "idem-initial", "corr-initial", 0);
        var first = store.createOrGet(command).await().indefinitely().record();
        var completed = store.complete(new AwaitCompletionCommand("tenant", first.interactionId(), null,
            "completion", "approved", "alice", 15_000L)).await().indefinitely().record();
        var replay = store.createOrGet(command).await().indefinitely();
        assertTrue(replay.duplicate());
        assertEquals(completed, replay.record());
        var fresh = store.createOrGet(occurrenceCommand("reissue", "idem-reissue", "corr-reissue", 0)).await().indefinitely();
        assertFalse(fresh.duplicate());
        assertNotEquals(completed.interactionId(), fresh.record().interactionId());
        assertNotEquals(completed.unitId(), fresh.record().unitId());
        assertEquals(completed, store.get("tenant", completed.interactionId()).await().indefinitely().orElseThrow());
    }

    @Test
    void qualifiedImportConflictsCannotReplaceInteractionOrLookupOwner() {
        var store = new InMemoryAwaitInteractionStore();
        var first = store.createOrGet(occurrenceCommand("initial", "idem-initial", "corr-initial", 0)).await().indefinitely().record();
        for (var conflict : java.util.List.of(
            copyIdentity(first, first.interactionId(), occurrenceCommand("reissue", "idem-initial", "corr-initial", 0).unitId(), 0, first.idempotencyKey(), first.correlationId()),
            copyIdentity(first, first.interactionId(), first.unitId(), 1, first.idempotencyKey(), first.correlationId()),
            copyIdentity(first, first.interactionId(), first.unitId(), 0, "foreign-idem", first.correlationId()),
            copyIdentity(first, first.interactionId(), first.unitId(), 0, first.idempotencyKey(), "foreign-corr"),
            copyIdentity(first, "foreign-interaction", first.unitId(), 0, first.idempotencyKey(), "foreign-corr"),
            copyIdentity(first, "foreign-interaction", first.unitId(), 0, "foreign-idem", first.correlationId()))) {
            assertThrows(IllegalStateException.class, () -> store.importRecord(conflict).await().indefinitely());
            assertEquals(first, store.get("tenant", first.interactionId()).await().indefinitely().orElseThrow());
            assertEquals(first, store.findByCorrelation("tenant", first.correlationId()).await().indefinitely().orElseThrow());
            assertTrue(store.get("tenant", "foreign-interaction").await().indefinitely().isEmpty());
        }
        assertThrows(IllegalStateException.class, () -> store.createOrGet(
            occurrenceCommand("reissue", "idem-initial", "corr-other", 0)).await().indefinitely());
        assertThrows(IllegalStateException.class, () -> store.createOrGet(
            occurrenceCommand("reissue", "idem-other", "corr-initial", 0)).await().indefinitely());
    }

    private AwaitCreateCommand occurrenceCommand(String cause, String idem, String correlation, int itemIndex) {
        String unit = new org.pipelineframework.awaitable.AwaitUnitOccurrence("tenant", "execution-1", "page", "review", 0, cause).unitId();
        return new AwaitCreateCommand("tenant", "execution-1", "review", 0, String.class.getName(), "causation", idem,
            correlation, "request", "alice", "finance", "local", unit, itemIndex, 10_000L, 70_000L, Long.MAX_VALUE);
    }

    private AwaitInteractionRecord copyIdentity(AwaitInteractionRecord original, String id, String unit, Integer index, String idem, String correlation) {
        return new AwaitInteractionRecord(original.tenantId(), original.executionId(), original.stepId(), original.stepIndex(),
            original.outputType(), id, correlation, original.causationId(), idem, original.version(), original.status(),
            original.requestPayload(), original.responsePayload(), unit, index, original.actor(), original.assignee(), original.group(),
            original.transportType(), original.transportMetadata(), original.deadlineEpochMs(), original.createdAtEpochMs(),
            original.updatedAtEpochMs(), original.ttlEpochS(), original.transportOutputType());
    }

    @Test
    void currentUnitPagingRetainsDistinctImportedMembersWhoseDelimitedKeysCollide() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        store.importRecord(importedItem("c", "a#b")).await().indefinitely();
        store.importRecord(importedItem("b#c", "a")).await().indefinitely();

        assertEquals(2, store.findByUnit("tenant", "unit-1").await().indefinitely().size());
        assertTrue(store.get("tenant", "c").await().indefinitely().isPresent());
        assertTrue(store.get("tenant", "b#c").await().indefinitely().isPresent());
        var ids = new java.util.ArrayList<String>();
        java.util.Optional<org.pipelineframework.awaitable.AwaitInteractionCursor> cursor = java.util.Optional.empty();
        for (int pageNumber = 0; pageNumber < 3; pageNumber++) {
            var page = store.findByUnitPage("tenant", "unit-1", 1, cursor).await().indefinitely();
            assertTrue(page.interactions().size() <= 1);
            ids.addAll(page.interactions().stream().map(AwaitInteractionRecord::interactionId).toList());
            cursor = page.nextCursor();
            if (cursor.isEmpty()) {
                break;
            }
        }
        assertTrue(cursor.isEmpty(), "Stable finite unit traversal must terminate");
        assertEquals(List.of("b#c", "c"), ids,
            "Both retained unit members must be enumerated exactly once in tuple order");
    }

    @Test
    void currentUnitPagingMatchesDeclaredTupleOrderAcrossCausationPrefixes() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        String parentId = "ffffffff-ffff-ffff-ffff-ffffffffffff";
        String childId = "00000000-0000-0000-0000-000000000000";
        store.importRecord(importedItem(parentId, "a")).await().indefinitely();
        store.importRecord(importedItem(childId, "a#b")).await().indefinitely();

        var first = store.findByUnitPage("tenant", "unit-1", 1, java.util.Optional.empty()).await().indefinitely();
        var second = store.findByUnitPage("tenant", "unit-1", 1, first.nextCursor()).await().indefinitely();

        assertEquals(List.of(parentId, childId), java.util.stream.Stream.concat(
            first.interactions().stream(), second.interactions().stream())
            .map(AwaitInteractionRecord::interactionId).toList());
        assertTrue(first.nextCursor().isPresent());
        assertTrue(second.nextCursor().isEmpty());
    }

    private AwaitInteractionRecord importedItem(String interactionId, String causationId) {
        var template = new InMemoryAwaitInteractionStore().createOrGet(
            itemCommand("template", "template-correlation", 0)).await().indefinitely().record();
        return new AwaitInteractionRecord(template.tenantId(), template.executionId(), template.stepId(),
            template.stepIndex(), template.outputType(), interactionId, "corr-" + interactionId,
            causationId, "idem-" + interactionId, template.version(), template.status(),
            template.requestPayload(), template.responsePayload(), template.unitId(), template.itemIndex(),
            template.actor(), template.assignee(), template.group(), template.transportType(),
            template.transportMetadata(), template.deadlineEpochMs(), template.createdAtEpochMs(),
            template.updatedAtEpochMs(), template.ttlEpochS(), template.transportOutputType());
    }

    @Test
    void currentUnitPagingFindsPendingAfterMoreThan100CompletedMembers() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        for (int index = 0; index < 103; index++) {
            var created = store.createOrGet(itemCommand("page-" + index, "corr-page-" + index, index))
                .await().indefinitely().record();
            if (index < 101) {
                store.complete(new AwaitCompletionCommand("tenant", created.interactionId(), null,
                    "completion-" + index, "approved", "provider", 12_000L)).await().indefinitely();
            }
        }
        var first = store.findByUnitPage("tenant", "unit-1", 100, java.util.Optional.empty()).await().indefinitely();
        assertEquals(100, first.interactions().size());
        assertTrue(first.interactions().stream().allMatch(record -> record.status().terminal()));
        assertTrue(first.nextCursor().isPresent());
        var second = store.findByUnitPage("tenant", "unit-1", 100, first.nextCursor()).await().indefinitely();
        assertEquals(3, second.interactions().size());
        assertEquals(101, second.interactions().stream().filter(record -> !record.status().terminal())
            .findFirst().orElseThrow().itemIndex());
        assertTrue(second.nextCursor().isEmpty());
        assertThrows(IllegalArgumentException.class, () -> store.findByUnitPage(
            "tenant", "other-unit", 100, first.nextCursor()).await().indefinitely());
        assertThrows(IllegalArgumentException.class, () -> store.findByUnitPage(
            "tenant", "unit-1", 101, java.util.Optional.empty()).await().indefinitely());
    }

    @Test
    void commandCompletionCasRetainsEvidenceAndRejectsStaleOrMutatedAuthority() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(new AwaitCreateCommand("tenant", "execution-1", "review", 0,
            String.class.getName(), String.class.getName(), "cause-1", "ack-idem", "ack-corr", "request",
            "alice", "finance", "interaction-api", "unit-1", null,
            Map.of("completionMode", "CONNECTOR_CALLBACK", "commandCompletionMode", "ACKNOWLEDGEMENT_REQUIRED",
                "commandAcknowledgementType", String.class.getName(),
                "tpf.await.completion.projector", "test.Projector"), 10_000L, 70_000L, Long.MAX_VALUE))
            .await().indefinitely().record();
        var dispatching = store.markDispatching("tenant", created.interactionId(), created.version(), 11_000L)
            .await().indefinitely().orElseThrow();
        var staged = new AwaitInteractionRecord(dispatching.tenantId(), dispatching.executionId(),
            dispatching.stepId(), dispatching.stepIndex(), dispatching.outputType(), dispatching.interactionId(),
            dispatching.correlationId(), dispatching.causationId(), dispatching.idempotencyKey(),
            dispatching.version() + 1, AwaitInteractionStatus.COMPLETION_OBSERVED, dispatching.requestPayload(),
            dispatching.responsePayload(), dispatching.unitId(), dispatching.itemIndex(), "provider",
            dispatching.assignee(), dispatching.group(), dispatching.transportType(), dispatching.transportMetadata(),
            dispatching.deadlineEpochMs(), dispatching.createdAtEpochMs(), 12_000L, dispatching.ttlEpochS(),
            dispatching.transportOutputType(), Optional.empty(), Optional.of("callback"));
        assertEquals(Optional.of("callback"), store.compareAndSetCommandCompletion(dispatching, staged)
            .await().indefinitely().orElseThrow().stagedCompletionPayload());
        assertTrue(store.compareAndSetCommandCompletion(dispatching, staged).await().indefinitely().isEmpty());
        assertEquals(Optional.of("callback"), store.get("tenant", created.interactionId())
            .await().indefinitely().orElseThrow().stagedCompletionPayload());
        var forged = new AwaitInteractionRecord(staged.tenantId(), "different-execution", staged.stepId(),
            staged.stepIndex(), staged.outputType(), staged.interactionId(), staged.correlationId(),
            staged.causationId(), staged.idempotencyKey(), staged.version() + 1, AwaitInteractionStatus.COMPLETED,
            staged.requestPayload(), "result", staged.unitId(), staged.itemIndex(), staged.actor(), staged.assignee(),
            staged.group(), staged.transportType(), staged.transportMetadata(), staged.deadlineEpochMs(),
            staged.createdAtEpochMs(), 13_000L, staged.ttlEpochS(), staged.transportOutputType(),
            Optional.of("ack"), staged.stagedCompletionPayload());
        assertThrows(IllegalArgumentException.class, () -> store.compareAndSetCommandCompletion(staged, forged)
            .await().indefinitely());
        var forgedExpected = new AwaitInteractionRecord(staged.tenantId(), "different-execution", staged.stepId(),
            staged.stepIndex(), staged.outputType(), staged.interactionId(), staged.correlationId(),
            staged.causationId(), staged.idempotencyKey(), staged.version(), staged.status(), staged.requestPayload(),
            staged.responsePayload(), staged.unitId(), staged.itemIndex(), staged.actor(), staged.assignee(),
            staged.group(), staged.transportType(), staged.transportMetadata(), staged.deadlineEpochMs(),
            staged.createdAtEpochMs(), staged.updatedAtEpochMs(), staged.ttlEpochS(), staged.transportOutputType(),
            staged.commandAcknowledgement(), staged.stagedCompletionPayload());
        var forgedMetadata = new java.util.HashMap<>(forgedExpected.transportMetadata());
        forgedMetadata.put("commandSettlement", "SUCCEEDED");
        var forgedNext = new AwaitInteractionRecord(forgedExpected.tenantId(), forgedExpected.executionId(),
            forgedExpected.stepId(), forgedExpected.stepIndex(), forgedExpected.outputType(),
            forgedExpected.interactionId(), forgedExpected.correlationId(), forgedExpected.causationId(),
            forgedExpected.idempotencyKey(), forgedExpected.version() + 1, AwaitInteractionStatus.COMPLETED,
            forgedExpected.requestPayload(), "result", forgedExpected.unitId(), forgedExpected.itemIndex(),
            forgedExpected.actor(), forgedExpected.assignee(), forgedExpected.group(), forgedExpected.transportType(),
            forgedMetadata, forgedExpected.deadlineEpochMs(), forgedExpected.createdAtEpochMs(), 13_000L,
            forgedExpected.ttlEpochS(), forgedExpected.transportOutputType(), Optional.of("ack"),
            forgedExpected.stagedCompletionPayload());
        assertTrue(forgedExpected.permitsCommandCompletionReplacement(forgedNext));
        assertThrows(IllegalArgumentException.class, () -> store.compareAndSetCommandCompletion(
            forgedExpected, forgedNext).await().indefinitely());
    }

    @Test
    void reconstructsCompletedInteractionsByExecutionIdentity() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-execution", 10_000L, 70_000L))
            .await().indefinitely();
        store.complete(new AwaitCompletionCommand(
            "tenant", created.record().interactionId(), null, "completion", "approved", "provider", 12_000L))
            .await().indefinitely();

        var records = store.findByExecution("tenant", created.record().executionId(), 10)
            .await().indefinitely();

        assertEquals(1, records.size());
        assertEquals(AwaitInteractionStatus.COMPLETED, records.getFirst().status());
    }

    @Test
    void itemCompletionAtomicallyProjectsRediscoverableContinuationWork() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(itemCommand("item-0", "corr-0", 0)).await().indefinitely();

        store.complete(new AwaitCompletionCommand(
            "tenant", created.record().interactionId(), null, "completion-0", "approved", "provider", 12_000L))
            .await().indefinitely();

        AwaitItemContinuationCommand projected = store.findDueItemContinuations(12_000L, 10)
            .await().indefinitely().getFirst();
        assertEquals(created.record().interactionId(), projected.interactionId());
        assertEquals(1, projected.attempt());

        store.rescheduleItemContinuation(projected, 2, 15_000L).await().indefinitely();
        assertTrue(store.findDueItemContinuations(14_999L, 10).await().indefinitely().isEmpty());
        AwaitItemContinuationCommand retried = store.findDueItemContinuations(15_000L, 10)
            .await().indefinitely().getFirst();
        assertEquals(2, retried.attempt());

        store.completeItemContinuation(retried).await().indefinitely();
        assertTrue(store.findDueItemContinuations(20_000L, 10).await().indefinitely().isEmpty());
    }

    @Test
    void createOrGetDeduplicatesActiveInteractionByIdempotencyKey() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCreateCommand command = createCommand("idem-1", 10_000L, 70_000L);

        var first = store.createOrGet(command).await().indefinitely();
        var duplicate = store.createOrGet(command).await().indefinitely();

        assertFalse(first.duplicate());
        assertTrue(duplicate.duplicate());
        assertEquals(first.record().interactionId(), duplicate.record().interactionId());
        assertEquals(AwaitInteractionStatus.WAITING, first.record().status());
    }

    @Test
    void markDispatchedUsesOptimisticVersion() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var stale = store.markDispatching(
            "tenant",
            created.record().interactionId(),
            99L,
            11_000L).await().indefinitely();
        var claimed = store.markDispatching(
            "tenant",
            created.record().interactionId(),
            created.record().version(),
            11_000L).await().indefinitely();
        var updated = store.markDispatched(
            "tenant",
            created.record().interactionId(),
            claimed.orElseThrow().version(),
            Map.of("messageId", "m-1"),
            12_000L).await().indefinitely();

        assertTrue(stale.isEmpty());
        assertTrue(claimed.isPresent());
        assertEquals(AwaitInteractionStatus.DISPATCHING, claimed.get().status());
        assertTrue(updated.isPresent());
        assertEquals(AwaitInteractionStatus.DISPATCHED, updated.get().status());
        assertEquals("m-1", updated.get().transportMetadata().get("messageId"));
    }

    @Test
    void markDispatchingPersistsRecoveryMetadata() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        store.markDispatching(
            "tenant",
            created.record().interactionId(),
            created.record().version(),
            Map.of("admissionReservationId", "reservation-1"),
            11_000L).await().indefinitely().orElseThrow();
        var reloaded = store.get("tenant", created.record().interactionId()).await().indefinitely().orElseThrow();

        assertEquals(AwaitInteractionStatus.DISPATCHING, reloaded.status());
        assertEquals("reservation-1", reloaded.transportMetadata().get("admissionReservationId"));
    }

    @Test
    void dispatchedInteractionCannotBeClaimedAgain() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        var claimed = store.markDispatching(
            "tenant",
            created.record().interactionId(),
            created.record().version(),
            11_000L).await().indefinitely().orElseThrow();
        var dispatched = store.markDispatched(
            "tenant",
            created.record().interactionId(),
            claimed.version(),
            Map.of("messageId", "m-1"),
            12_000L).await().indefinitely().orElseThrow();

        var duplicateClaim = store.markDispatching(
            "tenant",
            created.record().interactionId(),
            dispatched.version(),
            13_000L).await().indefinitely();

        assertTrue(duplicateClaim.isEmpty());
    }

    @Test
    void completionCanResolveByCorrelationAndIsIdempotent() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var completed = store.complete(new AwaitCompletionCommand(
            "tenant",
            null,
            created.record().correlationId(),
            "completion-1",
            Map.of("decision", "approved"),
            "alice",
            12_000L)).await().indefinitely();
        var duplicate = store.complete(new AwaitCompletionCommand(
            "tenant",
            created.record().interactionId(),
            null,
            "completion-1",
            Map.of("decision", "approved"),
            "alice",
            13_000L)).await().indefinitely();

        assertFalse(completed.duplicate());
        assertTrue(duplicate.duplicate());
        assertEquals(AwaitInteractionStatus.COMPLETED, completed.record().status());
        assertEquals("alice", completed.record().actor());
    }

    @Test
    void completionAfterDeadlineMarksTimedOutAndFails() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 20_000L)).await().indefinitely();

        assertThrows(AwaitInteractionTerminalException.class, () -> store.complete(new AwaitCompletionCommand(
            "tenant",
            created.record().interactionId(),
            null,
            "completion-1",
            Map.of("decision", "approved"),
            "alice",
            21_000L)).await().indefinitely());

        var stored = store.get("tenant", created.record().interactionId()).await().indefinitely().orElseThrow();
        assertEquals(AwaitInteractionStatus.TIMED_OUT, stored.status());
        assertEquals(java.util.List.of(stored), store.findDueTimeoutConsequences(21_000L, 100).await().indefinitely());
    }

    @Test
    void queryPendingFiltersByTenantAndAssignee() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        store.createOrGet(new AwaitCreateCommand(
            "tenant", "execution-2", "review", 0, String.class.getName(),
            "cause-2", "idem-2", "corr-2", Map.of("orderId", "o-2"),
            "bob", "finance", "interaction-api", "unit-2", null,
            10_000L, 70_000L, Long.MAX_VALUE)).await().indefinitely();

        var alice = store.queryPending("tenant", "alice", null, null, 10).await().indefinitely();
        var finance = store.queryPending("tenant", null, "finance", null, 10).await().indefinitely();

        assertEquals(1, alice.size());
        assertEquals("alice", alice.getFirst().assignee());
        assertEquals(2, finance.size());
    }

    @Test
    void queryPendingTreatsBlankFiltersAsUnset() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        store.createOrGet(new AwaitCreateCommand(
            "tenant", "execution-2", "review", 0, String.class.getName(),
            "cause-2", "idem-2", "corr-2", Map.of("orderId", "o-2"),
            "bob", "finance", "interaction-api", "unit-2", null,
            10_000L, 70_000L, Long.MAX_VALUE)).await().indefinitely();

        var noAssignee = store.queryPending("tenant", "", null, "", 10).await().indefinitely();
        var whitespaceAssignee = store.queryPending("tenant", "   ", "   ", " ", 10).await().indefinitely();

        assertEquals(2, noAssignee.size());
        assertEquals(2, whitespaceAssignee.size());
    }

    @Test
    void getReturnsEmptyForUnknownInteractionId() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();

        var result = store.get("tenant", "unknown-id").await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void getReturnsRecordAfterCreation() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var found = store.get("tenant", created.record().interactionId()).await().indefinitely();

        assertTrue(found.isPresent());
        assertEquals(created.record().interactionId(), found.get().interactionId());
    }

    @Test
    void findByCorrelationReturnsEmptyForUnknownId() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();

        var result = store.findByCorrelation("tenant", "unknown-corr").await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void findByCorrelationReturnsRecordAfterCreation() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        String correlationId = created.record().correlationId();

        var found = store.findByCorrelation("tenant", correlationId).await().indefinitely();

        assertTrue(found.isPresent());
        assertEquals(created.record().interactionId(), found.get().interactionId());
    }

    @Test
    void findByUnitReturnsItemsInIndexOrder() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        store.createOrGet(itemCommand("idem-2", "corr-2", 2)).await().indefinitely();
        store.createOrGet(itemCommand("idem-0", "corr-0", 0)).await().indefinitely();
        store.createOrGet(itemCommand("idem-1", "corr-1", 1)).await().indefinitely();

        List<AwaitInteractionRecord> records = store.findByUnit(
                "tenant",
                "unit-1")
            .await().indefinitely();

        assertEquals(3, records.size());
        assertEquals(List.of(0, 1, 2), records.stream().map(AwaitInteractionRecord::itemIndex).toList());
        assertTrue(records.stream().allMatch(AwaitInteractionRecord::itemInteraction));
    }

    @Test
    void cancelTransitionsToTerminalState() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        String interactionId = created.record().interactionId();

        var cancelled = store.cancel("tenant", interactionId, 0L, "user request", 15_000L)
            .await().indefinitely();

        assertTrue(cancelled.isPresent());
        assertEquals(AwaitInteractionStatus.CANCELLED, cancelled.get().status());
        assertTrue(cancelled.get().status().terminal());
    }

    @Test
    void cancelWithWrongVersionReturnsEmpty() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var result = store.cancel("tenant", created.record().interactionId(), 999L, "reason", 15_000L)
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void failTransitionsToFailedStatus() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        String interactionId = created.record().interactionId();

        var failed = store.fail("tenant", interactionId, 0L, "transport error", 15_000L)
            .await().indefinitely();

        assertTrue(failed.isPresent());
        assertEquals(AwaitInteractionStatus.FAILED, failed.get().status());
    }

    @Test
    void markTimedOutTransitionsToTimedOutStatus() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        String interactionId = created.record().interactionId();

        var timedOut = store.markTimedOut("tenant", interactionId, 0L, 71_000L)
            .await().indefinitely();

        assertTrue(timedOut.isPresent());
        assertEquals(AwaitInteractionStatus.TIMED_OUT, timedOut.get().status());
    }

    @Test
    void timeoutAdmissionRetainsWorkUntilScopedVersionMatchingAcknowledgement() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("retained-timeout", 10_000L, 70_000L)).await().indefinitely().record();
        var timedOut = store.markTimedOut(created.tenantId(), created.interactionId(), created.version(), 71_000L)
            .await().indefinitely().orElseThrow();
        assertEquals(created.version() + 1, timedOut.version());
        assertEquals(java.util.List.of(timedOut), store.findDueTimeoutConsequences(71_000L, 1).await().indefinitely());
        assertTrue(store.findTimedOut(71_000L, 100).await().indefinitely().isEmpty());

        for (var mismatched : java.util.List.of(
            timeoutAcknowledgement(timedOut, "foreign-tenant", timedOut.version(), timedOut.executionId(), timedOut.unitId()),
            timeoutAcknowledgement(timedOut, timedOut.tenantId(), timedOut.version() + 1, timedOut.executionId(), timedOut.unitId()),
            timeoutAcknowledgement(timedOut, timedOut.tenantId(), timedOut.version(), "foreign-execution", timedOut.unitId()),
            timeoutAcknowledgement(timedOut, timedOut.tenantId(), timedOut.version(), timedOut.executionId(), "foreign-unit"))) {
            assertThrows(IllegalArgumentException.class, () -> store.completeTimeoutConsequences(mismatched).await().indefinitely());
            assertEquals(java.util.List.of(timedOut), store.findDueTimeoutConsequences(71_000L, 1).await().indefinitely());
        }
        assertThrows(IllegalArgumentException.class, () -> store.completeTimeoutConsequences(created).await().indefinitely());
        store.completeTimeoutConsequences(timedOut).await().indefinitely();
        store.completeTimeoutConsequences(timedOut).await().indefinitely();
        assertTrue(store.findDueTimeoutConsequences(71_000L, 1).await().indefinitely().isEmpty());
        assertEquals(timedOut, store.get(created.tenantId(), created.interactionId()).await().indefinitely().orElseThrow());
    }

    @Test
    void completionAndTimeoutHaveOneWinnerAndOnlyTimeoutWinnerRetainsWork() throws Exception {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("timeout-completion-race", 10_000L, 70_000L)).await().indefinitely().record();
        var ready = new java.util.concurrent.CountDownLatch(2);
        var start = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var completion = executor.submit(() -> {
                ready.countDown();
                assertTrue(start.await(5, java.util.concurrent.TimeUnit.SECONDS));
                try {
                    store.complete(new AwaitCompletionCommand(created.tenantId(), created.interactionId(), created.correlationId(),
                        "completion-race", "approved", "provider", 15_000L)).await().indefinitely();
                    return true;
                } catch (AwaitInteractionTerminalException timeoutWon) {
                    return false;
                }
            });
            var timeout = executor.submit(() -> {
                ready.countDown();
                assertTrue(start.await(5, java.util.concurrent.TimeUnit.SECONDS));
                return store.markTimedOut(created.tenantId(), created.interactionId(), created.version(), 71_000L)
                    .await().indefinitely().isPresent();
            });
            assertTrue(ready.await(5, java.util.concurrent.TimeUnit.SECONDS));
            start.countDown();
            boolean completionWon = completion.get(5, java.util.concurrent.TimeUnit.SECONDS);
            boolean timeoutWon = timeout.get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertTrue(completionWon ^ timeoutWon);
            var stored = store.get(created.tenantId(), created.interactionId()).await().indefinitely().orElseThrow();
            assertEquals(timeoutWon ? AwaitInteractionStatus.TIMED_OUT : AwaitInteractionStatus.COMPLETED, stored.status());
            var pending = store.findDueTimeoutConsequences(71_000L, 100).await().indefinitely();
            assertEquals(timeoutWon ? java.util.List.of(stored) : java.util.List.of(), pending);
        }
    }

    @Test
    void expiredPendingPrefixCannotStarveRetainedTimeoutWorkAcrossBoundedCalls() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        for (int index = 0; index < 101; index++) {
            var created = store.createOrGet(createCommand("expired-timeout-" + index, 10_000L, 70_000L, 90L))
                .await().indefinitely().record();
            store.markTimedOut(created.tenantId(), created.interactionId(), created.version(), 71_000L)
                .await().indefinitely();
        }
        var retained = store.createOrGet(createCommand("retained-last-timeout", 10_000L, 70_000L)).await().indefinitely().record();
        var timedOut = store.markTimedOut(retained.tenantId(), retained.interactionId(), retained.version(), 72_000L)
            .await().indefinitely().orElseThrow();
        // Exactly one bounded page is consumed: expired entries are removed, but the caller owns repetition.
        assertTrue(store.findDueTimeoutConsequences(100_000L, 100).await().indefinitely().isEmpty());
        assertEquals(java.util.List.of(timedOut), store.findDueTimeoutConsequences(100_000L, 100).await().indefinitely());
        for (int limit : new int[] {0, -1, 101, Integer.MAX_VALUE}) {
            assertThrows(IllegalArgumentException.class, () -> store.findDueTimeoutConsequences(100_000L, limit));
        }
    }

    private AwaitInteractionRecord timeoutAcknowledgement(AwaitInteractionRecord original, String tenant,
        long version, String execution, String unit) {
        var acknowledgement = org.mockito.Mockito.mock(AwaitInteractionRecord.class);
        org.mockito.Mockito.when(acknowledgement.status()).thenReturn(AwaitInteractionStatus.TIMED_OUT);
        org.mockito.Mockito.when(acknowledgement.tenantId()).thenReturn(tenant);
        org.mockito.Mockito.when(acknowledgement.interactionId()).thenReturn(original.interactionId());
        org.mockito.Mockito.when(acknowledgement.version()).thenReturn(version);
        org.mockito.Mockito.when(acknowledgement.executionId()).thenReturn(execution);
        org.mockito.Mockito.when(acknowledgement.unitId()).thenReturn(unit);
        return acknowledgement;
    }

    @Test
    void transitionOnTerminalInteractionReturnsEmpty() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        String interactionId = created.record().interactionId();

        // First cancel
        store.cancel("tenant", interactionId, 0L, "user request", 15_000L).await().indefinitely();
        // Attempting to cancel again should return empty since it's terminal
        var result = store.cancel("tenant", interactionId, 1L, "again", 16_000L).await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void findTimedOutReturnsOnlyExpiredNonTerminal() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        // Create one that will be expired
        store.createOrGet(createCommand("idem-1", 10_000L, 20_000L)).await().indefinitely();
        // Create one that won't expire
        store.createOrGet(new AwaitCreateCommand(
            "tenant", "execution-2", "review", 0, String.class.getName(),
            "cause-2", "idem-2", "corr-2", Map.of(), null, null, "interaction-api",
            "unit-2", null, 10_000L, 80_000L, Long.MAX_VALUE)).await().indefinitely();

        var timedOut = store.findTimedOut(21_000L, 10).await().indefinitely();

        assertEquals(1, timedOut.size());
        assertEquals("idem-1", timedOut.getFirst().idempotencyKey());
    }

    @Test
    void findTimedOutRespectsLimit() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        // Create multiple expired interactions
        for (int i = 0; i < 5; i++) {
            store.createOrGet(new AwaitCreateCommand(
                "tenant", "execution-" + i, "review", 0, String.class.getName(),
                "cause-" + i, "idem-" + i, "corr-" + i, Map.of(), null, null, "interaction-api",
                "unit-" + i, null, 10_000L, 20_000L, Long.MAX_VALUE)).await().indefinitely();
        }

        var timedOut = store.findTimedOut(21_000L, 3).await().indefinitely();

        assertEquals(3, timedOut.size());
    }

    @Test
    void queryPendingFiltersByStepId() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        store.createOrGet(new AwaitCreateCommand(
            "tenant", "execution-2", "fraud-check", 0, String.class.getName(),
            "cause-2", "idem-2", "corr-2", Map.of(), "alice", "finance", "interaction-api",
            "unit-2", null, 10_000L, 70_000L, Long.MAX_VALUE)).await().indefinitely();

        var reviewSteps = store.queryPending("tenant", null, null, "review", 10).await().indefinitely();
        var fraudSteps = store.queryPending("tenant", null, null, "fraud-check", 10).await().indefinitely();

        assertEquals(1, reviewSteps.size());
        assertEquals("review", reviewSteps.getFirst().stepId());
        assertEquals(1, fraudSteps.size());
        assertEquals("fraud-check", fraudSteps.getFirst().stepId());
    }

    @Test
    void queryPendingExcludesTerminalInteractions() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        // Complete the interaction
        store.complete(new AwaitCompletionCommand(
            "tenant", created.record().interactionId(), null, "completion-1", "result", "alice", 15_000L))
            .await().indefinitely();

        var pending = store.queryPending("tenant", null, null, null, 10).await().indefinitely();

        assertTrue(pending.isEmpty());
    }

    @Test
    void queryPendingExcludesDifferentTenant() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var otherTenant = store.queryPending("other-tenant", null, null, null, 10).await().indefinitely();
        var sameTenant = store.queryPending("tenant", null, null, null, 10).await().indefinitely();

        assertTrue(otherTenant.isEmpty());
        assertEquals(1, sameTenant.size());
    }

    @Test
    void providerNameIsMemory() {
        assertEquals("memory", new InMemoryAwaitInteractionStore().providerName());
    }

    @Test
    void priorityIsNegative() {
        assertTrue(new InMemoryAwaitInteractionStore().priority() < 0);
    }

    @Test
    void completionCanResolveByInteractionId() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var completed = store.complete(new AwaitCompletionCommand(
            "tenant",
            created.record().interactionId(),
            null,
            "completion-1",
            "result-payload",
            "charlie",
            12_000L)).await().indefinitely();

        assertFalse(completed.duplicate());
        assertEquals(AwaitInteractionStatus.COMPLETED, completed.record().status());
        assertEquals("result-payload", completed.record().responsePayload());
        assertEquals("charlie", completed.record().actor());
    }

    @Test
    void completionThrowsWhenNoMatchFound() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();

        assertThrows(AwaitInteractionNotFoundException.class, () -> store.complete(new AwaitCompletionCommand(
            "tenant", "nonexistent-id", null, "completion-1", null, null, 12_000L))
            .await().indefinitely());
    }

    @Test
    void completionThrowsForOtherTerminalStates() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        // Cancel to put in terminal state
        store.cancel("tenant", created.record().interactionId(), 0L, "cancelled", 15_000L)
            .await().indefinitely();

        assertThrows(AwaitInteractionTerminalException.class, () -> store.complete(new AwaitCompletionCommand(
            "tenant", created.record().interactionId(), null, "completion-1", "result", "alice", 16_000L))
            .await().indefinitely());
    }

    @Test
    void createsNewInteractionAfterTerminalDeduplication() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCreateCommand command = createCommand("idem-1", 10_000L, 70_000L);
        var first = store.createOrGet(command).await().indefinitely();

        // Complete the first interaction
        store.complete(new AwaitCompletionCommand(
            "tenant", first.record().interactionId(), null, "completion-1", "result", "alice", 15_000L))
            .await().indefinitely();

        // Create new one with same idempotency key - completed state is terminal, so new record is created
        var second = store.createOrGet(command).await().indefinitely();

        assertFalse(second.duplicate());
        // A new interaction id should be generated
        assertNotNull(second.record().interactionId());
    }

    @Test
    void versionIncrementsOnMarkDispatched() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        assertEquals(0L, created.record().version());

        var claimed = store.markDispatching(
            "tenant", created.record().interactionId(), 0L, 11_000L)
            .await().indefinitely().orElseThrow();
        var dispatched = store.markDispatched(
            "tenant", created.record().interactionId(), claimed.version(), Map.of(), 12_000L)
            .await().indefinitely();

        assertTrue(dispatched.isPresent());
        assertEquals(2L, dispatched.get().version());
    }

    private AwaitCreateCommand createCommand(String idempotencyKey, long nowEpochMs, long deadlineEpochMs) {
        return createCommand(idempotencyKey, nowEpochMs, deadlineEpochMs, Long.MAX_VALUE);
    }

    private AwaitCreateCommand createCommand(String idempotencyKey, long nowEpochMs, long deadlineEpochMs, long ttlEpochS) {
        return new AwaitCreateCommand(
            "tenant",
            "execution-1",
            "review",
            0,
            String.class.getName(),
            "cause-1",
            idempotencyKey,
            "corr-1",
            Map.of("orderId", "o-1"),
            "alice",
            "finance",
            "interaction-api",
            "unit-1",
            null,
            nowEpochMs,
            deadlineEpochMs,
            ttlEpochS);
    }

    private AwaitCreateCommand itemCommand(String idempotencyKey, String correlationId, int itemIndex) {
        return new AwaitCreateCommand(
            "tenant",
            "execution-1",
            "await-payment-provider",
            3,
            String.class.getName(),
            "execution-1:3:" + itemIndex,
            idempotencyKey,
            correlationId,
            Map.of("index", itemIndex),
            null,
            null,
            "kafka",
            "unit-1",
            itemIndex,
            10_000L,
            70_000L,
            Long.MAX_VALUE);
    }
}

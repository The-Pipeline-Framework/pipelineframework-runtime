package org.pipelineframework.awaitable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import java.util.Set;
import java.util.function.Supplier;
import org.pipelineframework.awaitable.AwaitUnitOccurrence;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.AwaitUnitCreateCommand;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;

class InMemoryAwaitUnitStoreTest {
    @Test
    void qualifiedUnitPrimaryMembershipIsImmutableAcrossSeals() {
        var store = new InMemoryAwaitUnitStore();
        assertPrimarySeal(() -> store);
    }

    /** Identical store-level consequences for memory and actual physical Dynamo. */
    static void assertPrimarySeal(Supplier<AwaitUnitStore> factory) {
        var cases = new java.util.ArrayList<org.junit.jupiter.api.function.Executable>();
        cases.add(() -> assertPrimaryCase(factory, "open-existing", AwaitUnitStatus.WAITING_EXTERNAL, false, "original"));
        cases.add(() -> assertPrimaryCase(factory, "dispatch-existing", AwaitUnitStatus.WAITING_EXTERNAL, true, "original"));
        cases.add(() -> assertPrimaryCase(factory, "dispatch-empty", AwaitUnitStatus.WAITING_EXTERNAL, true, ""));
        for (AwaitUnitStatus status : java.util.List.of(AwaitUnitStatus.COMPLETED, AwaitUnitStatus.FAILED,
                AwaitUnitStatus.TIMED_OUT, AwaitUnitStatus.CANCELLED, AwaitUnitStatus.EXPIRED)) {
            cases.add(() -> assertPrimaryCase(factory, status.name() + "-existing", status, false, "original"));
            cases.add(() -> assertPrimaryCase(factory, status.name() + "-empty", status, false, ""));
        }
        assertAll("qualified membership seals", cases);
        String id = new AwaitUnitOccurrence("tenant", "execution", "execution", "review", 0, "open-empty").unitId();
        var open = primaryFixture(factory, id, AwaitUnitStatus.WAITING_EXTERNAL, false, "");
        var attached = factory.get().attachPrimaryInteraction("tenant", id, "first", 3_000).await().indefinitely().orElseThrow();
        assertEquals("first", attached.primaryInteractionId());
        assertEquals(open.version() + 1, attached.version());
        assertEquals(attached, factory.get().attachPrimaryInteraction("tenant", id, "first", 4_000)
            .await().indefinitely().orElseThrow(), "same-primary replay preserves first committed membership and time");
        assertTrue(factory.get().attachPrimaryInteraction("tenant", id, "different", 5_000)
            .await().indefinitely().isEmpty());
        assertEquals(attached, factory.get().get("tenant", id).await().indefinitely().orElseThrow());
    }

    private static void assertPrimaryCase(Supplier<AwaitUnitStore> factory, String cause,
        AwaitUnitStatus status, boolean dispatchSealed, String primary) {
        String id = new AwaitUnitOccurrence("tenant", "execution", "execution", "review", 0, cause).unitId();
        var original = primaryFixture(factory, id, status, dispatchSealed, primary);
        assertAll(cause,
            () -> {
                if (!primary.isEmpty()) assertEquals(original, factory.get().attachPrimaryInteraction("tenant", id,
                    primary, 3_000).await().indefinitely().orElseThrow(), "same-primary retry cannot mutate the retained record");
            },
            () -> assertEquals(original, factory.get().get("tenant", id).await().indefinitely().orElseThrow()),
            () -> assertTrue(factory.get().attachPrimaryInteraction("tenant", id, "different", 4_000)
                .await().indefinitely().isEmpty(), "different/new primary must lose the sealed membership transition"),
            () -> assertEquals(original, factory.get().get("tenant", id).await().indefinitely().orElseThrow(),
                "rejected membership must preserve state/version/first time"));
    }

    private static AwaitUnitRecord primaryFixture(Supplier<AwaitUnitStore> factory, String id,
        AwaitUnitStatus status, boolean dispatchSealed, String primary) {
        var record = factory.get().createOrGet(new AwaitUnitCreateCommand("tenant", id, "execution", "review", 0,
            "ONE_TO_ONE", 1_000, Long.MAX_VALUE)).await().indefinitely();
        if (!primary.isEmpty()) record = factory.get().attachPrimaryInteraction("tenant", id, primary, 2_000)
            .await().indefinitely().orElseThrow();
        if (dispatchSealed) record = factory.get().markDispatchComplete("tenant", id, 1, 2_500)
            .await().indefinitely().orElseThrow();
        if (status.terminal()) record = factory.get().markTerminal("tenant", id, status, 2_600)
            .await().indefinitely().orElseThrow();
        return record;
    }

    @Test
    void duplicateOwnerConflictsCannotReplaceExistingProgress() {
        var store = new InMemoryAwaitUnitStore();
        var command = createCommand();
        store.createOrGet(command).await().indefinitely();
        var progress = store.recordItemCompleted("tenant", "unit-1", "item:0", 11_000L).await().indefinitely().orElseThrow();
        assertEquals(progress, store.createOrGet(command).await().indefinitely());
        for (var conflict : java.util.List.of(
            new AwaitUnitCreateCommand("tenant", "unit-1", "foreign", command.stepId(), command.stepIndex(), command.cardinality(), 12_000L, command.ttlEpochS()),
            new AwaitUnitCreateCommand("tenant", "unit-1", command.executionId(), "foreign", command.stepIndex(), command.cardinality(), 12_000L, command.ttlEpochS()),
            new AwaitUnitCreateCommand("tenant", "unit-1", command.executionId(), command.stepId(), 99, command.cardinality(), 12_000L, command.ttlEpochS()),
            new AwaitUnitCreateCommand("tenant", "unit-1", command.executionId(), command.stepId(), command.stepIndex(), "ONE_TO_MANY", 12_000L, command.ttlEpochS()))) {
            assertThrows(IllegalStateException.class, () -> store.createOrGet(conflict).await().indefinitely());
            var imported = new AwaitUnitRecord(conflict.tenantId(), conflict.unitId(), conflict.executionId(), conflict.stepId(),
                conflict.stepIndex(), conflict.cardinality(), 0, AwaitUnitStatus.WAITING_EXTERNAL, null, null, 0,
                java.util.Set.of(), false, 10_000L, 10_000L, conflict.ttlEpochS());
            assertThrows(IllegalStateException.class, () -> store.importRecord(imported).await().indefinitely());
            assertEquals(progress, store.get("tenant", "unit-1").await().indefinitely().orElseThrow());
        }
    }

    @Test
    void qualifiedOccurrencePreservesTerminalProgressAndSeparatesFreshCause() {
        var store = new InMemoryAwaitUnitStore();
        String firstId = new org.pipelineframework.awaitable.AwaitUnitOccurrence("tenant", "execution-1", "page", "review", 0, "initial").unitId();
        String freshId = new org.pipelineframework.awaitable.AwaitUnitOccurrence("tenant", "execution-1", "page", "review", 0, "reissue").unitId();
        var completed = new AwaitUnitRecord("tenant", firstId, "execution-1", "review", 0, "ONE_TO_ONE", 9,
            AwaitUnitStatus.COMPLETED, "original", 1, 1, java.util.Set.of("item:0"), true, 1_000L, 2_000L, 0);
        store.importRecord(completed).await().indefinitely();
        var retry = new AwaitUnitCreateCommand("tenant", firstId, "execution-1", "review", 0, "ONE_TO_ONE", 3_000L, Long.MAX_VALUE);
        assertEquals(completed, store.createOrGet(retry).await().indefinitely());
        assertThrows(IllegalStateException.class, () -> store.importRecord(new AwaitUnitRecord("tenant", firstId,
            "execution-1", "review", 0, "ONE_TO_ONE", 0, AwaitUnitStatus.WAITING_EXTERNAL, "foreign-primary",
            null, 0, java.util.Set.of(), false, 1_000L, 1_000L, 0)).await().indefinitely());
        assertEquals(completed, store.importRecord(new AwaitUnitRecord("tenant", firstId, "execution-1", "review", 0,
            "ONE_TO_ONE", 0, AwaitUnitStatus.WAITING_EXTERNAL, null, null, 0, java.util.Set.of(), false,
            1_000L, 1_000L, 0)).await().indefinitely(), "stale pre-attachment import retains existing progress");
        assertThrows(IllegalStateException.class, () -> store.createOrGet(new AwaitUnitCreateCommand(
            "tenant", firstId, "execution-1", "review", 0, "ONE_TO_MANY", 3_000L, Long.MAX_VALUE)).await().indefinitely());
        var fresh = store.createOrGet(new AwaitUnitCreateCommand("tenant", freshId, "execution-1", "review", 0,
            "ONE_TO_ONE", 3_000L, Long.MAX_VALUE)).await().indefinitely();
        assertEquals(AwaitUnitStatus.WAITING_EXTERNAL, fresh.status());
        assertEquals(completed, store.get("tenant", firstId).await().indefinitely().orElseThrow());
    }

    @Test
    void duplicateItemCompletionDoesNotIncrementCompletedItemCount() {
        InMemoryAwaitUnitStore store = new InMemoryAwaitUnitStore();
        store.createOrGet(createCommand()).await().indefinitely();

        var first = store.recordItemCompleted("tenant", "unit-1", "interaction-1", 11_000L).await().indefinitely().orElseThrow();
        var duplicate = store.recordItemCompleted("tenant", "unit-1", "interaction-1", 12_000L).await().indefinitely().orElseThrow();
        var dispatchComplete = store.markDispatchComplete("tenant", "unit-1", 1, 13_000L).await().indefinitely().orElseThrow();

        assertEquals(1, first.completedItemCount());
        assertEquals(1, duplicate.completedItemCount());
        assertEquals(1, dispatchComplete.completedItemCount());
        assertEquals(AwaitUnitStatus.COMPLETED, dispatchComplete.status());
    }

    @Test
    void distinctItemCompletionsIncrementCompletedItemCount() {
        InMemoryAwaitUnitStore store = new InMemoryAwaitUnitStore();
        store.createOrGet(createCommand()).await().indefinitely();

        store.recordItemCompleted("tenant", "unit-1", "interaction-1", 11_000L).await().indefinitely();
        var second = store.recordItemCompleted("tenant", "unit-1", "interaction-2", 12_000L).await().indefinitely().orElseThrow();
        var dispatchComplete = store.markDispatchComplete("tenant", "unit-1", 2, 13_000L).await().indefinitely().orElseThrow();

        assertEquals(2, second.completedItemCount());
        assertEquals(2, dispatchComplete.completedItemCount());
        assertEquals(AwaitUnitStatus.COMPLETED, dispatchComplete.status());
    }

    @Test
    void continuationCompletionFactsAreIdempotentAndDoNotChangeAdmittedCount() {
        InMemoryAwaitUnitStore store = new InMemoryAwaitUnitStore();
        store.createOrGet(createCommand()).await().indefinitely();
        store.recordItemCompleted("tenant", "unit-1", "item:0", 11_000L).await().indefinitely();
        store.recordItemCompleted("tenant", "unit-1", "item:1", 12_000L).await().indefinitely();
        store.markDispatchComplete("tenant", "unit-1", 2, 13_000L).await().indefinitely();

        var first = store.recordItemContinuationCompleted(
            "tenant", "unit-1", AwaitUnitRecord.continuationCompletionKey(0), 14_000L).await().indefinitely().orElseThrow();
        var duplicate = store.recordItemContinuationCompleted(
            "tenant", "unit-1", AwaitUnitRecord.continuationCompletionKey(0), 15_000L).await().indefinitely().orElseThrow();
        var completed = store.recordItemContinuationCompleted(
            "tenant", "unit-1", AwaitUnitRecord.continuationCompletionKey(1), 16_000L).await().indefinitely().orElseThrow();

        assertEquals(2, first.completedItemCount());
        assertEquals(1, first.completedContinuationItemCount());
        assertEquals(1, duplicate.completedContinuationItemCount());
        assertEquals(2, completed.completedItemCount());
        assertEquals(2, completed.completedContinuationItemCount());
    }

    @Test
    void rejectsBlankContinuationCompletionKey() {
        InMemoryAwaitUnitStore store = new InMemoryAwaitUnitStore();
        store.createOrGet(createCommand()).await().indefinitely();

        assertThrows(IllegalArgumentException.class, () -> store.recordItemContinuationCompleted(
            "tenant", "unit-1", null, 14_000L).await().indefinitely());
        assertThrows(IllegalArgumentException.class, () -> store.recordItemContinuationCompleted(
            "tenant", "unit-1", "", 14_000L).await().indefinitely());
        assertThrows(IllegalArgumentException.class, () -> store.recordItemContinuationCompleted(
            "tenant", "unit-1", "  ", 14_000L).await().indefinitely());
    }

    @Test
    void terminalNonCompletedUnitIgnoresContinuationCompletionFact() {
        InMemoryAwaitUnitStore store = new InMemoryAwaitUnitStore();
        AwaitUnitRecord failed = new AwaitUnitRecord(
            "tenant", "unit-1", "execution-1", "AwaitPaymentProvider", 1, "ONE_TO_ONE", 1L,
            AwaitUnitStatus.FAILED, null, 1, 1, java.util.Set.of("item:0"), true,
            10_000L, 10_000L, 9_999_999_999L);
        store.importRecord(failed).await().indefinitely();

        AwaitUnitRecord unchanged = store.recordItemContinuationCompleted(
            "tenant", "unit-1", AwaitUnitRecord.continuationCompletionKey(0), 14_000L)
            .await().indefinitely().orElseThrow();

        assertEquals(failed, unchanged);
        assertEquals(0, unchanged.completedContinuationItemCount());
    }

    @Test
    void importedCompletedItemKeysPreserveDuplicateProtection() {
        InMemoryAwaitUnitStore store = new InMemoryAwaitUnitStore();
        store.importRecord(new AwaitUnitRecord(
            "tenant",
            "unit-1",
            "execution-1",
            "AwaitPaymentProvider",
            1,
            "ONE_TO_ONE",
            1L,
            AwaitUnitStatus.WAITING_EXTERNAL,
            null,
            2,
            1,
            java.util.Set.of("item:0"),
            true,
            10_000L,
            10_000L,
            9_999_999_999L)).await().indefinitely();

        var duplicate = store.recordItemCompleted("tenant", "unit-1", "item:0", 11_000L).await().indefinitely().orElseThrow();
        var second = store.recordItemCompleted("tenant", "unit-1", "item:1", 12_000L).await().indefinitely().orElseThrow();

        assertEquals(1, duplicate.completedItemCount());
        assertEquals(2, second.completedItemCount());
        assertEquals(AwaitUnitStatus.COMPLETED, second.status());
    }

    @Test
    void rejectsCompletedItemKeyCountMismatch() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitUnitRecord(
            "tenant",
            "unit-1",
            "execution-1",
            "AwaitPaymentProvider",
            1,
            "ONE_TO_ONE",
            1L,
            AwaitUnitStatus.WAITING_EXTERNAL,
            null,
            2,
            2,
            java.util.Set.of("item:0"),
            true,
            10_000L,
            10_000L,
            9_999_999_999L));
    }

    private static AwaitUnitCreateCommand createCommand() {
        return new AwaitUnitCreateCommand(
            "tenant",
            "unit-1",
            "execution-1",
            "AwaitPaymentProvider",
            1,
            "ONE_TO_ONE",
            10_000L,
            9_999_999_999L);
    }
}

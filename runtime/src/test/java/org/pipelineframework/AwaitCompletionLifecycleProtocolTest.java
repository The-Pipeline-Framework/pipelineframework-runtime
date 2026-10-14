package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.orchestrator.*;

/** Independent immutable lifecycle oracles, inherited unchanged by real Dynamo stores. */
class AwaitCompletionLifecycleProtocolTest {
    protected static final Duration WAIT = Duration.ofSeconds(20);
    protected record Stores(ExecutionStateStore executions, AwaitInteractionStore interactions) {}

    protected Supplier<Stores> stores() {
        Stores shared = new Stores(new InMemoryExecutionStateStore(), new InMemoryAwaitInteractionStore());
        return () -> shared;
    }

    @Test
    void concurrentDifferentClocksAndLostReplyRetainOneFirstAdmission() throws Exception {
        Supplier<Stores> factory = stores();
        long now = System.currentTimeMillis();
        var interaction = imported(factory.get(), "tenant", "parent", "interaction", "unit", 0,
            AwaitInteractionStatus.WAITING, now, 0);
        try (var callers = Executors.newFixedThreadPool(2)) {
            var start = new CyclicBarrier(2);
            var first = callers.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                return await(factory.get().interactions().complete(completion(interaction, now + 10)));
            });
            var second = callers.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                return await(factory.get().interactions().complete(completion(interaction, now + 20)));
            });
            var a = first.get(30, TimeUnit.SECONDS);
            var b = second.get(30, TimeUnit.SECONDS);
            assertEquals(1, Stream.of(a, b).filter(result -> !result.duplicate()).count());
        }
        var firstFact = fact(factory.get(), interaction);
        assertTrue(Set.of(now + 10, now + 20).contains(firstFact.firstAdmittedAtEpochMs()));
        assertEquals(0, firstFact.ttlEpochS(), "imported no-expiry retention must remain no-expiry");
        var durable = await(factory.get().interactions().get("tenant", "interaction")).orElseThrow();
        assertEquals(firstFact.firstAdmittedAtEpochMs(), durable.updatedAtEpochMs());
        assertEquals(1, durable.version());
        // Discard the reply as a caller with an ambiguous post-commit transport outcome would.
        await(factory.get().interactions().complete(completion(interaction, now + 30)));
        assertEquals(firstFact, fact(factory.get(), interaction));
        assertEquals(List.of(firstFact), enumerate(factory.get(), "tenant", 1));
    }

    @Test
    void commandObservedCompletionClockSurvivesLaterDispatchSettlement() {
        Supplier<Stores> factory = stores();
        long now = System.currentTimeMillis();
        var interaction = imported(factory.get(), "tenant", "parent", "callback", "unit", 0,
            AwaitInteractionStatus.DISPATCHING, now, 0, Map.of("completionMode", "CONNECTOR_CALLBACK"));
        var observed = await(factory.get().interactions().complete(completion(interaction, now + 1))).record();
        assertEquals(AwaitInteractionStatus.COMPLETION_OBSERVED, observed.status());
        var admitted = fact(factory.get(), interaction);
        assertEquals(now + 1, admitted.firstAdmittedAtEpochMs());
        assertEquals(List.of(admitted), enumerate(factory.get(), "tenant", 1));
        var settled = await(factory.get().interactions().settleCommandDispatch(observed,
            CommandDispatchSettlement.SUCCEEDED, now + 10)).orElseThrow();
        assertEquals(AwaitInteractionStatus.COMPLETED, settled.status());
        assertEquals(now + 10, settled.updatedAtEpochMs());
        assertEquals(admitted, fact(factory.get(), interaction),
            "dispatch settlement must not replace the earlier completion admission clock");
        assertTrue(await(factory.get().interactions().complete(completion(interaction, now + 20))).duplicate());
        assertEquals(admitted, fact(factory.get(), interaction));
    }

    @TestFactory
    Stream<DynamicTest> releaseReplyLossAndLaterSequentialAwaitPreserveOriginalAcknowledgement() {
        return Stream.of(false, true).map(items -> DynamicTest.dynamicTest(
            items ? "item continuation parent CAS" : "scalar parent CAS", () -> {
                Supplier<Stores> factory = stores();
                long now = System.currentTimeMillis();
                var parent = waiting(factory.get(), "tenant", "unit-first", now);
                String firstUnit = parent.awaitUnitId();
                String secondUnit = qualifiedUnit(parent, "AwaitNext", 4);
                assertTrue(ack(factory.get(), "tenant", parent.executionId(), firstUnit).isEmpty());
                var released = items
                    ? await(factory.get().executions().markAwaitItemContinuationsCompleted("tenant",
                        parent.executionId(), firstUnit, 3,
                        new ExecutionInputSnapshot(ExecutionInputShape.MULTI, List.of("approved")), now + 10)).orElseThrow()
                    : await(factory.get().executions().markAwaitCompleted("tenant",
                        parent.executionId(), firstUnit, 3, now + 10)).orElseThrow();
                // Forget the CAS response and recover through a new store wrapper.
                var firstAck = ack(factory.get(), "tenant", parent.executionId(), firstUnit).orElseThrow();
                assertAll(
                    () -> assertEquals(AwaitReleaseDisposition.CONTINUATION_QUEUED, firstAck.disposition()),
                    () -> assertEquals(now + 10, firstAck.acknowledgedAtEpochMs()),
                    () -> assertEquals(released.version(), firstAck.parentVersion()),
                    () -> assertEquals(2, firstAck.stepIndex()),
                    () -> assertEquals("pipeline", firstAck.pipelineId()),
                    () -> assertEquals("contract", firstAck.contractVersion()),
                    () -> assertEquals("release", firstAck.releaseVersion()));
                var later = await(factory.get().executions().markWaitingExternal("tenant", parent.executionId(),
                    released.version(), "await-next", secondUnit, 4, now + 20)).orElseThrow();
                assertTrue(await(factory.get().executions().markAwaitCompleted("tenant", parent.executionId(),
                    firstUnit, 3, now + 30)).isEmpty(), "old replay cannot release the later Await");
                assertEquals(firstAck, ack(factory.get(), "tenant", parent.executionId(), firstUnit).orElseThrow());
                assertTrue(ack(factory.get(), "tenant", parent.executionId(), secondUnit).isEmpty());
                assertEquals(later, await(factory.get().executions().getExecution("tenant", parent.executionId())).orElseThrow());
                var next = await(factory.get().executions().markAwaitCompleted("tenant", parent.executionId(),
                    secondUnit, 5, now + 40)).orElseThrow();
                assertEquals(next.version(), ack(factory.get(), "tenant", parent.executionId(), secondUnit)
                    .orElseThrow().parentVersion());
                assertEquals(firstAck, ack(factory.get(), "tenant", parent.executionId(), firstUnit).orElseThrow());
            }));
    }

    @Test
    void retirementKeepsFactAndDuplicateDoesNotReopenOrMutateInteraction() {
        Supplier<Stores> factory = stores();
        long now = System.currentTimeMillis();
        var parent = waiting(factory.get(), "tenant", "unit", now);
        String unit = parent.awaitUnitId();
        var interaction = imported(factory.get(), "tenant", parent.executionId(), "interaction", unit, 0,
            AwaitInteractionStatus.WAITING, now, 0);
        await(factory.get().interactions().complete(completion(interaction, now + 1)));
        var admitted = fact(factory.get(), interaction);
        assertEquals(List.of(admitted), enumerate(factory.get(), "tenant", 1));
        assertTrue(ack(factory.get(), "tenant", parent.executionId(), unit).isEmpty());
        await(factory.get().executions().markAwaitCompleted("tenant", parent.executionId(), unit, 3, now + 10));
        assertEquals(AwaitReleaseDisposition.CONTINUATION_QUEUED,
            ack(factory.get(), "tenant", parent.executionId(), unit).orElseThrow().disposition());
        var completed = await(factory.get().interactions().get("tenant", "interaction")).orElseThrow();
        await(factory.get().interactions().retireCompletionAdmission(admitted));
        await(factory.get().interactions().retireCompletionAdmission(admitted));
        assertTrue(enumerate(factory.get(), "tenant", 1).isEmpty());
        assertEquals(admitted, fact(factory.get(), interaction));
        assertTrue(await(factory.get().interactions().complete(completion(interaction, now + 20))).duplicate());
        assertTrue(enumerate(factory.get(), "tenant", 1).isEmpty());
        assertEquals(admitted, fact(factory.get(), interaction));
        assertEquals(completed, await(factory.get().interactions().get("tenant", "interaction")).orElseThrow());
    }

    @Test
    void allItemAdmissionsBeyondOneHundredAreCountedBeforeAnyUnitRelease() {
        Stores stores = stores().get();
        long now = System.currentTimeMillis();
        var parent = waiting(stores, "tenant", "unit", now);
        String unit = parent.awaitUnitId();
        Set<String> expected = new HashSet<>();
        for (int item = 0; item < 103; item++) {
            String id = "item#" + item;
            var interaction = imported(stores, "tenant", parent.executionId(), id, unit, item,
                AwaitInteractionStatus.WAITING, now, 0);
            await(stores.interactions().complete(completion(interaction, now + item + 1)));
            expected.add(id);
        }
        var other = imported(stores, "other", "other-parent", "other-item", "unit", 0,
            AwaitInteractionStatus.WAITING, now, 0);
        await(stores.interactions().complete(completion(other, now + 1)));
        for (int limit : List.of(1, 17, 100)) {
            var admissions = enumerate(stores, "tenant", limit);
            assertEquals(103, admissions.size());
            assertEquals(expected, admissions.stream().map(AwaitCompletionAdmission::interactionId)
                .collect(java.util.stream.Collectors.toSet()));
            assertEquals(103, admissions.stream().map(fact -> fact.itemIndex().orElseThrow()).distinct().count());
            assertTrue(admissions.stream().allMatch(fact -> fact.unitId().equals(Optional.of(unit))));
        }
        assertTrue(ack(stores, "tenant", parent.executionId(), unit).isEmpty(),
            "per-item admissions alone must not masquerade as a final unit acknowledgement");
        assertEquals(ExecutionStatus.WAITING_EXTERNAL,
            await(stores.executions().getExecution("tenant", parent.executionId())).orElseThrow().status());
    }

    @Test
    void tenantCursorAndFactScopeCannotRetireOrObserveAnotherTenant() {
        Stores stores = stores().get();
        long now = System.currentTimeMillis();
        var parent = waiting(stores, "tenant", "unit", now);
        String unit = parent.awaitUnitId();
        for (String tenant : List.of("tenant", "other")) {
            for (String id : List.of("one", "two")) {
                var record = imported(stores, tenant, parent.executionId(), id, tenant.equals("tenant") ? unit : "unit", 0, AwaitInteractionStatus.WAITING, now, 0);
                await(stores.interactions().complete(completion(record, now + 1)));
            }
        }
        var page = await(stores.interactions().findOpenCompletions("tenant", 1, Optional.empty()));
        var cursor = page.nextCursor().orElseThrow();
        assertThrows(IllegalArgumentException.class,
            () -> await(stores.interactions().findOpenCompletions("other", 1, Optional.of(cursor))));
        for (int invalid : List.of(0, 101)) assertThrows(IllegalArgumentException.class,
            () -> await(stores.interactions().findOpenCompletions("tenant", invalid, Optional.empty())));
        assertThrows(IllegalArgumentException.class, () -> await(stores.interactions().findOpenCompletions(
            "tenant", 1, Optional.of(new AwaitCompletionCursor("tenant", "invalid-provider-position")))));
        var retained = await(stores.interactions().getCompletionAdmission("tenant", "one")).orElseThrow();
        var forged = new AwaitCompletionAdmission(retained.schemaVersion(), retained.tenantId(),
            retained.executionId(), retained.interactionId(), retained.unitId(), retained.stepId(), retained.stepIndex(),
            retained.itemIndex(), retained.causationId(), retained.firstAdmittedAtEpochMs() + 1, retained.ttlEpochS());
        await(stores.executions().markAwaitCompleted("tenant", parent.executionId(), unit, 3, now + 10));
        assertTrue(ack(stores, "tenant", parent.executionId(), unit).isPresent());
        assertThrows(RuntimeException.class, () -> await(stores.interactions().retireCompletionAdmission(forged)));
        assertEquals(2, enumerate(stores, "tenant", 1).size());
        assertEquals(2, enumerate(stores, "other", 1).size());
    }

    @Test
    void delimiterIdentitiesRemainDistinctAcrossPointReadsAndProjectionRetirement() {
        Stores stores = stores().get();
        long now = System.currentTimeMillis();
        var parentA = waiting(stores, "scope", "unit", now);
        var parentB = waiting(stores, "scope#a", "unit", now);
        var a = imported(stores, "scope", parentA.executionId(), "a#b", parentA.awaitUnitId(), 0, AwaitInteractionStatus.WAITING, now, 0);
        var b = imported(stores, "scope#a", parentB.executionId(), "b", parentB.awaitUnitId(), 0, AwaitInteractionStatus.WAITING, now, 0);
        await(stores.interactions().complete(completion(a, now + 1)));
        await(stores.interactions().complete(completion(b, now + 2)));
        var af = fact(stores, a);
        var bf = fact(stores, b);
        assertNotEquals(af, bf);
        assertEquals(List.of(af), enumerate(stores, a.tenantId(), 1));
        assertEquals(List.of(bf), enumerate(stores, b.tenantId(), 1));
        await(stores.executions().markAwaitCompleted(a.tenantId(), parentA.executionId(), parentA.awaitUnitId(), 3, now + 10));
        assertTrue(ack(stores, a.tenantId(), parentA.executionId(), parentA.awaitUnitId()).isPresent());
        await(stores.interactions().retireCompletionAdmission(af));
        assertTrue(enumerate(stores, a.tenantId(), 1).isEmpty());
        assertEquals(List.of(bf), enumerate(stores, b.tenantId(), 1));
        assertEquals(af, fact(stores, a));
    }

    @Test
    void mixedItemAdmissionThenTimeoutRetainsTerminalAcknowledgementWithoutResumeClaim() {
        Supplier<Stores> factory = stores();
        long now = System.currentTimeMillis();
        var parent = waiting(factory.get(), "tenant", "unit", now);
        String unit = parent.awaitUnitId();
        var completed = imported(factory.get(), "tenant", parent.executionId(), "done", unit, 0,
            AwaitInteractionStatus.WAITING, now, 0);
        var pending = imported(factory.get(), "tenant", parent.executionId(), "pending", unit, 1,
            AwaitInteractionStatus.WAITING, now, 0);
        await(factory.get().interactions().complete(completion(completed, now + 1)));
        var admitted = fact(factory.get(), completed);
        assertTrue(await(factory.get().interactions().getCompletionAdmission("tenant", pending.interactionId())).isEmpty());
        long timeoutAt = pending.deadlineEpochMs() + 1;
        assertEquals(AwaitInteractionStatus.TIMED_OUT, await(factory.get().interactions().markTimedOut(
            "tenant", pending.interactionId(), pending.version(), timeoutAt)).orElseThrow().status());
        assertTrue(await(factory.get().executions().markTerminalFailure("tenant", parent.executionId(),
            parent.version() + 1, ExecutionStatus.FAILED, "timeout", "AWAIT_TIMEOUT", "pending timed out", timeoutAt)).isEmpty());
        assertTrue(ack(factory.get(), "tenant", parent.executionId(), unit).isEmpty());
        var failed = await(factory.get().executions().markTerminalFailure("tenant", parent.executionId(),
            parent.version(), ExecutionStatus.FAILED, "timeout", "AWAIT_TIMEOUT", "pending timed out", timeoutAt)).orElseThrow();
        var firstAck = ack(factory.get(), "tenant", parent.executionId(), unit).orElseThrow();
        assertEquals(AwaitReleaseDisposition.TERMINAL_FAILED, firstAck.disposition());
        assertEquals(failed.version(), firstAck.parentVersion());
        assertEquals(timeoutAt, firstAck.acknowledgedAtEpochMs());
        assertTrue(await(factory.get().executions().markTerminalFailure("tenant", parent.executionId(),
            parent.version(), ExecutionStatus.FAILED, "timeout", "AWAIT_TIMEOUT", "pending timed out", timeoutAt + 1)).isEmpty());
        assertEquals(firstAck, ack(factory.get(), "tenant", parent.executionId(), unit).orElseThrow());
        await(factory.get().interactions().retireCompletionAdmission(admitted));
        assertTrue(enumerate(factory.get(), "tenant", 1).isEmpty());
        assertEquals(admitted, fact(factory.get(), completed));
        assertEquals(ExecutionStatus.FAILED, await(factory.get().executions().getExecution("tenant", parent.executionId()))
            .orElseThrow().status());
    }

    @Test
    void completedLegacyImportAndItsDuplicateCannotInventAnAdmissionClock() {
        Stores stores = stores().get();
        long now = System.currentTimeMillis();
        var legacy = imported(stores, "tenant", "parent", "legacy", "unit", 0,
            AwaitInteractionStatus.COMPLETED, now, 0);
        assertTrue(await(stores.interactions().getCompletionAdmission("tenant", "legacy")).isEmpty());
        assertTrue(enumerate(stores, "tenant", 1).isEmpty());
        assertTrue(await(stores.interactions().complete(completion(legacy, now + 1))).duplicate());
        assertTrue(await(stores.interactions().getCompletionAdmission("tenant", "legacy")).isEmpty(),
            "legacy unknown must remain unknown; a replay clock is not the first admission clock");
        assertTrue(enumerate(stores, "tenant", 1).isEmpty());
        assertEquals(legacy, await(stores.interactions().get("tenant", "legacy")).orElseThrow());
    }

    protected static AwaitInteractionRecord imported(Stores stores, String tenant, String parent,
        String id, String unit, int item, AwaitInteractionStatus status, long now, long ttl) {
        return imported(stores, tenant, parent, id, unit, item, status, now, ttl, Map.of());
    }

    private static AwaitInteractionRecord imported(Stores stores, String tenant, String parent,
        String id, String unit, int item, AwaitInteractionStatus status, long now, long ttl, Map<String, Object> metadata) {
        var occurrence = AwaitUnitOccurrence.fromUnitId(unit);
        String step = occurrence.map(AwaitUnitOccurrence::stepId).orElse("AwaitApproval");
        int stepIndex = occurrence.map(AwaitUnitOccurrence::stepIndex).orElse(2);
        var record = new AwaitInteractionRecord(tenant, parent, step, stepIndex, String.class.getName(),
            id, "correlation-" + id, "cause-" + id, "request-" + id, 0, status, "request", "response",
            unit, item, "actor", "reviewer", "reviewers", "test", metadata, now + 60_000, now, now, ttl);
        return await(stores.interactions().importRecord(record));
    }

    protected static AwaitCompletionCommand completion(AwaitInteractionRecord record, long now) {
        return new AwaitCompletionCommand(record.tenantId(), record.interactionId(), record.correlationId(),
            "completion-" + record.interactionId(), "approved", "actor", now);
    }

    protected static AwaitCompletionAdmission fact(Stores stores, AwaitInteractionRecord record) {
        return await(stores.interactions().getCompletionAdmission(record.tenantId(), record.interactionId())).orElseThrow();
    }

    protected static ExecutionRecord<Object, Object> waiting(Stores stores, String tenant, String unit, long now) {
        var created = await(stores.executions().createOrGetExecution(new ExecutionCreateCommand(tenant,
            "parent", "pipeline", "contract", "release", new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
            ExecutionResultShape.SINGLE, now, now / 1000 + 3600))).record();
        return await(stores.executions().markWaitingExternal(tenant, created.executionId(), created.version(),
            "await", qualifiedUnit(created, "AwaitApproval", 2), 2, now)).orElseThrow();
    }

    protected static String qualifiedUnit(ExecutionRecord<?, ?> parent, String stepId, int stepIndex) {
        return new AwaitUnitOccurrence(parent.tenantId(), parent.executionId(), parent.executionId(),
            stepId, stepIndex, parent.awaitOccurrenceCause().orElseThrow()).unitId();
    }

    protected static Optional<AwaitUnitReleaseAcknowledgement> ack(Stores stores, String tenant, String parent, String unit) {
        return await(stores.executions().getAwaitUnitReleaseAcknowledgement(tenant, parent, unit));
    }

    protected static List<AwaitCompletionAdmission> enumerate(Stores stores, String tenant, int limit) {
        var result = new ArrayList<AwaitCompletionAdmission>();
        Optional<AwaitCompletionCursor> cursor = Optional.empty();
        Set<AwaitCompletionCursor> seen = new HashSet<>();
        do {
            var page = await(stores.interactions().findOpenCompletions(tenant, limit, cursor));
            assertTrue(page.evaluatedEntryCount() <= limit);
            assertTrue(page.admissions().stream().allMatch(fact -> tenant.equals(fact.tenantId())));
            result.addAll(page.admissions());
            cursor = page.nextCursor();
            cursor.ifPresent(next -> assertTrue(seen.add(next), "continuation must progress"));
            assertTrue(seen.size() <= 110, "bounded unchanged fixture must terminate");
        } while (cursor.isPresent());
        assertEquals(result.size(), result.stream().map(AwaitCompletionAdmission::interactionId).distinct().count());
        return result;
    }

    protected static <T> T await(Uni<T> operation) {
        return operation.await().atMost(WAIT);
    }
}

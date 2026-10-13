package org.pipelineframework.awaitable.store;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitUnitCreateCommand;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;

/**
 * In-memory await unit store for local development and tests.
 */
@ApplicationScoped
public class InMemoryAwaitUnitStore implements AwaitUnitStore {

    private final Object lock = new Object();
    private final Map<String, AwaitUnitRecord> unitsByScopedId = new HashMap<>();
    private final Map<String, Set<String>> completedItemsByScopedId = new HashMap<>();

    @Override
    public String providerName() {
        return "memory";
    }

    @Override
    public int priority() {
        return -100;
    }

    @Override
    public Uni<AwaitUnitRecord> createOrGet(AwaitUnitCreateCommand command) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(command.nowEpochMs());
                String scopedId = scopedUnitId(command.tenantId(), command.unitId());
                AwaitUnitRecord existing = unitsByScopedId.get(scopedId);
                if (existing != null) {
                    return requireSameOwner(existing, command.tenantId(), command.unitId(), command.executionId(),
                        command.stepId(), command.stepIndex(), command.cardinality());
                }
                AwaitUnitRecord created = new AwaitUnitRecord(
                    command.tenantId(),
                    command.unitId(),
                    command.executionId(),
                    command.stepId(),
                    command.stepIndex(),
                    command.cardinality(),
                    0L,
                    AwaitUnitStatus.WAITING_EXTERNAL,
                    null,
                    null,
                    0,
                    java.util.Set.of(),
                    false,
                    command.nowEpochMs(),
                    command.nowEpochMs(),
                    command.ttlEpochS());
                unitsByScopedId.put(scopedId, created);
                return created;
            }
        });
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> get(String tenantId, String unitId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(System.currentTimeMillis());
                return Optional.ofNullable(unitsByScopedId.get(scopedUnitId(tenantId, unitId)));
            }
        });
    }

    @Override
    public Uni<AwaitUnitRecord> importRecord(AwaitUnitRecord record) {
        return Uni.createFrom().item(() -> {
            if (record == null) {
                throw new IllegalArgumentException("Await unit record is required");
            }
            synchronized (lock) {
                purgeExpired(System.currentTimeMillis());
                String scopedId = scopedUnitId(record.tenantId(), record.unitId());
                AwaitUnitRecord existing = unitsByScopedId.get(scopedId);
                if (existing != null) {
                    return requireImportedOwner(existing, record);
                }
                unitsByScopedId.put(scopedId, record);
                completedItemsByScopedId.put(scopedId, new HashSet<>(record.completedItemKeys()));
                return record;
            }
        });
    }

    private AwaitUnitRecord requireSameOwner(AwaitUnitRecord existing, String tenantId, String unitId,
        String executionId, String stepId, int stepIndex, String cardinality) {
        if (!existing.tenantId().equals(tenantId) || !existing.unitId().equals(unitId)
            || !existing.executionId().equals(executionId) || !existing.stepId().equals(stepId)
            || existing.stepIndex() != stepIndex || !existing.cardinality().equals(cardinality)) {
            throw new IllegalStateException("Await unit identity conflicts with its immutable owner");
        }
        return existing;
    }

    private AwaitUnitRecord requireImportedOwner(AwaitUnitRecord existing, AwaitUnitRecord incoming) {
        requireSameOwner(existing, incoming.tenantId(), incoming.unitId(), incoming.executionId(),
            incoming.stepId(), incoming.stepIndex(), incoming.cardinality());
        if (org.pipelineframework.awaitable.AwaitUnitOccurrence.fromUnitId(incoming.unitId()).isPresent()
            && incoming.primaryInteractionId() != null
            && !incoming.primaryInteractionId().equals(existing.primaryInteractionId())) {
            throw new IllegalStateException("Qualified Await import conflicts with retained primary membership");
        }
        return existing;
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> attachPrimaryInteraction(
        String tenantId,
        String unitId,
        String interactionId,
        long nowEpochMs) {
        var occurrence = org.pipelineframework.awaitable.AwaitUnitOccurrence.fromUnitId(unitId);
        if (occurrence.isPresent()) {
            if (interactionId == null || interactionId.isBlank() || !occurrence.orElseThrow().tenantId().equals(tenantId)) {
                return Uni.createFrom().failure(new IllegalArgumentException("Qualified primary attachment requires its tenant and interaction"));
            }
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    purgeExpired(nowEpochMs);
                    String scopedId = scopedUnitId(tenantId, unitId);
                    AwaitUnitRecord current = unitsByScopedId.get(scopedId);
                    if (current == null) return Optional.empty();
                    if (interactionId.equals(current.primaryInteractionId())) return Optional.of(current);
                    if (current.primaryInteractionId() != null || current.status().terminal() || current.dispatchComplete()) {
                        return Optional.empty();
                    }
                    AwaitUnitRecord attached = withPrimary(current, interactionId, nowEpochMs);
                    unitsByScopedId.put(scopedId, attached);
                    return Optional.of(attached);
                }
            });
        }
        return update(tenantId, unitId, nowEpochMs, current -> withPrimary(current, interactionId, nowEpochMs));
    }

    private AwaitUnitRecord withPrimary(AwaitUnitRecord current, String interactionId, long nowEpochMs) {
        return new AwaitUnitRecord(
            current.tenantId(),
            current.unitId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.cardinality(),
            current.version() + 1,
            current.status(),
            interactionId,
            current.expectedItemCount(),
            current.completedItemCount(),
            current.completedItemKeys(),
            current.dispatchComplete(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS());
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> markDispatchComplete(
        String tenantId,
        String unitId,
        int expectedItemCount,
        long nowEpochMs) {
        return update(tenantId, unitId, nowEpochMs, current -> {
            AwaitUnitStatus status = current.completedItemCount() == expectedItemCount
                ? AwaitUnitStatus.COMPLETED
                : current.status();
            return new AwaitUnitRecord(
                current.tenantId(),
                current.unitId(),
                current.executionId(),
                current.stepId(),
                current.stepIndex(),
                current.cardinality(),
                current.version() + 1,
                status,
                current.primaryInteractionId(),
                expectedItemCount,
                current.completedItemCount(),
                current.completedItemKeys(),
                true,
                current.createdAtEpochMs(),
                nowEpochMs,
                current.ttlEpochS());
        });
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> recordItemCompleted(
        String tenantId,
        String unitId,
        String itemCompletionKey,
        long nowEpochMs) {
        if (itemCompletionKey == null || itemCompletionKey.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("itemCompletionKey must not be blank"));
        }
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                String scopedId = scopedUnitId(tenantId, unitId);
                AwaitUnitRecord current = unitsByScopedId.get(scopedId);
                if (current == null || current.status().terminal()) {
                    return Optional.ofNullable(current);
                }
                Set<String> completedItems = completedItemsByScopedId.computeIfAbsent(scopedId, ignored -> new HashSet<>());
                if (!completedItems.add(itemCompletionKey)) {
                    return Optional.of(current);
                }
                int completed = current.completedItemCount() + 1;
                boolean terminal = current.dispatchComplete()
                    && current.expectedItemCount() != null
                    && completed >= current.expectedItemCount();
                AwaitUnitRecord updated = new AwaitUnitRecord(
                    current.tenantId(),
                    current.unitId(),
                    current.executionId(),
                    current.stepId(),
                    current.stepIndex(),
                    current.cardinality(),
                    current.version() + 1,
                    terminal ? AwaitUnitStatus.COMPLETED : current.status(),
                    current.primaryInteractionId(),
                    current.expectedItemCount(),
                    completed,
                    java.util.Set.copyOf(completedItems),
                    current.dispatchComplete(),
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS());
                unitsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> recordItemContinuationCompleted(
        String tenantId,
        String unitId,
        String continuationCompletionKey,
        long nowEpochMs) {
        if (continuationCompletionKey == null || continuationCompletionKey.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("continuationCompletionKey must not be blank"));
        }
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                String scopedId = scopedUnitId(tenantId, unitId);
                AwaitUnitRecord current = unitsByScopedId.get(scopedId);
                if (current == null || (current.status().terminal() && current.status() != AwaitUnitStatus.COMPLETED)) {
                    return Optional.ofNullable(current);
                }
                Set<String> completedItems = completedItemsByScopedId.computeIfAbsent(scopedId, ignored -> new HashSet<>());
                if (!completedItems.add(continuationCompletionKey)) {
                    return Optional.of(current);
                }
                AwaitUnitRecord updated = new AwaitUnitRecord(
                    current.tenantId(),
                    current.unitId(),
                    current.executionId(),
                    current.stepId(),
                    current.stepIndex(),
                    current.cardinality(),
                    current.version() + 1,
                    current.status(),
                    current.primaryInteractionId(),
                    current.expectedItemCount(),
                    current.completedItemCount(),
                    Set.copyOf(completedItems),
                    current.dispatchComplete(),
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS());
                unitsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> markCompleted(String tenantId, String unitId, long nowEpochMs) {
        return update(tenantId, unitId, nowEpochMs, current -> new AwaitUnitRecord(
            current.tenantId(),
            current.unitId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.cardinality(),
            current.version() + 1,
            AwaitUnitStatus.COMPLETED,
            current.primaryInteractionId(),
            current.expectedItemCount(),
            current.completedItemCount(),
            current.completedItemKeys(),
            current.dispatchComplete(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS()));
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> markTerminal(
        String tenantId,
        String unitId,
        AwaitUnitStatus status,
        long nowEpochMs) {
        return update(tenantId, unitId, nowEpochMs, current -> new AwaitUnitRecord(
            current.tenantId(),
            current.unitId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.cardinality(),
            current.version() + 1,
            status,
            current.primaryInteractionId(),
            current.expectedItemCount(),
            current.completedItemCount(),
            current.completedItemKeys(),
            current.dispatchComplete(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS()));
    }

    private Uni<Optional<AwaitUnitRecord>> update(
        String tenantId,
        String unitId,
        long nowEpochMs,
        java.util.function.Function<AwaitUnitRecord, AwaitUnitRecord> updater) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                String scopedId = scopedUnitId(tenantId, unitId);
                AwaitUnitRecord current = unitsByScopedId.get(scopedId);
                if (current == null || current.status().terminal()) {
                    return Optional.ofNullable(current);
                }
                AwaitUnitRecord updated = updater.apply(current);
                unitsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    private void purgeExpired(long nowEpochMs) {
        long nowEpochS = Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
        unitsByScopedId.entrySet().removeIf(entry -> {
            AwaitUnitRecord record = entry.getValue();
            boolean expired = record.ttlEpochS() > 0 && record.ttlEpochS() <= nowEpochS;
            if (expired) {
                completedItemsByScopedId.remove(entry.getKey());
            }
            return expired;
        });
    }

    private static String scopedUnitId(String tenantId, String unitId) {
        return tenantId + "::" + unitId;
    }
}

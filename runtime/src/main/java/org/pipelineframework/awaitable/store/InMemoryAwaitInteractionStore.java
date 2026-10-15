package org.pipelineframework.awaitable.store;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionAdmission;
import org.pipelineframework.awaitable.AwaitCompletionCursor;
import org.pipelineframework.awaitable.AwaitCompletionPage;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitCreateResult;
import org.pipelineframework.awaitable.AwaitInteractionNotFoundException;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionCursor;
import org.pipelineframework.awaitable.AwaitInteractionPage;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitInteractionTerminalException;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.AwaitItemContinuationCommand;

/**
 * In-memory await store intended for local development and tests.
 */
@ApplicationScoped
public class InMemoryAwaitInteractionStore implements
    AwaitInteractionStore,
    AwaitItemContinuationWorkStore,
    AwaitInteractionExecutionLookup {

    private static final Comparator<AwaitInteractionRecord> PENDING_ORDER =
        Comparator.comparingLong(AwaitInteractionRecord::deadlineEpochMs)
            .thenComparingLong(AwaitInteractionRecord::createdAtEpochMs)
            .thenComparing(AwaitInteractionRecord::interactionId);

    private final Object lock = new Object();
    private final Map<String, AwaitInteractionRecord> interactionsByScopedId = new HashMap<>();
    private final Map<String, java.util.NavigableMap<UnitPosition, String>> interactionIdsByUnit = new HashMap<>();
    private final Map<String, AwaitCompletionAdmission> completionAdmissions = new HashMap<>();
    private final Map<String, java.util.NavigableMap<String, AwaitCompletionAdmission>> openCompletionsByTenant = new HashMap<>();
    private final Map<String, String> interactionIdByScopedIdempotencyKey = new HashMap<>();
    private final Map<String, String> interactionIdByScopedCorrelation = new HashMap<>();
    private final Map<String, AwaitItemContinuationCommand> itemContinuationWorkByInteraction = new HashMap<>();
    private final java.util.NavigableSet<AwaitInteractionRecord> timeoutConsequences = new java.util.TreeSet<>(
        Comparator.comparingLong(AwaitInteractionRecord::updatedAtEpochMs)
            .thenComparing(AwaitInteractionRecord::tenantId).thenComparing(AwaitInteractionRecord::interactionId));

    @Override
    public String providerName() {
        return "memory";
    }

    @Override
    public int priority() {
        return -100;
    }

    @Override
    public Uni<AwaitCreateResult> createOrGet(AwaitCreateCommand command) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(command.nowEpochMs());
                String scopedKey = scopedIdempotencyKey(command.tenantId(), command.stepId(), command.idempotencyKey());
                String existingId = interactionIdByScopedIdempotencyKey.get(scopedKey);
                if (existingId != null) {
                    AwaitInteractionRecord existing = interactionsByScopedId.get(scopedInteractionId(command.tenantId(), existingId));
                    if (existing != null && (!existing.status().terminal() || existing.commandCallback()
                        || org.pipelineframework.awaitable.AwaitUnitOccurrence.fromUnitId(existing.unitId()).isPresent())) {
                        if (org.pipelineframework.awaitable.AwaitUnitOccurrence.fromUnitId(existing.unitId()).isPresent()
                            || org.pipelineframework.awaitable.AwaitUnitOccurrence.fromUnitId(command.unitId()).isPresent()) {
                            requireSameOwner(existing, command.tenantId(), command.unitId(), command.executionId(), command.stepId(),
                                command.stepIndex(), command.itemIndex(), command.idempotencyKey(), command.correlationId());
                        }
                        return new AwaitCreateResult(existing, true);
                    }
                    if (existing != null && org.pipelineframework.awaitable.AwaitUnitOccurrence.fromUnitId(command.unitId()).isPresent()) {
                        requireSameOwner(existing, command.tenantId(), command.unitId(), command.executionId(), command.stepId(),
                            command.stepIndex(), command.itemIndex(), command.idempotencyKey(), command.correlationId());
                    }
                }
                String interactionId = UUID.randomUUID().toString();
                AwaitInteractionRecord created = new AwaitInteractionRecord(
                    command.tenantId(),
                    command.executionId(),
                    command.stepId(),
                    command.stepIndex(),
                    command.outputType(),
                    interactionId,
                    command.correlationId(),
                    command.causationId(),
                    command.idempotencyKey(),
                    0L,
                    AwaitInteractionStatus.WAITING,
                    command.requestPayload(),
                    null,
                    command.unitId(),
                    command.itemIndex(),
                    null,
                    command.assignee(),
                    command.group(),
                    command.transportType(),
                    command.transportMetadata(),
                    command.deadlineEpochMs(),
                    command.nowEpochMs(),
                    command.nowEpochMs(),
                    command.ttlEpochS(),
                    command.transportOutputType());
                requireAvailableLookup(interactionIdByScopedCorrelation.get(
                    scopedCorrelation(created.tenantId(), created.correlationId())), created);
                interactionsByScopedId.put(scopedInteractionId(created.tenantId(), created.interactionId()), created);
                indexUnitInteraction(created);
                interactionIdByScopedIdempotencyKey.put(scopedKey, interactionId);
                interactionIdByScopedCorrelation.put(scopedCorrelation(command.tenantId(), command.correlationId()), interactionId);
                return new AwaitCreateResult(created, false);
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> get(String tenantId, String interactionId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                return Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(tenantId, interactionId)));
            }
        });
    }

    @Override
    public Uni<AwaitInteractionRecord> importRecord(AwaitInteractionRecord record) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                String scopedId = scopedInteractionId(record.tenantId(), record.interactionId());
                requireAvailableLookup(interactionIdByScopedIdempotencyKey.get(
                    scopedIdempotencyKey(record.tenantId(), record.stepId(), record.idempotencyKey())), record);
                requireAvailableLookup(interactionIdByScopedCorrelation.get(
                    scopedCorrelation(record.tenantId(), record.correlationId())), record);
                AwaitInteractionRecord existing = interactionsByScopedId.get(scopedId);
                if (existing != null) {
                    requireSameOwner(existing, record.tenantId(), record.unitId(), record.executionId(), record.stepId(),
                        record.stepIndex(), record.itemIndex(), record.idempotencyKey(), record.correlationId());
                    return existing;
                }
                interactionsByScopedId.put(scopedId, record);
                if (record.status() == AwaitInteractionStatus.TIMED_OUT) timeoutConsequences.add(record);
                indexUnitInteraction(record);
                interactionIdByScopedIdempotencyKey.put(
                    scopedIdempotencyKey(record.tenantId(), record.stepId(), record.idempotencyKey()),
                    record.interactionId());
                interactionIdByScopedCorrelation.put(
                    scopedCorrelation(record.tenantId(), record.correlationId()),
                    record.interactionId());
                return record;
            }
        });
    }

    private void requireSameOwner(AwaitInteractionRecord existing, String tenantId, String unitId,
        String executionId, String stepId, int stepIndex, Integer itemIndex, String idempotencyKey, String correlationId) {
        if (!existing.tenantId().equals(tenantId) || !Objects.equals(existing.unitId(), unitId)
            || !existing.executionId().equals(executionId) || !existing.stepId().equals(stepId)
            || existing.stepIndex() != stepIndex || !Objects.equals(existing.itemIndex(), itemIndex)
            || !Objects.equals(existing.idempotencyKey(), idempotencyKey) || !Objects.equals(existing.correlationId(), correlationId)) {
            throw new IllegalStateException("Await interaction identity conflicts with its immutable owner");
        }
    }

    private void requireAvailableLookup(String indexedId, AwaitInteractionRecord record) {
        if (indexedId != null && !indexedId.equals(record.interactionId())) {
            AwaitInteractionRecord indexed = interactionsByScopedId.get(scopedInteractionId(record.tenantId(), indexedId));
            if (org.pipelineframework.awaitable.AwaitUnitOccurrence.fromUnitId(record.unitId()).isPresent()
                || (indexed != null && org.pipelineframework.awaitable.AwaitUnitOccurrence.fromUnitId(indexed.unitId()).isPresent())) {
                throw new IllegalStateException("Qualified Await lookup belongs to a different interaction");
            }
        }
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> findByCorrelation(String tenantId, String correlationId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                String interactionId = interactionIdByScopedCorrelation.get(scopedCorrelation(tenantId, correlationId));
                return interactionId == null
                    ? Optional.empty()
                    : Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(tenantId, interactionId)));
            }
        });
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findByUnit(
        String tenantId,
        String unitId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                return interactionsByScopedId.values().stream()
                    .filter(record -> Objects.equals(record.tenantId(), tenantId))
                    .filter(record -> Objects.equals(record.unitId(), unitId))
                    .sorted(Comparator
                        .comparingInt((AwaitInteractionRecord record) -> record.itemIndex() == null
                            ? Integer.MAX_VALUE
                            : record.itemIndex())
                        .thenComparing(record -> nullToEmpty(record.causationId()))
                        .thenComparing(AwaitInteractionRecord::interactionId))
                    .toList();
            }
        });
    }

    @Override
    public Uni<AwaitInteractionPage> findByUnitPage(
        String tenantId, String unitId, int limit, Optional<AwaitInteractionCursor> cursor) {
        return Uni.createFrom().item(() -> {
            if (limit < 1 || limit > 100) {
                throw new IllegalArgumentException("Await page limit must be between 1 and 100");
            }
            Objects.requireNonNull(cursor, "cursor").ifPresent(value -> value.requireScope(tenantId, unitId));
            synchronized (lock) {
                var index = interactionIdsByUnit.getOrDefault(scopedUnitId(tenantId, unitId), new java.util.TreeMap<>());
                var remaining = cursor.map(value -> index.tailMap(UnitPosition.decode(value.position()), false)).orElse(index);
                var entries = remaining.entrySet().iterator();
                List<AwaitInteractionRecord> page = new ArrayList<>();
                Optional<UnitPosition> lastPosition = Optional.empty();
                long nowEpochS = Instant.now().getEpochSecond();
                for (int evaluated = 0; evaluated < limit && entries.hasNext(); evaluated++) {
                    var entry = entries.next();
                    lastPosition = Optional.of(entry.getKey());
                    var record = interactionsByScopedId.get(scopedInteractionId(tenantId, entry.getValue()));
                    if (record != null && (record.ttlEpochS() <= 0 || record.ttlEpochS() > nowEpochS)) {
                        page.add(record);
                    }
                }
                Optional<AwaitInteractionCursor> next = entries.hasNext()
                    ? lastPosition.map(position -> new AwaitInteractionCursor(tenantId, unitId, position.encode()))
                    : Optional.empty();
                return new AwaitInteractionPage(page, next);
            }
        });
    }

    private void indexUnitInteraction(AwaitInteractionRecord record) {
        if (record.unitId() != null && !record.unitId().isBlank()) {
            interactionIdsByUnit.computeIfAbsent(scopedUnitId(record.tenantId(), record.unitId()),
                ignored -> new java.util.TreeMap<>()).put(unitPosition(record), record.interactionId());
        }
    }

    private static String scopedUnitId(String tenantId, String unitId) {
        return compositeScopedKey("tenantId", tenantId, "unitId", unitId);
    }

    private static UnitPosition unitPosition(AwaitInteractionRecord record) {
        return new UnitPosition(record.itemIndex() == null ? Integer.MAX_VALUE : record.itemIndex(),
            nullToEmpty(record.causationId()), record.interactionId());
    }

    /** Tuple identity must not alias when imported identifiers contain delimiters. */
    private record UnitPosition(int itemIndex, String causationId, String interactionId)
        implements Comparable<UnitPosition> {
        private UnitPosition {
            Objects.requireNonNull(causationId, "causationId");
            Objects.requireNonNull(interactionId, "interactionId");
        }

        @Override
        public int compareTo(UnitPosition other) {
            int itemOrder = Integer.compare(itemIndex, other.itemIndex);
            if (itemOrder != 0) return itemOrder;
            int causeOrder = causationId.compareTo(other.causationId);
            return causeOrder != 0 ? causeOrder : interactionId.compareTo(other.interactionId);
        }

        private String encode() {
            try {
                return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                    PipelineJson.mapper().writeValueAsBytes(List.of(Integer.toString(itemIndex), causationId, interactionId)));
            } catch (java.io.IOException failure) {
                throw new IllegalArgumentException("Cannot encode Await unit cursor", failure);
            }
        }

        private static UnitPosition decode(String encoded) {
            try {
                List<String> values = PipelineJson.mapper().readValue(
                    java.util.Base64.getUrlDecoder().decode(encoded),
                    new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {});
                if (values.size() != 3 || values.stream().anyMatch(Objects::isNull)) {
                    throw new IllegalArgumentException("Await unit cursor has invalid position");
                }
                return new UnitPosition(Integer.parseInt(values.get(0)), values.get(1), values.get(2));
            } catch (java.io.IOException failure) {
                throw new IllegalArgumentException("Cannot decode Await unit cursor", failure);
            }
        }
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findByExecution(
        String tenantId,
        String executionId,
        int limit
    ) {
        if (limit <= 0) {
            return Uni.createFrom().item(List.of());
        }
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(System.currentTimeMillis());
                return interactionsByScopedId.values().stream()
                    .filter(record -> Objects.equals(record.tenantId(), tenantId))
                    .filter(record -> Objects.equals(record.executionId(), executionId))
                    .sorted(Comparator.comparingLong(AwaitInteractionRecord::createdAtEpochMs)
                        .thenComparing(AwaitInteractionRecord::interactionId)
                        .reversed())
                    .limit(limit)
                    .toList();
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markDispatching(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs,
            AwaitInteractionStatus.WAITING,
            current -> updateStatus(current, AwaitInteractionStatus.DISPATCHING, nowEpochMs, null, null));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markDispatching(
        String tenantId,
        String interactionId,
        long expectedVersion,
        Map<String, Object> transportMetadata,
        long nowEpochMs) {
        Map<String, Object> safeMetadata = transportMetadata == null ? Map.of() : Map.copyOf(transportMetadata);
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs,
            AwaitInteractionStatus.WAITING,
            current -> updateStatus(
                current, AwaitInteractionStatus.DISPATCHING, nowEpochMs, null, null, safeMetadata));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markDispatched(
        String tenantId,
        String interactionId,
        long expectedVersion,
        Map<String, Object> transportMetadata,
        long nowEpochMs) {
        Map<String, Object> safeMetadata = transportMetadata == null ? Map.of() : Map.copyOf(transportMetadata);
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs,
            AwaitInteractionStatus.DISPATCHING,
            current -> new AwaitInteractionRecord(
            current.tenantId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.outputType(),
            current.interactionId(),
            current.correlationId(),
            current.causationId(),
            current.idempotencyKey(),
            current.version() + 1,
            AwaitInteractionStatus.DISPATCHED,
            current.requestPayload(),
            current.responsePayload(),
            current.unitId(),
            current.itemIndex(),
            current.actor(),
            current.assignee(),
            current.group(),
            current.transportType(),
            safeMetadata,
            current.deadlineEpochMs(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS(),
            current.transportOutputType()));
    }

    @Override
    public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(command.nowEpochMs());
                AwaitInteractionRecord current = resolveForCompletion(command)
                    .orElseThrow(() -> new AwaitInteractionNotFoundException("No await interaction matches completion"));
                return completeResolvedLocked(current, command);
            }
        });
    }

    private AwaitCompletionResult completeResolvedLocked(
        AwaitInteractionRecord current,
        AwaitCompletionCommand command
    ) {
        if (current.status() == AwaitInteractionStatus.COMPLETED || current.status() == AwaitInteractionStatus.COMPLETION_OBSERVED) {
            projectItemContinuation(current, command.nowEpochMs());
            return new AwaitCompletionResult(current, true);
        }
        if (current.status().terminal()) {
            throw new AwaitInteractionTerminalException("Await interaction is terminal: " + current.status());
        }
        if (current.deadlineEpochMs() <= command.nowEpochMs()) {
            AwaitInteractionRecord timedOut = updateStatus(current, AwaitInteractionStatus.TIMED_OUT, command.nowEpochMs(), null, null);
            timeoutConsequences.add(timedOut);
            interactionsByScopedId.put(scopedInteractionId(timedOut.tenantId(), timedOut.interactionId()), timedOut);
            throw new AwaitInteractionTerminalException("Await interaction timed out before completion");
        }
        AwaitInteractionRecord completed = new AwaitInteractionRecord(
            current.tenantId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.outputType(),
            current.interactionId(),
            current.correlationId(),
            current.causationId(),
            current.idempotencyKey(),
            current.version() + 1,
            current.observedCompletionStatus(),
            current.requestPayload(),
            command.responsePayload(),
            current.unitId(),
            current.itemIndex(),
            command.actor(),
            current.assignee(),
            current.group(),
            current.transportType(),
            current.transportMetadata(),
            current.deadlineEpochMs(),
            current.createdAtEpochMs(),
            command.nowEpochMs(),
            current.ttlEpochS(),
            current.transportOutputType());
        String scopedId = scopedInteractionId(completed.tenantId(), completed.interactionId());
        // Construct and validate the fact before changing semantic state. The lock is the completion CAS boundary.
        AwaitCompletionAdmission admission = new AwaitCompletionAdmission(
            AwaitCompletionAdmission.CURRENT_SCHEMA_VERSION, completed.tenantId(), completed.executionId(),
            completed.interactionId(), Optional.ofNullable(completed.unitId()), completed.stepId(), completed.stepIndex(),
            completed.itemIndex() == null ? OptionalInt.empty() : OptionalInt.of(completed.itemIndex()),
            Optional.ofNullable(completed.causationId()), command.nowEpochMs(), completed.ttlEpochS());
        if (!completionAdmissions.containsKey(scopedId)) {
            completionAdmissions.put(scopedId, admission);
            openCompletionsByTenant.computeIfAbsent(admission.tenantId(), ignored -> new java.util.TreeMap<>())
                .put(admission.interactionId(), admission);
        }
        interactionsByScopedId.put(scopedId, completed);
        projectItemContinuation(completed, command.nowEpochMs());
        return new AwaitCompletionResult(completed, false);
    }

    @Override
    public Uni<Optional<AwaitCompletionAdmission>> getCompletionAdmission(String tenantId, String interactionId) {
        requireCompletionIdentity(tenantId, "tenantId");
        requireCompletionIdentity(interactionId, "interactionId");
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String key = scopedInteractionId(tenantId, interactionId);
                var admission = Optional.ofNullable(completionAdmissions.get(key));
                if (admission.filter(value -> completionExpired(value, Instant.now().getEpochSecond())).isPresent()) {
                    completionAdmissions.remove(key);
                    var open = openCompletionsByTenant.get(tenantId);
                    if (open != null) open.remove(interactionId);
                    return Optional.empty();
                }
                return admission;
            }
        });
    }

    @Override
    public Uni<AwaitCompletionPage> findOpenCompletions(
        String tenantId, int pageLimit, Optional<AwaitCompletionCursor> cursor) {
        requireCompletionIdentity(tenantId, "tenantId");
        if (pageLimit < 1 || pageLimit > 100) {
            throw new IllegalArgumentException("Completion page limit must be between 1 and 100");
        }
        Objects.requireNonNull(cursor, "cursor").ifPresent(value -> value.requireScope(tenantId));
        Optional<String> after = cursor.map(value -> decodeCompletionPosition(tenantId, value.position()));
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                var index = openCompletionsByTenant.getOrDefault(tenantId, new java.util.TreeMap<>());
                var remaining = after.map(value -> index.tailMap(value, false)).orElse(index);
                var entries = remaining.entrySet().iterator();
                var admissions = new ArrayList<AwaitCompletionAdmission>();
                Optional<String> last = Optional.empty();
                int evaluated = 0;
                long nowEpochS = Instant.now().getEpochSecond();
                while (evaluated < pageLimit && entries.hasNext()) {
                    var entry = entries.next();
                    evaluated++;
                    last = Optional.of(entry.getKey());
                    if (completionExpired(entry.getValue(), nowEpochS)) {
                        completionAdmissions.remove(scopedInteractionId(tenantId, entry.getKey()), entry.getValue());
                        entries.remove();
                    } else {
                        admissions.add(entry.getValue());
                    }
                }
                Optional<AwaitCompletionCursor> next = entries.hasNext()
                    ? last.map(value -> new AwaitCompletionCursor(tenantId, encodeCompletionPosition(tenantId, value)))
                    : Optional.empty();
                return new AwaitCompletionPage(admissions, next, evaluated);
            }
        });
    }

    @Override
    public Uni<Void> retireCompletionAdmission(AwaitCompletionAdmission admission) {
        Objects.requireNonNull(admission, "admission");
        return Uni.createFrom().voidItem().invoke(() -> {
            synchronized (lock) {
                var retained = completionAdmissions.get(scopedInteractionId(admission.tenantId(), admission.interactionId()));
                if (!admission.equals(retained)) {
                    throw new IllegalArgumentException("Completion retirement does not match the retained admission");
                }
                var open = openCompletionsByTenant.get(admission.tenantId());
                if (open != null) open.remove(admission.interactionId(), admission);
            }
        });
    }

    private static boolean completionExpired(AwaitCompletionAdmission admission, long nowEpochS) {
        return admission.ttlEpochS() > 0 && admission.ttlEpochS() <= nowEpochS;
    }

    private static void requireCompletionIdentity(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    }

    private static String encodeCompletionPosition(String tenantId, String interactionId) {
        try {
            return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                PipelineJson.mapper().writeValueAsBytes(List.of("memory-completion-v1", tenantId, interactionId)));
        } catch (java.io.IOException failure) {
            throw new IllegalArgumentException("Cannot encode completion cursor", failure);
        }
    }

    private static String decodeCompletionPosition(String tenantId, String position) {
        try {
            List<String> values = PipelineJson.mapper().readValue(java.util.Base64.getUrlDecoder().decode(position),
                new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {});
            if (values.size() != 3 || !"memory-completion-v1".equals(values.get(0))
                || !tenantId.equals(values.get(1))) {
                throw new IllegalArgumentException("Completion cursor has invalid provider position or tenant");
            }
            requireCompletionIdentity(values.get(2), "cursor interactionId");
            return values.get(2);
        } catch (java.io.IOException failure) {
            throw new IllegalArgumentException("Cannot decode completion cursor", failure);
        }
    }

    @Override
    public Uni<List<AwaitItemContinuationCommand>> findDueItemContinuations(long nowEpochMs, int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                return itemContinuationWorkByInteraction.values().stream()
                    .filter(command -> command.nowEpochMs() <= nowEpochMs)
                    .sorted(java.util.Comparator
                        .comparingLong(AwaitItemContinuationCommand::nowEpochMs)
                        .thenComparing(AwaitItemContinuationCommand::interactionId))
                    .limit(Math.max(0, limit))
                    .toList();
            }
        });
    }

    @Override
    public Uni<Void> rescheduleItemContinuation(
        AwaitItemContinuationCommand command,
        int nextAttempt,
        long dueEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String key = scopedInteractionId(command.tenantId(), command.interactionId());
                AwaitItemContinuationCommand current = itemContinuationWorkByInteraction.get(key);
                if (current != null && sameWork(current, command)) {
                    itemContinuationWorkByInteraction.put(key, new AwaitItemContinuationCommand(
                        command.tenantId(), command.executionId(), command.unitId(), command.interactionId(),
                        command.itemIndex(), nextAttempt, dueEpochMs));
                }
                return Boolean.TRUE;
            }
        }).replaceWithVoid();
    }

    @Override
    public Uni<Void> completeItemContinuation(AwaitItemContinuationCommand command) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String key = scopedInteractionId(command.tenantId(), command.interactionId());
                AwaitItemContinuationCommand current = itemContinuationWorkByInteraction.get(key);
                if (current != null && sameWork(current, command)) {
                    itemContinuationWorkByInteraction.remove(key);
                }
                return Boolean.TRUE;
            }
        }).replaceWithVoid();
    }

    private void projectItemContinuation(AwaitInteractionRecord record, long dueEpochMs) {
        if (!record.itemInteraction()) {
            return;
        }
        String key = scopedInteractionId(record.tenantId(), record.interactionId());
        itemContinuationWorkByInteraction.putIfAbsent(key, new AwaitItemContinuationCommand(
            record.tenantId(), record.executionId(), record.unitId(), record.interactionId(),
            record.itemIndex(), 1, dueEpochMs));
    }

    private static boolean sameWork(
        AwaitItemContinuationCommand current,
        AwaitItemContinuationCommand candidate) {
        return current.tenantId().equals(candidate.tenantId())
            && current.executionId().equals(candidate.executionId())
            && current.unitId().equals(candidate.unitId())
            && current.interactionId().equals(candidate.interactionId())
            && current.itemIndex() == candidate.itemIndex();
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> fail(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs, null,
            current -> updateStatus(current, AwaitInteractionStatus.FAILED, nowEpochMs, null, null));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> cancel(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs, null,
            current -> updateStatus(current, AwaitInteractionStatus.CANCELLED, nowEpochMs, null, null));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markTimedOut(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs, null,
            current -> updateStatus(current, AwaitInteractionStatus.TIMED_OUT, nowEpochMs, null, null));
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findTimedOut(long nowEpochMs, int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                List<AwaitInteractionRecord> records = interactionsByScopedId.values().stream()
                    .filter(record -> !record.status().terminal())
                    .filter(record -> record.deadlineEpochMs() <= nowEpochMs)
                    .sorted(PENDING_ORDER)
                    .limit(Math.max(0, limit))
                    .toList();
                return List.copyOf(records);
            }
        });
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findDueTimeoutConsequences(long nowEpochMs, int limit) {
        if (limit < 1 || limit > 100) throw new IllegalArgumentException("Timeout page limit must be between 1 and 100");
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                var entries = timeoutConsequences.iterator();
                var due = new ArrayList<AwaitInteractionRecord>();
                for (int evaluated = 0; evaluated < limit && entries.hasNext(); evaluated++) {
                    var record = entries.next();
                    if (record.updatedAtEpochMs() > nowEpochMs) break;
                    if (record.ttlEpochS() > 0 && record.ttlEpochS() <= Instant.ofEpochMilli(nowEpochMs).getEpochSecond()) {
                        entries.remove();
                    } else {
                        due.add(record);
                    }
                }
                return List.copyOf(due);
            }
        });
    }

    @Override
    public Uni<Void> completeTimeoutConsequences(AwaitInteractionRecord interaction) {
        Objects.requireNonNull(interaction, "interaction");
        return Uni.createFrom().voidItem().invoke(() -> {
            synchronized (lock) {
                String scopedId = scopedInteractionId(interaction.tenantId(), interaction.interactionId());
                var current = interactionsByScopedId.get(scopedId);
                if (interaction.status() != AwaitInteractionStatus.TIMED_OUT || current == null
                    || current.status() != AwaitInteractionStatus.TIMED_OUT || current.version() != interaction.version()
                    || !Objects.equals(current.executionId(), interaction.executionId())
                    || !Objects.equals(current.unitId(), interaction.unitId())) {
                    throw new IllegalArgumentException("Timeout acknowledgement does not match the retained interaction");
                }
                timeoutConsequences.remove(current);
            }
        });
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> queryPending(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                String normalizedAssignee = normalizeFilter(assignee);
                String normalizedGroup = normalizeFilter(group);
                String normalizedStepId = normalizeFilter(stepId);
                List<AwaitInteractionRecord> records = new ArrayList<>();
                for (AwaitInteractionRecord record : interactionsByScopedId.values()) {
                    if (record.status().terminal() || !Objects.equals(record.tenantId(), tenantId)) {
                        continue;
                    }
                    if (normalizedAssignee != null && !Objects.equals(normalizedAssignee, record.assignee())) {
                        continue;
                    }
                    if (normalizedGroup != null && !Objects.equals(normalizedGroup, record.group())) {
                        continue;
                    }
                    if (normalizedStepId != null && !Objects.equals(normalizedStepId, record.stepId())) {
                        continue;
                    }
                    records.add(record);
                }
                records.sort(PENDING_ORDER);
                return List.copyOf(records.subList(0, Math.min(records.size(), Math.max(0, limit))));
            }
        });
    }

    private Uni<Optional<AwaitInteractionRecord>> transition(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs,
        AwaitInteractionStatus requiredStatus,
        java.util.function.Function<AwaitInteractionRecord, AwaitInteractionRecord> transition) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                String scopedId = scopedInteractionId(tenantId, interactionId);
                AwaitInteractionRecord current = interactionsByScopedId.get(scopedId);
                if (current == null || current.version() != expectedVersion || current.status().terminal()) {
                    return Optional.empty();
                }
                if (requiredStatus != null && current.status() != requiredStatus) {
                    return Optional.empty();
                }
                AwaitInteractionRecord updated = transition.apply(current);
                interactionsByScopedId.put(scopedId, updated);
                if (updated.status() == AwaitInteractionStatus.TIMED_OUT) {
                    timeoutConsequences.add(updated);
                }
                return Optional.of(updated);
            }
        });
    }

    @Override
    public boolean supportsCommandCompletion() {
        return true;
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> settleCommandDispatch(AwaitInteractionRecord expected,
        org.pipelineframework.awaitable.CommandDispatchSettlement settlement, long nowEpochMs) {
        return transition(expected.tenantId(), expected.interactionId(), expected.version(), nowEpochMs,
            expected.status(), current -> current.settleCommandDispatch(settlement, nowEpochMs));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> compareAndSetCommandCompletion(
        AwaitInteractionRecord expected, AwaitInteractionRecord replacement) {
        if (!expected.permitsCommandCompletionReplacement(replacement)) {
            return Uni.createFrom().failure(new IllegalArgumentException("Invalid Command completion transition"));
        }
        return transition(expected.tenantId(), expected.interactionId(), expected.version(),
            replacement.updatedAtEpochMs(), expected.status(), current -> {
                if (!current.equals(expected) || !current.permitsCommandCompletionReplacement(replacement)) {
                    throw new IllegalArgumentException("Command completion CAS expected state differs from durable state");
                }
                return replacement;
            });
    }

    private Optional<AwaitInteractionRecord> resolveForCompletion(AwaitCompletionCommand command) {
        if (command.interactionId() != null && !command.interactionId().isBlank()) {
            return Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(command.tenantId(), command.interactionId())));
        }
        String interactionId = interactionIdByScopedCorrelation.get(scopedCorrelation(command.tenantId(), command.correlationId()));
        return interactionId == null
            ? Optional.empty()
            : Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(command.tenantId(), interactionId)));
    }

    private AwaitInteractionRecord updateStatus(
        AwaitInteractionRecord current,
        AwaitInteractionStatus status,
        long nowEpochMs,
        Object responsePayload,
        String actor) {
        return updateStatus(current, status, nowEpochMs, responsePayload, actor, current.transportMetadata());
    }

    private AwaitInteractionRecord updateStatus(
        AwaitInteractionRecord current,
        AwaitInteractionStatus status,
        long nowEpochMs,
        Object responsePayload,
        String actor,
        Map<String, Object> transportMetadata) {
        return new AwaitInteractionRecord(
            current.tenantId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.outputType(),
            current.interactionId(),
            current.correlationId(),
            current.causationId(),
            current.idempotencyKey(),
            current.version() + 1,
            status,
            current.requestPayload(),
            responsePayload == null ? current.responsePayload() : responsePayload,
            current.unitId(),
            current.itemIndex(),
            actor == null ? current.actor() : actor,
            current.assignee(),
            current.group(),
            current.transportType(),
            transportMetadata,
            current.deadlineEpochMs(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS(),
            current.transportOutputType(), current.commandAcknowledgement(), current.stagedCompletionPayload());
    }

    private void purgeExpired(long nowEpochMs) {
        long nowEpochS = Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
        var iterator = interactionsByScopedId.entrySet().iterator();
        while (iterator.hasNext()) {
            AwaitInteractionRecord record = iterator.next().getValue();
            if (record.ttlEpochS() > 0 && record.ttlEpochS() <= nowEpochS) {
                iterator.remove();
                timeoutConsequences.remove(record);
                if (record.unitId() != null && !record.unitId().isBlank()) {
                    var unitIndex = interactionIdsByUnit.get(scopedUnitId(record.tenantId(), record.unitId()));
                    if (unitIndex != null) {
                        unitIndex.remove(unitPosition(record));
                        if (unitIndex.isEmpty()) {
                            interactionIdsByUnit.remove(scopedUnitId(record.tenantId(), record.unitId()));
                        }
                    }
                }
                itemContinuationWorkByInteraction.remove(scopedInteractionId(
                    record.tenantId(), record.interactionId()));
                interactionIdByScopedIdempotencyKey.remove(scopedIdempotencyKey(
                    record.tenantId(), record.stepId(), record.idempotencyKey()));
                interactionIdByScopedCorrelation.remove(scopedCorrelation(record.tenantId(), record.correlationId()));
            }
        }
    }

    private static String scopedInteractionId(String tenantId, String interactionId) {
        return compositeScopedKey("tenantId", tenantId, "interactionId", interactionId);
    }

    private static String scopedIdempotencyKey(String tenantId, String stepId, String idempotencyKey) {
        return compositeScopedKey("tenantStep", tenantId + ":" + stepId, "idempotencyKey", idempotencyKey);
    }

    private static String scopedCorrelation(String tenantId, String correlationId) {
        return compositeScopedKey("tenantId", tenantId, "correlationId", correlationId);
    }

    private static String compositeScopedKey(String leftName, String left, String rightName, String right) {
        String safeLeft = Objects.requireNonNull(left, leftName + " must not be null");
        String safeRight = Objects.requireNonNull(right, rightName + " must not be null");
        return safeLeft.length() + ":" + safeLeft + ":" + safeRight.length() + ":" + safeRight;
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String normalizeFilter(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}

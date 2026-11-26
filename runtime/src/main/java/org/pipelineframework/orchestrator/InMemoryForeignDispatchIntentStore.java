package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-safe, append-only in-memory {@link ForeignDispatchIntentStore} for local development
 * and deterministic tests. Never mutates or deletes committed reservation records.
 */
public final class InMemoryForeignDispatchIntentStore implements ForeignDispatchIntentStore {

    /** Immutable settlement record paired with a reservation key. */
    public record SettlementRecord(
        ForeignDispatchIntent intent,
        SettlementStatus status,
        long settledAtEpochMs
    ) {
        public SettlementRecord {
            Objects.requireNonNull(intent, "intent must not be null");
            Objects.requireNonNull(status, "status must not be null");
        }
    }

    private final ConcurrentMap<String, ForeignDispatchIntent> reservations = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SettlementRecord> settlements = new ConcurrentHashMap<>();

    @Override
    public ReservationOutcome reserve(ForeignDispatchIntent intent) {
        Objects.requireNonNull(intent, "intent must not be null");
        ForeignDispatchIntent existing = reservations.putIfAbsent(intent.storageKey(), intent);
        return existing == null
            ? ReservationOutcome.RESERVED
            : ReservationOutcome.ALREADY_DISPATCHED_UNCERTAIN;
    }

    @Override
    public void recordSettlement(ForeignDispatchIntent intent, SettlementStatus status, long settledAtEpochMs) {
        Objects.requireNonNull(intent, "intent must not be null");
        Objects.requireNonNull(status, "status must not be null");
        settlements.putIfAbsent(intent.storageKey(), new SettlementRecord(intent, status, settledAtEpochMs));
    }

    public Optional<ForeignDispatchIntent> findReservation(String storageKey) {
        Objects.requireNonNull(storageKey, "storageKey must not be null");
        return Optional.ofNullable(reservations.get(storageKey));
    }

    public Optional<SettlementRecord> findSettlement(String storageKey) {
        Objects.requireNonNull(storageKey, "storageKey must not be null");
        return Optional.ofNullable(settlements.get(storageKey));
    }

    public int reservationCount() {
        return reservations.size();
    }
}

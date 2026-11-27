/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.runtime.spring.polyglot;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Append-only pre-send dispatch-intent store for Spring-hosted foreign function calls.
 */
public interface SpringForeignDispatchIntentStore {

    enum ReservationOutcome {
        RESERVED,
        ALREADY_DISPATCHED_UNCERTAIN
    }

    enum SettlementStatus {
        VALIDATED_SUCCEEDED,
        VALIDATED_RETRYABLE,
        VALIDATED_TERMINAL,
        UNCERTAIN
    }

    ReservationOutcome reserve(SpringForeignDispatchIntent intent);

    void recordSettlement(SpringForeignDispatchIntent intent, SettlementStatus status, long settledAtEpochMs);

    static SpringForeignDispatchIntentStore noop() {
        return NoopStore.INSTANCE;
    }

    static InMemory inMemory() {
        return new InMemory();
    }

    final class NoopStore implements SpringForeignDispatchIntentStore {
        private static final NoopStore INSTANCE = new NoopStore();

        private NoopStore() {
        }

        @Override
        public ReservationOutcome reserve(SpringForeignDispatchIntent intent) {
            Objects.requireNonNull(intent, "intent");
            return ReservationOutcome.RESERVED;
        }

        @Override
        public void recordSettlement(SpringForeignDispatchIntent intent, SettlementStatus status, long settledAtEpochMs) {
            Objects.requireNonNull(intent, "intent");
            Objects.requireNonNull(status, "status");
        }
    }

    /** Thread-safe append-only in-memory store for local/Spring runs and deterministic tests. */
    final class InMemory implements SpringForeignDispatchIntentStore {
        public record SettlementRecord(
            SpringForeignDispatchIntent intent,
            SettlementStatus status,
            long settledAtEpochMs
        ) {
            public SettlementRecord {
                Objects.requireNonNull(intent, "intent");
                Objects.requireNonNull(status, "status");
            }
        }

        private final ConcurrentMap<String, SpringForeignDispatchIntent> reservations = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, SettlementRecord> settlements = new ConcurrentHashMap<>();

        @Override
        public ReservationOutcome reserve(SpringForeignDispatchIntent intent) {
            Objects.requireNonNull(intent, "intent");
            return reservations.putIfAbsent(intent.storageKey(), intent) == null
                ? ReservationOutcome.RESERVED
                : ReservationOutcome.ALREADY_DISPATCHED_UNCERTAIN;
        }

        @Override
        public void recordSettlement(SpringForeignDispatchIntent intent, SettlementStatus status, long settledAtEpochMs) {
            Objects.requireNonNull(intent, "intent");
            Objects.requireNonNull(status, "status");
            settlements.putIfAbsent(intent.storageKey(), new SettlementRecord(intent, status, settledAtEpochMs));
        }

        public Optional<SpringForeignDispatchIntent> findReservation(String storageKey) {
            Objects.requireNonNull(storageKey, "storageKey");
            return Optional.ofNullable(reservations.get(storageKey));
        }

        public Optional<SettlementRecord> findSettlement(String storageKey) {
            Objects.requireNonNull(storageKey, "storageKey");
            return Optional.ofNullable(settlements.get(storageKey));
        }

        public int reservationCount() {
            return reservations.size();
        }
    }
}

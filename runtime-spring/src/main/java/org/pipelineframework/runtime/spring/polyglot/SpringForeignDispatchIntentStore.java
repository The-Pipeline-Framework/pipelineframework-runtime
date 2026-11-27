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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Append-only pre-send dispatch-intent store for Spring-hosted foreign function calls.
 *
 * <p>Production deployments must provide a durable store (such as {@link #fileBacked(Path)} or a
 * database-backed Spring bean) so pre-send reservations survive process restarts and suppress
 * ambiguous resends. {@link #inMemory()} and {@link #noop()} are strictly for local development
 * and deterministic unit tests.</p>
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

    /**
     * Local- and test-only no-op store that never persists reservations.
     */
    static SpringForeignDispatchIntentStore noop() {
        return NoopStore.INSTANCE;
    }

    /**
     * Local- and test-only in-memory store. Reservations are lost when the process restarts.
     */
    static InMemory inMemory() {
        return new InMemory();
    }

    /**
     * Durable filesystem-backed store using atomic exclusive file creation ({@code CREATE_NEW})
     * and {@code fsync} so pre-send dispatch intents survive process restarts and crashes.
     */
    static FileBacked fileBacked(Path directory) {
        return new FileBacked(directory);
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

    /**
     * Thread-safe append-only in-memory store strictly for local development and deterministic tests.
     * Reservations do not survive process restarts.
     */
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

    /**
     * Durable filesystem-backed pre-send dispatch-intent store using exclusive creation and fsync.
     */
    final class FileBacked implements SpringForeignDispatchIntentStore {
        private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
        };

        private final Path directory;

        public FileBacked(Path directory) {
            this.directory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
            try {
                Files.createDirectories(this.directory);
            } catch (IOException failure) {
                throw new IllegalStateException("Failed creating dispatch-intent directory " + this.directory, failure);
            }
        }

        public Path directory() {
            return directory;
        }

        @Override
        public ReservationOutcome reserve(SpringForeignDispatchIntent intent) {
            Objects.requireNonNull(intent, "intent");
            Path file = reservationFile(intent.storageKey());
            byte[] payload = serializeIntent(intent);
            try (FileChannel channel = FileChannel.open(
                file,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
                ByteBuffer buf = ByteBuffer.wrap(payload);
                while (buf.hasRemaining()) {
                    channel.write(buf);
                }
                channel.force(true);
                return ReservationOutcome.RESERVED;
            } catch (FileAlreadyExistsException duplicate) {
                return ReservationOutcome.ALREADY_DISPATCHED_UNCERTAIN;
            } catch (IOException failure) {
                throw new IllegalStateException("Failed persisting pre-send dispatch intent " + intent.storageKey(), failure);
            }
        }

        @Override
        public void recordSettlement(SpringForeignDispatchIntent intent, SettlementStatus status, long settledAtEpochMs) {
            Objects.requireNonNull(intent, "intent");
            Objects.requireNonNull(status, "status");
            Path target = settlementFile(intent.storageKey());
            if (Files.exists(target)) {
                return;
            }
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("storageKey", intent.storageKey());
            doc.put("status", status.name());
            doc.put("settledAtEpochMs", settledAtEpochMs);
            byte[] bytes;
            try {
                bytes = SpringPolyglotJson.exactMapper().writeValueAsBytes(doc);
            } catch (IOException failure) {
                throw new IllegalStateException("Failed serializing dispatch settlement", failure);
            }
            Path temp = directory.resolve(target.getFileName() + "." + UUID.randomUUID() + ".tmp");
            try {
                try (FileChannel channel = FileChannel.open(
                    temp,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                    ByteBuffer buf = ByteBuffer.wrap(bytes);
                    while (buf.hasRemaining()) {
                        channel.write(buf);
                    }
                    channel.force(true);
                }
                try {
                    Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
                } catch (FileAlreadyExistsException alreadySettled) {
                    Files.deleteIfExists(temp);
                } catch (AtomicMoveNotSupportedException fallback) {
                    if (!Files.exists(target)) {
                        Files.move(temp, target);
                    } else {
                        Files.deleteIfExists(temp);
                    }
                }
            } catch (IOException failure) {
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException ignored) {
                    // Best-effort cleanup.
                }
                throw new IllegalStateException("Failed persisting dispatch settlement " + intent.storageKey(), failure);
            }
        }

        public boolean hasReservation(String storageKey) {
            Objects.requireNonNull(storageKey, "storageKey");
            return Files.exists(reservationFile(storageKey));
        }

        public Optional<SettlementStatus> findSettlementStatus(String storageKey) {
            Objects.requireNonNull(storageKey, "storageKey");
            Path file = settlementFile(storageKey);
            if (!Files.exists(file)) {
                return Optional.empty();
            }
            try {
                Map<String, Object> doc = SpringPolyglotJson.exactMapper().readValue(Files.readAllBytes(file), MAP_TYPE);
                Object raw = doc.get("status");
                return raw instanceof String s ? Optional.of(SettlementStatus.valueOf(s)) : Optional.empty();
            } catch (IOException | RuntimeException failure) {
                throw new IllegalStateException("Failed reading settlement file " + file, failure);
            }
        }

        private Path reservationFile(String storageKey) {
            String hash = SpringForeignDispatchIntent.sha256Hex(storageKey.getBytes(StandardCharsets.UTF_8));
            return directory.resolve("intent-" + hash + ".json");
        }

        private Path settlementFile(String storageKey) {
            String hash = SpringForeignDispatchIntent.sha256Hex(storageKey.getBytes(StandardCharsets.UTF_8));
            return directory.resolve("settlement-" + hash + ".json");
        }

        private static byte[] serializeIntent(SpringForeignDispatchIntent intent) {
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("storageKey", intent.storageKey());
            doc.put("tenantId", intent.tenantId());
            doc.put("executionId", intent.executionId());
            doc.put("executionVersion", intent.executionVersion());
            doc.put("attempt", intent.attempt());
            doc.put("transitionKey", intent.transitionKey());
            doc.put("stepIndex", intent.stepIndex());
            doc.put("stepId", intent.stepId());
            doc.put("targetUri", intent.targetUri());
            doc.put("requestDigest", intent.requestDigest());
            doc.put("createdAtEpochMs", intent.createdAtEpochMs());
            try {
                return SpringPolyglotJson.exactMapper().writeValueAsBytes(doc);
            } catch (IOException failure) {
                throw new IllegalStateException("Failed serializing dispatch intent", failure);
            }
        }
    }
}

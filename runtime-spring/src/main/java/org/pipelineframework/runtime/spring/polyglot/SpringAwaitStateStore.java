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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.UnaryOperator;

import com.fasterxml.jackson.core.type.TypeReference;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.runtime.core.TpfExecutionContext;

/**
 * Durable state store SPI for {@link SpringAwaitResumeCoordinator} suspended Await units.
 *
 * <p>Production deployments should supply a durable implementation (such as {@link #fileBacked(Path)}
 * or a database-backed Spring bean). {@link #inMemory()} is strictly for local development and tests.</p>
 */
public interface SpringAwaitStateStore {

    /** Persists a newly suspended Await unit state. */
    void saveNew(String storageKey, SpringAwaitResumeCoordinator.SuspendedAwaitState state);

    /** Loads a persisted Await unit state by its unambiguous storage key. */
    Optional<SpringAwaitResumeCoordinator.SuspendedAwaitState> find(String storageKey);

    /**
     * Atomically reads, transforms, and persists an existing Await unit state if present.
     *
     * @return the persisted state after transformation, or {@link Optional#empty()} if absent
     */
    Optional<SpringAwaitResumeCoordinator.SuspendedAwaitState> updateAtomically(
        String storageKey,
        UnaryOperator<SpringAwaitResumeCoordinator.SuspendedAwaitState> updater
    );

    /**
     * Creates a durable, crash-safe file-backed store using atomic temp-write, fsync, and rename.
     */
    static FileBacked fileBacked(Path directory) {
        return new FileBacked(directory);
    }

    /**
     * Creates an in-memory store strictly for local development and deterministic unit tests.
     * State does not survive process restarts.
     */
    static InMemory inMemory() {
        return new InMemory();
    }

    /**
     * Local- and test-only in-memory Await state store. Does not survive process restarts.
     */
    final class InMemory implements SpringAwaitStateStore {
        private final ConcurrentMap<String, SpringAwaitResumeCoordinator.SuspendedAwaitState> states =
            new ConcurrentHashMap<>();

        @Override
        public void saveNew(String storageKey, SpringAwaitResumeCoordinator.SuspendedAwaitState state) {
            Objects.requireNonNull(storageKey, "storageKey");
            Objects.requireNonNull(state, "state");
            if (states.putIfAbsent(storageKey, state) != null) {
                throw new IllegalStateException("Await unit already exists for key " + storageKey);
            }
        }

        @Override
        public Optional<SpringAwaitResumeCoordinator.SuspendedAwaitState> find(String storageKey) {
            Objects.requireNonNull(storageKey, "storageKey");
            return Optional.ofNullable(states.get(storageKey));
        }

        @Override
        public Optional<SpringAwaitResumeCoordinator.SuspendedAwaitState> updateAtomically(
            String storageKey,
            UnaryOperator<SpringAwaitResumeCoordinator.SuspendedAwaitState> updater
        ) {
            Objects.requireNonNull(storageKey, "storageKey");
            Objects.requireNonNull(updater, "updater");
            return Optional.ofNullable(states.computeIfPresent(storageKey, (k, existing) -> {
                SpringAwaitResumeCoordinator.SuspendedAwaitState next = updater.apply(existing);
                return Objects.requireNonNull(next, "updater must not return null");
            }));
        }
    }

    /**
     * Durable filesystem-backed Await state store persisting each unit as an fsynced JSON document.
     */
    final class FileBacked implements SpringAwaitStateStore {
        private static final int SCHEMA_VERSION = 1;
        private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
        };

        private final Path directory;
        private final ConcurrentMap<String, Object> keyLocks = new ConcurrentHashMap<>();

        public FileBacked(Path directory) {
            this.directory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
            try {
                Files.createDirectories(this.directory);
            } catch (IOException failure) {
                throw new IllegalStateException("Failed creating Await state directory " + this.directory, failure);
            }
        }

        public Path directory() {
            return directory;
        }

        @Override
        public void saveNew(String storageKey, SpringAwaitResumeCoordinator.SuspendedAwaitState state) {
            Objects.requireNonNull(storageKey, "storageKey");
            Objects.requireNonNull(state, "state");
            Object lock = lockFor(storageKey);
            synchronized (lock) {
                Path target = fileForKey(storageKey);
                if (Files.exists(target)) {
                    throw new IllegalStateException("Await unit already exists for key " + storageKey);
                }
                writeAtomically(target, serialize(state));
            }
        }

        @Override
        public Optional<SpringAwaitResumeCoordinator.SuspendedAwaitState> find(String storageKey) {
            Objects.requireNonNull(storageKey, "storageKey");
            Object lock = lockFor(storageKey);
            synchronized (lock) {
                Path target = fileForKey(storageKey);
                if (!Files.exists(target)) {
                    return Optional.empty();
                }
                return Optional.of(readState(target));
            }
        }

        @Override
        public Optional<SpringAwaitResumeCoordinator.SuspendedAwaitState> updateAtomically(
            String storageKey,
            UnaryOperator<SpringAwaitResumeCoordinator.SuspendedAwaitState> updater
        ) {
            Objects.requireNonNull(storageKey, "storageKey");
            Objects.requireNonNull(updater, "updater");
            Object lock = lockFor(storageKey);
            synchronized (lock) {
                Path target = fileForKey(storageKey);
                if (!Files.exists(target)) {
                    return Optional.empty();
                }
                SpringAwaitResumeCoordinator.SuspendedAwaitState current = readState(target);
                SpringAwaitResumeCoordinator.SuspendedAwaitState next =
                    Objects.requireNonNull(updater.apply(current), "updater must not return null");
                if (!next.equals(current)) {
                    writeAtomically(target, serialize(next));
                }
                return Optional.of(next);
            }
        }

        private Object lockFor(String storageKey) {
            return keyLocks.computeIfAbsent(storageKey, ignored -> new Object());
        }

        private Path fileForKey(String storageKey) {
            String hash = SpringForeignDispatchIntent.sha256Hex(storageKey.getBytes(StandardCharsets.UTF_8));
            return directory.resolve("await-" + hash + ".json");
        }

        private void writeAtomically(Path target, byte[] bytes) {
            Path temp = directory.resolve(target.getFileName() + "." + UUID.randomUUID() + ".tmp");
            try {
                try (FileChannel ch = FileChannel.open(
                    temp,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                    ByteBuffer buf = ByteBuffer.wrap(bytes);
                    while (buf.hasRemaining()) {
                        ch.write(buf);
                    }
                    ch.force(true);
                }
                try {
                    Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException fallback) {
                    Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException failure) {
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException ignored) {
                    // Best-effort temp cleanup.
                }
                throw new IllegalStateException("Failed persisting Await unit state to " + target, failure);
            }
        }

        private SpringAwaitResumeCoordinator.SuspendedAwaitState readState(Path file) {
            try {
                byte[] bytes = Files.readAllBytes(file);
                Map<String, Object> root = SpringPolyglotJson.exactMapper().readValue(bytes, MAP_TYPE);
                return deserialize(root);
            } catch (IOException | RuntimeException failure) {
                throw new IllegalStateException("Failed reading persisted Await unit state from " + file, failure);
            }
        }

        private static byte[] serialize(SpringAwaitResumeCoordinator.SuspendedAwaitState state) {
            AwaitUnitRecord u = state.unit();
            Map<String, Object> unitMap = new LinkedHashMap<>();
            unitMap.put("tenantId", u.tenantId());
            unitMap.put("unitId", u.unitId());
            unitMap.put("executionId", u.executionId());
            unitMap.put("stepId", u.stepId());
            unitMap.put("stepIndex", u.stepIndex());
            unitMap.put("cardinality", u.cardinality());
            unitMap.put("version", u.version());
            unitMap.put("status", u.status().name());
            unitMap.put("primaryInteractionId", u.primaryInteractionId());
            unitMap.put("expectedItemCount", u.expectedItemCount());
            unitMap.put("completedItemCount", u.completedItemCount());
            unitMap.put("completedItemKeys", new ArrayList<>(u.completedItemKeys()));
            unitMap.put("dispatchComplete", u.dispatchComplete());
            unitMap.put("createdAtEpochMs", u.createdAtEpochMs());
            unitMap.put("updatedAtEpochMs", u.updatedAtEpochMs());
            unitMap.put("ttlEpochS", u.ttlEpochS());

            Map<String, String> ctxMap = new LinkedHashMap<>();
            state.capturedContext().versionTag().ifPresent(v -> ctxMap.put("versionTag", v));
            state.capturedContext().replayMode().ifPresent(v -> ctxMap.put("replayMode", v));
            state.capturedContext().cachePolicy().ifPresent(v -> ctxMap.put("cachePolicy", v));

            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("schemaVersion", SCHEMA_VERSION);
            doc.put("phase", state.phase().name());
            state.admittedCompletionKey().ifPresent(k -> doc.put("admittedCompletionKey", k));
            doc.put("unit", unitMap);
            doc.put("suspendedPayload", state.suspendedPayload().toDurableMap());
            doc.put("capturedContext", ctxMap);
            state.completionPayload().ifPresent(p -> doc.put("completionPayload", p.toDurableMap()));
            try {
                return SpringPolyglotJson.exactMapper().writeValueAsBytes(doc);
            } catch (IOException failure) {
                throw new IllegalStateException("Failed serializing Await unit state", failure);
            }
        }

        private static SpringAwaitResumeCoordinator.SuspendedAwaitState deserialize(Map<String, Object> root) {
            int version = requiredInt(root, "schemaVersion");
            if (version != SCHEMA_VERSION) {
                throw new IllegalStateException("Unsupported persisted Await state schemaVersion: " + version);
            }
            SpringAwaitResumeCoordinator.ResumePhase phase =
                SpringAwaitResumeCoordinator.ResumePhase.valueOf(requiredString(root, "phase"));
            Optional<String> admittedCompletionKey = optionalString(root, "admittedCompletionKey");

            Map<?, ?> unitMap = requiredMap(root, "unit");
            Set<String> completedKeys = new LinkedHashSet<>();
            Object rawKeys = unitMap.get("completedItemKeys");
            if (rawKeys instanceof List<?> list) {
                for (Object item : list) {
                    if (item instanceof String s && !s.isBlank()) {
                        completedKeys.add(s);
                    }
                }
            }
            AwaitUnitRecord unit = new AwaitUnitRecord(
                requiredString(unitMap, "tenantId"),
                requiredString(unitMap, "unitId"),
                requiredString(unitMap, "executionId"),
                requiredString(unitMap, "stepId"),
                requiredInt(unitMap, "stepIndex"),
                requiredString(unitMap, "cardinality"),
                requiredLong(unitMap, "version"),
                AwaitUnitStatus.valueOf(requiredString(unitMap, "status")),
                requiredString(unitMap, "primaryInteractionId"),
                requiredInt(unitMap, "expectedItemCount"),
                requiredInt(unitMap, "completedItemCount"),
                completedKeys,
                requiredBoolean(unitMap, "dispatchComplete"),
                requiredLong(unitMap, "createdAtEpochMs"),
                requiredLong(unitMap, "updatedAtEpochMs"),
                requiredLong(unitMap, "ttlEpochS"));

            TransitionAwaitSuspension suspension = new TransitionAwaitSuspension(
                unit.tenantId(), unit.executionId(), unit.unitId(), unit.stepIndex(), unit, List.of());

            SpringCanonicalPayload suspendedPayload = SpringCanonicalPayload.fromDurableMap(root.get("suspendedPayload"))
                .orElseThrow(() -> new IllegalStateException("Missing or malformed suspendedPayload"));

            Optional<SpringCanonicalPayload> completionPayload = root.containsKey("completionPayload")
                ? Optional.of(SpringCanonicalPayload.fromDurableMap(root.get("completionPayload"))
                    .orElseThrow(() -> new IllegalStateException("Malformed completionPayload")))
                : Optional.empty();

            Optional<String> versionTag = Optional.empty();
            Optional<String> replayMode = Optional.empty();
            Optional<String> cachePolicy = Optional.empty();
            if (root.get("capturedContext") instanceof Map<?, ?> ctxMap) {
                versionTag = optionalString(ctxMap, "versionTag");
                replayMode = optionalString(ctxMap, "replayMode");
                cachePolicy = optionalString(ctxMap, "cachePolicy");
            }
            TpfExecutionContext.Snapshot snapshot = new TpfExecutionContext.Snapshot(versionTag, replayMode, cachePolicy);

            return new SpringAwaitResumeCoordinator.SuspendedAwaitState(
                unit, suspension, suspendedPayload, snapshot, completionPayload, phase, admittedCompletionKey);
        }

        private static Map<?, ?> requiredMap(Map<?, ?> map, String key) {
            Object val = map.get(key);
            if (!(val instanceof Map<?, ?> nested)) {
                throw new IllegalStateException("Missing object field '" + key + "'");
            }
            return nested;
        }

        private static String requiredString(Map<?, ?> map, String key) {
            Object val = map.get(key);
            if (!(val instanceof String s) || s.isBlank()) {
                throw new IllegalStateException("Missing string field '" + key + "'");
            }
            return s;
        }

        private static Optional<String> optionalString(Map<?, ?> map, String key) {
            Object val = map.get(key);
            if (val instanceof String s && !s.isBlank()) {
                return Optional.of(s);
            }
            return Optional.empty();
        }

        private static int requiredInt(Map<?, ?> map, String key) {
            Object val = map.get(key);
            if (!(val instanceof Number n)) {
                throw new IllegalStateException("Missing numeric field '" + key + "'");
            }
            return n.intValue();
        }

        private static long requiredLong(Map<?, ?> map, String key) {
            Object val = map.get(key);
            if (!(val instanceof Number n)) {
                throw new IllegalStateException("Missing numeric field '" + key + "'");
            }
            return n.longValue();
        }

        private static boolean requiredBoolean(Map<?, ?> map, String key) {
            Object val = map.get(key);
            if (!(val instanceof Boolean b)) {
                throw new IllegalStateException("Missing boolean field '" + key + "'");
            }
            return b;
        }
    }
}

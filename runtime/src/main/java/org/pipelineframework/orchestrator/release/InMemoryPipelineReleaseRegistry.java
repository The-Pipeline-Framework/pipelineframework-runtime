package org.pipelineframework.orchestrator.release;

import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineReleaseStatus;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * In-memory release registry for local/dev coordinator skeletons.
 */
public class InMemoryPipelineReleaseRegistry implements PipelineReleaseRegistry {

    private final Object lock = new Object();
    private final Map<String, PipelineReleaseRecord> releasesByKey = new HashMap<>();
    private final Map<List<String>, ActivationOperationReceipt> activationOperations = new HashMap<>();
    private final Map<List<String>, List<MemoryActivationEvent>> activationEvents = new HashMap<>();

    @Override
    public boolean supportsCurrentActivationObservation() { return true; }

    @Override
    public Uni<CurrentActivationObservation> currentActivationObservation(String tenant, String pipeline) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                var events = activationEvents.getOrDefault(List.of(tenant, pipeline), List.of());
                if (events.isEmpty()) {
                    boolean legacyActive = releasesByKey.values().stream().anyMatch(record -> tenant.equals(record.tenantId())
                        && pipeline.equals(record.pipelineId()) && record.status() == PipelineReleaseStatus.ACTIVE);
                    return new CurrentActivationObservation(1, tenant, pipeline, legacyActive
                        ? CurrentActivationObservation.State.LEGACY_UNKNOWN : CurrentActivationObservation.State.NONE, Optional.empty());
                }
                var event = events.getLast();
                var release = releasesByKey.get(key(tenant, pipeline, event.releaseVersion()));
                if (release == null) throw new IllegalStateException("Current activation lacks immutable registered Release");
                return new CurrentActivationObservation(1, tenant, pipeline, CurrentActivationObservation.State.IDENTIFIED,
                    Optional.of(new CurrentActivationEvent(event.activationId(), release.contractVersion(), release.releaseVersion(),
                        PipelineReleaseEvidence.from(release), event.activatedAtEpochMs())));
            }
        });
    }

    @Override
    public boolean supportsActivationOperations() {
        return true;
    }

    @Override
    public Uni<Optional<ActivationOperationReceipt>> getActivationOperation(
        String tenantId, String pipelineId, String operationKey) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return Optional.ofNullable(activationOperations.get(List.of(tenantId, pipelineId, operationKey)));
            }
        });
    }

    @Override
    public Uni<ActivationOperationReceipt> activateOnce(ActivationOperationCommand command) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                PipelineReleaseRecord release = command.release();
                List<String> operation = List.of(release.tenantId(), release.pipelineId(), command.operationKey());
                ActivationOperationReceipt existing = activationOperations.get(operation);
                if (existing != null) {
                    existing.requireIntent(command);
                    return existing;
                }
                PipelineReleaseRecord registered = releasesByKey.get(key(release.tenantId(), release.pipelineId(), release.releaseVersion()));
                if (registered == null || !registered.tenantId().equals(release.tenantId())
                    || !registered.pipelineId().equals(release.pipelineId())
                    || !PipelineReleaseRecordMetadata.sameImmutableMetadata(registered, release)) {
                    throw new IllegalStateException("Activation requires matching registered immutable Release metadata");
                }
                ActivationOperationReceipt receipt = new ActivationOperationReceipt(1, command.operationKey(),
                    java.util.UUID.randomUUID().toString(), release.tenantId(), release.pipelineId(),
                    release.contractVersion(), release.releaseVersion(), PipelineReleaseEvidence.from(registered), command.activatedAtEpochMs());
                activateLocked(release.tenantId(), release.pipelineId(), release.releaseVersion(),
                    command.activatedAtEpochMs(), receipt.activationId());
                activationOperations.put(operation, receipt);
                return receipt;
            }
        });
    }

    @Override
    public Uni<PipelineReleaseRecord> register(PipelineReleaseRecord record) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String key = key(record.tenantId(), record.pipelineId(), record.releaseVersion());
                PipelineReleaseRecord existing = releasesByKey.get(key);
                if (existing != null) {
                    if (!PipelineReleaseRecordMetadata.sameImmutableMetadata(existing, record)) {
                        throw new IllegalStateException(
                            "Release version is already registered with different metadata");
                    }
                    return existing;
                }
                releasesByKey.put(key, record);
                return record;
            }
        });
    }

    @Override
    public Uni<List<PipelineReleaseRecord>> list(String tenantId, String pipelineId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return releasesByKey.values().stream()
                    .filter(record -> record.tenantId().equals(tenantId) && record.pipelineId().equals(pipelineId))
                    .sorted(Comparator.comparingLong(PipelineReleaseRecord::createdAtEpochMs))
                    .toList();
            }
        });
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> get(String tenantId, String pipelineId, String releaseVersion) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return Optional.ofNullable(releasesByKey.get(key(tenantId, pipelineId, releaseVersion)));
            }
        });
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> active(String tenantId, String pipelineId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return releasesByKey.values().stream()
                    .filter(record -> record.tenantId().equals(tenantId)
                        && record.pipelineId().equals(pipelineId)
                        && record.status() == PipelineReleaseStatus.ACTIVE)
                    .findFirst();
            }
        });
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> activate(
        String tenantId,
        String pipelineId,
        String releaseVersion,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return activateLocked(tenantId, pipelineId, releaseVersion, nowEpochMs, java.util.UUID.randomUUID().toString());
            }
        });
    }

    private Optional<PipelineReleaseRecord> activateLocked(String tenantId, String pipelineId,
        String releaseVersion, long nowEpochMs, String activationId) {
                String selectedKey = key(tenantId, pipelineId, releaseVersion);
                if (!releasesByKey.containsKey(selectedKey)) {
                    return Optional.empty();
                }
                releasesByKey.replaceAll((key, record) -> {
                    if (!record.tenantId().equals(tenantId) || !record.pipelineId().equals(pipelineId)) {
                        return record;
                    }
                    if (key.equals(selectedKey)) {
                        return record.withStatus(PipelineReleaseStatus.ACTIVE, nowEpochMs);
                    }
                    if (record.status() == PipelineReleaseStatus.ACTIVE) {
                        return record.withStatus(PipelineReleaseStatus.REGISTERED, nowEpochMs);
                    }
                    return record;
                });
                List<String> scope = List.of(tenantId, pipelineId);
                List<MemoryActivationEvent> events = new java.util.ArrayList<>(activationEvents.getOrDefault(scope, List.of()));
                events.add(new MemoryActivationEvent(activationId, releaseVersion, nowEpochMs));
                activationEvents.put(scope, List.copyOf(events));
                return Optional.of(releasesByKey.get(selectedKey));
    }

    private record MemoryActivationEvent(String activationId, String releaseVersion, long activatedAtEpochMs) { }

    private static String key(String tenantId, String pipelineId, String releaseVersion) {
        return tenantId + "\u001f" + pipelineId + "\u001f" + releaseVersion;
    }

}

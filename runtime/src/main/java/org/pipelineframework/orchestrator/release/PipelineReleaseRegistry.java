package org.pipelineframework.orchestrator.release;

import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * Local/dev registry for release metadata and active release pointers.
 */
public interface PipelineReleaseRegistry {

    /** Records immutable physical deployment evidence for one entry of a registered Release. */
    default Uni<PipelineReleaseEntryBinding> registerEntryBinding(PipelineReleaseEntryBinding binding) {
        return Uni.createFrom().failure(new UnsupportedOperationException("Release entry bindings are unsupported"));
    }

    /** Reads the binding of the exact scoped Release entry without altering activation or worker state. */
    default Uni<Optional<PipelineReleaseEntryBinding>> getEntryBinding(
        String tenantId, String pipelineId, String contractVersion, String releaseVersion, String entryId) {
        return Uni.createFrom().failure(new UnsupportedOperationException("Release entry bindings are unsupported"));
    }

    default boolean supportsCurrentActivationObservation() { return false; }

    /** Strong read-only observation; legacy unknown identity is never inferred from release or time. */
    default Uni<CurrentActivationObservation> currentActivationObservation(String tenantId, String pipelineId) {
        return Uni.createFrom().failure(new UnsupportedOperationException("Current activation observation is unsupported"));
    }

    default boolean supportsActivationOperations() {
        return false;
    }

    /** Atomically commits the activation event, common ordered head and immutable operation receipt. */
    default Uni<ActivationOperationReceipt> activateOnce(ActivationOperationCommand command) {
        return Uni.createFrom().failure(new UnsupportedOperationException("Activation operations are unsupported"));
    }

    /** Atomically changes only the observed head; replay returns its receipt without reactivation. */
    default Uni<ActivationOperationReceipt> activateOnceIfCurrent(
        ActivationOperationCommand command, CurrentActivationObservation expected) {
        return Uni.createFrom().failure(new UnsupportedOperationException("Conditional activation is unsupported"));
    }

    /** Strong read-only historical inquiry; unknown never authorizes an activation. */
    default Uni<Optional<ActivationOperationReceipt>> getActivationOperation(
        String tenantId, String pipelineId, String operationKey) {
        return Uni.createFrom().failure(new UnsupportedOperationException("Activation operations are unsupported"));
    }

    Uni<PipelineReleaseRecord> register(PipelineReleaseRecord record);

    Uni<List<PipelineReleaseRecord>> list(String tenantId, String pipelineId);

    Uni<Optional<PipelineReleaseRecord>> get(String tenantId, String pipelineId, String releaseVersion);

    Uni<Optional<PipelineReleaseRecord>> active(String tenantId, String pipelineId);

    Uni<Optional<PipelineReleaseRecord>> activate(
        String tenantId,
        String pipelineId,
        String releaseVersion,
        long nowEpochMs);
}

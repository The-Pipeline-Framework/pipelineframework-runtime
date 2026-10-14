package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import org.pipelineframework.awaitable.AwaitCompletionAdmission;
import org.pipelineframework.awaitable.AwaitUnitReleaseAcknowledgement;

/** First semantic admission/release evidence; never a provider callback or driver-resume measurement. */
public record AwaitCompletionObservation(
    String tenantId, String interactionId,
    Optional<AwaitCompletionAdmission> admission,
    Optional<AwaitUnitReleaseAcknowledgement> acknowledgement,
    State state, OptionalLong admissionToReleaseLatencyMs
) {
    public enum State { OUTSTANDING, RELEASED, TERMINAL_FAILED, UNKNOWN }

    public AwaitCompletionObservation {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(interactionId, "interactionId");
        Objects.requireNonNull(admission, "admission");
        Objects.requireNonNull(acknowledgement, "acknowledgement");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(admissionToReleaseLatencyMs, "admissionToReleaseLatencyMs");
        if (tenantId.isBlank() || interactionId.isBlank()) throw new IllegalArgumentException("Observation identity must not be blank");
        admission.ifPresent(value -> {
            if (!tenantId.equals(value.tenantId()) || !interactionId.equals(value.interactionId())) {
                throw new IllegalArgumentException("Observation admission identity differs");
            }
        });
        if (admissionToReleaseLatencyMs.isPresent()
            && (state != State.RELEASED || admissionToReleaseLatencyMs.getAsLong() < 0)) {
            throw new IllegalArgumentException("Only an observed nonnegative continuation-release latency is measurable");
        }
    }
}

package org.pipelineframework.orchestrator.release;

import java.util.Objects;
import java.util.Optional;

/** Read-only current-event observation; legacy history without trustworthy identity is explicit. */
public record CurrentActivationObservation(int schemaVersion, String tenantId, String pipelineId,
    State state, Optional<CurrentActivationEvent> identifiedEvent) {
    public enum State { NONE, IDENTIFIED, LEGACY_UNKNOWN }

    public CurrentActivationObservation {
        if (schemaVersion != 1) throw new IllegalArgumentException("Unsupported current activation schemaVersion");
        if (tenantId == null || tenantId.isBlank() || pipelineId == null || pipelineId.isBlank()) {
            throw new IllegalArgumentException("Current activation scope must not be blank");
        }
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(identifiedEvent, "identifiedEvent");
        if ((state == State.IDENTIFIED) != identifiedEvent.isPresent()) {
            throw new IllegalArgumentException("Identified activation event must agree with observation state");
        }
        identifiedEvent.ifPresent(event -> {
            if (!pipelineId.equals(event.immutableReleaseIdentity().descriptor().pipelineId())) {
                throw new IllegalArgumentException("Current activation evidence differs from Pipeline scope");
            }
        });
    }
}

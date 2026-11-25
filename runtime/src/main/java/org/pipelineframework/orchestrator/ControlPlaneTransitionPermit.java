package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.Optional;

/**
 * Permit for a tenant-scoped transition admission.
 */
@FunctionalInterface
public interface ControlPlaneTransitionPermit extends AutoCloseable {

    ControlPlaneTransitionPermit NOOP = () -> {
    };

    /**
     * Release the transition permit.
     */
    @Override
    void close();

    /**
     * Optional maximum active duration covered by this permit's underlying lease.
     * Callers enforcing distributed permits must bound transition execution within this duration.
     *
     * @return maximum active transition duration, or empty when unbounded by a lease
     */
    default Optional<Duration> maxActiveDuration() {
        return Optional.empty();
    }

    /**
     * No-op transition permit.
     *
     * @return no-op permit
     */
    static ControlPlaneTransitionPermit noop() {
        return NOOP;
    }
}

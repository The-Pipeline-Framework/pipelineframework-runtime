package org.pipelineframework.telemetry;

import io.quarkus.runtime.annotations.Recorder;
import java.util.function.Supplier;

/** Carries immutable augmentation facts into the runtime; it owns no instruments. */
@Recorder
public class TelemetryCapabilitiesRecorder {
    public Supplier<TelemetryCapabilities> capabilities(boolean present, boolean tracing, boolean metrics,
                                                         boolean logs, boolean micrometer) {
        return () -> new TelemetryCapabilities(present, tracing, metrics, logs, micrometer);
    }
}

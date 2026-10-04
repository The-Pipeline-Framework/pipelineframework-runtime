package org.pipelineframework.extension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.quarkus.deployment.Capabilities;
import io.quarkus.deployment.Capability;
import io.quarkus.opentelemetry.runtime.config.build.OTelBuildConfig;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TelemetryCapabilityBuildStepsTest {
    private final TelemetryCapabilityBuildSteps steps = new TelemetryCapabilityBuildSteps();

    @Test
    void capturesAbsentDisabledAndEnabledBuildSignals() {
        var config = mock(OTelBuildConfig.class, RETURNS_DEEP_STUBS);
        when(config.enabled()).thenReturn(true);
        when(config.traces().enabled()).thenReturn(Optional.empty());
        when(config.metrics().enabled()).thenReturn(Optional.empty());
        when(config.logs().enabled()).thenReturn(Optional.empty());
        var absent = steps.resolve(new Capabilities(Set.of()), config, false, false);
        assertFalse(absent.openTelemetryPresent());
        assertFalse(absent.metricsCapable());
        var capabilities = new Capabilities(Set.of(Capability.OPENTELEMETRY_TRACER,
            Capability.OPENTELEMETRY_METRICS, Capability.OPENTELEMETRY_LOGS));
        var ordinary = steps.resolve(capabilities, config, false, true);
        assertTrue(ordinary.tracingCapable());
        assertFalse(ordinary.metricsCapable());
        assertTrue(ordinary.micrometerCapable());
        assertTrue(steps.resolve(capabilities, config, true, true).metricsCapable());
        when(config.metrics().enabled()).thenReturn(Optional.of(false));
        assertFalse(steps.resolve(capabilities, config, true, true).metricsCapable());
        when(config.metrics().enabled()).thenReturn(Optional.of(true));
        assertTrue(steps.resolve(capabilities, config, false, false).metricsCapable());
        when(config.enabled()).thenReturn(false);
        assertFalse(steps.resolve(capabilities, config, true, true).tracingCapable());
        assertFalse(steps.resolve(capabilities, config, true, true).metricsCapable());
    }
}

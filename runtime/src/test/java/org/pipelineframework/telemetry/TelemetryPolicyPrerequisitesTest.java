package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.PipelineStepConfig;

class TelemetryPolicyPrerequisitesTest {
    @Test
    void replayCanOmitMetricsButRetainsEveryExistingPrerequisiteAndGuardIsIndependent() {
        var config = mock(PipelineStepConfig.class, RETURNS_DEEP_STUBS);
        when(config.telemetry().enabled()).thenReturn(true);
        when(config.telemetry().tracing().enabled()).thenReturn(true);
        when(config.telemetry().tracing().perItem()).thenReturn(true);
        when(config.telemetry().replay().enabled()).thenReturn(true);
        when(config.telemetry().replay().exporter()).thenReturn("file");
        when(config.telemetry().replay().filePath()).thenReturn(Optional.of("replay.json"));
        when(config.killSwitch().retryAmplification().enabled()).thenReturn(true);
        assertTrue(TelemetryPolicy.from(config, true).replayEnabled());
        assertFalse(TelemetryPolicy.from(config, true).metricsEnabled());
        assertFalse(TelemetryPolicy.from(config, false).replayEnabled());
        when(config.telemetry().replay().filePath()).thenReturn(Optional.empty());
        assertFalse(TelemetryPolicy.from(config, true).replayEnabled());
        when(config.telemetry().replay().filePath()).thenReturn(Optional.of("replay.json"));
        when(config.telemetry().replay().exporter()).thenReturn("none");
        assertFalse(TelemetryPolicy.from(config, true).replayEnabled());
        when(config.telemetry().replay().exporter()).thenReturn("file");
        when(config.telemetry().tracing().perItem()).thenReturn(false);
        assertFalse(TelemetryPolicy.from(config, true).replayEnabled());
        when(config.telemetry().tracing().perItem()).thenReturn(true);
        when(config.telemetry().tracing().enabled()).thenReturn(false);
        assertFalse(TelemetryPolicy.from(config, true).replayEnabled());
        when(config.telemetry().enabled()).thenReturn(false);
        assertFalse(TelemetryPolicy.from(config, true).metricsEnabled());
        assertFalse(TelemetryPolicy.from(config, true).tracingEnabled());
        assertTrue(TelemetryPolicy.from(config, true).retryAmplificationEnabled());
    }
}

package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

class TelemetryConfigReporterTest {
    @Test
    void instrumentationCapabilityAndExporterRoutingRemainIndependent() {
        var config = mock(Config.class);
        when(config.getOptionalValue(anyString(), eq(Boolean.class))).thenReturn(Optional.empty());
        when(config.getOptionalValue(anyString(), eq(String.class))).thenReturn(Optional.empty());
        var reporter = new TelemetryConfigReporter();
        var capable = new TelemetryCapabilities(true, true, true, false, false);
        var unavailable = new TelemetryCapabilities(true, false, false, false, false);
        try (var sdk = new TelemetryTestRuntime()) {
            var enabled = sdk.policy(true, true, true);
            assertTrue(reporter.status(enabled, capable, config).stream().allMatch(s -> s.requested() && s.capable()));
            assertTrue(reporter.status(sdk.policy(false, true, true), capable, config).stream().noneMatch(s -> s.requested()));
            assertTrue(reporter.status(enabled, unavailable, config).stream().noneMatch(s -> s.capable()));
            when(config.getOptionalValue("quarkus.otel.metrics.exporter", String.class)).thenReturn(Optional.of("none"));
            when(config.getOptionalValue("quarkus.otel.exporter.otlp.enabled", Boolean.class)).thenReturn(Optional.of(false));
            var noExporter = reporter.status(enabled, capable, config).getFirst();
            assertTrue(noExporter.requested());
            assertTrue(noExporter.capable());
            assertEquals("none", noExporter.exporters());
            assertFalse(noExporter.otlpEnabled());
            when(config.getOptionalValue("quarkus.otel.sdk.disabled", Boolean.class)).thenReturn(Optional.of(true));
            assertTrue(reporter.status(enabled, capable, config).stream().allMatch(s -> s.sdkDisabled()));
        }
    }
}

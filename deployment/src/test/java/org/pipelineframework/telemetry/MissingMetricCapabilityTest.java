package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import io.quarkus.test.QuarkusExtensionTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@org.junit.jupiter.api.parallel.Isolated
class MissingMetricCapabilityTest {
    @RegisterExtension
    static final QuarkusExtensionTest APPLICATION = new QuarkusExtensionTest()
        .withApplicationRoot(archive -> archive.addClass(ManagedTelemetryOwnershipTest.TestHandler.class))
        .overrideConfigKey("quarkus.devservices.enabled", "false")
        .overrideConfigKey("quarkus.http.test-port", "0")
        .overrideConfigKey("pipeline.telemetry.enabled", "true")
        .overrideConfigKey("pipeline.telemetry.metrics.enabled", "true")
        .overrideConfigKey("pipeline.telemetry.tracing.enabled", "true")
        .overrideConfigKey("quarkus.otel.enabled", "true")
        .overrideConfigKey("quarkus.otel.metrics.enabled", "false")
        .overrideConfigKey("quarkus.otel.traces.enabled", "true")
        .overrideConfigKey("quarkus.otel.exporter.otlp.enabled", "false")
        .setLogRecordPredicate(record -> record.getMessage().contains("TPF telemetry configuration mismatch"))
        .assertLogRecords(records -> assertTrue(records.stream().anyMatch(record ->
            String.format(record.getMessage(), record.getParameters()).contains("metrics requested but absent"))));

    @Inject TelemetryPolicySource source;
    @Inject TelemetryCapabilities capabilities;

    @Test
    void requestedButAbsentMetricsWarnsAndDoesNotPreventStartup() {
        assertTrue(source.telemetryPolicy().metricsEnabled());
        assertFalse(capabilities.metricsCapable());
        assertTrue(capabilities.tracingCapable());
    }
}

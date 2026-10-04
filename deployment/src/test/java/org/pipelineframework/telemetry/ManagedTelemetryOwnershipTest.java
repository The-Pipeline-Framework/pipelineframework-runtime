package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import io.quarkus.test.QuarkusExtensionTest;
import org.junit.jupiter.api.extension.RegisterExtension;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitTelemetry;

@org.junit.jupiter.api.parallel.Isolated
class ManagedTelemetryOwnershipTest {
    @Inject Instance<TelemetryPolicySource> sources;
    @Inject TelemetryCapabilities capabilities;
    @Inject PipelineTelemetryRuntime pipeline;
    @Inject AwaitTelemetry await;
    @Inject PageExecutionTelemetry pages;
    @Inject QueryObservationTelemetry query;

    @Test
    void augmentationWiresOnePolicyWithIndependentBuildCapabilitiesAndNoExporter() throws ClassNotFoundException {
        assertTrue(sources.isResolvable());
        assertTrue(io.quarkus.arc.Arc.container().instance(Class.forName("org.pipelineframework.telemetry.HttpMetricsRecorder")).isAvailable());
        assertTrue(io.quarkus.arc.Arc.container().instance(Class.forName("org.pipelineframework.telemetry.RpcMetricsRecorder")).isAvailable());
        assertTrue(io.quarkus.arc.Arc.container().instance(Class.forName("org.pipelineframework.telemetry.ApmCompatibilityMetricsRecorder")).isAvailable());
        assertTrue(io.quarkus.arc.Arc.container().instance(Class.forName("org.pipelineframework.telemetry.BackpressureBufferMetricsRecorder")).isAvailable());
        assertTrue(io.quarkus.arc.Arc.container().instance(Class.forName("org.pipelineframework.telemetry.GrpcClientTracingRecorder")).isAvailable());
        var policy = sources.get().telemetryPolicy();
        assertEquals(policy, pipeline.telemetryPolicy());
        assertTrue(policy.metricsEnabled());
        assertTrue(policy.tracingEnabled());
        assertTrue(capabilities.metricsCapable());
        assertTrue(capabilities.tracingCapable());
        // Force managed boundary construction without an execution or exporter dependency.
        await.recordProviderAdmitted();
        assertNotNull(pages);
        assertNotNull(query);
    }

    public static class TestHandler implements com.amazonaws.services.lambda.runtime.RequestStreamHandler {
        @Override public void handleRequest(java.io.InputStream input, java.io.OutputStream output,
                                           com.amazonaws.services.lambda.runtime.Context context) throws java.io.IOException {
            output.write("{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @RegisterExtension
    static final QuarkusExtensionTest APPLICATION = new QuarkusExtensionTest().withApplicationRoot(archive -> archive.addClass(TestHandler.class))
        .overrideConfigKey("quarkus.devservices.enabled", "false")
        .overrideConfigKey("quarkus.http.test-port", "0")
        .overrideConfigKey("pipeline.telemetry.enabled", "true")
        .overrideConfigKey("pipeline.telemetry.metrics.enabled", "true")
        .overrideConfigKey("pipeline.telemetry.tracing.enabled", "true")
        .overrideConfigKey("quarkus.otel.enabled", "true")
        .overrideConfigKey("quarkus.otel.metrics.enabled", "true")
        .overrideConfigKey("quarkus.otel.traces.enabled", "true")
        .overrideConfigKey("quarkus.otel.sdk.disabled", "false")
        .overrideConfigKey("quarkus.otel.exporter.otlp.enabled", "false");
}

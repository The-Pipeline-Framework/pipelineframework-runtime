package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitTelemetry;

class TelemetryExportIndependenceTest {
    @Test
    void anEnabledInstrumentWorksWithoutAnExporter() {
        try (var provider = SdkTracerProvider.builder().build()) {
            var runtime = runtime(provider);
            var span = runtime.tracer("proof").spanBuilder("proof").startSpan();
            assertTrue(span.isRecording());
            span.end();
            assertDoesNotThrow(() -> new AwaitTelemetry(() -> tracingPolicy(), runtime).recordProviderAdmitted());
        }
    }

    @Test
    void exporterFailureDoesNotDisableInstrumentationOrFailTheOperation() {
        var attempts = new AtomicInteger();
        SpanExporter failing = new SpanExporter() {
            @Override public CompletableResultCode export(Collection<SpanData> spans) {
                attempts.addAndGet(spans.size());
                return CompletableResultCode.ofFailure();
            }
            @Override public CompletableResultCode flush() { return CompletableResultCode.ofSuccess(); }
            @Override public CompletableResultCode shutdown() { return CompletableResultCode.ofSuccess(); }
        };
        try (var provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(failing)).build()) {
            var telemetry = new AwaitTelemetry(() -> tracingPolicy(), runtime(provider));
            assertDoesNotThrow(telemetry::recordProviderAdmitted);
            assertTrue(attempts.get() > 0);
        }
    }

    private TelemetryPolicy tracingPolicy() {
        return new TelemetryPolicy(true, false, true, false, false, false,
            java.time.Duration.ofSeconds(30), 10, 3, RetryAmplificationGuardMode.FAIL_FAST);
    }

    private TelemetryRuntime runtime(SdkTracerProvider provider) {
        return new TelemetryRuntime() {
            @Override public Meter meter(String scope) { return OpenTelemetry.noop().getMeter(scope); }
            @Override public Tracer tracer(String scope) { return provider.get(scope); }
            @Override public void flush() { provider.forceFlush(); }
        };
    }
}

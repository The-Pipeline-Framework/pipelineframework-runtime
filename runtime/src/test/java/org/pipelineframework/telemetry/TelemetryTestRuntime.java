package org.pipelineframework.telemetry;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/** Each fixture owns its SDK; no JVM-global registration or reset is needed. */
public final class TelemetryTestRuntime implements TelemetryRuntime, AutoCloseable {
    public final InMemoryMetricReader reader = InMemoryMetricReader.create();
    public final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    private final SdkMeterProvider meters = SdkMeterProvider.builder().registerMetricReader(reader).build();
    private final SdkTracerProvider tracers = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
    public final AtomicInteger meterRequests = new AtomicInteger();
    public final AtomicInteger tracerRequests = new AtomicInteger();

    @Override public Meter meter(String scope) { meterRequests.incrementAndGet(); return meters.get(scope); }
    @Override public Tracer tracer(String scope) { tracerRequests.incrementAndGet(); return tracers.get(scope); }
    @Override public void flush() { meters.forceFlush(); tracers.forceFlush(); }
    @Override public void close() { tracers.close(); meters.close(); }

    public TelemetryPolicy policy(boolean framework, boolean metrics, boolean tracing) {
        return new TelemetryPolicy(framework, framework && metrics, framework && tracing, false, false, false,
            Duration.ofSeconds(30), 10d, 3, RetryAmplificationGuardMode.FAIL_FAST);
    }
}

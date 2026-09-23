package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.paging.PagedSourceCompletion;

class PageExecutionTelemetryTest {
    private InMemoryMetricReader metricReader;
    private SdkMeterProvider meterProvider;
    private PageExecutionTelemetry telemetry;

    @BeforeEach
    void setUp() {
        metricReader = InMemoryMetricReader.create();
        meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
        telemetry = new PageExecutionTelemetry(new TelemetryRuntime() {
            @Override public Meter meter(String instrumentationScope) { return sdk.getMeter(instrumentationScope); }
            @Override public Tracer tracer(String instrumentationScope) { return sdk.getTracer(instrumentationScope); }
            @Override public void flush() { }
        });
    }

    @AfterEach
    void tearDown() {
        meterProvider.shutdown();
    }

    @Test
    void recordsPageProgressReplayAndARealDemandStall() throws Exception {
        PageExecutionTelemetry.PageObservation observation = telemetry.open(true);
        AssertSubscriber<Integer> subscriber = Multi.createFrom().publisher(
            observation.observeDemand(twoItemPublisher()))
            .subscribe().withSubscriber(AssertSubscriber.create(0));

        subscriber.request(1).awaitItems(1);
        Thread.sleep(5);
        subscriber.request(1).awaitCompletion(Duration.ofSeconds(1));
        observation.complete(new PagedSourceCompletion(2, Optional.empty(), true));

        var metrics = metricReader.collectAllMetrics();
        Set<String> names = metrics.stream().map(metric -> metric.getName()).collect(Collectors.toSet());
        assertTrue(names.containsAll(Set.of(
            "tpf.page.completed.total",
            "tpf.page.records.consumed.total",
            "tpf.page.replayed.total",
            "tpf.page.demand.stalls.total",
            "tpf.page.duration",
            "tpf.page.demand.stall.duration")));
        assertEquals(1L, longSum("tpf.page.completed.total"));
        assertEquals(2L, longSum("tpf.page.records.consumed.total"));
        assertEquals(1L, longSum("tpf.page.replayed.total"));
        assertEquals(1L, longSum("tpf.page.demand.stalls.total"));
    }

    private long longSum(String name) {
        return metricReader.collectAllMetrics().stream()
            .filter(metric -> name.equals(metric.getName()))
            .findFirst().orElseThrow()
            .getLongSumData().getPoints().stream()
            .mapToLong(point -> point.getValue()).sum();
    }

    private static Flow.Publisher<Integer> twoItemPublisher() {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int emitted;

            @Override public void request(long n) {
                if (n > 0 && emitted < 2) {
                    subscriber.onNext(++emitted);
                    if (emitted == 2) {
                        subscriber.onComplete();
                    }
                }
            }

            @Override public void cancel() { }
        });
    }
}

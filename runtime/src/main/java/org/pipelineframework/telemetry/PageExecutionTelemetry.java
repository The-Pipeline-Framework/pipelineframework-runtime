package org.pipelineframework.telemetry;

import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.pipelineframework.paging.PagedSourceCompletion;

/** Low-cardinality signals for bounded source-page execution. */
@ApplicationScoped
public final class PageExecutionTelemetry {
    private static final AttributeKey<Boolean> EXHAUSTED = AttributeKey.booleanKey("tpf.page.exhausted");

    private final LongCounter completed;
    private final LongCounter records;
    private final LongCounter replayed;
    private final LongCounter demandStalls;
    private final DoubleHistogram duration;
    private final DoubleHistogram demandStallDuration;

    @Inject
    public PageExecutionTelemetry(TelemetryRuntime runtime) {
        Meter meter = runtime.meter("org.pipelineframework.paging");
        completed = meter.counterBuilder("tpf.page.completed.total")
            .setDescription("Normally completed source pages").setUnit("pages").build();
        records = meter.counterBuilder("tpf.page.records.consumed.total")
            .setDescription("Logical source records consumed by completed pages").setUnit("records").build();
        replayed = meter.counterBuilder("tpf.page.replayed.total")
            .setDescription("Source page attempts replayed after prior execution attempts").setUnit("pages").build();
        demandStalls = meter.counterBuilder("tpf.page.demand.stalls.total")
            .setDescription("Intervals where an open page had no downstream demand").setUnit("stalls").build();
        duration = meter.histogramBuilder("tpf.page.duration")
            .setDescription("Source page lifetime through resource release").setUnit("ms").build();
        demandStallDuration = meter.histogramBuilder("tpf.page.demand.stall.duration")
            .setDescription("Time an open source page waited for more downstream demand").setUnit("ms").build();
    }

    public static PageExecutionTelemetry disabled() {
        return new PageExecutionTelemetry(new NoopTelemetryRuntime());
    }

    public PageObservation open(boolean replay) {
        if (replay) {
            replayed.add(1);
        }
        return new PageObservation(this, System.nanoTime());
    }

    private void complete(long startedNanos, PagedSourceCompletion result) {
        Attributes attributes = Attributes.of(EXHAUSTED, result.exhausted());
        completed.add(1, attributes);
        records.add(result.consumedRecords(), attributes);
        duration.record(elapsedMillis(startedNanos), attributes);
    }

    private void stall(long startedNanos) {
        double elapsed = elapsedMillis(startedNanos);
        if (elapsed > 0d) {
            demandStalls.add(1);
            demandStallDuration.record(elapsed);
        }
    }

    private static double elapsedMillis(long startedNanos) {
        return (System.nanoTime() - startedNanos) / (double) TimeUnit.MILLISECONDS.toNanos(1);
    }

    public static final class PageObservation {
        private final PageExecutionTelemetry telemetry;
        private final long startedNanos;
        private final AtomicBoolean completed = new AtomicBoolean();

        private PageObservation(PageExecutionTelemetry telemetry, long startedNanos) {
            this.telemetry = telemetry;
            this.startedNanos = startedNanos;
        }

        public <T> Flow.Publisher<T> observeDemand(Flow.Publisher<T> source) {
            return downstream -> source.subscribe(new Flow.Subscriber<>() {
                private final AtomicLong outstanding = new AtomicLong();
                private volatile long demandDepletedNanos;
                private volatile boolean delivered;

                @Override public void onSubscribe(Flow.Subscription upstream) {
                    downstream.onSubscribe(new Flow.Subscription() {
                        @Override public void request(long n) {
                            if (n > 0) {
                                long depletedAt = demandDepletedNanos;
                                if (delivered && outstanding.get() == 0 && depletedAt > 0) {
                                    telemetry.stall(depletedAt);
                                }
                                outstanding.updateAndGet(current -> current > Long.MAX_VALUE - n
                                    ? Long.MAX_VALUE : current + n);
                                demandDepletedNanos = 0;
                            }
                            upstream.request(n);
                        }

                        @Override public void cancel() { upstream.cancel(); }
                    });
                }

                @Override public void onNext(T item) {
                    delivered = true;
                    long remaining = outstanding.updateAndGet(current -> current == Long.MAX_VALUE
                        ? Long.MAX_VALUE : Math.max(0, current - 1));
                    if (remaining == 0) {
                        demandDepletedNanos = System.nanoTime();
                    }
                    downstream.onNext(item);
                }

                @Override public void onError(Throwable throwable) { downstream.onError(throwable); }
                @Override public void onComplete() { downstream.onComplete(); }
            });
        }

        public void complete(PagedSourceCompletion result) {
            if (completed.compareAndSet(false, true)) {
                telemetry.complete(startedNanos, result);
            }
        }
    }
}

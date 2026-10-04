package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pipelineframework.telemetry.TelemetryTestRuntime;
import org.pipelineframework.telemetry.derivation.TransitionTelemetryDerivation;

class TransitionTelemetryOwnershipTest {
    @ParameterizedTest
    @CsvSource({"false,true,true", "true,false,false", "true,true,false", "true,false,true", "true,true,true"})
    void workerSignalsRespectTheMasterAndSignalSwitches(boolean framework, boolean metrics, boolean tracing) {
        try (var sdk = new TelemetryTestRuntime()) {
            var policy = sdk.policy(framework, metrics, tracing);
            var recorder = new TransitionWorkerMetrics(() -> policy, sdk);
            recorder.record(new TransitionTelemetryDerivation.MetricSignal(TransitionTelemetryDerivation.Metric.DISPATCHED, 1, Map.of()));
            new TransitionWorkerTracing(() -> policy, sdk).record(
                new TransitionTelemetryDerivation.SpanPlan("tpf.transition.dispatched", Map.of()));
            assertEquals(policy.metricsEnabled(), sdk.meterRequests.get() > 0);
            assertEquals(policy.tracingEnabled(), sdk.tracerRequests.get() > 0);
            assertEquals(policy.metricsEnabled(), !sdk.reader.collectAllMetrics().isEmpty());
            assertEquals(policy.tracingEnabled(), !sdk.exporter.getFinishedSpanItems().isEmpty());
            recorder.close();
        }
    }

    @Test
    void separateInstancesNeverShareActivityAndShutdownRemovesTheGauge() {
        try (var first = new TelemetryTestRuntime(); var second = new TelemetryTestRuntime()) {
            var one = new TransitionWorkerMetrics(() -> first.policy(true, true, false), first);
            var two = new TransitionWorkerMetrics(() -> second.policy(true, true, false), second);
            one.record(new TransitionTelemetryDerivation.MetricSignal(TransitionTelemetryDerivation.Metric.ACTIVE, 1, Map.of()));
            assertEquals(1L, first.reader.collectAllMetrics().stream().filter(m -> m.getName().endsWith(".active"))
                .findFirst().orElseThrow().getLongGaugeData().getPoints().iterator().next().getValue());
            assertEquals(0L, second.reader.collectAllMetrics().stream().filter(m -> m.getName().endsWith(".active"))
                .findFirst().orElseThrow().getLongGaugeData().getPoints().iterator().next().getValue());
            one.close();
            one.close();
            assertTrue(first.reader.collectAllMetrics().stream().noneMatch(m -> m.getName().endsWith(".active")));
            two.close();
        }
    }
}

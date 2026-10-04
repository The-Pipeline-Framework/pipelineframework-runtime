package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pipelineframework.awaitable.AwaitTelemetry;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.connector.*;
import org.pipelineframework.paging.PagedSourceCompletion;

class TelemetryOwnershipTest {
    @ParameterizedTest
    @CsvSource({"false,true,true", "true,false,false", "true,true,false", "true,false,true", "true,true,true"})
    void pipelineAndBoundariesUseTheSamePolicy(boolean framework, boolean metrics, boolean tracing) {
        try (var sdk = new TelemetryTestRuntime()) {
            var policy = sdk.policy(framework, metrics, tracing);
            TelemetryPolicySource source = () -> policy;
            var config = mock(PipelineStepConfig.class, RETURNS_DEEP_STUBS);
            var runtime = new PipelineTelemetryRuntime(config, new NoopPipelineReplayExporter(), Optional.empty(), sdk, policy);
            var run = runtime.startRun("input", 1, ParallelismPolicy.AUTO, 2);
            var result = runtime.instrumentStepUni(getClass(), Uni.createFrom().item("result"), run, false);
            assertEquals("result", ((Uni<?>) runtime.instrumentRunCompletion(result, run)).await().indefinitely());
            new AwaitTelemetry(source, sdk).recordDroppedCompletion("kafka", "duplicate");
            new AwaitTelemetry(source, sdk).recordProviderAdmitted();
            new PageExecutionTelemetry(source, sdk).open(true)
                .complete(new PagedSourceCompletion(2, Optional.empty(), true));
            var replay = mock(PipelineReplayTelemetry.class);
            new ObjectIngestReplayTelemetry(sdk, policy, replay).listed("source", "s3", 2);
            new ObjectPublishReplayTelemetry(sdk, policy, replay).published("target", "s3", "object-123", 5);
            new QueryObservationTelemetry(source, sdk).record(
                new ConnectorOperationIdentity(ConnectorProviderId.of("tpf.llm.openai"), "turn", ConnectorOperationKind.QUERY, 1),
                QueryObservation.live(Optional.of(new QueryTokenUsage(OptionalLong.of(2), OptionalLong.of(1), OptionalLong.of(3))),
                    Optional.of("model"), Optional.of("stop")));
            new HttpMetricsRecorder(source, sdk).instrumentClient("service", "method", Uni.createFrom().item(1))
                .await().indefinitely();
            new GrpcClientTracingRecorder(source, sdk).traceUnary("service", "method", Uni.createFrom().item(1))
                .await().indefinitely();
            new ApmCompatibilityMetricsRecorder(source, sdk).recordOrchestratorSuccess(2);
            var buffer = new BackpressureBufferMetricsRecorder(source, sdk);
            assertEquals(List.of(1, 2), buffer.buffer(Multi.createFrom().items(1, 2), getClass(), 2)
                .collect().asList().await().indefinitely());
            assertEquals(policy.metricsEnabled(), sdk.meterRequests.get() > 0);
            assertEquals(policy.tracingEnabled(), sdk.tracerRequests.get() > 0);
            assertEquals(policy.metricsEnabled(), !sdk.reader.collectAllMetrics().isEmpty());
            assertEquals(policy.tracingEnabled(), !sdk.exporter.getFinishedSpanItems().isEmpty());
            for (var metric : sdk.reader.collectAllMetrics()) {
                metric.getData().getPoints().forEach(point -> point.getAttributes().forEach((key, value) ->
                    assertFalse(key.getKey().matches(".*(execution|interaction|correlation|object_key|run_id).*"), key.getKey())));
            }
            runtime.shutdownRetryAmplificationScheduler();
            buffer.close();
        }
    }

    @Test
    void policyResolutionDoesNotConstructTheExecutionComposition() {
        var config = mock(PipelineStepConfig.class, RETURNS_DEEP_STUBS);
        when(config.telemetry().enabled()).thenReturn(true);
        when(config.telemetry().metrics().enabled()).thenReturn(true);
        var source = new ConfiguredTelemetryPolicy(config);
        assertTrue(source.telemetryPolicy().metricsEnabled());
        assertSame(source.telemetryPolicy(), source.telemetryPolicy());
        assertFalse(PipelineTelemetryRuntime.class.isAssignableFrom(ConfiguredTelemetryPolicy.class));
    }

    @Test
    void closingOnePipelineRemovesItsGaugeCallbacksWithoutClosingTheSdk() {
        try (var sdk = new TelemetryTestRuntime()) {
            var attributes = new PipelineMetricAttributes(Optional.empty(), Optional.empty());
            var recorder = new PipelineMetricsRecorder(sdk.policy(true, true, false), sdk, attributes);
            assertTrue(sdk.reader.collectAllMetrics().stream().anyMatch(metric -> metric.getName().equals("tpf.pipeline.max_concurrency")));
            recorder.close();
            recorder.close();
            assertTrue(sdk.reader.collectAllMetrics().stream().noneMatch(metric -> metric.getName().equals("tpf.pipeline.max_concurrency")));
            var replacement = new PipelineMetricsRecorder(sdk.policy(true, true, false), sdk, attributes);
            assertTrue(sdk.reader.collectAllMetrics().stream().anyMatch(metric -> metric.getName().equals("tpf.pipeline.max_concurrency")));
            replacement.close();
        }
    }
}

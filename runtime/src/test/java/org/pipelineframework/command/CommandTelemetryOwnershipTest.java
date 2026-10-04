package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pipelineframework.telemetry.TelemetryTestRuntime;

class CommandTelemetryOwnershipTest {
    @ParameterizedTest
    @CsvSource({"false,true", "true,false", "true,true"})
    void commandEffectsRespectFrameworkMetricsPolicy(boolean framework, boolean metrics) {
        try (var sdk = new TelemetryTestRuntime()) {
            var policy = sdk.policy(framework, metrics, false);
            var recorder = new CommandEffectMetricsRecorder(() -> policy, sdk);
            var descriptor = new CommandDescriptor("write", "effect", "Input", "Output", "id-generator",
                CommandDuplicatePolicy.RETURN_RECORDED, Map.of());
            recorder.recordTransition(descriptor, CommandEffectStatus.SUCCEEDED);
            recorder.recordDuplicate(descriptor, "recorded");
            recorder.recordAdmission(descriptor, "admitted");
            assertEquals(policy.metricsEnabled(), sdk.meterRequests.get() > 0);
            assertEquals(policy.metricsEnabled(), !sdk.reader.collectAllMetrics().isEmpty());
        }
    }
}

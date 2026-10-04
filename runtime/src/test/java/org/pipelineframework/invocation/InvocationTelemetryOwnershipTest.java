package org.pipelineframework.invocation;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pipelineframework.runtime.core.resilience.CircuitIdentity;
import org.pipelineframework.runtime.core.resilience.CircuitScope;
import org.pipelineframework.runtime.core.resilience.CircuitStateTransition;
import org.pipelineframework.telemetry.TelemetryTestRuntime;

class InvocationTelemetryOwnershipTest {
    @ParameterizedTest
    @CsvSource({"false,true", "true,false", "true,true"})
    void invocationAndCircuitDiagnosticsShareMetricsPolicy(boolean framework, boolean metrics) {
        try (var sdk = new TelemetryTestRuntime()) {
            var policy = sdk.policy(framework, metrics, false);
            var boundary = new TransportBoundaryDescriptor("grpc", "remote.operation");
            new TransportBoundaryDiagnostics(() -> policy, sdk).recordCircuitRejected(boundary);
            new CircuitTelemetry(() -> policy, sdk).onTransition(new CircuitIdentity("remote"),
                CircuitScope.LOCAL_PROCESS, CircuitStateTransition.CLOSED_TO_OPEN);
            assertEquals(policy.metricsEnabled(), sdk.meterRequests.get() > 0);
            assertEquals(policy.metricsEnabled(), !sdk.reader.collectAllMetrics().isEmpty());
        }
    }
}

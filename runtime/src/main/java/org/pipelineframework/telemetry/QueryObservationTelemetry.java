package org.pipelineframework.telemetry;

import java.util.Objects;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.telemetry.derivation.QueryObservationDerivation;

/** Coordinates independent metric and tracing adapters at the Query invocation/capture boundary. */
@Singleton
public final class QueryObservationTelemetry {
    private final QueryObservationMetrics metrics;
    private final QueryObservationTracing tracing;

    @Inject
    public QueryObservationTelemetry(TelemetryPolicySource source, TelemetryRuntime runtime) {
        this(source.telemetryPolicy(), runtime);
    }

    public QueryObservationTelemetry(TelemetryPolicy policy, TelemetryRuntime runtime) {
        this(policy.metricsEnabled() ? runtime : new NoopTelemetryRuntime(),
            policy.tracingEnabled() ? runtime : new NoopTelemetryRuntime());
    }

    public QueryObservationTelemetry(TelemetryRuntime runtime) {
        Objects.requireNonNull(runtime, "telemetry runtime must not be null");
        metrics = new QueryObservationMetrics(runtime);
        tracing = new QueryObservationTracing(runtime);
    }

    public static QueryObservationTelemetry global() {
        return new QueryObservationTelemetry(
            TelemetryCompatibilityAccess.metricsRuntime(),
            TelemetryCompatibilityAccess.tracingRuntime());
    }

    private QueryObservationTelemetry(TelemetryRuntime metricsRuntime, TelemetryRuntime tracingRuntime) {
        metrics = new QueryObservationMetrics(metricsRuntime);
        tracing = new QueryObservationTracing(tracingRuntime);
    }

    public void record(ConnectorOperationIdentity operation, QueryObservation observation) {
        QueryObservationDerivation.Signal signal;
        try {
            signal = QueryObservationDerivation.derive(operation, observation);
        } catch (RuntimeException ignored) {
            return;
        }
        try {
            metrics.record(signal);
        } catch (RuntimeException ignored) {
            // Metric export must never change the application result or suppress tracing.
        }
        try {
            tracing.record(signal);
        } catch (RuntimeException ignored) {
            // Observation telemetry must never change the application result.
        }
    }
}

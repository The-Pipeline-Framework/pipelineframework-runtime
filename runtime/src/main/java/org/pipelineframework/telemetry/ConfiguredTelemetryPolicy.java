package org.pipelineframework.telemetry;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.config.PipelineStepConfig;

/** Owns framework intent without constructing instruments, exporters or execution services. */
@ApplicationScoped
public class ConfiguredTelemetryPolicy implements TelemetryPolicySource {
    private final TelemetryPolicy policy;

    @Inject
    public ConfiguredTelemetryPolicy(PipelineStepConfig config) {
        policy = TelemetryPolicy.from(config, PipelineReplayTopologyLoader.load().isPresent());
    }

    @Override
    public TelemetryPolicy telemetryPolicy() {
        return policy;
    }
}

package org.pipelineframework.extension;

import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.deployment.Capabilities;
import io.quarkus.deployment.Capability;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.pkg.builditem.CurateOutcomeBuildItem;
import io.quarkus.opentelemetry.runtime.config.build.OTelBuildConfig;
import jakarta.inject.Singleton;
import org.pipelineframework.telemetry.TelemetryCapabilities;
import org.pipelineframework.telemetry.TelemetryCapabilitiesRecorder;

/** Captures actual build capabilities independently of framework policy and exporter routing. */
public class TelemetryCapabilityBuildSteps {
    @BuildStep
    @Record(ExecutionTime.STATIC_INIT)
    SyntheticBeanBuildItem capabilities(Capabilities capabilities, OTelBuildConfig config,
                                       CurateOutcomeBuildItem application, TelemetryCapabilitiesRecorder recorder) {
        boolean bridge = application.getApplicationModel().getDependencies().stream()
            .anyMatch(dependency -> "quarkus-micrometer-opentelemetry".equals(dependency.getArtifactId()));
        boolean micrometer = application.getApplicationModel().getDependencies().stream()
            .anyMatch(dependency -> "quarkus-micrometer".equals(dependency.getArtifactId()));
        TelemetryCapabilities resolved = resolve(capabilities, config, bridge, micrometer);
        return SyntheticBeanBuildItem.configure(TelemetryCapabilities.class).scope(Singleton.class).unremovable()
            .supplier(recorder.capabilities(resolved.openTelemetryPresent(), resolved.tracingCapable(),
                resolved.metricsCapable(), resolved.logsCapable(), resolved.micrometerCapable())).done();
    }

    TelemetryCapabilities resolve(Capabilities capabilities, OTelBuildConfig config,
                                  boolean bridge, boolean micrometer) {
        boolean present = capabilities.isCapabilityWithPrefixPresent("io.quarkus.opentelemetry");
        boolean enabled = present && config.enabled();
        return new TelemetryCapabilities(present,
            enabled && capabilities.isPresent(Capability.OPENTELEMETRY_TRACER) && config.traces().enabled().orElse(true),
            enabled && capabilities.isPresent(Capability.OPENTELEMETRY_METRICS) && config.metrics().enabled().orElse(bridge),
            enabled && capabilities.isPresent(Capability.OPENTELEMETRY_LOGS) && config.logs().enabled().orElse(bridge),
            micrometer);
    }
}

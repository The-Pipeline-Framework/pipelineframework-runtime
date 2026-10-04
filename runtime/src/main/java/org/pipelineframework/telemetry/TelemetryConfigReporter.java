/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.telemetry;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.enterprise.inject.Instance;
import java.util.List;

import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

/**
 * Reports framework intent and immutable build capabilities separately from exporter routing.
 */
@ApplicationScoped
public class TelemetryConfigReporter {

    private static final Logger logger = Logger.getLogger(TelemetryConfigReporter.class);
    @Inject
    TelemetryPolicySource policySource;
    @Inject
    Instance<TelemetryCapabilities> capabilities;

    private static final String LICENSE_ENV = "NEW_RELIC_LICENSE_KEY";

    /**
     * Default constructor.
     */
    public TelemetryConfigReporter() {
    }

    void onStart(@Observes StartupEvent event) {
        Config runtimeConfig = ConfigProvider.getConfig();
        TelemetryPolicy policy = policySource.telemetryPolicy();
        if (!capabilities.isResolvable()) {
            logger.warn("TPF telemetry build capabilities are unavailable; use a matching TPF deployment artifact");
        }
        for (SignalStatus signal : capabilities.isResolvable()
            ? status(policy, capabilities.get(), runtimeConfig) : List.<SignalStatus>of()) {
            logger.infof("TPF telemetry: signal=%s requested=%s capable=%s sdkDisabled=%s exporters=%s otlpEnabled=%s delivery=unverified",
                signal.signal(), signal.requested(), signal.capable(), signal.sdkDisabled(), signal.exporters(), signal.otlpEnabled());
            if (signal.requested() && (!signal.capable() || signal.sdkDisabled())) {
                logger.warnf("TPF telemetry configuration mismatch: %s requested but %s; application startup continues",
                    signal.signal(), !signal.capable() ? "absent from the built artifact" : "the SDK is disabled");
            }
        }
        String licenseKey = System.getenv(LICENSE_ENV);
        if (licenseKey == null || licenseKey.isBlank()) {
            return;
        }
        Config config = ConfigProvider.getConfig();
        String enabled = config.getOptionalValue("quarkus.otel.enabled", String.class).orElse("<unset>");
        String endpoint = config.getOptionalValue("quarkus.otel.exporter.otlp.endpoint", String.class).orElse("<unset>");
        String headers = config.getOptionalValue("quarkus.otel.exporter.otlp.headers", String.class).orElse("<unset>");
        String headersHint = headers.isBlank() || "<unset>".equals(headers) ? "<unset>" : "<redacted>";

        logger.infof(
            "NR telemetry config: quarkus.otel.enabled=%s, otlp.endpoint=%s, otlp.headers=%s",
            enabled,
            endpoint,
            headersHint
        );
    }
    List<SignalStatus> status(TelemetryPolicy policy, TelemetryCapabilities capabilities, Config config) {
        boolean sdkDisabled = config.getOptionalValue("quarkus.otel.sdk.disabled", Boolean.class).orElse(false);
        return List.of(
            new SignalStatus("metrics", policy.metricsEnabled(), capabilities.metricsCapable(), sdkDisabled,
                config.getOptionalValue("quarkus.otel.metrics.exporter", String.class).orElse("platform-default"),
                otlpEnabled(config, "metrics")),
            new SignalStatus("tracing", policy.tracingEnabled(), capabilities.tracingCapable(), sdkDisabled,
                config.getOptionalValue("quarkus.otel.traces.exporter", String.class).orElse("platform-default"),
                otlpEnabled(config, "traces")));
    }

    private boolean otlpEnabled(Config config, String signal) {
        return config.getOptionalValue("quarkus.otel.exporter.otlp." + signal + ".enabled", Boolean.class)
            .orElseGet(() -> config.getOptionalValue("quarkus.otel.exporter.otlp.enabled", Boolean.class).orElse(true));
    }

    record SignalStatus(String signal, boolean requested, boolean capable, boolean sdkDisabled,
                        String exporters, boolean otlpEnabled) { }

}

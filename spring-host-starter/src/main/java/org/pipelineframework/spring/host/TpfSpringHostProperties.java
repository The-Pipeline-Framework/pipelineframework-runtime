/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.spring.host;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the Spring Boot ADR-0092 Foreign Function &amp; Connector Host.
 */
@ConfigurationProperties(prefix = "tpf.host")
public class TpfSpringHostProperties {

    /** Whether the TPF foreign host endpoints are enabled. */
    private boolean enabled = true;

    /**
     * Shared HMAC-SHA256 secret used to verify TPF requests and answer loopback connector
     * challenges. Falls back to environment variable {@code TPF_FUNCTION_HOST_SECRET} when blank.
     */
    private String sharedSecret = "";

    /** Maximum permitted clock skew for {@code X-TPF-Worker-Timestamp} signatures. */
    private Duration maxClockSkew = Duration.ofMinutes(5);

    /** Maximum buffered byte size for JSON request envelopes (unary, stream, connector). */
    private int maxRequestBytes = 4 * 1024 * 1024;

    /** Maximum byte size for a single NDJSON line frame on {@code /reduce} and {@code /duplex}. */
    private int maxFrameBytes = 4 * 1024 * 1024;

    /** Upper bound on remembered nonces within the replay window. */
    private int maxNonceCacheSize = 65_536;

    /** Optional map of {@code stepId -> outputTypeId} overrides for streaming steps. */
    private Map<String, String> stepOutputTypes = new LinkedHashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getSharedSecret() {
        return sharedSecret;
    }

    public void setSharedSecret(String sharedSecret) {
        this.sharedSecret = sharedSecret == null ? "" : sharedSecret;
    }

    public Duration getMaxClockSkew() {
        return maxClockSkew;
    }

    public void setMaxClockSkew(Duration maxClockSkew) {
        if (maxClockSkew != null) {
            this.maxClockSkew = maxClockSkew;
        }
    }

    public int getMaxRequestBytes() {
        return maxRequestBytes;
    }

    public void setMaxRequestBytes(int maxRequestBytes) {
        this.maxRequestBytes = maxRequestBytes;
    }

    public int getMaxFrameBytes() {
        return maxFrameBytes;
    }

    public void setMaxFrameBytes(int maxFrameBytes) {
        this.maxFrameBytes = maxFrameBytes;
    }

    public int getMaxNonceCacheSize() {
        return maxNonceCacheSize;
    }

    public void setMaxNonceCacheSize(int maxNonceCacheSize) {
        this.maxNonceCacheSize = maxNonceCacheSize;
    }

    public Map<String, String> getStepOutputTypes() {
        return stepOutputTypes;
    }

    public void setStepOutputTypes(Map<String, String> stepOutputTypes) {
        this.stepOutputTypes = stepOutputTypes == null ? new LinkedHashMap<>() : new LinkedHashMap<>(stepOutputTypes);
    }
}

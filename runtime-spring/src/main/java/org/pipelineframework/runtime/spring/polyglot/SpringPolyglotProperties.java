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

package org.pipelineframework.runtime.spring.polyglot;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot configuration properties for Spring-hosted polyglot foreign function steps.
 */
@ConfigurationProperties(prefix = "pipeline.polyglot")
public class SpringPolyglotProperties {

    private boolean enabled = false;
    private String functionHostUri = "";
    private Map<String, String> stepUris = new LinkedHashMap<>();
    private String sharedSecret = "";
    private boolean allowLoopbackHttp = false;
    private Duration requestTimeout = Duration.ofSeconds(30);
    private Duration streamIdleTimeout = Duration.ofSeconds(60);
    private int maxResponseBytes = SpringCanonicalFunctionHostStep.DEFAULT_MAX_RESPONSE_BYTES;
    private String contractResourcePath = PipelineContractDescriptor.RESOURCE_PATH;
    private String stateDir = "";
    private boolean allowInMemoryStores = false;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getFunctionHostUri() {
        return functionHostUri;
    }

    public void setFunctionHostUri(String functionHostUri) {
        this.functionHostUri = functionHostUri == null ? "" : functionHostUri;
    }

    public Map<String, String> getStepUris() {
        return stepUris;
    }

    public void setStepUris(Map<String, String> stepUris) {
        this.stepUris = stepUris == null ? new LinkedHashMap<>() : new LinkedHashMap<>(stepUris);
    }

    public String getSharedSecret() {
        return sharedSecret;
    }

    public void setSharedSecret(String sharedSecret) {
        this.sharedSecret = sharedSecret == null ? "" : sharedSecret;
    }

    public boolean isAllowLoopbackHttp() {
        return allowLoopbackHttp;
    }

    public void setAllowLoopbackHttp(boolean allowLoopbackHttp) {
        this.allowLoopbackHttp = allowLoopbackHttp;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public Duration getStreamIdleTimeout() {
        return streamIdleTimeout;
    }

    public void setStreamIdleTimeout(Duration streamIdleTimeout) {
        this.streamIdleTimeout = streamIdleTimeout;
    }

    public int getMaxResponseBytes() {
        return maxResponseBytes;
    }

    public void setMaxResponseBytes(int maxResponseBytes) {
        this.maxResponseBytes = maxResponseBytes;
    }

    public String getContractResourcePath() {
        return contractResourcePath;
    }

    public void setContractResourcePath(String contractResourcePath) {
        this.contractResourcePath = contractResourcePath == null || contractResourcePath.isBlank()
            ? PipelineContractDescriptor.RESOURCE_PATH
            : contractResourcePath;
    }

    /**
     * Directory for durable file-backed dispatch-intent and Await state stores when custom store
     * beans are not supplied.
     */
    public String getStateDir() {
        return stateDir;
    }

    public void setStateDir(String stateDir) {
        this.stateDir = stateDir == null ? "" : stateDir.trim();
    }

    /**
     * Explicit opt-in for local- and test-only in-memory dispatch-intent and Await state stores.
     * Defaults to {@code false} so production Spring polyglot deployments fail closed if no
     * durable store is configured.
     */
    public boolean isAllowInMemoryStores() {
        return allowInMemoryStores;
    }

    public void setAllowInMemoryStores(boolean allowInMemoryStores) {
        this.allowInMemoryStores = allowInMemoryStores;
    }
}

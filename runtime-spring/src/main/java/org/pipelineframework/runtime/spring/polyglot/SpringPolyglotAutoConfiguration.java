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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import org.pipelineframework.runtime.core.PipelineRunnerCore;
import org.pipelineframework.runtime.core.PipelineUnaryStep;
import org.pipelineframework.runtime.spring.SpringPipelineRunner;
import org.pipelineframework.runtime.spring.SpringRuntimeAdaptersAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot auto-configuration that wires {@link SpringCanonicalFunctionHostStep} instances,
 * pre-send {@link SpringForeignDispatchIntentStore}, {@link SpringAwaitResumeCoordinator}, and
 * {@link SpringCheckpointHandoffCodec} from compiler-emitted {@code META-INF/pipeline/pipeline-contract.json}.
 */
@AutoConfiguration(before = SpringRuntimeAdaptersAutoConfiguration.class)
@EnableConfigurationProperties(SpringPolyglotProperties.class)
public class SpringPolyglotAutoConfiguration {

    private static final Set<String> SUPPORTED_CARDINALITIES =
        Set.of("ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_ONE", "MANY_TO_MANY");

    /** Immutable registry of contract-derived Spring polyglot steps in pipeline order. */
    public record SpringPolyglotStepRegistry(
        String pipelineId,
        String catalogFingerprint,
        List<SpringCanonicalFunctionHostStep> orderedSteps
    ) {
        public SpringPolyglotStepRegistry {
            Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(catalogFingerprint, "catalogFingerprint");
            orderedSteps = List.copyOf(Objects.requireNonNull(orderedSteps, "orderedSteps"));
        }

        public Optional<SpringCanonicalFunctionHostStep> findByStepId(String stepId) {
            return orderedSteps.stream().filter(s -> s.spec().stepId().equals(stepId)).findFirst();
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public SpringForeignDispatchIntentStore springForeignDispatchIntentStore() {
        return SpringForeignDispatchIntentStore.inMemory();
    }

    @Bean
    @ConditionalOnMissingBean
    public SpringCheckpointHandoffCodec springCheckpointHandoffCodec() {
        return new SpringCheckpointHandoffCodec();
    }

    @Bean
    @ConditionalOnMissingBean
    public SpringAwaitResumeCoordinator springAwaitResumeCoordinator() {
        return new SpringAwaitResumeCoordinator();
    }

    @Bean
    @ConditionalOnProperty(prefix = "pipeline.polyglot", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public SpringPolyglotStepRegistry springPolyglotStepRegistry(
        SpringPolyglotProperties properties,
        SpringForeignDispatchIntentStore intentStore
    ) {
        return buildRegistry(properties, intentStore, Thread.currentThread().getContextClassLoader());
    }

    @Bean
    @ConditionalOnProperty(prefix = "pipeline.polyglot", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean(SpringPipelineRunner.class)
    public SpringPipelineRunner polyglotSpringPipelineRunner(
        PipelineRunnerCore runnerCore,
        SpringPolyglotStepRegistry registry,
        List<PipelineUnaryStep<?, ?>> registeredUnarySteps
    ) {
        boolean allUnary = registry.orderedSteps().stream()
            .allMatch(step -> "ONE_TO_ONE".equals(step.spec().cardinality()));
        if (!allUnary) {
            return new SpringPipelineRunner(runnerCore, registeredUnarySteps);
        }
        List<PipelineUnaryStep<?, ?>> unarySteps = new ArrayList<>(registry.orderedSteps());
        return new SpringPipelineRunner(runnerCore, unarySteps);
    }

    public static SpringPolyglotStepRegistry buildRegistry(
        SpringPolyglotProperties properties,
        SpringForeignDispatchIntentStore intentStore,
        ClassLoader classLoader
    ) {
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(intentStore, "intentStore");
        if (properties.getSharedSecret().isBlank()) {
            throw new IllegalStateException(
                "pipeline.polyglot.shared-secret must be configured when pipeline.polyglot.enabled=true");
        }
        JsonNode contract = loadContractJson(properties.getContractResourcePath(), classLoader);
        String pipelineId = contract.path("pipelineId").asText("local-pipeline");
        String catalogFingerprint = contract.path("canonicalCatalogFingerprint").asText("");
        if (catalogFingerprint.isBlank()) {
            throw new IllegalStateException(
                "Compiled contract at " + properties.getContractResourcePath()
                    + " is missing canonicalCatalogFingerprint");
        }
        JsonNode canonicalTypes = contract.path("canonicalTypes");
        if (!canonicalTypes.isObject() || canonicalTypes.isEmpty()) {
            throw new IllegalStateException(
                "Compiled contract at " + properties.getContractResourcePath()
                    + " has no canonicalTypes definitions");
        }
        JsonNode stepsNode = contract.path("steps");
        if (!stepsNode.isArray() || stepsNode.isEmpty()) {
            throw new IllegalStateException(
                "Compiled contract at " + properties.getContractResourcePath() + " has no steps");
        }

        List<SpringCanonicalFunctionHostStep> steps = new ArrayList<>();
        for (int i = 0; i < stepsNode.size(); i++) {
            JsonNode stepNode = stepsNode.get(i);
            int stepIndex = stepNode.path("stepIndex").asInt(i);
            String authoredName = stepNode.path("authoredName").asText("");
            String stepId = toKebabCase(authoredName);
            String cardinality = stepNode.path("cardinality").asText("ONE_TO_ONE").toUpperCase(Locale.ROOT);
            if (!SUPPORTED_CARDINALITIES.contains(cardinality)) {
                throw new IllegalArgumentException(
                    "Unsupported polyglot step cardinality '" + cardinality + "' for step '" + stepId + "'");
            }
            String inputTypeId = stepNode.path("inputType").asText("");
            String outputTypeId = stepNode.path("outputType").asText("");
            String inputFp = definitionFingerprint(canonicalTypes, inputTypeId);
            String outputFp = definitionFingerprint(canonicalTypes, outputTypeId);
            String uriText = resolveStepUri(stepId, properties);
            URI origin = SpringCanonicalFunctionHostStep.normalizeOrigin(uriText, properties.isAllowLoopbackHttp());

            SpringCanonicalFunctionHostStep.Spec spec = new SpringCanonicalFunctionHostStep.Spec(
                stepId,
                stepIndex,
                cardinality,
                inputTypeId,
                inputFp,
                outputTypeId,
                outputFp,
                catalogFingerprint,
                origin,
                properties.getSharedSecret(),
                properties.getRequestTimeout(),
                properties.getStreamIdleTimeout(),
                intentStore,
                properties.getMaxResponseBytes());
            steps.add(new SpringCanonicalFunctionHostStep(spec, properties.isAllowLoopbackHttp()));
        }
        return new SpringPolyglotStepRegistry(pipelineId, catalogFingerprint, steps);
    }

    private static JsonNode loadContractJson(String resourcePath, ClassLoader classLoader) {
        ClassLoader effective = classLoader != null ? classLoader : SpringPolyglotAutoConfiguration.class.getClassLoader();
        try (InputStream in = effective.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("Compiled pipeline contract not found on classpath: " + resourcePath);
            }
            return SpringPolyglotJson.exactMapper().readTree(in);
        } catch (IOException failure) {
            throw new IllegalStateException("Failed reading compiled pipeline contract: " + resourcePath, failure);
        }
    }

    private static String definitionFingerprint(JsonNode canonicalTypes, String typeId) {
        JsonNode binding = canonicalTypes.get(typeId);
        if (binding == null || !binding.isObject()) {
            throw new IllegalStateException("Canonical type '" + typeId + "' not found in compiled contract");
        }
        String fp = binding.path("definitionFingerprint").asText("");
        if (fp.isBlank()) {
            throw new IllegalStateException("Canonical type '" + typeId + "' is missing definitionFingerprint");
        }
        return fp;
    }

    private static String resolveStepUri(String stepId, SpringPolyglotProperties properties) {
        Map<String, String> overrides = properties.getStepUris();
        if (overrides.containsKey(stepId) && !overrides.get(stepId).isBlank()) {
            return overrides.get(stepId);
        }
        if (!properties.getFunctionHostUri().isBlank()) {
            return properties.getFunctionHostUri();
        }
        throw new IllegalStateException(
            "No function host URI configured for polyglot step '" + stepId
                + "' (set pipeline.polyglot.function-host-uri or pipeline.polyglot.step-uris." + stepId + ")");
    }

    static String toKebabCase(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("Step authoredName must not be blank");
        }
        String trimmed = raw.trim();
        StringBuilder out = new StringBuilder(trimmed.length() + 4);
        boolean lastDash = false;
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (Character.isLetterOrDigit(c)) {
                if (Character.isUpperCase(c) && out.length() > 0 && !lastDash
                    && Character.isLowerCase(trimmed.charAt(i - 1))) {
                    out.append('-');
                }
                out.append(Character.toLowerCase(c));
                lastDash = false;
            } else if (!lastDash && out.length() > 0) {
                out.append('-');
                lastDash = true;
            }
        }
        int end = out.length();
        while (end > 0 && out.charAt(end - 1) == '-') {
            end--;
        }
        return out.substring(0, end);
    }
}

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

package org.pipelineframework.config.pipeline;

import java.io.InputStream;
import java.util.Enumeration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jboss.logging.Logger;

/**
 * Loads the pipeline execution order from the generated resource.
 */
public final class PipelineOrderResourceLoader {

    private static final String RESOURCE = "META-INF/pipeline/order.json";
    private static final String ORCHESTRATOR_CLIENTS_RESOURCE = "META-INF/pipeline/orchestrator-clients.properties";
    private static final Logger LOG = Logger.getLogger(PipelineOrderResourceLoader.class);

    private PipelineOrderResourceLoader() {
    }

    /**
     * Loads the pipeline execution order from the generated order resource.
     *
     * @return the ordered list of step class names, if available
     */
    public static Optional<List<String>> loadOrder() {
        ClassLoader classLoader = PipelineResources.resolveClassLoader();
        try {
            if (classLoader != null) {
                Enumeration<java.net.URL> resources = classLoader.getResources(RESOURCE);
                List<List<String>> candidates = new ArrayList<>();
                while (resources.hasMoreElements()) {
                    java.net.URL url = resources.nextElement();
                    try (InputStream stream = url.openStream()) {
                        candidates.add(parseOrder(stream));
                    }
                }
                if (!candidates.isEmpty()) {
                    return Optional.of(selectBestOrderCandidate(candidates, classLoader));
                }
            }

            InputStream stream = PipelineResources.openResource(classLoader, RESOURCE);
            try (InputStream streamToRead = stream) {
                if (stream == null) {
                    logMissingResource(classLoader);
                    return Optional.empty();
                }
                return Optional.of(parseOrder(streamToRead));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read pipeline order resource.", e);
        }
    }

    /** Exact canonical boundaries of the selected physical worker order, when emitted by its compiler. */
    public static Optional<List<StepBoundary>> loadBoundaries() {
        List<String> selected = loadOrder().orElse(List.of());
        if (selected.isEmpty()) return Optional.empty();
        ClassLoader loader = PipelineResources.resolveClassLoader();
        try {
            Enumeration<java.net.URL> resources = loader.getResources(RESOURCE);
            List<StepBoundary> resolved = null;
            while (resources.hasMoreElements()) {
                try (InputStream stream = resources.nextElement().openStream()) {
                    Map<?,?> data = PipelineJson.mapper().readValue(stream, Map.class);
                    if (!selected.equals(data.get("order"))) continue;
                    if (!data.containsKey("boundarySchemaVersion") && !data.containsKey("stepBoundaries")) continue;
                    if (!versionOne(data.get("boundarySchemaVersion"))
                        || !(data.get("stepBoundaries") instanceof List<?> boundaries) || boundaries.size() != selected.size()) {
                        throw new IllegalStateException("Invalid physical boundary metadata");
                    }
                    List<StepBoundary> candidate = new ArrayList<>();
                    for (int index = 0; index < selected.size(); index++) {
                        if (!(boundaries.get(index) instanceof Map<?,?> boundary)
                            || !selected.get(index).equals(boundary.get("stepClass"))
                            || !(boundary.get("inputTypeId") instanceof String input) || input.isBlank()
                            || !(boundary.get("outputTypeId") instanceof String output) || output.isBlank()) {
                            throw new IllegalStateException("Physical boundary differs from generated order");
                        }
                        candidate.add(new StepBoundary(selected.get(index), input, output));
                    }
                    if (resolved != null && !resolved.equals(candidate)) throw new IllegalStateException("Conflicting physical boundary resources");
                    resolved = List.copyOf(candidate);
                }
            }
            return Optional.ofNullable(resolved);
        } catch (Exception failure) {
            throw new IllegalStateException("Failed to read physical boundary metadata", failure);
        }
    }

    private static boolean versionOne(Object value) {
        if (!(value instanceof Number number) || number instanceof Double || number instanceof Float) return false;
        try { return new java.math.BigDecimal(number.toString()).compareTo(java.math.BigDecimal.ONE) == 0; }
        catch (NumberFormatException invalid) { return false; }
    }

    public record StepBoundary(String stepClass, String inputTypeId, String outputTypeId) { }

    public static List<DefinitionBoundary> loadDefinitionBoundaries() {
        List<String> selected = loadOrder().orElse(List.of());
        var definitions = new java.util.LinkedHashMap<String, DefinitionBoundary>();
        try {
            var resources = PipelineResources.resolveClassLoader().getResources(RESOURCE);
            while (resources.hasMoreElements()) {
                try (InputStream stream = resources.nextElement().openStream()) {
                    Map<?,?> data = PipelineJson.mapper().readValue(stream, Map.class);
                    if (!selected.equals(data.get("order")) || !data.containsKey("definitionBoundaries")) continue;
                    if (!versionOne(data.get("boundarySchemaVersion"))
                        || !(data.get("definitionBoundaries") instanceof List<?> entries)) {
                        throw new IllegalStateException("Invalid nested physical boundary metadata");
                    }
                    for (Object entry : entries) {
                        DefinitionBoundary definition = PipelineJson.mapper().convertValue(entry, DefinitionBoundary.class);
                        if (definition.invocationClass() == null || definition.invocationClass().isBlank()
                            || definition.definitionId() == null || definition.definitionId().isBlank()
                            || definition.callsiteId() == null || definition.callsiteId().isBlank()
                            || definition.stepBoundaries() == null || definition.stepBoundaries().isEmpty()
                            || definition.stepBoundaries().stream().anyMatch(boundary -> boundary.stepClass() == null || boundary.stepClass().isBlank()
                                || boundary.inputTypeId() == null || boundary.inputTypeId().isBlank()
                                || boundary.outputTypeId() == null || boundary.outputTypeId().isBlank())) {
                            throw new IllegalStateException("Incomplete nested physical boundary metadata");
                        }
                        var previous = definitions.putIfAbsent(definition.invocationClass(), definition);
                        if (previous != null && !previous.equals(definition)) throw new IllegalStateException("Conflicting nested physical boundary metadata");
                    }
                }
            }
            return List.copyOf(definitions.values());
        } catch (Exception failure) {
            throw new IllegalStateException("Failed to read nested physical boundary metadata", failure);
        }
    }

    public record DefinitionBoundary(String invocationClass, String definitionId, String callsiteId,
        List<StepBoundary> stepBoundaries) {
        public DefinitionBoundary { stepBoundaries = stepBoundaries == null ? null : List.copyOf(stepBoundaries); }
    }


    private static List<String> parseOrder(InputStream stream) throws Exception {
        Map<?, ?> data = PipelineJson.mapper().readValue(stream, Map.class);
        Object value = data.get("order");
        if (!(value instanceof List<?> list)) {
            throw new IllegalStateException("Pipeline order resource is missing an order array.");
        }
        return list.stream()
            .filter(item -> item != null && !item.toString().isBlank())
            .map(Object::toString)
            .toList();
    }

    private static List<String> selectBestOrderCandidate(List<List<String>> candidates, ClassLoader classLoader) {
        List<String> best = candidates.getFirst();
        int bestLoadableCount = countLoadableSteps(best, classLoader);
        boolean bestAllLoadable = bestLoadableCount == best.size();

        for (int i = 1; i < candidates.size(); i++) {
            List<String> candidate = candidates.get(i);
            int loadableCount = countLoadableSteps(candidate, classLoader);
            boolean allLoadable = loadableCount == candidate.size();
            if (allLoadable && !bestAllLoadable) {
                best = candidate;
                bestLoadableCount = loadableCount;
                bestAllLoadable = true;
                continue;
            }
            if (allLoadable == bestAllLoadable && loadableCount > bestLoadableCount) {
                best = candidate;
                bestLoadableCount = loadableCount;
                bestAllLoadable = allLoadable;
            }
        }

        if (!bestAllLoadable) {
            LOG.warnf(
                "Selected pipeline order has unloadable steps (%d/%d loadable); verify duplicate order.json resources on classpath.",
                bestLoadableCount,
                best.size());
            return best.stream().filter(name -> isLoadable(name, classLoader)).toList();
        }
        return best;
    }

    private static int countLoadableSteps(List<String> order, ClassLoader classLoader) {
        int count = 0;
        for (String stepClass : order) {
            if (isLoadable(stepClass, classLoader)) {
                count++;
            }
        }
        return count;
    }

    private static boolean isLoadable(String stepClassName, ClassLoader classLoader) {
        if (stepClassName == null || stepClassName.isBlank()) {
            return false;
        }
        try {
            Class.forName(stepClassName, false, classLoader);
            return true;
        } catch (Throwable ignored) {
            return false;
        }
    }

    /**
     * Returns whether an orchestrator is present and therefore order metadata is required.
     *
     * @return true if order metadata is required, false otherwise
     */
    public static boolean requiresOrder() {
        ClassLoader classLoader = PipelineResources.resolveClassLoader();
        InputStream orchestratorClients = PipelineResources.openResource(classLoader, ORCHESTRATOR_CLIENTS_RESOURCE);
        try (InputStream orchestratorClientsStream = orchestratorClients) {
            return orchestratorClientsStream != null;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read orchestrator client metadata resource.", e);
        }
    }


    private static void logMissingResource(ClassLoader classLoader) {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        String loaderName = classLoader != null ? classLoader.getClass().getName() : "null";
        LOG.debugf("Pipeline order resource not found. classLoader=%s", loaderName);
        try {
            if (classLoader == null) {
                return;
            }
            Enumeration<java.net.URL> resources = classLoader.getResources(RESOURCE);
            if (!resources.hasMoreElements()) {
                LOG.debugf("No resources visible for %s", RESOURCE);
                return;
            }
            while (resources.hasMoreElements()) {
                LOG.debugf("Resource candidate: %s", resources.nextElement());
            }
        } catch (Exception e) {
            LOG.debug("Failed to enumerate pipeline order resources", e);
        }
    }
}

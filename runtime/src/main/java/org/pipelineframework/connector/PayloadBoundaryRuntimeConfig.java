package org.pipelineframework.connector;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;

/** Loads the packaged pipeline binding configuration for generated payload resources. */
public final class PayloadBoundaryRuntimeConfig {
    private PayloadBoundaryRuntimeConfig() {
    }

    public static PipelineYamlConfig load() {
        PipelineYamlConfigLoader loader = new PipelineYamlConfigLoader();
        String explicit = System.getProperty("pipeline.config", System.getenv("PIPELINE_CONFIG"));
        if (explicit != null && !explicit.isBlank()) {
            return loader.load(Path.of(explicit));
        }
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PayloadBoundaryRuntimeConfig.class.getClassLoader();
        }
        URL resource = new PipelineYamlConfigLocator().locateResource(classLoader)
            .orElseThrow(() -> new IllegalStateException("packaged pipeline.yaml is required for payload boundaries"));
        try (InputStream input = resource.openStream()) {
            return loader.load(input);
        } catch (IOException failure) {
            throw new IllegalStateException("failed to load payload boundary bindings", failure);
        }
    }
}

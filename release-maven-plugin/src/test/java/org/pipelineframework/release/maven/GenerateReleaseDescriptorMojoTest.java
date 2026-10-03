package org.pipelineframework.release.maven;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.maven.plugin.MojoExecutionException;
import org.junit.jupiter.api.Test;

class GenerateReleaseDescriptorMojoTest {
    @Test
    void explicitSkipNeedsNeitherVersionNorBuildOutputs() throws Exception {
        var mojo = new GenerateReleaseDescriptorMojo();
        var skip = GenerateReleaseDescriptorMojo.class.getDeclaredField("skip");
        skip.setAccessible(true);
        skip.setBoolean(mojo, true);
        assertDoesNotThrow(mojo::execute);
    }

    @Test
    void enabledProductionStillRequiresExplicitVersionBeforeReadingBuildOutputs() {
        var error = assertThrows(MojoExecutionException.class, new GenerateReleaseDescriptorMojo()::execute);
        assertTrue(error.getMessage().contains("tpf.release.version is required"));
    }
}

package org.pipelineframework.orchestrator;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelineReleaseIdentityResolverTest {
    @Test
    void missingCompiledMetadataIsExplicitWhileLegacyContractFallbackRemainsAvailable() {
        var loader = mock(PipelineContractDescriptorLoader.class);
        when(loader.load()).thenReturn(Optional.empty());
        var resolver = new PipelineReleaseIdentityResolver();
        resolver.contractLoader = loader;
        assertTrue(resolver.compiledContract().isEmpty());
        assertEquals(PipelineContractDescriptor.localFallback(), resolver.contract());
        assertTrue(resolver.compiledContract().isEmpty());
        verify(loader, times(1)).load();
    }
}

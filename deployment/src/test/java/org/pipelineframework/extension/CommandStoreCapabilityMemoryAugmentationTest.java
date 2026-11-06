package org.pipelineframework.extension;

import io.quarkus.test.QuarkusExtensionTest;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;
import org.pipelineframework.command.CommandEffectStore;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.orchestrator.release.PipelineReleaseRuntimeCapabilities;
import static org.junit.jupiter.api.Assertions.*;

/** Checks emitted facts after real Quarkus bean selection and resource generation. */
@Isolated
class CommandStoreCapabilityMemoryAugmentationTest {
    @RegisterExtension
    static final QuarkusExtensionTest APPLICATION = new QuarkusExtensionTest()
        .withApplicationRoot(archive -> archive.addClass(Handler.class))
        .overrideConfigKey("pipeline.command.effect-store.provider", "memory")
        .overrideConfigKey("quarkus.devservices.enabled", "false")
        .overrideConfigKey("quarkus.http.test-port", "0")
        .overrideConfigKey("quarkus.lambda.mock-event-server.test-port", "0");

    @Inject Instance<CommandEffectStore> stores;

    @Test
    void generatedMemoryFactsCannotAuthorizeDynamoRecovery() throws Exception {
        assertTrue(stores.get() instanceof InMemoryCommandEffectStore);
        try (var input = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(PipelineReleaseRuntimeCapabilities.RESOURCE_PATH)) {
            assertNotNull(input);
            var facts = PipelineReleaseRuntimeCapabilities.parse(input.readAllBytes());
            var selected = facts.capabilities().get(PipelineReleaseRuntimeCapabilities.COMMAND_EFFECT_DYNAMODB);
            assertEquals("memory", selected.provider());
            assertEquals(java.util.List.of(InMemoryCommandEffectStore.class.getName()), selected.selectedBeans());
            assertFalse(selected.recoverySupported());
            assertThrows(IllegalArgumentException.class,
                () -> facts.requireRecoverable(PipelineReleaseRuntimeCapabilities.COMMAND_EFFECT_DYNAMODB));
        }
    }

    public static class Handler implements com.amazonaws.services.lambda.runtime.RequestHandler<String, String> {
        public String handleRequest(String input, com.amazonaws.services.lambda.runtime.Context context) { return input; }
    }
}

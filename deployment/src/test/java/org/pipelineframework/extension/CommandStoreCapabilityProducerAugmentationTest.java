package org.pipelineframework.extension;

import io.quarkus.test.QuarkusExtensionTest;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Qualifier;
import jakarta.inject.Singleton;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;
import org.pipelineframework.command.CommandEffectStore;
import org.pipelineframework.command.DynamoCommandEffectStore;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.orchestrator.release.PipelineReleaseRuntimeCapabilities;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import static org.junit.jupiter.api.Assertions.*;

/** A qualified unrelated store must not change the default producer selected by Commands. */
@Isolated
class CommandStoreCapabilityProducerAugmentationTest {
    @RegisterExtension
    static final QuarkusExtensionTest APPLICATION = new QuarkusExtensionTest()
        .withApplicationRoot(archive -> archive.addClasses(Stores.class, Unrelated.class,
            CommandStoreCapabilityMemoryAugmentationTest.Handler.class))
        .overrideConfigKey("pipeline.command.effect-store.provider", "custom")
        .overrideConfigKey("quarkus.devservices.enabled", "false")
        .overrideConfigKey("quarkus.http.test-port", "0")
        .overrideConfigKey("quarkus.lambda.mock-event-server.test-port", "0");

    @Inject Instance<CommandEffectStore> stores;
    @Inject @Any Instance<CommandEffectStore> allStores;

    @Test
    void generatedFactsIdentifyTheProducedStoreAndIgnoreQualifiedBeans() throws Exception {
        assertTrue(stores.get() instanceof DynamoCommandEffectStore);
        assertEquals(1, stores.stream().count());
        assertEquals(2, allStores.stream().count());
        try (var input = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(PipelineReleaseRuntimeCapabilities.RESOURCE_PATH)) {
            assertNotNull(input);
            var facts = PipelineReleaseRuntimeCapabilities.parse(input.readAllBytes());
            var selected = facts.requireRecoverable(PipelineReleaseRuntimeCapabilities.COMMAND_EFFECT_DYNAMODB);
            assertEquals("dynamo", selected.provider());
            assertEquals(java.util.List.of(DynamoCommandEffectStore.class.getName()), selected.selectedBeans());
        }
    }

    @ApplicationScoped
    public static class Stores {
        private final DynamoDbClient client = DynamoDbClient.builder().region(Region.US_EAST_1)
            .credentialsProvider(AnonymousCredentialsProvider.create()).build();

        @Produces @Singleton
        DynamoCommandEffectStore commands() { return new DynamoCommandEffectStore(client, "fixture-effects"); }

        @Produces @Singleton @Unrelated
        CommandEffectStore unrelated() { return new InMemoryCommandEffectStore(); }

        @PreDestroy void close() { client.close(); }
    }

    @Qualifier @Retention(RetentionPolicy.RUNTIME)
    public @interface Unrelated {}
}

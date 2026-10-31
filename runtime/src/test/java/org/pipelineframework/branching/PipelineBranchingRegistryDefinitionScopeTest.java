package org.pipelineframework.branching;

import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URLClassLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineBranchingResourceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineBranchingRegistryDefinitionScopeTest {

    @TempDir
    Path tempDir;

    @Test
    void loadsDefinitionCompletionAndRetainsLegacyMetadataCompatibility() throws Exception {
        Path metadata = tempDir.resolve("META-INF/pipeline/branching.json");
        Files.createDirectories(metadata.getParent());
        String step = """
            {"runtimeStepClass":"%s","step":"shared","index":0,
             "inputRuntimeClass":"java.lang.Object","acceptedRuntimeClasses":["java.lang.Object"]}
            """.formatted(SharedStep.class.getName());
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new java.net.URL[]{tempDir.toUri().toURL()}, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            Files.writeString(metadata, "{\"steps\":[" + step + "]}");
            assertEquals(List.of(), PipelineBranchingResourceLoader.load().orElseThrow().definitionCompletions());
            Files.writeString(metadata, """
                {"steps":[%s],"definitionCompletions":[
                  {"definitionId":"$root","outputRuntimeClass":"%s",
                   "acceptedContracts":["Approved"],"acceptedRuntimeClasses":["%s"],"variants":[]}]}
                """.formatted(step, Approved.class.getName(), Approved.class.getName()));
            var loaded = PipelineBranchingResourceLoader.load().orElseThrow();
            assertEquals(1, loaded.definitionCompletions().size());
            var registry = new PipelineBranchingRegistry(loaded);
            Approved value = new Approved("yes");
            assertSame(value, registry.completeDefinition("$root", new Outcome.Approved(value)));
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void completesConcreteAndUnionOutputsIndependentlyPerDefinition() {
        var root = new PipelineBranchingResourceLoader.DefinitionCompletion("$root", Outcome.class.getName(),
            List.of("Approved", "Rejected"), List.of(Approved.class.getName(), Rejected.class.getName()), List.of(
                new BranchVariantIdentity("Outcome", "approved", "Approved"),
                new BranchVariantIdentity("Outcome", "rejected", "Rejected")));
        var child = new PipelineBranchingResourceLoader.DefinitionCompletion("child", Approved.class.getName(),
            List.of("Approved"), List.of(Approved.class.getName()), List.of());
        var registry = new PipelineBranchingRegistry(new PipelineBranchingResourceLoader.BranchingResource(
            -1, List.of(), List.of(root, child)));
        Approved approved = new Approved("yes");
        Rejected rejected = new Rejected("no");
        assertEquals(new Outcome.Approved(approved), registry.completeDefinition("$root", approved));
        assertEquals(new Outcome.Rejected(rejected), registry.completeDefinition("$root", rejected));
        Outcome wrapped = new Outcome.Approved(approved);
        assertSame(wrapped, registry.completeDefinition("$root", wrapped));
        assertSame(approved, registry.completeDefinition("child", wrapped));
        assertSame(approved, registry.completeDefinition("legacy", approved));
        assertThrows(PipelineBranchRoutingException.class, () -> registry.completeDefinition("child", rejected));
        assertThrows(PipelineBranchRoutingException.class, () -> registry.completeDefinition("$root", "unrelated"));
    }

    record Approved(String value) { }
    record Rejected(String value) { }
    sealed interface Outcome {
        record Approved(PipelineBranchingRegistryDefinitionScopeTest.Approved value) implements Outcome { }
        record Rejected(PipelineBranchingRegistryDefinitionScopeTest.Rejected value) implements Outcome { }
    }

    @Test
    void preservesDescriptorsForTheSameRuntimeClassInRootAndChildDefinitions() {
        String runtimeClass = SharedStep.class.getName();
        var root = step("$root", 2, 1, "Root shared", runtimeClass);
        var child = step("inner", 1, 0, "Child shared", runtimeClass);
        var registry = new PipelineBranchingRegistry(
            new PipelineBranchingResourceLoader.BranchingResource(2, List.of(root, child)));

        assertEquals("Root shared", registry.descriptorFor(SharedStep.class).orElseThrow().stepName());
        assertEquals(
            "Child shared",
            registry.descriptorFor("inner", 1, SharedStep.class).orElseThrow().stepName());
    }

    private PipelineBranchingResourceLoader.BranchingStep step(
        String definitionId,
        int terminalStepIndex,
        int index,
        String name,
        String runtimeClass
    ) {
        return new PipelineBranchingResourceLoader.BranchingStep(
            definitionId,
            terminalStepIndex,
            index,
            name,
            runtimeClass,
            Object.class.getName(),
            List.of(Object.class.getName()),
            List.of(Object.class.getName()),
            List.of(),
            List.of(),
            List.of(),
            false);
    }

    static final class SharedStep {
    }
}

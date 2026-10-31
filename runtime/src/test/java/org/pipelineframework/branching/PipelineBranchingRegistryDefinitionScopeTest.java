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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void rejectsNonObjectDefinitionCompletionEntries() {
        var error = assertThrows(IllegalStateException.class, () -> loadMetadata(
            "{\"steps\":[],\"definitionCompletions\":[42]}"));
        assertTrue(error.getCause().getMessage().contains("must be an object"));
    }

    @Test
    void rejectsDuplicateDefinitionCompletionIds() {
        var completion = new PipelineBranchingResourceLoader.DefinitionCompletion("child", Approved.class.getName(),
            List.of("Approved"), List.of(Approved.class.getName()), List.of());
        assertThrows(IllegalStateException.class, () -> new PipelineBranchingRegistry(
            new PipelineBranchingResourceLoader.BranchingResource(-1, List.of(), List.of(completion, completion))));
    }

    @Test
    void rejectsBlankDefinitionCompletionId() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineBranchingResourceLoader.DefinitionCompletion(
            " ", Approved.class.getName(), List.of("Approved"), List.of(Approved.class.getName()), List.of()));
    }

    @Test
    void rejectsBlankDefinitionCompletionOutputClass() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineBranchingResourceLoader.DefinitionCompletion(
            "child", " ", List.of("Approved"), List.of(Approved.class.getName()), List.of()));
    }

    @Test
    void rejectsEmptyDefinitionCompletionAcceptedContracts() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineBranchingResourceLoader.DefinitionCompletion(
            "child", Approved.class.getName(), List.of(), List.of(), List.of()));
    }

    @Test
    void rejectsUnpairedDefinitionCompletionContractsAndClasses() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineBranchingResourceLoader.DefinitionCompletion(
            "child", Approved.class.getName(), List.of("Approved", "Rejected"),
            List.of(Approved.class.getName()), List.of()));
    }

    @Test
    void selectsCompatibleCompletionTypesBeforeRankingDuplicateResources() throws Exception {
        Path stale = tempDir.resolve("stale");
        Path current = tempDir.resolve("current");
        String step = """
            {"runtimeStepClass":"%s","step":"shared","index":0,
             "inputRuntimeClass":"java.lang.Object","acceptedContracts":["Object"],
             "acceptedRuntimeClasses":["java.lang.Object"]}
            """.formatted(SharedStep.class.getName());
        String completion = """
            {"definitionId":"$root","outputRuntimeClass":"%s",
             "acceptedContracts":["Approved"],"acceptedRuntimeClasses":["%s"],"variants":[]}
            """;
        writeMetadata(current, "{\"steps\":[" + step + "],\"definitionCompletions\":["
            + completion.formatted(Approved.class.getName(), Approved.class.getName()) + "]}");
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        for (boolean invalidOutput : List.of(true, false)) {
            String incompatible = completion.formatted(
                invalidOutput ? "missing.Output" : Approved.class.getName(),
                invalidOutput ? Approved.class.getName() : "missing.Accepted");
            writeMetadata(stale, "{\"steps\":[" + step + "," + step + "],\"definitionCompletions\":["
                + incompatible + "]}");
            for (List<Path> order : List.of(List.of(stale, current), List.of(current, stale))) {
                var urls = new java.net.URL[]{order.get(0).toUri().toURL(), order.get(1).toUri().toURL()};
                try (URLClassLoader loader = new URLClassLoader(urls, previous)) {
                    Thread.currentThread().setContextClassLoader(loader);
                    var selected = PipelineBranchingResourceLoader.load().orElseThrow();
                    assertEquals(1, selected.steps().size());
                    assertEquals(1, selected.definitionCompletions().size());
                    assertEquals(Approved.class.getName(), selected.definitionCompletions().getFirst().outputRuntimeClass());
                    assertEquals(List.of(Approved.class.getName()),
                        selected.definitionCompletions().getFirst().acceptedRuntimeClasses());
                    Approved value = new Approved("yes");
                    assertSame(value, new PipelineBranchingRegistry(selected)
                        .completeDefinition("$root", new Outcome.Approved(value)));
                } finally {
                    Thread.currentThread().setContextClassLoader(previous);
                }
            }
        }
    }

    @Test
    void legacyDuplicatesCannotReplaceTypedCompletionContracts() throws Exception {
        Path legacy = tempDir.resolve("legacy");
        Path typed = tempDir.resolve("typed");
        String step = """
            {"runtimeStepClass":"%s","step":"shared","index":0,
             "inputRuntimeClass":"java.lang.Object","acceptedContracts":["Object"],
             "acceptedRuntimeClasses":["java.lang.Object"]}
            """.formatted(SharedStep.class.getName());
        writeMetadata(legacy, "{\"steps\":[" + step + "," + step + "]}");
        for (String invalid : List.of("none", "output", "accepted")) {
            writeMetadata(typed, """
                {"steps":[%s],"definitionCompletions":[
                  {"definitionId":"$root","outputRuntimeClass":"%s",
                   "acceptedContracts":["Approved"],"acceptedRuntimeClasses":["%s"],
                   "variants":[{"unionName":"Outcome","discriminator":"approved","payloadContract":"Approved"}]}]}
                """.formatted(step, invalid.equals("output") ? "missing.Output" : Outcome.class.getName(),
                    invalid.equals("accepted") ? "missing.Accepted" : Approved.class.getName()));
            for (List<Path> order : List.of(List.of(legacy, typed), List.of(typed, legacy))) {
                if (invalid.equals("none")) {
                    var selected = loadMetadata(order);
                    assertEquals(1, selected.steps().size());
                    assertEquals(1, selected.definitionCompletions().size());
                    Approved value = new Approved("yes");
                    assertEquals(new Outcome.Approved(value), new PipelineBranchingRegistry(selected)
                        .completeDefinition("$root", value));
                } else {
                    var error = assertThrows(IllegalStateException.class, () -> loadMetadata(order));
                    assertTrue(error.getCause().getMessage().contains("all definition completions"),
                        error.getMessage());
                }
            }
        }
    }

    @Test
    void rejectsResourcesWhenNoDefinitionCompletionTypesAreLoadable() {
        var error = assertThrows(IllegalStateException.class, () -> loadMetadata("""
            {"steps":[],"definitionCompletions":[
              {"definitionId":"$root","outputRuntimeClass":"missing.Output",
               "acceptedContracts":["Approved"],"acceptedRuntimeClasses":["%s"]}]}
            """.formatted(Approved.class.getName())));
        assertTrue(error.getCause().getMessage().contains("No pipeline branching resource"));
    }

    private PipelineBranchingResourceLoader.BranchingResource loadMetadata(String json) throws Exception {
        writeMetadata(tempDir, json);
        return loadMetadata(List.of(tempDir));
    }

    private PipelineBranchingResourceLoader.BranchingResource loadMetadata(List<Path> directories) throws Exception {
        java.net.URL[] urls = new java.net.URL[directories.size()];
        for (int index = 0; index < directories.size(); index++) {
            urls[index] = directories.get(index).toUri().toURL();
        }
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(urls, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            return PipelineBranchingResourceLoader.load().orElseThrow();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private void writeMetadata(Path directory, String json) throws Exception {
        Path metadata = directory.resolve("META-INF/pipeline/branching.json");
        Files.createDirectories(metadata.getParent());
        Files.writeString(metadata, json);
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

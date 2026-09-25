package org.pipelineframework;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import com.google.gson.JsonParser;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.branching.PipelineBranchingRegistry;
import org.pipelineframework.config.pipeline.PipelineBranchingResourceLoader;
import org.pipelineframework.config.pipeline.PipelineOrderResourceLoader;
import org.pipelineframework.processor.PipelineStepProcessor;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DirectRootBranchingProductizationTest {

    @TempDir
    Path tempDir;

    @Test
    void compiledDirectRootBranchesJoinProductionMetadataAndRouteByRuntimeClass() throws Exception {
        Fixture fixture = compile("first");
        Fixture repeated = compile("repeated");

        assertThat(fixture.compilation()).succeeded();
        assertThat(repeated.compilation()).succeeded();
        assertEquals(fixture.metadata("order.json"), repeated.metadata("order.json"));
        assertEquals(fixture.metadata("branching.json"), repeated.metadata("branching.json"));

        List<String> rawOrder = rawOrder(fixture.metadata("order.json"));
        List<String> branchingClasses = rawBranchingClasses(fixture.metadata("branching.json"));
        for (String runtimeClass : branchingClasses) {
            assertEquals(1, Collections.frequency(rawOrder, runtimeClass),
                () -> runtimeClass + " must join exactly once to generated order metadata " + rawOrder);
        }
        assertTrue(rawOrder.contains("com.example.direct.CreateApplicationService"));
        assertTrue(rawOrder.contains("com.example.direct.CreateEnvironmentService"));
        assertTrue(rawOrder.contains("com.example.direct.ProjectResultService"));

        Path classes = compileRuntimeFixture(fixture, rawOrder);
        Path metadata = classes.resolve("META-INF/pipeline");
        Files.createDirectories(metadata);
        Files.writeString(metadata.resolve("order.json"), fixture.metadata("order.json"));
        Files.writeString(metadata.resolve("branching.json"), fixture.metadata("branching.json"));

        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new java.net.URL[]{classes.toUri().toURL()}, previous)) {
            Thread.currentThread().setContextClassLoader(loader);

            List<String> loadedOrder = PipelineOrderResourceLoader.loadOrder().orElseThrow();
            PipelineBranchingResourceLoader.BranchingResource branching =
                PipelineBranchingResourceLoader.load().orElseThrow();
            assertEquals(rawOrder, loadedOrder);
            assertEquals(branchingClasses, branching.steps().stream()
                .map(PipelineBranchingResourceLoader.BranchingStep::runtimeStepClass)
                .toList());

            PipelineBranchingRegistry registry = new PipelineBranchingRegistry();
            for (String runtimeClass : branchingClasses) {
                assertTrue(registry.descriptorFor(loader.loadClass(runtimeClass)).isPresent(),
                    () -> "Registry must resolve the actual runtime class " + runtimeClass);
            }

            List<Object> steps = instantiateSteps(loader, loadedOrder);
            List<Object> reversed = new ArrayList<>(steps);
            Collections.reverse(reversed);
            List<Object> ordered = new PipelineStepOrderer().orderSteps(reversed);
            assertEquals(loadedOrder, ordered.stream().map(step -> step.getClass().getName()).toList());

            PipelineRunnerTestHarness.Harness harness = PipelineRunnerTestHarness.createHarness();
            Object application = loader.loadClass("com.example.direct.domain.CreateApplicationCommand")
                .getConstructor(String.class).newInstance("app-1");
            Object applicationResult = ((Uni<?>) harness.runner()
                .runWithContext(Uni.createFrom().item(application), ordered).result()).await().indefinitely();
            assertEquals("DeploymentResult[id=app-1, kind=application]", applicationResult.toString());
            assertEquals(1, staticInt(loader, "com.example.direct.CreateApplicationService", "calls"));
            assertEquals(0, staticInt(loader, "com.example.direct.CreateEnvironmentService", "calls"));

            Object environment = loader.loadClass("com.example.direct.domain.CreateEnvironmentCommand")
                .getConstructor(String.class).newInstance("env-1");
            Object environmentResult = ((Uni<?>) harness.runner()
                .runWithContext(Uni.createFrom().item(environment), ordered).result()).await().indefinitely();
            assertEquals("DeploymentResult[id=env-1, kind=environment]", environmentResult.toString());
            assertEquals(1, staticInt(loader, "com.example.direct.CreateApplicationService", "calls"));
            assertEquals(1, staticInt(loader, "com.example.direct.CreateEnvironmentService", "calls"));
            assertEquals(2, staticInt(loader, "com.example.direct.PersistenceObserverService", "calls"));
            assertEquals(2, staticInt(loader, "com.example.direct.ProjectResultService", "calls"));
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private Fixture compile(String id) throws IOException {
        Path projectRoot = tempDir.resolve(id);
        Files.createDirectories(projectRoot);
        Files.writeString(projectRoot.resolve("pom.xml"),
            "<project><modelVersion>4.0.0</modelVersion><groupId>x</groupId><artifactId>x</artifactId><version>1</version></project>");
        Path generated = projectRoot.resolve("module/target/generated-sources/pipeline");
        Files.createDirectories(generated);
        Path config = projectRoot.resolve("pipeline.yaml");
        Files.writeString(config, yaml());
        Map<String, String> sources = sources();
        List<JavaFileObject> files = sources.entrySet().stream()
            .map(entry -> JavaFileObjects.forSourceString(entry.getKey(), entry.getValue()))
            .toList();
        Compilation compilation = Compiler.javac().withProcessors(new PipelineStepProcessor()).withOptions(
            "-Apipeline.config=" + config.toString().replace('\\', '/'),
            "-Apipeline.generatedSourcesDir=" + generated.toString().replace('\\', '/'),
            "-Apipeline.transport=LOCAL").compile(files);
        return new Fixture(projectRoot, generated, compilation, sources);
    }

    private Path compileRuntimeFixture(Fixture fixture, List<String> order) throws IOException {
        Path sourceRoot = fixture.projectRoot().resolve("runtime-src");
        Path classes = fixture.projectRoot().resolve("runtime-classes");
        Files.createDirectories(sourceRoot);
        Files.createDirectories(classes);
        List<Path> javaSources = new ArrayList<>();
        for (Map.Entry<String, String> source : fixture.sources().entrySet()) {
            Path file = sourceRoot.resolve(source.getKey().replace('.', '/') + ".java");
            Files.createDirectories(file.getParent());
            Files.writeString(file, source.getValue());
            javaSources.add(file);
        }
        for (String className : order) {
            if (fixture.sources().containsKey(className)) {
                continue;
            }
            String simpleName = className.substring(className.lastIndexOf('.') + 1);
            try (var files = Files.walk(fixture.generatedRoot())) {
                Path generated = files
                    .filter(path -> path.getFileName().toString().equals(simpleName + ".java"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing generated runtime source for " + className));
                javaSources.add(generated);
            }
        }
        try (var files = Files.walk(fixture.generatedRoot())) {
            files.filter(path -> path.getFileName().toString().startsWith("ObservePersistence"))
                .filter(path -> path.getFileName().toString().endsWith("SideEffectService.java"))
                .forEach(javaSources::add);
        }
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var fileManager = ToolProvider.getSystemJavaCompiler().getStandardFileManager(null, null, null)) {
            boolean success = ToolProvider.getSystemJavaCompiler().getTask(null, fileManager, diagnostics,
                List.of("-proc:none", "-classpath", System.getProperty("java.class.path"), "-d", classes.toString()),
                null, fileManager.getJavaFileObjectsFromPaths(javaSources)).call();
            assertTrue(success, () -> diagnostics.getDiagnostics().stream()
                .map(diagnostic -> diagnostic.getKind() + ": " + diagnostic.getMessage(null))
                .collect(java.util.stream.Collectors.joining(System.lineSeparator())));
        }
        return classes;
    }

    private List<Object> instantiateSteps(ClassLoader loader, List<String> order) throws Exception {
        List<Object> steps = new ArrayList<>();
        for (String className : order) {
            Object step = loader.loadClass(className).getConstructor().newInstance();
            if (className.contains("SideEffect")) {
                Field service = step.getClass().getDeclaredField("service");
                set(step, "service", service.getType().getConstructor().newInstance());
            }
            steps.add(step);
        }
        return steps;
    }

    private static void set(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static int staticInt(ClassLoader loader, String className, String fieldName) throws Exception {
        return loader.loadClass(className).getField(fieldName).getInt(null);
    }

    private static List<String> rawOrder(String json) {
        return JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("order").asList().stream()
            .map(element -> element.getAsString())
            .toList();
    }

    private static List<String> rawBranchingClasses(String json) {
        return JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("steps").asList().stream()
            .map(element -> element.getAsJsonObject().get("runtimeStepClass").getAsString())
            .toList();
    }

    private String yaml() {
        return """
            version: 3
            appName: Direct root branching
            basePackage: com.example.direct
            transport: LOCAL
            contract: { input: DeploymentState, output: DeploymentResult }
            types:
              DeploymentState:
                variants:
                  createApplication: CreateApplicationCommand
                  createEnvironment: CreateEnvironmentCommand
                  application: ApplicationRecord
                  environment: EnvironmentRecord
              CreateApplicationCommand: { fields: [[id, string]] }
              CreateEnvironmentCommand: { fields: [[id, string]] }
              ApplicationRecord: { fields: [[id, string]] }
              EnvironmentRecord: { fields: [[id, string]] }
              DeploymentResult:
                fields:
                  - [id, string]
                  - [kind, string]
            steps:
              - { name: Create application, service: com.example.direct.CreateApplicationService, cardinality: ONE_TO_ONE, input: DeploymentState, output: ApplicationRecord, accepts: [CreateApplicationCommand], java: { input: com.example.direct.domain.CreateApplicationCommand, output: com.example.direct.domain.ApplicationRecord } }
              - { name: Create environment, service: com.example.direct.CreateEnvironmentService, cardinality: ONE_TO_ONE, input: DeploymentState, output: EnvironmentRecord, accepts: [CreateEnvironmentCommand], java: { input: com.example.direct.domain.CreateEnvironmentCommand, output: com.example.direct.domain.EnvironmentRecord } }
              - { name: Project result, service: com.example.direct.ProjectResultService, cardinality: ONE_TO_ONE, input: DeploymentState, output: DeploymentResult, accepts: [ApplicationRecord, EnvironmentRecord], terminal: true, java: { input: com.example.direct.domain.DeploymentState, output: com.example.direct.domain.DeploymentResult } }
            aspects:
              persistence:
                enabled: true
                scope: STEPS
                position: AFTER_STEP
                order: 0
                config:
                  targetSteps: [CreateApplicationService, CreateEnvironmentService]
                  pluginImplementationClass: com.example.direct.PersistenceObserverService
            """;
    }

    private Map<String, String> sources() {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("com.example.direct.domain.DeploymentState", """
            package com.example.direct.domain;
            public sealed interface DeploymentState permits CreateApplicationCommand, CreateEnvironmentCommand, ApplicationRecord, EnvironmentRecord { String id(); }
            """);
        sources.put("com.example.direct.domain.CreateApplicationCommand", "package com.example.direct.domain; public record CreateApplicationCommand(String id) implements DeploymentState { }");
        sources.put("com.example.direct.domain.CreateEnvironmentCommand", "package com.example.direct.domain; public record CreateEnvironmentCommand(String id) implements DeploymentState { }");
        sources.put("com.example.direct.domain.ApplicationRecord", "package com.example.direct.domain; public record ApplicationRecord(String id) implements DeploymentState { }");
        sources.put("com.example.direct.domain.EnvironmentRecord", "package com.example.direct.domain; public record EnvironmentRecord(String id) implements DeploymentState { }");
        sources.put("com.example.direct.domain.DeploymentResult", "package com.example.direct.domain; public record DeploymentResult(String id, String kind) { }");
        sources.put("com.example.direct.CreateApplicationService", service(
            "CreateApplicationService", "CreateApplicationCommand", "ApplicationRecord",
            "new ApplicationRecord(input.id())"));
        sources.put("com.example.direct.CreateEnvironmentService", service(
            "CreateEnvironmentService", "CreateEnvironmentCommand", "EnvironmentRecord",
            "new EnvironmentRecord(input.id())"));
        sources.put("com.example.direct.ProjectResultService", """
            package com.example.direct;
            import com.example.direct.domain.*;
            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveService;
            public class ProjectResultService implements ReactiveService<DeploymentState, DeploymentResult> {
              public static int calls;
              public Uni<DeploymentResult> process(DeploymentState input) {
                calls++;
                if (input instanceof ApplicationRecord value) return Uni.createFrom().item(new DeploymentResult(value.id(), "application"));
                if (input instanceof EnvironmentRecord value) return Uni.createFrom().item(new DeploymentResult(value.id(), "environment"));
                throw new IllegalArgumentException("Unexpected branch input " + input.getClass().getName());
              }
            }
            """);
        sources.put("com.example.direct.PersistenceObserverService", """
            package com.example.direct;
            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveSideEffectService;
            public class PersistenceObserverService<T> implements ReactiveSideEffectService<T> {
              public static int calls;
              public Uni<T> process(T input) { calls++; return Uni.createFrom().item(input); }
            }
            """);
        sources.put("com.example.direct.PersistencePluginHost", """
            package com.example.direct;
            import org.pipelineframework.annotation.PipelinePlugin;
            @PipelinePlugin("persistence")
            public class PersistencePluginHost { }
            """);
        return Map.copyOf(sources);
    }

    private String service(String name, String input, String output, String expression) {
        return """
            package com.example.direct;
            import com.example.direct.domain.*;
            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveService;
            public class %s implements ReactiveService<%s, %s> {
              public static int calls;
              public Uni<%s> process(%s input) { calls++; return Uni.createFrom().item(%s); }
            }
            """.formatted(name, input, output, output, input, expression);
    }

    private record Fixture(
        Path projectRoot,
        Path generatedRoot,
        Compilation compilation,
        Map<String, String> sources
    ) {
        String metadata(String name) throws IOException {
            return compilation.generatedFile(StandardLocation.CLASS_OUTPUT, "META-INF/pipeline", name)
                .orElseThrow(() -> new IllegalStateException("Missing generated metadata " + name))
                .getCharContent(true).toString();
        }
    }
}

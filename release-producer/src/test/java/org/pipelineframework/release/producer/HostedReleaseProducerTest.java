package org.pipelineframework.release.producer;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.*;
import static org.junit.jupiter.api.Assertions.*;

class HostedReleaseProducerTest {
    @TempDir Path directory;

    /** Verifies that the independent consumer accepts the producer's package associations for both named contracts. */
    @Test void originalProducerReleaseIsAcceptedByIndependentConsumerForUnrelatedContracts() throws Exception {
        for (String name : List.of("document-index", "temperature-conversion")) {
            Path root = Files.createDirectories(directory.resolve(name));
            var request = request(root, name, name);
            var producer = new DefaultPipelineReleaseProducer();
            var produced = producer.produce(request);
            Path descriptor = root.resolve("pipeline-release.json");
            producer.write(descriptor, produced);
            var loaded = PipelineJson.mapper().readValue(Files.readAllBytes(descriptor), PipelineReleaseDescriptor.class);
            var consumer = new PipelineReleaseClosureResolver(Map.of(PipelineReleaseArtifactUri.Scheme.FILE,
                artifact -> Files.newInputStream(Path.of(java.net.URI.create(artifact.uri())))),
                path -> PipelineJson.mapper().readValue(path.toFile(), PipelineContractDescriptor.class));
            var resolved = consumer.resolve(loaded, root.resolve("resolved"));
            assertEquals(produced, resolved.descriptor());
            assertEquals(name, resolved.contract().pipelineId());
            assertEquals(2, loaded.schemaVersion());
            assertEquals(2, loaded.deployment().entries().size());
            assertEquals(1, loaded.artifacts().stream().filter(a -> !a.stepIds().isEmpty()).count());
        }
    }

    @Test void producerRejectsOtherCompiledApplicationDespiteValidZipAndSeparateTruth() throws Exception {
        var request = request(directory, "document-index", "temperature-conversion");
        var failure = assertThrows(IllegalArgumentException.class, () -> new DefaultPipelineReleaseProducer().produce(request));
        assertTrue(failure.getMessage().contains("Compiled Truth differs"));
    }

    @Test void independentConsumerAlsoRejectsWrongPackageWithRecomputedArtifactHash() throws Exception {
        var request = request(directory, "document-index", "document-index");
        var produced = new DefaultPipelineReleaseProducer().produce(request);
        var wrong = request(Files.createDirectories(directory.resolve("other")), "document-index", "temperature-conversion");
        var wrongFile = wrong.artifacts().getFirst().file();
        String digest = "sha256:" + java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(wrongFile)));
        var old = produced.artifacts().getFirst();
        var artifact = new PipelineReleaseArtifactDescriptor(old.artifactId(), old.kind(), wrongFile.toUri().toString(), digest, old.stepIds(), old.capabilities());
        var corrupted = new PipelineReleaseDescriptor(2, produced.pipelineId(), produced.contractVersion(), produced.releaseVersion(), produced.compiledTruthArtifactId(), List.of(artifact, produced.artifacts().get(1)), produced.deployment());
        var consumer = new PipelineReleaseClosureResolver(Map.of(PipelineReleaseArtifactUri.Scheme.FILE,
            a -> Files.newInputStream(Path.of(java.net.URI.create(a.uri())))),
            path -> PipelineJson.mapper().readValue(path.toFile(), PipelineContractDescriptor.class));
        assertThrows(IllegalArgumentException.class, () -> consumer.resolve(corrupted, directory.resolve("rejected")));
        try (var files = Files.list(directory.resolve("rejected"))) { assertEquals(0, files.count()); }
    }

    /** Checks that a matching contract cannot compensate for changed or missing execution order. */
    @Test void producerRejectsChangedOrMissingOrderDespiteUnchangedContract() throws Exception {
        for (var order : List.of(Optional.of("[\"Other\"]"), Optional.<String>empty())) {
            Path root = Files.createTempDirectory(directory, "producer-order-");
            var request = request(root, "document-index", "document-index", order);
            var failure = assertThrows(IllegalArgumentException.class,
                () -> new DefaultPipelineReleaseProducer().produce(request));
            assertTrue(failure.getMessage().contains(order.isPresent() ? "Compiled Truth differs" : "different Compiled Truth resource set"), failure.getMessage());
        }
    }

    /** Rehashes modified packages so the consumer must detect order drift during resource verification. */
    @Test void consumerRejectsChangedOrMissingOrderAfterArtifactHashIsUpdated() throws Exception {
        for (var order : List.of(Optional.of("[\"Other\"]"), Optional.<String>empty())) {
            Path root = Files.createTempDirectory(directory, "consumer-order-");
            var producer = new DefaultPipelineReleaseProducer();
            var produced = producer.produce(request(root, "document-index", "document-index"));
            var changed = request(Files.createDirectories(root.resolve("changed")), "document-index", "document-index", order);
            var changedFile = changed.artifacts().getFirst().file();
            String digest = "sha256:" + java.util.HexFormat.of().formatHex(
                java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(changedFile)));
            var old = produced.artifacts().getFirst();
            var artifact = new PipelineReleaseArtifactDescriptor(old.artifactId(), old.kind(),
                changedFile.toUri().toString(), digest, old.stepIds(), old.capabilities());
            var corrupted = new PipelineReleaseDescriptor(2, produced.pipelineId(), produced.contractVersion(),
                produced.releaseVersion(), produced.compiledTruthArtifactId(),
                List.of(artifact, produced.artifacts().get(1)), produced.deployment());
            var consumer = new PipelineReleaseClosureResolver(Map.of(PipelineReleaseArtifactUri.Scheme.FILE,
                a -> Files.newInputStream(Path.of(java.net.URI.create(a.uri())))),
                path -> PipelineJson.mapper().readValue(path.toFile(), PipelineContractDescriptor.class));
            var failure = assertThrows(IllegalArgumentException.class,
                () -> consumer.resolve(corrupted, root.resolve("rejected")));
            assertTrue(failure.getMessage().contains(order.isPresent() ? "Compiled Truth differs" : "different Compiled Truth resource set"), failure.getMessage());
            try (var files = Files.list(root.resolve("rejected"))) { assertEquals(0, files.count()); }
        }
    }

    @Test void legacySerializationDoesNotGainDeploymentField() throws Exception {
        var request = request(directory, "document-index", "document-index");
        var legacyRequest = new ReleaseProductionRequest(request.contract(), request.releaseVersion(), request.compiledTruthArtifactId(), request.compiledTruthDirectory(), request.artifacts(), true);
        var producer = new DefaultPipelineReleaseProducer();
        var json = PipelineJson.mapper().readTree(producer.serialize(producer.produce(legacyRequest)));
        assertEquals(1, json.path("schemaVersion").intValue());
        assertFalse(json.has("deployment"));
    }

    @Test void restFunctionPlacementContractPassesProducerAndConsumerAndRejectsModuleMismatch() throws Exception {
        Path root = Files.createDirectories(directory.resolve("rest-function-placement"));
        var placement = new PipelinePlacementDescriptor(
            PipelinePlacementDescriptor.CURRENT_SCHEMA_VERSION,
            List.of(
                new PipelinePlacementDescriptor.InvocationPlacement(
                    "$root", "Run Review", "review-flow", false,
                    PipelinePlacementDescriptor.LOCAL_IN_SEGMENT, "app", "application"),
                new PipelinePlacementDescriptor.InvocationPlacement(
                    "$root", "Continue Review", "$root", true,
                    PipelinePlacementDescriptor.LOCAL_IN_SEGMENT, "app", "application")),
            List.of(
                new PipelinePlacementDescriptor.ExecutionSegment(
                    "segment-root", PipelinePlacementDescriptor.ROOT_ENTRY, "",
                    "app", "application", true, List.of("$root", "review-flow"), List.of("review-flow:Nested Wait")),
                new PipelinePlacementDescriptor.ExecutionSegment(
                    "segment-await-1", PipelinePlacementDescriptor.AWAIT_RESUME, "review-flow:Nested Wait",
                    "app", "application", true, List.of("$root", "review-flow"), List.of("review-flow:Nested Wait"))));
        var contract = new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            "rest-function-nested",
            "contract-rf-nested",
            "hash-rf-nested",
            "FUNCTION",
            "REST",
            "app",
            true,
            "PIPELINE_RUNTIME",
            List.of(new PipelineBundleStepDescriptor(0, "Run Review", "service", "ONE_TO_ONE", "WorkChoice", "LoopOutcome",
                "rfproof.pipeline.PipelineInvocation_root_RunReview", "", Map.of())),
            new PipelineBundleCapabilities(true, List.of("rest")),
            Map.of(),
            "",
            org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor.empty(),
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            placement);

        Path metadata = Files.createDirectories(root.resolve("classes/META-INF/pipeline"));
        Files.write(metadata.resolve("pipeline-contract.json"), PipelineJson.mapper().writeValueAsBytes(contract));
        Files.writeString(metadata.resolve("order.json"), "{\"order\":[\"rfproof.pipeline.PipelineInvocation_root_RunReview\"]}");
        Files.writeString(metadata.resolve("platform.json"), "{\"platform\":\"FUNCTION\",\"transport\":\"REST\",\"module\":\"app\"}");
        var bytes = new ByteArrayOutputStream();
        try (var jar = new JarOutputStream(bytes)) {
            add(jar, PipelineContractDescriptor.RESOURCE_PATH, PipelineJson.mapper().writeValueAsBytes(contract));
            add(jar, "META-INF/pipeline/order.json", "{\"order\":[\"rfproof.pipeline.PipelineInvocation_root_RunReview\"]}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            add(jar, "META-INF/pipeline/platform.json", "{\"platform\":\"FUNCTION\",\"transport\":\"REST\",\"module\":\"app\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try (var in = getClass().getResourceAsStream("/" + getClass().getName().replace('.', '/') + ".class")) {
                add(jar, getClass().getName().replace('.', '/') + ".class", in.readAllBytes());
            }
        }
        Path zip = root.resolve("application.zip");
        try (var out = new JarOutputStream(Files.newOutputStream(zip))) { add(out, "lib/application.jar", bytes.toByteArray()); }
        var producer = new DefaultPipelineReleaseProducer();
        var inputs = producer.materialize(List.of(
            new ReleaseArtifactInput("application", "lambda-zip", zip, zip.toUri().toString(), List.of("Run Review"), List.of("local", "rest")),
            new ReleaseArtifactInput("truth", "compiled-truth", metadata, root.resolve("truth.jar").toUri().toString(), List.of(), List.of())),
            "truth", metadata, root).stream()
            .map(a -> new ReleaseArtifactInput(a.artifactId(), a.kind(), a.file(), a.file().toUri().toString(), a.stepIds(), a.capabilities()))
            .toList();
        var payload = Map.of("payloadTypeId", "java.lang.String", "payloadEncoding", "json", "payload", "\"sample\"");
        var validDeployment = new PipelineReleaseDeployment("aws-durable-java-zip-v1", List.of(
            new PipelineReleaseDeployment.Entry("http", "application", "app", "lib/application.jar", getClass().getName(), "HTTP", List.of("route")),
            new PipelineReleaseDeployment.Entry("work", "application", "app", "lib/application.jar", getClass().getName(), "SQS", List.of("queue"))),
            List.of(new PipelineReleaseDeployment.Binding("route", "HTTP_ROUTE"), new PipelineReleaseDeployment.Binding("queue", "SQS_QUEUE")),
            new PipelineReleaseDeployment.Check("http", payload, payload, 1000));

        var produced = producer.produce(new ReleaseProductionRequest(contract, "1.0.0", "truth", metadata, inputs, true, validDeployment));
        Path descriptor = root.resolve("pipeline-release.json");
        producer.write(descriptor, produced);
        var loaded = PipelineJson.mapper().readValue(Files.readAllBytes(descriptor), PipelineReleaseDescriptor.class);
        var consumer = new PipelineReleaseClosureResolver(Map.of(PipelineReleaseArtifactUri.Scheme.FILE,
            artifact -> Files.newInputStream(Path.of(java.net.URI.create(artifact.uri())))),
            path -> PipelineJson.mapper().readValue(path.toFile(), PipelineContractDescriptor.class));
        var resolved = consumer.resolve(loaded, root.resolve("resolved"));
        assertEquals("FUNCTION", resolved.contract().platform());
        assertEquals("REST", resolved.contract().transport());
        assertTrue(resolved.contract().placement().present());
        assertEquals(placement, resolved.contract().placement());

        var mismatchedDeployment = new PipelineReleaseDeployment("aws-durable-java-zip-v1", List.of(
            new PipelineReleaseDeployment.Entry("http", "application", "wrong-module", "lib/application.jar", getClass().getName(), "HTTP", List.of("route"))),
            List.of(new PipelineReleaseDeployment.Binding("route", "HTTP_ROUTE")),
            new PipelineReleaseDeployment.Check("http", payload, payload, 1000));
        var failure = assertThrows(IllegalArgumentException.class, () ->
            producer.produce(new ReleaseProductionRequest(contract, "1.0.0", "truth", metadata, inputs, true, mismatchedDeployment)));
        assertTrue(failure.getMessage().contains("placement"), failure.getMessage());
    }

    @Test void restWorkerCommandReleaseRejectsNonRecoverableMemoryStoreAndAcceptsDynamoStore() throws Exception {
        var contract = new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            "rest-worker-command",
            "contract-rw-cmd",
            "hash-rw-cmd",
            "FUNCTION",
            "REST",
            "worker-app",
            false,
            "PIPELINE_RUNTIME",
            List.of(new PipelineBundleStepDescriptor(0, "Submit", "command", "ONE_TO_ONE",
                "com.example.Input", "com.example.Output", "com.example.SubmitStep", "", Map.of())),
            new PipelineBundleCapabilities(true, List.of("rest")),
            Map.of(
                "Input", Map.of("runtimeClass", "com.example.Input", "definitionFingerprint", "fp-in"),
                "Output", Map.of("runtimeClass", "com.example.Output", "definitionFingerprint", "fp-out")),
            "catalog-rw-cmd");
        var payload = Map.of("payloadTypeId", "com.example.Input", "payloadEncoding", "json", "payload", "{}");
        var deployment = new PipelineReleaseDeployment(
            "aws-durable-java21-rest-worker-v1",
            List.of(new PipelineReleaseDeployment.Entry(
                "worker", "worker", "worker-app", ".", getClass().getName(),
                "AWS_DURABLE_REST_WORKER", List.of("command-effects"))),
            List.of(new PipelineReleaseDeployment.Binding("command-effects", "aws:dynamodb:command-effects")),
            new PipelineReleaseDeployment.Check("worker", payload, payload, 1000));

        // 1. Default in-memory store fact is rejected before Release production succeeds.
        Path memoryRoot = Files.createDirectories(directory.resolve("rest-worker-memory"));
        var memoryRequest = restWorkerCommandRequest(memoryRoot, contract, deployment,
            "schemaVersion=1\ncommandEffectStore.provider=memory\ncommandEffectStore.recoverySupported=false\n"
                + "commandEffectStore.selectedBeans=org.pipelineframework.command.InMemoryCommandEffectStore\n");
        var rejected = assertThrows(IllegalArgumentException.class,
            () -> new DefaultPipelineReleaseProducer().produce(memoryRequest));
        assertTrue(rejected.getMessage().contains("command-effects") || rejected.getMessage().contains("recovery"),
            rejected.getMessage());

        // 2. Explicit Dynamo recoverable store fact is accepted.
        Path dynamoRoot = Files.createDirectories(directory.resolve("rest-worker-dynamo"));
        var dynamoRequest = restWorkerCommandRequest(dynamoRoot, contract, deployment,
            "schemaVersion=1\ncommandEffectStore.provider=dynamo\ncommandEffectStore.recoverySupported=true\n"
                + "commandEffectStore.selectedBeans=org.pipelineframework.command.DynamoCommandEffectStore\n");
        var produced = new DefaultPipelineReleaseProducer().produce(dynamoRequest);
        assertEquals(2, produced.schemaVersion());
        assertEquals("aws-durable-java21-rest-worker-v1", produced.deployment().profile());
    }

    private ReleaseProductionRequest restWorkerCommandRequest(
        Path root,
        PipelineContractDescriptor contract,
        PipelineReleaseDeployment deployment,
        String capabilitiesProperties
    ) throws Exception {
        Path metadata = Files.createDirectories(root.resolve("classes/META-INF/pipeline"));
        Files.write(metadata.resolve("pipeline-contract.json"), PipelineJson.mapper().writeValueAsBytes(contract));
        Files.writeString(metadata.resolve("order.json"), "{\"order\":[\"com.example.SubmitStep\"]}");
        Path zip = root.resolve("function.zip");
        try (var out = new JarOutputStream(Files.newOutputStream(zip))) {
            add(out, PipelineContractDescriptor.RESOURCE_PATH, PipelineJson.mapper().writeValueAsBytes(contract));
            add(out, "META-INF/pipeline/order.json", "{\"order\":[\"com.example.SubmitStep\"]}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            add(out, PipelineReleaseRuntimeCapabilities.RESOURCE_PATH,
                capabilitiesProperties.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try (var in = getClass().getResourceAsStream("/" + getClass().getName().replace('.', '/') + ".class")) {
                add(out, getClass().getName().replace('.', '/') + ".class", in.readAllBytes());
            }
        }
        var inputs = List.of(new ReleaseArtifactInput(
            "worker", "lambda-zip", zip, zip.toUri().toString(), List.of("Submit"), List.of("local", "rest")));
        return new ReleaseProductionRequest(contract, "1.0.0", "worker", metadata, inputs, true, deployment);
    }

    private ReleaseProductionRequest request(Path root, String name, String packagedName) throws Exception {
        return request(root, name, packagedName, Optional.of("[\"Transform\"]"));
    }

    /** Builds a package whose order resource can independently differ from its original carrier. */
    private ReleaseProductionRequest request(Path root, String name, String packagedName, Optional<String> packagedOrder) throws Exception {
        Path metadata = Files.createDirectories(root.resolve("classes/META-INF/pipeline"));
        var contract = contract(name);
        Files.write(metadata.resolve("pipeline-contract.json"), PipelineJson.mapper().writeValueAsBytes(contract));
        Files.writeString(metadata.resolve("order.json"), "[\"Transform\"]");
        var bytes = new ByteArrayOutputStream();
        try (var jar = new JarOutputStream(bytes)) {
            add(jar, PipelineContractDescriptor.RESOURCE_PATH, PipelineJson.mapper().writeValueAsBytes(contract(packagedName)));
            if (packagedOrder.isPresent()) {
                add(jar, "META-INF/pipeline/order.json", packagedOrder.orElseThrow().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            // Real compiled test-class bytes; package verifier does not claim application execution.
            try (var in = getClass().getResourceAsStream("/" + getClass().getName().replace('.', '/') + ".class")) {
                add(jar, getClass().getName().replace('.', '/') + ".class", in.readAllBytes());
            }
        }
        Path zip = root.resolve("application.zip");
        try (var out = new JarOutputStream(Files.newOutputStream(zip))) { add(out, "lib/application.jar", bytes.toByteArray()); }
        var producer = new DefaultPipelineReleaseProducer();
        var inputs = producer.materialize(List.of(
            new ReleaseArtifactInput("application", "lambda-zip", zip, zip.toUri().toString(), List.of("Transform"), List.of("local")),
            new ReleaseArtifactInput("truth", "compiled-truth", metadata, root.resolve("truth.jar").toUri().toString(), List.of(), List.of())), "truth", metadata, root);
        // materializer's carrier is the authoritative build artifact; give its exact file URI to the local resolver.
        inputs = inputs.stream().map(a -> new ReleaseArtifactInput(a.artifactId(), a.kind(), a.file(), a.file().toUri().toString(), a.stepIds(), a.capabilities())).toList();
        var payload = Map.of("payloadTypeId", "java.lang.String", "payloadEncoding", "json", "payload", "\"sample\"");
        var deployment = new PipelineReleaseDeployment("aws-durable-java-zip-v1", List.of(
            new PipelineReleaseDeployment.Entry("http", "application", name, "lib/application.jar", getClass().getName(), "HTTP", List.of("route")),
            new PipelineReleaseDeployment.Entry("work", "application", name, "lib/application.jar", getClass().getName(), "SQS", List.of("queue"))),
            List.of(new PipelineReleaseDeployment.Binding("route", "HTTP_ROUTE"), new PipelineReleaseDeployment.Binding("queue", "SQS_QUEUE")),
            new PipelineReleaseDeployment.Check("http", payload, payload, 1000));
        return new ReleaseProductionRequest(contract, "A", "truth", metadata, inputs, true, deployment);
    }
    private PipelineContractDescriptor contract(String name) {
        return new PipelineContractDescriptor(1, name, "contract-"+name, "hash-"+name, "FUNCTION", "REST", name, false, "monolith",
            List.of(new PipelineBundleStepDescriptor(0,"Transform","service","ONE_TO_ONE","input","output", "example.Transform", "", Map.of())),
            new PipelineBundleCapabilities(true,List.of()));
    }
    private static void add(JarOutputStream out, String name, byte[] bytes) throws Exception {
        out.putNextEntry(new JarEntry(name)); out.write(bytes); out.closeEntry();
    }
}

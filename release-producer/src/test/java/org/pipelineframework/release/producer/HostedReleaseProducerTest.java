package org.pipelineframework.release.producer;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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

    @Test void legacySerializationDoesNotGainDeploymentField() throws Exception {
        var request = request(directory, "document-index", "document-index");
        var legacyRequest = new ReleaseProductionRequest(request.contract(), request.releaseVersion(), request.compiledTruthArtifactId(), request.compiledTruthDirectory(), request.artifacts(), true);
        var producer = new DefaultPipelineReleaseProducer();
        var json = PipelineJson.mapper().readTree(producer.serialize(producer.produce(legacyRequest)));
        assertEquals(1, json.path("schemaVersion").intValue());
        assertFalse(json.has("deployment"));
    }

    private ReleaseProductionRequest request(Path root, String name, String packagedName) throws Exception {
        Path metadata = Files.createDirectories(root.resolve("classes/META-INF/pipeline"));
        var contract = contract(name);
        Files.write(metadata.resolve("pipeline-contract.json"), PipelineJson.mapper().writeValueAsBytes(contract));
        Files.writeString(metadata.resolve("order.json"), "[\"Transform\"]");
        var bytes = new ByteArrayOutputStream();
        try (var jar = new JarOutputStream(bytes)) {
            add(jar, PipelineContractDescriptor.RESOURCE_PATH, PipelineJson.mapper().writeValueAsBytes(contract(packagedName)));
            add(jar, "META-INF/pipeline/order.json", Files.readAllBytes(metadata.resolve("order.json")));
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

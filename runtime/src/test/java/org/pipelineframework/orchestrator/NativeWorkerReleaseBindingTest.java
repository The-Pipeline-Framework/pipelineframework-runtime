package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.deployment.release.CredentialResolver;
import org.pipelineframework.deployment.release.ResolverProfile;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding.Language;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding.Profiles;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding.Target;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.serialization.PolyglotInt32JsonCodec;

class NativeWorkerReleaseBindingTest {
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON = PipelineJson.mapper();
    private static final String PIPELINE = "native-worker";
    private static final String RELEASE = "release-1";
    private static final String HASH = "1".repeat(64);
    private static final String ARTIFACT = "application";
    private static final String TRUTH = "compiled-truth";
    private static final String PROJECTION = "META-INF/pipeline/native-execution-projection.json";
    private static final List<String> TYPES = List.of("InputValue", "FirstValue", "SecondValue", "OutputValue");
    private static final List<String> STEPS = List.of("A", "B", "C");
    private static final List<String> CAPABILITIES = List.of("local", "rest");
    private static final Profiles PROFILES = new Profiles(TransitionPayloadEncoding.JSON, PolyglotInt32JsonCodec.ENCODING,
        NativeWorkerReleaseBinding.EXECUTION_PROFILE, NativeWorkerReleaseBinding.RESULT_PROFILE);

    @TempDir Path temp;

    @Test
    void preparesBothVerifiedRunnablesAndRetainsExactTruthAndCommand() throws IOException {
        for (Language language : Language.values()) {
            var fixture = fixture(language);
            var target = fixture.target();
            var binding = prepare(target, fixture, "prepare-" + language);
            assertArrayEquals(fixture.projection(), binding.projectionBytes());
            var result = binding.projectionBytes();
            result[0] ^= 1;
            assertArrayEquals(fixture.projection(), binding.projectionBytes());
            Path root = binding.verifyPreparedTree();
            for (String file : language.requiredArchiveFiles()) {
                assertEquals("source", Files.readString(root.resolve(file)));
            }

            var command = command();
            var attempt = binding.pin(command);
            assertSame(command, attempt.command());
            assertSame(target, attempt.target());
            assertEquals("InputValue", attempt.expectedInputTypeId());
            assertEquals("OutputValue", attempt.expectedOutputTypeId());
            assertEquals(NativeWorkerArchive.sha256(fixture.projection()), attempt.projectionDigest());
            assertEquals(fixture.truth(), attempt.compiledTruth());
            assertEquals(URI.create("https://worker.example:443"), attempt.target().endpoint());
            attempt.requireCommand(changed(command, Map.of()));
            assertEquals(" execution ", attempt.command().executionId());
            assertEquals("trace\t", attempt.command().traceId());
            assertEquals("not JSON", attempt.command().payload());
        }
    }

    @Test
    void revalidationRejectsRunnableRuntimeAndProjectionMutationsAndRestoresOnExactBytes() throws IOException {
        var fixture = fixture(Language.NODE);
        var binding = prepare(fixture.target(), fixture, "prepared");
        Path root = binding.verifyPreparedTree();
        for (Path path : List.of(root.resolve("app/business.js"),
                root.resolve("node_modules/@tpf/experimental-node-local/index.mjs"),
                root.resolveSibling("release").resolve("compiled-truth").resolve(PROJECTION))) {
            byte[] original = Files.readAllBytes(path);
            byte[] changed = original.clone();
            changed[0] ^= 1;
            Files.write(path, changed);
            assertThrows(IOException.class, binding::verifyPreparedTree);
            Files.write(path, original);
            assertEquals(root, binding.verifyPreparedTree());
        }
        Path extra = Files.writeString(root.resolve("app/shadow.js"), "extra");
        assertThrows(IOException.class, binding::verifyPreparedTree);
        Files.delete(extra);
        Path runtime = root.resolve("node_modules/@tpf/experimental-node-local/index.mjs");
        Files.delete(runtime);
        assertThrows(IOException.class, binding::verifyPreparedTree);
        Files.createSymbolicLink(runtime, root.resolve("app/business.js"));
        assertThrows(IOException.class, binding::verifyPreparedTree);
    }

    @Test
    void checksTheTrustedReleaseAndRejectsIncompatibleArchivePaths() throws IOException {
        var fixture = fixture(Language.NODE);
        var target = fixture.target();
        var differentContract = JSON.<ObjectNode>valueToTree(target.coordinatorContract())
            .put("canonicalCatalogFingerprint", "different catalog");
        var differentTarget = fixture.target(JSON.treeToValue(differentContract, PipelineContractDescriptor.class),
            Language.NODE, RELEASE, fixture.runnable().digest(), "worker", "key");
        assertThrows(IllegalArgumentException.class, () -> prepare(differentTarget, fixture, "different-contract"));
        assertThrows(IllegalArgumentException.class, () -> prepare(fixture.target(target.coordinatorContract(),
            Language.NODE, "other-release", fixture.runnable().digest(), "worker", "key"), fixture, "wrong-release"));
        assertThrows(IllegalArgumentException.class, () -> prepare(fixture.target(target.coordinatorContract(),
            Language.NODE, RELEASE, "sha256:" + HASH, "worker", "key"), fixture, "wrong-runnable"));
        assertThrows(IllegalArgumentException.class, () -> prepare(fixture.target(target.coordinatorContract(),
            Language.PYTHON, RELEASE, fixture.runnable().digest(), "worker", "key"), fixture, "wrong-language"));

        var unsafe = fixture(Language.NODE, Map.of("app/../shadow.js", "unsafe"), Set.of());
        Path rejected = temp.resolve("unsafe");
        assertThrows(IOException.class, () -> prepare(unsafe.target(), unsafe, rejected.getFileName().toString()));
        try (var children = Files.list(rejected)) {
            assertFalse(children.findAny().isPresent());
        }
        var incomplete = fixture(Language.PYTHON, Map.of(), Set.of("tpf_experimental_python_local/_admission.py"));
        assertThrows(IOException.class, () -> prepare(incomplete.target(), incomplete, "incomplete"));
    }

    @Test
    void rejectsOversizedVerifiedProjectionAndLeavesWorkspaceReusable() throws IOException {
        int limit = 1024 * 1024;
        byte[] original = projection(Language.NODE);
        byte[] oversized = Arrays.copyOf(original, limit + 1);
        Arrays.fill(oversized, original.length, oversized.length, (byte) ' ');
        var rejected = fixture(Language.NODE, Map.of(), Set.of(), oversized);

        var failure = assertThrows(IOException.class, () -> prepare(rejected.target(), rejected, "size-limited"));
        assertEquals("Native worker execution projection exceeds the 1 MiB limit", failure.getMessage());
        try (var children = Files.list(temp.resolve("size-limited"))) {
            assertFalse(children.findAny().isPresent());
        }

        byte[] atLimit = Arrays.copyOf(oversized, limit);
        var accepted = fixture(Language.NODE, Map.of(), Set.of(), atLimit);
        var binding = prepare(accepted.target(), accepted, "size-limited");
        assertArrayEquals(atLimit, binding.projectionBytes());
    }

    @Test
    void pinsOnlyTheAdmittedScopeRangeContextTypeAndEncoding() throws IOException {
        var fixture = fixture(Language.NODE);
        var binding = prepare(fixture.target(), fixture, "prepared");
        var command = command();
        for (var changes : List.<Map<String, ?>>of(
                Map.of("tenantId", "other-tenant"), Map.of("pipelineId", "other-pipeline"),
                Map.of("contractVersion", "other-contract"), Map.of("releaseVersion", "other-release"),
                Map.of("currentStepIndex", 1), Map.of("stopBeforeStepIndex", 3), Map.of("stopBeforeStepIndex", -2),
                Map.of("resultShape", "MATERIALIZED_MULTI"), Map.of("awaitOccurrenceCause", "unexpected-await"),
                Map.of("pageContext", Map.of("pageIndex", 0, "sourceIdentity", "page", "maxRecords", 1)),
                Map.of("redriveIntent", "REISSUE_COMMAND", "redriveCommandId", "effect", "redriveReason", "manual"),
                Map.of("executionId", " \t\r\n"), Map.of("traceId", "\n"),
                Map.of("payloadTypeId", "FirstValue"), Map.of("payloadEncoding", TransitionPayloadEncoding.JSON))) {
            var incompatible = changed(command, changes);
            assertThrows(IllegalArgumentException.class, () -> binding.pin(incompatible), changes.toString());
        }
        assertThrows(IllegalArgumentException.class, () -> new Profiles("json", PROFILES.payloadEncoding(),
            PROFILES.executionProfile(), PROFILES.resultProfile()));
        assertThrows(IllegalArgumentException.class, () -> new Profiles(PROFILES.outerEncoding(), "other",
            PROFILES.executionProfile(), PROFILES.resultProfile()));
        assertThrows(IllegalArgumentException.class, () -> new Profiles(PROFILES.outerEncoding(), PROFILES.payloadEncoding(),
            "other", PROFILES.resultProfile()));
        assertThrows(IllegalArgumentException.class, () -> new Profiles(PROFILES.outerEncoding(), PROFILES.payloadEncoding(),
            PROFILES.executionProfile(), "legacy"));
    }

    @Test
    void preservesNonAsciiWhitespaceInContextIdentifiers() throws IOException {
        var fixture = fixture(Language.NODE);
        var binding = prepare(fixture.target(), fixture, "prepared");
        var command = changed(command(), Map.of("executionId", "\u00a0", "traceId", "\u2003"));

        var retained = binding.pin(command).command();

        assertEquals("\u00a0", retained.executionId());
        assertEquals("\u2003", retained.traceId());
    }

    @Test
    void retainedAttemptSurvivesDeploymentReplacementAndRejectsOtherCommand() throws IOException {
        var fixture = fixture(Language.NODE);
        var originalContract = fixture.contract();
        var originalTarget = fixture.target(originalContract, Language.NODE, RELEASE, fixture.runnable().digest(),
            "worker-old", "key-old");
        changeCatalog(originalContract, "changed-original");
        changeCatalog(originalTarget.coordinatorContract(), "changed-accessor-copy");
        var first = prepare(originalTarget, fixture, "first");
        var command = command();
        var retained = first.pin(command);

        var newTarget = fixture.target(fixture.contract(), Language.NODE, RELEASE, fixture.runnable().digest(),
            "worker-new", "key-new");
        var replacement = prepare(newTarget, fixture, "second");
        var next = changed(command, Map.of("attempt", 2, "executionVersion", 8, "transitionKey", "next"));
        var newAttempt = replacement.pin(next);
        assertEquals("worker-old", retained.target().workerId());
        assertEquals("key-old", retained.target().sharedSecretRef());
        assertEquals("worker-new", newAttempt.target().workerId());
        assertEquals(PROFILES, retained.target().profiles());
        assertEquals(fixture.contract(), retained.target().coordinatorContract());
        retained.requireCommand(command);
        assertThrows(IllegalArgumentException.class, () -> retained.requireCommand(next));
        assertThrows(IllegalArgumentException.class, () -> newAttempt.requireCommand(command));
        for (var change : List.<Map<String, ?>>of(Map.of("executionVersion", 9), Map.of("attempt", 3),
                Map.of("transitionKey", "stale"), Map.of("executionId", "another"),
                Map.of("currentStepIndex", 1), Map.of("payload", "another byte sequence"))) {
            var other = changed(command, change);
            assertThrows(IllegalArgumentException.class, () -> retained.requireCommand(other));
        }
    }

    @SuppressWarnings("unchecked")
    private static void changeCatalog(PipelineContractDescriptor contract, String value) {
        var definition = (Map<String, Object>) contract.canonicalTypes().get(TYPES.getFirst()).get("definition");
        definition.put("id", value);
    }

    private NativeWorkerReleaseBinding prepare(Target target, Fixture fixture, String workspace) throws IOException {
        return NativeWorkerReleaseBinding.prepare(target, fixture.descriptor(), temp.resolve(workspace),
            ResolverProfile.localOnly(), CredentialResolver.NONE);
    }

    private Fixture fixture(Language language) throws IOException {
        return fixture(language, Map.of(), Set.of());
    }

    private Fixture fixture(Language language, Map<String, String> extraFiles, Set<String> omittedFiles) throws IOException {
        return fixture(language, extraFiles, omittedFiles, projection(language));
    }

    private static byte[] projection(Language language) throws IOException {
        return JSON.writeValueAsBytes(Map.of(
            "schemaVersion", 1, "kind", language == Language.NODE ? "compiler-node-local" : "compiler-python-local",
            "profile", PROFILES.executionProfile(),
            "pipeline", Map.of("id", PIPELINE, "platform", "COMPUTE", "transport", "LOCAL", "execution", "DIRECT",
                "inputType", TYPES.getFirst(), "outputType", TYPES.getLast()),
            "steps", STEPS.stream().map(name -> {
                int index = STEPS.indexOf(name);
                return Map.of("id", name, "inputType", TYPES.get(index), "outputType", TYPES.get(index + 1));
            }).toList(),
            "types", TYPES.stream().map(id -> Map.of("id", id)).toList()));
    }

    private Fixture fixture(Language language, Map<String, String> extraFiles, Set<String> omittedFiles,
            byte[] projection) throws IOException {
        Path directory = Files.createTempDirectory(temp, "fixture-");
        var contract = newContract();
        var members = new LinkedHashMap<String, byte[]>();
        for (String path : language.requiredArchiveFiles()) {
            if (!omittedFiles.contains(path)) {
                members.put(path, "source".getBytes(StandardCharsets.UTF_8));
            }
        }
        extraFiles.forEach((path, content) -> members.put(path, content.getBytes(StandardCharsets.UTF_8)));
        Path runnable = zip(directory.resolve("runnable.zip"), members);
        Path truth = zip(directory.resolve("truth.zip"), Map.of(
            PipelineContractDescriptor.RESOURCE_PATH, JSON.writeValueAsBytes(contract), PROJECTION, projection));
        var runnableArtifact = artifact(ARTIFACT, "application-archive", runnable, STEPS, CAPABILITIES);
        var truthArtifact = artifact(TRUTH, "compiled-truth", truth, List.of(), List.of());
        var descriptor = new PipelineReleaseDescriptor(1, PIPELINE, contract.contractVersion(), RELEASE,
            TRUTH, List.of(runnableArtifact, truthArtifact));
        Path descriptorFile = directory.resolve("pipeline-release.json");
        Files.write(descriptorFile, JSON.writeValueAsBytes(descriptor));
        return new Fixture(language, descriptorFile, runnableArtifact, truthArtifact, projection);
    }

    private static PipelineContractDescriptor newContract() throws IOException {
        var catalog = new LinkedHashMap<String, Object>();
        TYPES.forEach(type -> catalog.put(type, Map.of("definition", Map.of("id", type))));
        var steps = STEPS.stream().map(name -> {
            int index = STEPS.indexOf(name);
            return Map.of("index", index, "authoredName", name, "kind", "internal", "cardinality", "ONE_TO_ONE",
                "inputTypeId", TYPES.get(index), "outputTypeId", TYPES.get(index + 1));
        }).toList();
        var node = JSON.createObjectNode().put("schemaVersion", 2).put("pipelineId", PIPELINE)
            .put("contractVersion", "sha256:" + HASH).put("contractHash", HASH).put("platform", "COMPUTE")
            .put("transport", "LOCAL").put("runtimeLayout", "MONOLITH").put("pluginHost", false);
        node.set("steps", JSON.valueToTree(steps));
        node.set("canonicalTypes", JSON.valueToTree(catalog));
        node.set("capabilities", JSON.valueToTree(Map.of("localTransitionExecution", true,
            "transitionWorkerProtocols", CAPABILITIES)));
        return JSON.treeToValue(node, PipelineContractDescriptor.class);
    }

    private static Path zip(Path file, Map<String, byte[]> entries) throws IOException {
        try (var archive = new ZipOutputStream(Files.newOutputStream(file))) {
            for (var entry : entries.entrySet()) {
                archive.putNextEntry(new ZipEntry(entry.getKey()));
                archive.write(entry.getValue());
                archive.closeEntry();
            }
        }
        return file;
    }

    private static PipelineReleaseArtifactDescriptor artifact(String id, String kind, Path file, List<String> steps,
            List<String> capabilities) throws IOException {
        return new PipelineReleaseArtifactDescriptor(id, kind, file.toUri().toASCIIString(),
            NativeWorkerArchive.sha256(Files.readAllBytes(file)), steps, capabilities);
    }

    private static TransitionCommandEnvelope command() {
        return new TransitionCommandEnvelope("tenant", " execution ", PIPELINE, "sha256:" + HASH, RELEASE, 0, -1,
            1, ExecutionResultShape.SINGLE, 7, "transition", "trace\t", TYPES.getFirst(), PROFILES.payloadEncoding(), "not JSON");
    }

    private static TransitionCommandEnvelope changed(TransitionCommandEnvelope command, Map<String, ?> fields) throws IOException {
        ObjectNode node = JSON.valueToTree(command);
        for (var field : fields.entrySet()) {
            JsonNode value = JSON.valueToTree(field.getValue());
            node.set(field.getKey(), value);
        }
        return JSON.treeToValue(node, TransitionCommandEnvelope.class);
    }

    private record Fixture(Language language, Path descriptor, PipelineReleaseArtifactDescriptor runnable,
            PipelineReleaseArtifactDescriptor truth, byte[] projection) {
        PipelineContractDescriptor contract() throws IOException {
            return newContract();
        }

        Target target() throws IOException {
            return target(contract(), language, RELEASE, runnable.digest(), "worker", "key");
        }

        Target target(PipelineContractDescriptor contract, Language selectedLanguage, String releaseVersion,
                String runnableDigest, String worker, String key) throws IOException {
            return new Target("tenant", worker, URI.create("https://WORKER.example/"), key, selectedLanguage, releaseVersion,
                NativeWorkerArchive.sha256(Files.readAllBytes(descriptor)), runnable.artifactId(), runnableDigest, contract, PROFILES);
        }
    }
}

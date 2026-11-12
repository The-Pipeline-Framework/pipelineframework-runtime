package org.pipelineframework.orchestrator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.deployment.release.CredentialResolver;
import org.pipelineframework.deployment.release.ReleaseVerifier;
import org.pipelineframework.deployment.release.ResolverProfile;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactKind;
import org.pipelineframework.orchestrator.release.ResolvedPipelineRelease;
import org.pipelineframework.orchestrator.release.ResolvedPipelineReleaseArtifact;
import org.pipelineframework.serialization.PolyglotInt32JsonCodec;

/** Deployment-owned preparation and attempt selection for the experimental native whole-bundle profile. */
public final class NativeWorkerReleaseBinding {
    public static final String EXECUTION_PROFILE = "tpf-polyglot-unary-policy/1";
    public static final String RESULT_PROFILE = "tpf-polyglot-failed-result/1";

    private static final String PROJECTION_RESOURCE = "META-INF/pipeline/native-execution-projection.json";
    private static final Pattern SHA256 = Pattern.compile("sha256:[0-9a-f]{64}");
    private static final Set<String> WORKER_CAPABILITIES = Set.of("local", "rest");
    private static final ObjectMapper JSON = PipelineJson.mapper();

    private final Target target;
    private final NativeWorkerArchive archive;
    private final Path projectionFile;
    private final byte[] projectionBytes;
    private final String projectionDigest;
    private final PipelineReleaseArtifactDescriptor compiledTruth;
    private final String inputTypeId;
    private final String outputTypeId;

    private NativeWorkerReleaseBinding(Target target, NativeWorkerArchive archive, Path projectionFile,
            byte[] projectionBytes, PipelineReleaseArtifactDescriptor compiledTruth, List<String> boundaries) {
        this.target = target;
        this.archive = archive;
        this.projectionFile = projectionFile;
        this.projectionBytes = projectionBytes.clone();
        this.projectionDigest = NativeWorkerArchive.sha256(projectionBytes);
        this.compiledTruth = compiledTruth;
        this.inputTypeId = boundaries.getFirst();
        this.outputTypeId = boundaries.getLast();
    }

    public enum Language {
        NODE("compiler-node-local", "node_modules/@tpf/experimental-node-local/", Set.of(
            "package.json", "app/application.js", "app/models.js", "app/business.js",
            "node_modules/@tpf/experimental-node-local/package.json",
            "node_modules/@tpf/experimental-node-local/index.mjs",
            "node_modules/@tpf/experimental-node-local/payload-binding.mjs",
            "node_modules/@tpf/experimental-node-local/projection.mjs",
            "node_modules/@tpf/experimental-node-local/records.mjs")),
        PYTHON("compiler-python-local", "tpf_experimental_python_local/", Set.of(
            "app/__init__.py", "app/business.py", "app/generated/project/application.py",
            "app/generated/project/models.py", "tpf_experimental_python_local/__init__.py",
            "tpf_experimental_python_local/_admission.py", "tpf_experimental_python_local/_payload_binding.py"));

        private final String projectionKind;
        private final String runtimeDirectory;
        private final Set<String> requiredArchiveFiles;

        Language(String projectionKind, String runtimeDirectory, Set<String> requiredArchiveFiles) {
            this.projectionKind = projectionKind;
            this.runtimeDirectory = runtimeDirectory;
            this.requiredArchiveFiles = requiredArchiveFiles;
        }

        Set<String> requiredArchiveFiles() {
            return requiredArchiveFiles;
        }

        boolean includesArchiveFile(String name) {
            return requiredArchiveFiles.contains(name) || name.startsWith("app/")
                || name.startsWith(runtimeDirectory) || name.startsWith("META-INF/pipeline/");
        }
    }

    /** The four selections are supplied independently by trusted deployment configuration. */
    public record Profiles(String outerEncoding, String payloadEncoding, String executionProfile, String resultProfile) {
        public Profiles {
            if (!TransitionPayloadEncoding.JSON.equals(outerEncoding)
                || !PolyglotInt32JsonCodec.ENCODING.equals(payloadEncoding)
                || !EXECUTION_PROFILE.equals(executionProfile)
                || !RESULT_PROFILE.equals(resultProfile)) {
                throw new IllegalArgumentException("Unsupported native worker profile selection");
            }
        }
    }

    /** Scoped configuration from the deployment, including the coordinator's exact generated Contract. */
    public record Target(String tenantId, String workerId, URI endpoint, String sharedSecretRef,
            Language language, String releaseVersion, String descriptorDigest, String runnableArtifactId,
            String runnableArtifactDigest, PipelineContractDescriptor coordinatorContract, Profiles profiles) {
        public Target {
            requireCanonicalText(tenantId, "tenantId");
            requireCanonicalText(workerId, "workerId");
            requireCanonicalText(sharedSecretRef, "sharedSecretRef");
            requireCanonicalText(releaseVersion, "releaseVersion");
            requireCanonicalText(runnableArtifactId, "runnableArtifactId");
            requireDigest(descriptorDigest, "descriptorDigest");
            requireDigest(runnableArtifactDigest, "runnableArtifactDigest");
            endpoint = httpsOrigin(endpoint);
            Objects.requireNonNull(language, "language");
            Objects.requireNonNull(profiles, "profiles");
            coordinatorContract = copyContract(Objects.requireNonNull(coordinatorContract, "coordinatorContract"));
        }

        @Override
        public PipelineContractDescriptor coordinatorContract() {
            return copyContract(coordinatorContract);
        }

        public String pipelineId() {
            return coordinatorContract.pipelineId();
        }

        public String contractVersion() {
            return coordinatorContract.contractVersion();
        }

        private static PipelineContractDescriptor copyContract(PipelineContractDescriptor contract) {
            return JSON.convertValue(contract, PipelineContractDescriptor.class);
        }
    }

    /**
     * Verifies the pinned Release closure and prepares its native runnable in an empty deployment-owned workspace.
     * Run this blocking preparation on the deployment side of the boundary.
     */
    public static NativeWorkerReleaseBinding prepare(Target target, Path descriptorFile, Path workspace,
            ResolverProfile resolvers, CredentialResolver credentials) throws IOException {
        Objects.requireNonNull(target, "target");
        var contract = target.coordinatorContract();
        var boundaries = contractBoundaries(contract);
        Path root = Files.createDirectories(workspace.toAbsolutePath().normalize()).toRealPath();
        try (var children = Files.list(root)) {
            if (children.findAny().isPresent()) {
                throw new IOException("Native worker workspace must be empty");
            }
        }
        Path preparation = Files.createTempDirectory(root, "native-worker-");
        try {
            var resolved = new ReleaseVerifier().verify(descriptorFile, target.descriptorDigest(),
                preparation.resolve("release"), resolvers, credentials).resolved();
            var runnable = wholeBundle(target, contract, resolved);
            Path projection = resolved.compiledTruthDirectory().resolve(PROJECTION_RESOURCE);
            byte[] bytes = Files.readAllBytes(projection);
            requireProjection(target, boundaries, contract.steps(), bytes);
            var truth = resolved.descriptor().artifacts().stream()
                .filter(artifact -> artifact.artifactId().equals(resolved.descriptor().compiledTruthArtifactId()))
                .findFirst().orElseThrow();
            var archive = NativeWorkerArchive.materialize(runnable.file(), preparation.resolve("runnable"), target.language());
            return new NativeWorkerReleaseBinding(target, archive, projection, bytes, truth, boundaries);
        } catch (IOException | RuntimeException failure) {
            try {
                NativeWorkerArchive.deleteTree(preparation);
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    /**
     * Checks the prepared files immediately before controlled launch/import and returns the runnable root.
     * The deployment keeps the tree immutable during use and supplies the trusted interpreter.
     */
    public Path verifyPreparedTree() throws IOException {
        if (!Files.isRegularFile(projectionFile, LinkOption.NOFOLLOW_LINKS)
            || !projectionFile.equals(projectionFile.toRealPath())) {
            throw new IOException("Prepared native worker projection is unavailable");
        }
        try (var input = Files.newInputStream(projectionFile, LinkOption.NOFOLLOW_LINKS)) {
            if (!Arrays.equals(projectionBytes, input.readNBytes(projectionBytes.length + 1))) {
                throw new IOException("Prepared native worker projection changed");
            }
        }
        return archive.verifyTree();
    }

    public byte[] projectionBytes() {
        return projectionBytes.clone();
    }

    /** Checks typed envelope metadata before reading its application payload and retains the admitted command. */
    public Attempt pin(TransitionCommandEnvelope command) {
        Objects.requireNonNull(command, "command");
        if (!target.tenantId().equals(command.tenantId()) || !target.pipelineId().equals(command.pipelineId())
            || !target.contractVersion().equals(command.contractVersion())
            || !target.releaseVersion().equals(command.releaseVersion())) {
            throw new IllegalArgumentException("Transition command does not match the native worker Release binding");
        }
        if (command.currentStepIndex() != 0 || command.stopBeforeStepIndex() != -1
            || command.resultShape() != ExecutionResultShape.SINGLE
            || command.redriveIntent() != ExecutionRedriveIntent.REPLAY || command.redriveStepIndex() != -1
            || command.redriveCommandId().isPresent() || command.redriveReason().isPresent()
            || command.pageContext().isPresent() || command.awaitOccurrenceCause().isPresent()) {
            throw new IllegalArgumentException("Native worker requires the admitted whole-bundle SINGLE range");
        }
        if (!hasContextValue(command.executionId()) || !hasContextValue(command.traceId())) {
            throw new IllegalArgumentException("Native worker requires explicit executionId and traceId");
        }
        if (!target.profiles().payloadEncoding().equals(command.payloadEncoding())
            || !inputTypeId.equals(command.payloadTypeId())) {
            throw new IllegalArgumentException("Transition command does not match the pinned native input binding");
        }
        return new Attempt(target, command, compiledTruth, projectionDigest, inputTypeId, outputTypeId);
    }

    /** Retained dispatch expectation. The coordinator still owns active lease and settlement decisions. */
    public static final class Attempt {
        private final Target target;
        private final TransitionCommandEnvelope command;
        private final PipelineReleaseArtifactDescriptor compiledTruth;
        private final String projectionDigest;
        private final String expectedInputTypeId;
        private final String expectedOutputTypeId;

        private Attempt(Target target, TransitionCommandEnvelope command, PipelineReleaseArtifactDescriptor compiledTruth,
                String projectionDigest, String expectedInputTypeId, String expectedOutputTypeId) {
            this.target = target;
            this.command = command;
            this.compiledTruth = compiledTruth;
            this.projectionDigest = projectionDigest;
            this.expectedInputTypeId = expectedInputTypeId;
            this.expectedOutputTypeId = expectedOutputTypeId;
        }

        public Target target() { return target; }
        public TransitionCommandEnvelope command() { return command; }
        public PipelineReleaseArtifactDescriptor compiledTruth() { return compiledTruth; }
        public String projectionDigest() { return projectionDigest; }
        public String expectedInputTypeId() { return expectedInputTypeId; }
        public String expectedOutputTypeId() { return expectedOutputTypeId; }

        /** Compares the received command with the retained expectation before selecting a decoder. */
        public void requireCommand(TransitionCommandEnvelope received) {
            if (!command.equals(received)) {
                throw new IllegalArgumentException("Transition command differs from the retained native attempt");
            }
        }
    }

    private static List<String> contractBoundaries(PipelineContractDescriptor contract) {
        requireDigest(contract.contractVersion(), "Contract identity");
        if (contract.schemaVersion() != 2 || !contract.contractVersion().equals("sha256:" + contract.contractHash())
            || !"LOCAL".equals(contract.transport()) || !"COMPUTE".equals(contract.platform())
            || !"MONOLITH".equals(contract.runtimeLayout()) || contract.pluginHost() || contract.module() != null
            || !contract.capabilities().localTransitionExecution()
            || contract.capabilities().transitionWorkerProtocols().size() != WORKER_CAPABILITIES.size()
            || !WORKER_CAPABILITIES.containsAll(contract.capabilities().transitionWorkerProtocols())
            || contract.steps().size() != 3) {
            throw new IllegalArgumentException("Unsupported generated native worker Contract");
        }
        var steps = contract.steps();
        var input = steps.getFirst().inputTypeId();
        var previous = input;
        var boundaries = new ArrayList<String>();
        boundaries.add(input);
        for (int index = 0; index < steps.size(); index++) {
            var step = steps.get(index);
            if (step.index() != index || !"internal".equals(step.kind()) || !"ONE_TO_ONE".equals(step.cardinality())
                || !Objects.equals(previous, step.inputTypeId()) || step.outputTypeId() == null
                || step.runtimeClass() != null || step.clientClass() != null
                || !step.deferredCompletion().isEmpty() || !step.paging().isEmpty()) {
                throw new IllegalArgumentException("Unsupported native worker Contract range");
            }
            previous = step.outputTypeId();
            boundaries.add(previous);
        }
        new PolyglotInt32PayloadBinding(boundaries.toArray(String[]::new));
        if (!contract.canonicalTypes().keySet().equals(Set.copyOf(boundaries))) {
            throw new IllegalArgumentException("Native worker Contract canonical type catalog differs from its boundaries");
        }
        return List.copyOf(boundaries);
    }

    private static ResolvedPipelineReleaseArtifact wholeBundle(
            Target target, PipelineContractDescriptor contract, ResolvedPipelineRelease resolved) {
        if (!target.releaseVersion().equals(resolved.descriptor().releaseVersion())
            || !contract.equals(resolved.contract())) {
            throw new IllegalArgumentException("Pinned Release does not match the coordinator's generated native Contract");
        }
        var steps = contract.steps().stream().map(PipelineBundleStepDescriptor::authoredName).toList();
        var candidates = resolved.artifacts().stream().filter(artifact ->
            PipelineReleaseArtifactKind.APPLICATION_ARCHIVE.wireValue().equals(artifact.descriptor().kind())
            && artifact.descriptor().stepIds().containsAll(steps)
            && artifact.descriptor().capabilities().containsAll(WORKER_CAPABILITIES)).toList();
        if (candidates.size() != 1 || !target.runnableArtifactId().equals(candidates.getFirst().descriptor().artifactId())
            || !target.runnableArtifactDigest().equals(candidates.getFirst().descriptor().digest())) {
            throw new IllegalArgumentException("Pinned native Release must have one matching whole-bundle runnable");
        }
        return candidates.getFirst();
    }

    private static void requireProjection(Target target, List<String> boundaries, List<PipelineBundleStepDescriptor> steps,
            byte[] bytes) throws IOException {
        JsonNode projection = JSON.reader().with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(bytes);
        var pipeline = projection.path("pipeline");
        var projectedSteps = projection.path("steps");
        if (!projection.path("schemaVersion").isInt() || projection.path("schemaVersion").intValue() != 1
            || !target.language().projectionKind.equals(projection.path("kind").textValue())
            || !target.profiles().executionProfile().equals(projection.path("profile").textValue())
            || !target.pipelineId().equals(pipeline.path("id").textValue())
            || !"COMPUTE".equals(pipeline.path("platform").textValue())
            || !"LOCAL".equals(pipeline.path("transport").textValue())
            || !"DIRECT".equals(pipeline.path("execution").textValue())
            || !boundaries.getFirst().equals(pipeline.path("inputType").textValue())
            || !boundaries.getLast().equals(pipeline.path("outputType").textValue())
            || !projectedSteps.isArray() || projectedSteps.size() != steps.size()) {
            throw new IllegalArgumentException("Verified Compiled Truth projection differs from the native binding");
        }
        for (int index = 0; index < steps.size(); index++) {
            var step = steps.get(index);
            var projected = projectedSteps.get(index);
            if (!step.authoredName().equals(projected.path("id").textValue())
                || !step.inputTypeId().equals(projected.path("inputType").textValue())
                || !step.outputTypeId().equals(projected.path("outputType").textValue())) {
                throw new IllegalArgumentException("Verified Compiled Truth projection differs from the native Contract range");
            }
        }
        Set<String> types = new HashSet<>();
        var projectedTypes = projection.path("types");
        for (JsonNode type : projectedTypes) {
            types.add(type.path("id").textValue());
        }
        if (!projectedTypes.isArray() || projectedTypes.size() != boundaries.size()
            || !types.equals(Set.copyOf(boundaries))) {
            throw new IllegalArgumentException("Verified Compiled Truth projection differs from the native Contract catalog");
        }
    }

    private static URI httpsOrigin(URI endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        if (!"https".equalsIgnoreCase(endpoint.getScheme()) || endpoint.getHost() == null
            || endpoint.getRawUserInfo() != null || endpoint.getRawQuery() != null || endpoint.getRawFragment() != null
            || !Set.of("", "/").contains(endpoint.getRawPath()) || endpoint.getPort() == 0 || endpoint.getPort() > 65535) {
            throw new IllegalArgumentException("Native worker target requires a trusted HTTPS origin");
        }
        int port = endpoint.getPort() == -1 ? 443 : endpoint.getPort();
        return URI.create("https://" + endpoint.getHost().toLowerCase(Locale.ROOT) + ":" + port);
    }

    private static void requireCanonicalText(String value, String name) {
        if (value == null || value.isBlank() || !value.equals(value.strip())) {
            throw new IllegalArgumentException(name + " must be nonblank canonical text");
        }
    }

    private static void requireDigest(String digest, String name) {
        if (digest == null || !SHA256.matcher(digest).matches()) {
            throw new IllegalArgumentException(name + " must use sha256:<64 lowercase hex>");
        }
    }

    private static boolean hasContextValue(String value) {
        return value.chars().anyMatch(character ->
            character != ' ' && character != '\t' && character != '\r' && character != '\n');
    }
}

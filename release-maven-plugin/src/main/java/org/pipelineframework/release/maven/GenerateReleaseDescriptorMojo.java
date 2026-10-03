package org.pipelineframework.release.maven;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.release.producer.DefaultPipelineReleaseProducer;
import org.pipelineframework.release.producer.ReleaseArtifactInput;
import org.pipelineframework.release.producer.ReleaseProductionRequest;

/** Produces {@code pipeline-release.json} from packaged artifact bytes and Compiled Truth. */
@Mojo(name = "generate-release-descriptor", defaultPhase = LifecyclePhase.VERIFY, requiresProject = true, threadSafe = true)
public final class GenerateReleaseDescriptorMojo extends AbstractMojo {
    @Parameter(property = "tpf.release.skip", defaultValue = "false")
    private boolean skip;

    @Parameter(property = "tpf.release.version")
    private String releaseVersion;

    @Parameter(property = "tpf.release.output", defaultValue = "${project.build.directory}/pipeline-release.json", required = true)
    private File outputFile;

    @Parameter(
        property = "tpf.release.contractFile",
        defaultValue = "${project.build.outputDirectory}/META-INF/pipeline/pipeline-contract.json",
        required = true)
    private File contractFile;

    @Parameter(property = "tpf.release.artifactFile", defaultValue = "${project.artifact.file}")
    private File artifactFile;

    @Parameter(property = "tpf.release.artifactId", defaultValue = "${project.artifactId}")
    private String artifactId;

    @Parameter(property = "tpf.release.artifactKind", defaultValue = "jar")
    private String artifactKind;

    @Parameter(property = "tpf.release.artifactUri")
    private String artifactUri;

    @Parameter(property = "tpf.release.compiledTruthArtifactId")
    private String compiledTruthArtifactId;

    @Parameter(property = "tpf.release.allowLocalUris", defaultValue = "false")
    private boolean allowLocalUris;

    @Parameter
    private List<ReleaseArtifactConfiguration> artifacts;

    @Override
    public void execute() throws MojoExecutionException {
        if (skip) {
            getLog().info("Skipping Pipeline Release Descriptor production (tpf.release.skip=true)");
            return;
        }
        if (releaseVersion == null || releaseVersion.isBlank()) {
            throw new MojoExecutionException("tpf.release.version is required when release production is enabled");
        }
        try {
            DefaultPipelineReleaseProducer producer = new DefaultPipelineReleaseProducer();
            PipelineContractDescriptor contract = producer.loadContract(contractFile.toPath());
            List<ReleaseArtifactInput> configured = configuredArtifacts(contract);
            String carrierId = compiledTruthArtifactId == null || compiledTruthArtifactId.isBlank()
                ? inferredCompiledTruthArtifactId(configured)
                : compiledTruthArtifactId.trim();
            Path metadataDirectory = contractFile.toPath().toAbsolutePath().normalize().getParent();
            List<ReleaseArtifactInput> inputs = producer.materialize(
                configured, carrierId, metadataDirectory, outputFile.toPath().toAbsolutePath().normalize().getParent());
            var descriptor = producer.produce(new ReleaseProductionRequest(
                contract, releaseVersion, carrierId, metadataDirectory, inputs, allowLocalUris));
            producer.write(outputFile.toPath(), descriptor);
            getLog().info("Produced Pipeline Release Descriptor " + outputFile.toPath().toAbsolutePath().normalize());
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }

    private List<ReleaseArtifactInput> configuredArtifacts(PipelineContractDescriptor contract) {
        if (artifacts != null && !artifacts.isEmpty()) {
            return artifacts.stream()
                .map(artifact -> new ReleaseArtifactInput(
                    artifact.artifactId(),
                    artifact.kind(),
                    artifact.file() == null ? null : artifact.file().toPath(),
                    artifact.uri(),
                    artifact.stepIds(),
                    artifact.capabilities()))
                .toList();
        }

        File primaryArtifact = artifactFile;
        if (primaryArtifact == null) {
            throw new IllegalArgumentException("The Maven project has no packaged primary artifact");
        }
        String primaryArtifactId = artifactId;
        String primaryUri = artifactUri == null || artifactUri.isBlank()
            ? primaryArtifact.toPath().toAbsolutePath().normalize().toUri().toASCIIString()
            : artifactUri;
        return List.of(new ReleaseArtifactInput(
            primaryArtifactId,
            artifactKind,
            primaryArtifact.toPath(),
            primaryUri,
            DefaultPipelineReleaseProducer.defaultStepIds(contract),
            DefaultPipelineReleaseProducer.defaultCapabilities(contract)));
    }

    private static String inferredCompiledTruthArtifactId(List<ReleaseArtifactInput> configured) {
        if (configured.size() == 1) {
            return configured.getFirst().artifactId();
        }
        throw new IllegalArgumentException(
            "tpf.release.compiledTruthArtifactId is required for multi-artifact releases");
    }
}

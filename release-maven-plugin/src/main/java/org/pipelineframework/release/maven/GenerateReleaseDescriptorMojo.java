package org.pipelineframework.release.maven;

import java.io.File;
import java.util.List;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** Produces {@code pipeline-release.json} from packaged artifact bytes and Compiled Truth. */
@Mojo(name = "generate-release-descriptor", defaultPhase = LifecyclePhase.VERIFY, requiresProject = true, threadSafe = true)
public final class GenerateReleaseDescriptorMojo extends AbstractMojo {
    @Parameter(property = "tpf.release.version", required = true)
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

    @Parameter
    private List<ReleaseArtifactConfiguration> artifacts;

    @Override
    public void execute() throws MojoExecutionException {
        try {
            ReleaseDescriptorGenerator generator = new ReleaseDescriptorGenerator(PipelineJson.mapper());
            PipelineContractDescriptor contract = generator.loadContract(contractFile.toPath());
            List<ReleaseArtifactInput> inputs = configuredArtifacts(contract);
            var descriptor = generator.generate(contract, releaseVersion, inputs);
            generator.write(outputFile.toPath(), descriptor);
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
            ReleaseDescriptorGenerator.defaultStepIds(contract),
            ReleaseDescriptorGenerator.defaultCapabilities(contract)));
    }
}

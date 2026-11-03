package org.pipelineframework.deployment.release;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.util.repository.AuthenticationBuilder;
import org.pipelineframework.orchestrator.release.MavenArtifactCoordinates;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactResolver;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactUri;

public final class MavenArtifactResolverProvider implements ArtifactResolverProvider<MavenResolverConfiguration> {
    @Override public String scheme() { return "maven"; }
    @Override public Class<MavenResolverConfiguration> configurationType() { return MavenResolverConfiguration.class; }

    @Override
    public PipelineReleaseArtifactResolver create(MavenResolverConfiguration configuration, CredentialResolver credentials) {
        MavenSettingsReader.Settings settings = new MavenSettingsReader().read(configuration.settings());
        var localRepository = configuration.localRepository() != null
            ? configuration.localRepository()
            : settings.localRepository().orElse(Path.of(System.getProperty("user.home"), ".m2", "repository"));
        RepositorySystem system = repositorySystem();
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
        session.setLocalRepositoryManager(
                system.newLocalRepositoryManager(session, new LocalRepository(localRepository.toFile())));
        List<RemoteRepository> repositories = new ArrayList<>();
        for (int index = 0; index < configuration.repositories().size(); index++) {
            repositories.add(new RemoteRepository.Builder(
                "configured-" + index, "default", configuration.repositories().get(index).toASCIIString()).build());
        }
        for (MavenSettingsReader.Repository repository : settings.repositories()) {
            RemoteRepository.Builder builder = new RemoteRepository.Builder(
                repository.id(), "default", repository.url().toASCIIString());
            MavenSettingsReader.Server server = settings.servers().get(repository.id());
            if (server != null) {
                builder.setAuthentication(new AuthenticationBuilder()
                    .addUsername(server.username()).addPassword(server.password()).build());
            }
            repositories.add(builder.build());
        }
        return artifact -> open(system, session, repositories, artifact);
    }

    private static RepositorySystem repositorySystem() {
        DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
        locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
        locator.addService(TransporterFactory.class, FileTransporterFactory.class);
        locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
        locator.setErrorHandler(new DefaultServiceLocator.ErrorHandler() {
            @Override public void serviceCreationFailed(Class<?> type, Class<?> implementation, Throwable failure) {
                throw new IllegalStateException("Could not initialize Maven Resolver service " + implementation.getName(), failure);
            }
        });
        RepositorySystem system = locator.getService(RepositorySystem.class);
        if (system == null) throw new IllegalStateException("Could not initialize Maven Resolver");
        return system;
    }

    private static InputStream open(
        RepositorySystem system,
        RepositorySystemSession session,
        List<RemoteRepository> repositories,
        PipelineReleaseArtifactDescriptor descriptor
    ) throws IOException {
        MavenArtifactCoordinates coordinates = PipelineReleaseArtifactUri.parse(descriptor.uri()).maven()
            .orElseThrow(() -> new IllegalArgumentException("Maven resolver cannot resolve " + descriptor.uri()));
        String classifier = coordinates.classifier().isEmpty() ? "" : coordinates.classifier();
        var artifact = new DefaultArtifact(
            coordinates.groupId(), coordinates.artifactId(), classifier, coordinates.extension(), coordinates.version());
        try {
            var result = system.resolveArtifact(session, new ArtifactRequest(artifact, repositories, null));
            return Files.newInputStream(result.getArtifact().getFile().toPath());
        } catch (ArtifactResolutionException e) {
            throw new IOException("Could not resolve Maven artifact " + descriptor.uri(), e);
        }
    }
}

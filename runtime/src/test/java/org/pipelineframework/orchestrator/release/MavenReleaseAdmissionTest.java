package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.LocalControlPlaneSecretResolver;
import org.pipelineframework.orchestrator.LocalPipelineReleaseArtifactStore;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.release.dto.HostedReleaseRegisterRequest;

/** Only the external Maven repository is a fixture; admission, storage and registry are real. */
class MavenReleaseAdmissionTest {
    private static final String PIPELINE = "org.example.restaurant";
    private static final String CONTRACT = "sha256:contract";
    private static final String CARRIER = "maven:org.example:restaurant:zip:application:1.0.0";
    @TempDir Path directory;
    private PipelineReleaseRegistrar registrar;
    private HostedReleaseAdminResource admin;
    private Path repository;
    private Path carrier;

    @BeforeEach
    void setUp() throws Exception {
        repository = Files.createDirectories(directory.resolve("external-repository"));
        carrier = repository.resolve("org/example/restaurant/1.0.0/restaurant-1.0.0-application.zip");
        Files.createDirectories(carrier.getParent());
        archive(PIPELINE, "first");
        configure(repository.toUri().toString(), Optional.empty());
    }

    private void configure(String remote, Optional<Path> settings) {
        var builder = new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class)
            .withValidateUnknown(false)
            .withDefaultValue("pipeline.orchestrator.admin.enabled", "true")
            .withDefaultValue("pipeline.orchestrator.admin.admin-token", "test-token")
            .withDefaultValue("pipeline.orchestrator.releases.resolver.maven-repositories", remote)
            .withDefaultValue("pipeline.orchestrator.releases.resolver.maven-local-repository", directory.resolve("cache").toString());
        settings.ifPresent(path -> builder.withDefaultValue("pipeline.orchestrator.releases.resolver.maven-settings", path.toString()));
        var config = builder.build().getConfigMapping(PipelineOrchestratorConfig.class);
        registrar = new PipelineReleaseRegistrar();
        registrar.orchestratorConfig = config;
        registrar.artifactStore = new LocalPipelineReleaseArtifactStore(directory.resolve("store"));
        admin = new HostedReleaseAdminResource();
        admin.orchestratorConfig = config;
        admin.secretResolver = new LocalControlPlaneSecretResolver();
        admin.releaseRegistrar = registrar;
        admin.releaseRegistry = new InMemoryPipelineReleaseRegistry();
    }

    @Test
    void registrarAcceptsUnchangedCanonicalMavenApplicationArchive() throws Exception {
        Path descriptor = descriptor(CARRIER, digest(carrier), List.of());
        byte[] original = Files.readAllBytes(descriptor);
        var record = registrar.validate("tenant", PIPELINE, descriptor.toString(), 1000L);
        assertEquals(new PipelineReleaseDescriptorLoader().load(descriptor), record.descriptor());
        assertArrayEquals(original, Files.readAllBytes(descriptor));
        assertEquals(CARRIER, record.descriptor().artifacts().getFirst().uri());
        assertArrayEquals(Files.readAllBytes(carrier), Files.readAllBytes(Path.of(URI.create(record.primaryArtifactUri()))));
        assertEquals(digest(carrier), record.primaryArtifactDigest());
        registrar.verify(record);
    }

    @Test
    void nativeAdminAdmitsProducerShapedFastJarArchiveWithoutRewritingDescriptor() throws Exception {
        byte[] contractBytes;
        try (var jar = new java.util.jar.JarFile(carrier.toFile());
             var input = jar.getInputStream(jar.getJarEntry(PipelineContractDescriptor.RESOURCE_PATH))) {
            contractBytes = input.readAllBytes();
        }
        var nested = new java.io.ByteArrayOutputStream();
        try (var applicationJar = new JarOutputStream(nested)) {
            applicationJar.putNextEntry(new JarEntry(PipelineContractDescriptor.RESOURCE_PATH));
            applicationJar.write(contractBytes);
            applicationJar.closeEntry();
        }
        // ReleaseArtifactMaterializer flattens the quarkus-app directory into the ZIP:
        // root Compiled Truth plus app/*.jar, lib/* and quarkus-run.jar. These are external
        // artifact bytes only; no customer application code is loaded or executed here.
        try (var zip = new java.util.zip.ZipOutputStream(Files.newOutputStream(carrier))) {
            zipEntry(zip, PipelineContractDescriptor.RESOURCE_PATH, contractBytes);
            zipEntry(zip, "META-INF/pipeline/order.json", "[\"Validate\"]".getBytes());
            zipEntry(zip, "app/restaurant-1.0.0.jar", nested.toByteArray());
            zipEntry(zip, "quarkus-run.jar", nested.toByteArray());
            zipEntry(zip, "lib/main/fixture-dependency.jar", nested.toByteArray());
        }
        Path descriptor = descriptor(CARRIER, digest(carrier), List.of());
        byte[] original = Files.readAllBytes(descriptor);
        Response registered = register(descriptor);
        assertEquals(200, registered.getStatus());
        var record = (PipelineReleaseRecord) registered.getEntity();
        assertEquals(CARRIER, record.descriptor().artifacts().getFirst().uri());
        assertEquals(CONTRACT, record.contract().contractVersion());
        assertArrayEquals(original, Files.readAllBytes(descriptor));
        assertArrayEquals(Files.readAllBytes(carrier), Files.readAllBytes(Path.of(URI.create(record.primaryArtifactUri()))));
        assertEquals(200, admin.activate("tenant", PIPELINE, CONTRACT, "Bearer test-token").await().indefinitely().getStatus());
    }

    @Test
    void actualAdminRegistrationIsIdempotentAndActivationVerifiesStoredArtifact() throws Exception {
        Path descriptor = descriptor(CARRIER, digest(carrier), List.of());
        Response first = register(descriptor);
        assertEquals(200, first.getStatus());
        var original = (PipelineReleaseRecord) first.getEntity();
        assertEquals(original, register(descriptor).getEntity());
        assertEquals(1, admin.releaseRegistry.list("tenant", PIPELINE).await().indefinitely().size());
        Files.delete(carrier);
        assertEquals(200, admin.activate("tenant", PIPELINE, CONTRACT, "Bearer test-token").await().indefinitely().getStatus());
        Files.writeString(Path.of(URI.create(original.primaryArtifactUri())), "tampered");
        assertThrows(IllegalStateException.class, () -> registrar.verify(original));
        assertEquals(409, admin.activate("tenant", PIPELINE, CONTRACT, "Bearer test-token").await().indefinitely().getStatus());
    }

    @Test
    void rejectsChangedImmutableContentForRegisteredRelease() throws Exception {
        Path descriptor = descriptor(CARRIER, digest(carrier), List.of());
        assertEquals(200, register(descriptor).getStatus());
        archive(PIPELINE, "changed");
        // A second canonical coordinate avoids treating the Maven cache as a mutable repository.
        Path changed = carrier.resolveSibling("restaurant-1.0.0-rebuilt.zip");
        Files.copy(carrier, changed);
        descriptor = descriptor("maven:org.example:restaurant:zip:rebuilt:1.0.0", digest(changed), List.of());
        assertEquals(409, register(descriptor).getStatus());
        assertEquals(1, admin.releaseRegistry.list("tenant", PIPELINE).await().indefinitely().size());
    }

    @Test
    void rejectsBadPrimaryDigestBeforeRegistration() throws Exception {
        rejects(descriptor(CARRIER, "sha256:" + "0".repeat(64), List.of()));
    }

    @Test
    void rejectsMismatchedEmbeddedContractBeforeRegistration() throws Exception {
        archive("org.example.wrong", "wrong contract");
        rejects(descriptor(CARRIER, digest(carrier), List.of()));
    }

    @Test
    void rejectsMissingMavenCarrierBeforeRegistration() throws Exception {
        String digest = digest(carrier);
        Files.delete(carrier);
        rejects(descriptor(CARRIER, digest, List.of()));
    }

    @Test
    void verifiesEveryArtifactIncludingSecondaryDigest() throws Exception {
        Path secondary = Files.writeString(directory.resolve("secondary.bin"), "secondary");
        var artifact = artifact("secondary", "native-binary", secondary.toUri().toString(), "sha256:" + "0".repeat(64));
        rejects(descriptor(CARRIER, digest(carrier), List.of(artifact)));
        assertFalse(Files.exists(directory.resolve("store")), "invalid closure must not be stored");
    }

    @Test
    void rejectsMissingSecondaryArtifactBeforeRegistration() throws Exception {
        var artifact = artifact("missing", "native-binary", "maven:org.example:absent:1.0.0", "sha256:" + "0".repeat(64));
        rejects(descriptor(CARRIER, digest(carrier), List.of(artifact)));
    }

    @Test
    void acceptsSeveralCanonicalMavenArtifactsAndStoresEachDigest() throws Exception {
        Path secondary = carrier.resolveSibling("restaurant-1.0.0-worker.jar");
        Files.writeString(secondary, "immutable worker artifact");
        var artifact = artifact("worker", "jar", "maven:org.example:restaurant:jar:worker:1.0.0", digest(secondary));
        assertEquals(200, register(descriptor(CARRIER, digest(carrier), List.of(artifact))).getStatus());
        try (var files = Files.walk(directory.resolve("store"))) {
            var digests = files.filter(Files::isRegularFile).map(path -> {
                try { return digest(path); } catch (Exception e) { throw new IllegalStateException(e); }
            }).collect(java.util.stream.Collectors.toSet());
            assertEquals(java.util.Set.of(digest(carrier), digest(secondary)), digests);
        }
    }

    @Test
    void acceptsMixedMavenAndFileClosureAndStoresAllArtifacts() throws Exception {
        Path secondary = Files.writeString(directory.resolve("secondary.bin"), "secondary");
        var artifact = artifact("secondary", "native-binary", secondary.toUri().toString(), digest(secondary));
        assertEquals(200, register(descriptor(CARRIER, digest(carrier), List.of(artifact))).getStatus());
        try (var files = Files.walk(directory.resolve("store"))) {
            assertEquals(2, files.filter(Files::isRegularFile).count());
        }
    }

    @Test
    void privateMavenRepositoryUsesExplicitSettingsServerCredentials() throws Exception {
        var authenticated = new AtomicInteger();
        var server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        String authorization = "Basic " + java.util.Base64.getEncoder().encodeToString("fixture-user:fixture-password".getBytes());
        server.createContext("/", exchange -> {
            try {
                if (!authorization.equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                    exchange.getResponseHeaders().set("WWW-Authenticate", "Basic realm=fixture");
                    exchange.sendResponseHeaders(401, -1);
                    return;
                }
                authenticated.incrementAndGet();
                Path file = repository.resolve(exchange.getRequestURI().getPath().substring(1)).normalize();
                if (!file.startsWith(repository) || !Files.isRegularFile(file)) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                byte[] bytes = Files.readAllBytes(file);
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            } finally { exchange.close(); }
        });
        server.start();
        try {
            String remote = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
            Path settings = Files.writeString(directory.resolve("settings.xml"), """
                <settings><servers><server><id>private-fixture</id><username>fixture-user</username>
                <password>fixture-password</password></server></servers><activeProfiles><activeProfile>fixture</activeProfile>
                </activeProfiles><profiles><profile><id>fixture</id><repositories><repository><id>private-fixture</id>
                <url>%s</url></repository></repositories></profile></profiles></settings>
                """.formatted(remote));
            configure(directory.resolve("empty-public-repository").toUri().toString(), Optional.of(settings));
            assertEquals(200, register(descriptor(CARRIER, digest(carrier), List.of())).getStatus());
            assertTrue(authenticated.get() > 0);
        } finally { server.stop(0); }
    }

    @Test
    void rejectsRepositoryUrlCredentialsWithoutEchoingThem() throws Exception {
        configure("https://fixture-user:fixture-secret@example.invalid/maven", Optional.empty());
        Response response = register(descriptor(CARRIER, digest(carrier), List.of()));
        assertEquals(400, response.getStatus());
        assertFalse(response.getEntity().toString().contains("fixture-secret"));
    }

    @Test
    void rejectsUnreadableExplicitSettingsRatherThanFallingBackToPublicResolution() throws Exception {
        configure(repository.toUri().toString(), Optional.of(directory.resolve("missing-settings.xml")));
        rejects(descriptor(CARRIER, digest(carrier), List.of()));
    }

    @Test
    void malformedRepositoryUriDoesNotEchoCredentialText() throws Exception {
        configure("https://fixture-user:fixture-secret invalid@example.invalid/maven", Optional.empty());
        Response response = register(descriptor(CARRIER, digest(carrier), List.of()));
        assertEquals(400, response.getStatus());
        assertFalse(response.getEntity().toString().contains("fixture-secret"));
    }

    private void rejects(Path descriptor) {
        assertEquals(400, register(descriptor).getStatus());
        assertTrue(admin.releaseRegistry.list("tenant", PIPELINE).await().indefinitely().isEmpty());
    }

    private Response register(Path descriptor) {
        return admin.register("tenant", PIPELINE, "Bearer test-token", new HostedReleaseRegisterRequest(descriptor.toString()))
            .await().indefinitely();
    }

    private Path descriptor(String uri, String digest, List<PipelineReleaseArtifactDescriptor> secondary) throws Exception {
        var artifacts = new java.util.ArrayList<PipelineReleaseArtifactDescriptor>();
        artifacts.add(artifact("restaurant", "application-archive", uri, digest));
        artifacts.addAll(secondary);
        var descriptor = new PipelineReleaseDescriptor(1, PIPELINE, CONTRACT, CONTRACT, "restaurant", artifacts);
        Path path = directory.resolve("pipeline-release.json");
        PipelineJson.mapper().writerWithDefaultPrettyPrinter().writeValue(path.toFile(), descriptor);
        return path;
    }

    private PipelineReleaseArtifactDescriptor artifact(String id, String kind, String uri, String digest) {
        return new PipelineReleaseArtifactDescriptor(id, kind, uri, digest,
            id.equals("restaurant") ? List.of("Validate") : List.of(), List.of("local", "rest", "grpc", "sqs"));
    }

    private void archive(String pipelineId, String payload) throws Exception {
        var contract = new PipelineContractDescriptor(1, pipelineId, CONTRACT, "contract", "COMPUTE", "REST",
            "monolith-svc", false, "monolith", List.of(new PipelineBundleStepDescriptor(0, "Validate", "service",
                "ONE_TO_ONE", String.class.getName(), "Output", "Runtime", "Client", null)), PipelineBundleCapabilities.defaults());
        try (var zip = new JarOutputStream(Files.newOutputStream(carrier))) {
            zip.putNextEntry(new JarEntry(PipelineContractDescriptor.RESOURCE_PATH));
            zip.write(PipelineJson.mapper().writeValueAsBytes(contract));
            zip.closeEntry();
            zip.putNextEntry(new JarEntry("META-INF/pipeline/secondary.json"));
            zip.write(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            zip.closeEntry();
        }
    }

    private String digest(Path file) throws Exception {
        return "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
    }

    private void zipEntry(java.util.zip.ZipOutputStream zip, String name, byte[] bytes) throws Exception {
        zip.putNextEntry(new java.util.zip.ZipEntry(name));
        zip.write(bytes);
        zip.closeEntry();
    }
}

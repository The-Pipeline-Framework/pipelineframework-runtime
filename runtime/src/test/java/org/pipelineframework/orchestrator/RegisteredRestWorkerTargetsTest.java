package org.pipelineframework.orchestrator;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import com.sun.net.httpserver.HttpServer;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.junit.jupiter.api.*;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.orchestrator.release.*;
import org.pipelineframework.orchestrator.worker.*;
import static org.junit.jupiter.api.Assertions.*;

/** Remote REST execution and classpath contract bytes are fixtures; selection and registries are native. */
public class RegisteredRestWorkerTargetsTest {
    private final List<HttpServer> servers = new ArrayList<>();
    private final AtomicInteger aCalls = new AtomicInteger();
    private final AtomicInteger bCalls = new AtomicInteger();
    private final AtomicReference<String> aDigest = new AtomicReference<>("sha256:A");
    private final AtomicReference<String> aArtifactId = new AtomicReference<>("app");
    private final AtomicReference<String> capabilityRelease = new AtomicReference<>("A");
    private final AtomicReference<Integer> executeStatus = new AtomicReference<>(200);
    private final java.util.concurrent.atomic.AtomicLong executionDelayMillis = new java.util.concurrent.atomic.AtomicLong();
    private final AtomicInteger redirected = new AtomicInteger();
    private InMemoryPipelineWorkerRegistry workers;
    private InMemoryPipelineReleaseRegistry releases;
    private RegisteredRestWorkerTargets targets;
    private PipelineTransitionWorkerSelector selector;
    private RestPipelineTransitionWorker rest;
    private PipelineOrchestratorConfig config;
    private String aEndpoint;
    private String bEndpoint;

    @BeforeEach public void setup() throws Exception {
        aEndpoint = server("A", aCalls);
        bEndpoint = server("B", bCalls);
        workers = new InMemoryPipelineWorkerRegistry();
        releases = new InMemoryPipelineReleaseRegistry();
        releases.register(release("A", false)).await().indefinitely();
        releases.register(release("B", false)).await().indefinitely();
        configure(Map.of());
        register("A", "sha256:A", aEndpoint, System.currentTimeMillis());
        register("B", "sha256:B", bEndpoint, System.currentTimeMillis());
    }

    @AfterEach public void close() {
        if (rest != null) rest.close();
        servers.forEach(server -> server.stop(0));
    }

    private String server(String release, AtomicInteger calls) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String timestamp = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER);
            String nonce = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.NONCE_HEADER);
            String signature = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.SIGNATURE_HEADER);
            String expected = TransitionWorkerSignature.sign("test-secret", exchange.getRequestMethod(),
                exchange.getRequestURI().getPath(), timestamp, nonce, body);
            if (!expected.equals(signature)) { exchange.sendResponseHeaders(401, -1); exchange.close(); return; }
            byte[] response;
            int status = 200;
            if (exchange.getRequestURI().getPath().endsWith("capabilities")) {
                response = PipelineJson.mapper().writeValueAsBytes(new PipelineWorkerCapability("1", "rest", "pipeline",
                    "contract", calls == aCalls ? capabilityRelease.get() : release, calls == aCalls ? aArtifactId.get() : "app",
                    calls == aCalls ? aDigest.get() : "sha256:" + release, List.of(TransitionPayloadEncoding.JSON), List.of("rest")));
            } else if (exchange.getRequestURI().getPath().endsWith("execute")) {
                calls.incrementAndGet();
                if (calls == aCalls) java.util.concurrent.locks.LockSupport.parkNanos(
                    java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(executionDelayMillis.get()));
                var command = PipelineJson.mapper().readValue(body, TransitionCommandEnvelope.class);
                assertEquals(release, command.releaseVersion());
                response = PipelineJson.mapper().writeValueAsBytes(TransitionResultEnvelope.completed(
                    new JsonTransitionPayloadCodec(), List.of(release)).toWireResult());
                status = executeStatus.get();
                if (status == 302) exchange.getResponseHeaders().add("Location", bEndpoint + "/redirected");
            } else { redirected.incrementAndGet(); response = new byte[0]; }
            exchange.sendResponseHeaders(status, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start(); servers.add(server);
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private void configure(Map<String, String> overrides) {
        var builder = new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class).withValidateUnknown(false)
            .withDefaultValue("pipeline.orchestrator.mode", "QUEUE_ASYNC")
            .withDefaultValue("pipeline.orchestrator.strict-startup", "false")
            .withDefaultValue("pipeline.orchestrator.worker.targeting-mode", "registered-rest")
            .withDefaultValue("pipeline.orchestrator.worker.allow-loopback-http", "true");
        for (String release : List.of("A", "B")) {
            String prefix = "pipeline.orchestrator.worker.targets." + release + ".";
            Map.of("tenant-id", "tenant", "pipeline-id", "pipeline", "contract-version", "contract",
                "release-version", release, "worker-id", release, "endpoint", release.equals("A") ? aEndpoint : bEndpoint,
                "shared-secret-ref", "fixture:secret", "artifact-id", "app", "artifact-digest", "sha256:" + release)
                .forEach((key,value) -> builder.withDefaultValue(prefix + key, value));
        }
        overrides.forEach(builder::withDefaultValue);
        config = builder.build().getConfigMapping(PipelineOrchestratorConfig.class);
        if (rest != null) rest.close();
        rest = new RestPipelineTransitionWorker(java.net.http.HttpClient.newHttpClient(), new PipelineInvocationRuntime());
        rest.orchestratorConfig = config;
        rest.secretResolver = reference -> "test-secret";
        targets = new RegisteredRestWorkerTargets();
        targets.config = config; targets.releases = releases; targets.workers = workers; targets.rest = rest;
        targets.contractLoader = new PipelineContractDescriptorLoader() {
            @Override public Optional<PipelineContractDescriptor> load() {
                return Optional.of(load(new java.io.ByteArrayInputStream(contractBytes())));
            }
        };
        selector = new PipelineTransitionWorkerSelector();
        selector.orchestratorConfig = config; selector.registeredTargets = targets;
        selector.validateConfiguredTargets();
    }

    private void register(String release, String digest, String endpoint, long now) {
        workers.register(new PipelineWorkerRegistration("tenant", "pipeline", "contract", release,
            release, "rest", endpoint, "app", digest), now).await().indefinitely();
    }

    private static PipelineReleaseRecord release(String version, boolean modular) {
        var contract = new PipelineContractDescriptor(1, "pipeline", "contract", "hash", "COMPUTE", "REST", "app",
            false, "monolith", List.of(new PipelineBundleStepDescriptor(0, "Validate", "service", "ONE_TO_ONE",
                "java.lang.String", "java.lang.String", "Runtime", "Client", null)), PipelineBundleCapabilities.defaults());
        var artifact = new PipelineReleaseArtifactDescriptor("app", "application-archive", "maven:example:app:zip:" + version,
            "sha256:" + version, modular ? List.of() : List.of("Validate"), List.of("local", "rest", "grpc", "sqs"));
        var descriptor = new PipelineReleaseDescriptor(1, "pipeline", "contract", version, "app", List.of(artifact));
        return new PipelineReleaseRecord("tenant", "pipeline", "contract", version, PipelineReleaseStatus.REGISTERED,
            descriptor, "app", artifact.digest(), "", 0, "", contract, 1, 1, 0);
    }

    private static TransitionCommandEnvelope command(String tenant, String contract, String release) {
        return new TransitionCommandEnvelope(tenant, "execution", "pipeline", contract, release, 0, -1, 0,
            ExecutionResultShape.SINGLE, 1, "execution:0:0", "trace", "java.lang.String", TransitionPayloadEncoding.JSON, "\"input\"");
    }

    private List<?> execute(String release) {
        PipelineTransitionWorker local = command -> { fail("Registered mode must not fall back to local"); return UniSupport.failed(); };
        return selector.select(local).executeTransition(command("tenant", "contract", release)).await().indefinitely()
            .decodeOutputItems(new JsonTransitionPayloadCodec());
    }

    public PipelineTransitionWorkerSelector selectorForTest() { return selector; }
    public PipelineOrchestratorConfig configForTest() { return config; }
    public PipelineReleaseRegistry releasesForTest() { return releases; }
    public RegisteredRestWorkerTargets targetsForTest() { return targets; }
    public int aCallsForTest() { return aCalls.get(); }
    public int bCallsForTest() { return bCalls.get(); }
    public void activateBForTest() { releases.activate("tenant", "pipeline", "B", System.currentTimeMillis()).await().indefinitely(); }
    public void forgetCompiledContractForTest() {
        targets.contractLoader = new PipelineContractDescriptorLoader() {
            @Override public Optional<PipelineContractDescriptor> load() { return Optional.empty(); }
        };
    }

    private byte[] contractBytes() {
        try { return PipelineJson.mapper().writeValueAsBytes(release("A", false).contract()); }
        catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
    }

    @Test void selectedWorkerRoutesFromEachCommandPinAcrossActivationAndReconstruction() {
        assertEquals(List.of("A"), execute("A"));
        releases.activate("tenant", "pipeline", "B", 100).await().indefinitely();
        assertEquals(List.of("A"), execute("A"));
        assertEquals(List.of("B"), execute("B"));
        configure(Map.of());
        assertEquals(List.of("A"), execute("A"));
        assertEquals(3, aCalls.get()); assertEquals(1, bCalls.get());
    }

    @TestFactory Stream<DynamicTest> unverifiedCapabilityNeverDispatches() {
        return Stream.of("", "sha256:wrong").map(value -> DynamicTest.dynamicTest("digest=" + value, () -> {
            aDigest.set(value);
            assertThrows(RuntimeException.class, () -> execute("A")); assertEquals(0, aCalls.get());
        }));
    }

    @TestFactory Stream<DynamicTest> wrongImmutableScopeNeverDispatches() {
        return Stream.of(command("other", "contract", "A"), command("tenant", "wrong", "A"), command("tenant", "contract", "missing"))
            .map(command -> DynamicTest.dynamicTest(command.tenantId()+command.contractVersion()+command.releaseVersion(), () -> {
                assertThrows(RuntimeException.class, () -> targets.executeTransition(command).await().indefinitely());
                assertEquals(0, aCalls.get()); assertEquals(0, bCalls.get());
            }));
    }

    @TestFactory Stream<DynamicTest> lifecycleArtifactIdentityMustBeKnownAndExact() {
        return Stream.of(List.of("app", ""), List.of("app", "sha256:wrong"), List.of("", "sha256:A"),
            List.of("different-app", "sha256:A")).map(identity -> DynamicTest.dynamicTest(identity.toString(), () -> {
                workers.register(new PipelineWorkerRegistration("tenant", "pipeline", "contract", "A", "A", "rest",
                    aEndpoint, identity.get(0), identity.get(1)), System.currentTimeMillis()).await().indefinitely();
                assertThrows(RuntimeException.class, () -> execute("A")); assertEquals(0, aCalls.get());
            }));
    }

    @TestFactory Stream<DynamicTest> capabilityArtifactIdMustBeKnownAndExact() {
        return Stream.of("", "different-app").map(value -> DynamicTest.dynamicTest("artifact=" + value, () -> {
            aArtifactId.set(value); assertThrows(RuntimeException.class, () -> execute("A"));
            assertEquals(0, aCalls.get());
        }));
    }

    @Test void replicasUseStableOrderAndOnlyFailOverBeforeDispatch() throws Exception {
        var replicaCalls = new AtomicInteger();
        String replicaEndpoint = server("A", replicaCalls);
        var replicaConfig = new java.util.HashMap<>(Map.of("pipeline.orchestrator.worker.targets.replica.tenant-id", "tenant",
            "pipeline.orchestrator.worker.targets.replica.pipeline-id", "pipeline",
            "pipeline.orchestrator.worker.targets.replica.contract-version", "contract",
            "pipeline.orchestrator.worker.targets.replica.release-version", "A",
            "pipeline.orchestrator.worker.targets.replica.worker-id", "A-replica",
            "pipeline.orchestrator.worker.targets.replica.endpoint", replicaEndpoint,
            "pipeline.orchestrator.worker.targets.replica.shared-secret-ref", "fixture:secret",
            "pipeline.orchestrator.worker.targets.replica.artifact-id", "app",
            "pipeline.orchestrator.worker.targets.replica.artifact-digest", "sha256:A"));
        configure(replicaConfig);
        workers.register(new PipelineWorkerRegistration("tenant", "pipeline", "contract", "A", "A-replica", "rest",
            replicaEndpoint, "app", "sha256:A"), System.currentTimeMillis()).await().indefinitely();
        execute("A"); assertEquals(1, aCalls.get()); assertEquals(0, replicaCalls.get());
        executeStatus.set(302);
        assertThrows(RuntimeException.class, () -> execute("A"));
        assertEquals(2, aCalls.get()); assertEquals(0, replicaCalls.get());
        executeStatus.set(200); aDigest.set("sha256:wrong");
        execute("A"); assertEquals(2, aCalls.get()); assertEquals(1, replicaCalls.get());
        aDigest.set("sha256:A");
        replicaConfig.put("pipeline.orchestrator.worker.rest.request-timeout", "PT0.1S");
        configure(replicaConfig); executionDelayMillis.set(500);
        assertThrows(RemoteTransitionOutcomeUnknownException.class, () -> execute("A"));
        assertEquals(3, aCalls.get()); assertEquals(1, replicaCalls.get());
    }

    @Test void unknownGeneratedCoordinatorContractRejectsAvailabilityAndExecution() {
        forgetCompiledContractForTest();
        assertFalse(targets.check(new PipelineWorkerAvailabilityRequest("tenant", "pipeline", "contract", "A", "app", "sha256:A"))
            .await().indefinitely().available());
        assertThrows(RuntimeException.class, () -> execute("A")); assertEquals(0, aCalls.get());
    }

    @Test void differingGeneratedContractLayoutAndDistributedPlacementAreRejected() {
        targets.contractLoader = new PipelineContractDescriptorLoader() {
            @Override public Optional<PipelineContractDescriptor> load() {
                return Optional.of(new PipelineContractDescriptor(1, "pipeline", "contract", "hash", "COMPUTE", "REST", "app",
                    false, "monolith", List.of(), PipelineBundleCapabilities.defaults()));
            }
        };
        assertThrows(RuntimeException.class, () -> execute("A"));
        assertEquals(0, aCalls.get());
        configure(Map.of());
        var unsupported = release("modular", true);
        releases.register(unsupported).await().indefinitely();
        assertFalse(targets.check(new PipelineWorkerAvailabilityRequest("tenant", "pipeline", "contract", "modular", "app", "sha256:modular"))
            .await().indefinitely().available());
        assertThrows(RuntimeException.class, () -> execute("modular"));
    }

    @Test void staleDrainingAndRedirectedRegistrationNeverDispatch() {
        register("A", "sha256:A", aEndpoint, 1);
        assertThrows(RuntimeException.class, () -> execute("A"));
        register("A", "sha256:A", bEndpoint, System.currentTimeMillis());
        assertThrows(RuntimeException.class, () -> execute("A"));
        register("A", "sha256:A", aEndpoint, System.currentTimeMillis());
        workers.markDraining("tenant", "pipeline", "A", System.currentTimeMillis(), Duration.ofMinutes(2)).await().indefinitely();
        register("A", "sha256:A", aEndpoint, System.currentTimeMillis());
        assertThrows(RuntimeException.class, () -> execute("A")); assertEquals(0, aCalls.get());
    }

    @Test void registrationCannotChangeBoundIdentityOrEndpoint() {
        assertThrows(IllegalArgumentException.class, () -> targets.validateRegistration(new PipelineWorkerRegistration(
            "tenant", "pipeline", "contract", "A", "A", "rest", bEndpoint, "app", "sha256:A")));
        targets.validateRegistration(new PipelineWorkerRegistration("tenant", "pipeline", "contract", "A", "A", "rest",
            aEndpoint + "/", "app", "sha256:A"));
    }

    @Test void redirectIsNotFollowedAndExecutionFailureDoesNotFailOver() {
        executeStatus.set(302);
        assertThrows(RuntimeException.class, () -> execute("A"));
        assertEquals(1, aCalls.get()); assertEquals(0, bCalls.get()); assertEquals(0, redirected.get());
    }

    @Test void availabilityUsesSamePinnedTargetAndRejectsUnknownIdentity() {
        var request = new PipelineWorkerAvailabilityRequest("tenant", "pipeline", "contract", "A", "app", "sha256:A");
        assertTrue(targets.check(request).await().indefinitely().available());
        capabilityRelease.set("B");
        assertFalse(targets.check(request).await().indefinitely().available());
        assertThrows(RuntimeException.class, () -> execute("A"));
    }

    @TestFactory Stream<DynamicTest> unsafeOrUnsupportedConfigurationRejected() {
        return Stream.of(Map.of("pipeline.orchestrator.worker.targets.A.endpoint", "http://example.com"),
            Map.of("pipeline.orchestrator.worker.targets.A.endpoint", aEndpoint + "?redirect=1"),
            Map.of("pipeline.orchestrator.worker.targets.A.protocol", "sqs"),
            Map.of("pipeline.orchestrator.worker.targets.B.worker-id", "A"))
            .map(values -> DynamicTest.dynamicTest(values.toString(), () -> assertThrows(IllegalArgumentException.class, () -> configure(values))));
    }

    private static class UniSupport {
        static io.smallrye.mutiny.Uni<TransitionResultEnvelope> failed() {
            return io.smallrye.mutiny.Uni.createFrom().failure(new IllegalStateException("Unexpected local worker"));
        }
    }
}

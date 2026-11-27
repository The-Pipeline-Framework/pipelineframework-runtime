package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.polyglot.PolyglotConnectorConnection;
import org.pipelineframework.orchestrator.CanonicalFunctionHostManyToOneStep;
import org.pipelineframework.orchestrator.CanonicalFunctionHostOneToManyStep;
import org.pipelineframework.orchestrator.CanonicalFunctionHostStep;
import org.pipelineframework.orchestrator.DynamoForeignDispatchIntentStore;
import org.pipelineframework.orchestrator.ForeignDispatchIntentStore;
import org.pipelineframework.orchestrator.InMemoryForeignDispatchIntentStore;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PolyglotRuntimeConfig;
import org.pipelineframework.orchestrator.PolyglotRuntimeProducers;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

class PolyglotStepAutoWirerTest {

    @Test
    void enforcesHttpsByDefaultAndBuildsAllThreeCardinalitiesWithPerStepUris() {
        PipelineContractDescriptor contract = sampleStreamingContract();
        PipelineReleaseIdentityResolver resolver = resolverFor(contract);

        // Non-HTTPS rejected when allow-loopback-http=false
        PolyglotRuntimeConfig insecure = config(
            true, Optional.of("http://127.0.0.1:8080"), Map.of(),
            Optional.of("secret"), false, "memory", Optional.empty(), Map.of());
        assertThrows(IllegalArgumentException.class, () -> new PolyglotStepAutoWirer(
            insecure, resolver, new InMemoryForeignDispatchIntentStore(), Optional.empty(), Optional.empty()));

        // Missing shared-secret rejected fail-closed
        PolyglotRuntimeConfig noSecret = config(
            true, Optional.of("https://node.example.com"), Map.of(),
            Optional.empty(), false, "memory", Optional.empty(), Map.of());
        IllegalStateException missingSecret = assertThrows(IllegalStateException.class, () -> new PolyglotStepAutoWirer(
            noSecret, resolver, new InMemoryForeignDispatchIntentStore(), Optional.empty(), Optional.empty()));
        assertTrue(missingSecret.getMessage().contains("shared-secret"));

        // HTTPS + per-step override builds 1:1 -> 1:N -> N:1 cleanly
        PolyglotRuntimeConfig valid = config(
            true,
            Optional.of("https://node.example.com"),
            Map.of("split-lines", "https://python.example.com"),
            Optional.of("prod-hmac-secret"),
            false,
            "memory",
            Optional.empty(),
            Map.of());
        PolyglotStepAutoWirer wirer = new PolyglotStepAutoWirer(
            valid, resolver, new InMemoryForeignDispatchIntentStore(), Optional.empty(), Optional.empty());
        assertTrue(wirer.isActive());

        List<Object> steps = wirer.loadOrderedSteps();
        assertEquals(3, steps.size());
        assertInstanceOf(CanonicalFunctionHostStep.class, steps.get(0));
        assertInstanceOf(CanonicalFunctionHostOneToManyStep.class, steps.get(1));
        assertInstanceOf(CanonicalFunctionHostManyToOneStep.class, steps.get(2));
    }

    @Test
    @SuppressWarnings("unchecked")
    void producersWireIntentStoresAndConnectionResolverWithSecretMergingAndHttpsGuard() {
        PolyglotRuntimeProducers producers = new PolyglotRuntimeProducers();
        Instance<DynamoDbClient> emptyDynamo = mock(Instance.class);
        when(emptyDynamo.isUnsatisfied()).thenReturn(true);

        PolyglotRuntimeConfig memCfg = config(true, Optional.of("https://fn.example.com"), Map.of(),
            Optional.of("sec"), false, "memory", Optional.of("https://conn.example.com"),
            Map.of("fx-key", "fx-secret"));
        assertInstanceOf(InMemoryForeignDispatchIntentStore.class,
            producers.createIntentStore(memCfg, emptyDynamo));

        PolyglotRuntimeConfig noopCfg = config(true, Optional.of("https://fn.example.com"), Map.of(),
            Optional.of("sec"), false, "noop", Optional.empty(), Map.of());
        assertEquals(ForeignDispatchIntentStore.noop().getClass(),
            producers.createIntentStore(noopCfg, emptyDynamo).getClass());

        PolyglotRuntimeConfig dynamoCfg = config(true, Optional.of("https://fn.example.com"), Map.of(),
            Optional.of("sec"), false, "dynamo", Optional.empty(), Map.of());
        assertThrows(IllegalStateException.class, () -> producers.createIntentStore(dynamoCfg, emptyDynamo));

        Instance<DynamoDbClient> presentDynamo = mock(Instance.class);
        when(presentDynamo.isUnsatisfied()).thenReturn(false);
        when(presentDynamo.isAmbiguous()).thenReturn(false);
        when(presentDynamo.get()).thenReturn(mock(DynamoDbClient.class));
        assertInstanceOf(DynamoForeignDispatchIntentStore.class,
            producers.createIntentStore(dynamoCfg, presentDynamo));

        // ConnectionResolver delivers scoped secrets over HTTPS and rejects plain HTTP when allowLoopbackHttp=false
        var resolver = producers.createConnectionResolver(memCfg);
        var execCtx = ConnectorExecutionContext.empty();
        PolyglotConnectorConnection resolved = resolver.resolve(new ConnectionResolutionRequest<>(
            new ConnectionRef("polyglot-connector-host"), PolyglotConnectorConnection.class, execCtx))
            .toCompletableFuture().join();
        assertEquals("https://conn.example.com", resolved.hostUri().toString());
        assertEquals("fx-secret", resolved.authorizedSecrets().get("fx-key"));
        assertFalse(resolved.allowLoopbackHttp());

        PolyglotRuntimeConfig insecureConn = config(true, Optional.of("https://fn.example.com"), Map.of(),
            Optional.of("sec"), false, "memory", Optional.of("http://127.0.0.1:9000"), Map.of());
        var insecureResolver = producers.createConnectionResolver(insecureConn);
        CompletionException failure = assertThrows(CompletionException.class, () -> insecureResolver.resolve(
            new ConnectionResolutionRequest<>(
                new ConnectionRef("polyglot-connector-host"), PolyglotConnectorConnection.class, execCtx))
            .toCompletableFuture().join());
        assertInstanceOf(ConnectionResolutionException.class, failure.getCause());
    }

    private static PipelineReleaseIdentityResolver resolverFor(PipelineContractDescriptor contract) {
        return new PipelineReleaseIdentityResolver() {
            @Override
            public Optional<PipelineContractDescriptor> compiledContract() {
                return Optional.of(contract);
            }
        };
    }

    private static PipelineContractDescriptor sampleStreamingContract() {
        Map<String, Map<String, Object>> types = new LinkedHashMap<>();
        types.put("Order", recordDef("Order", List.of(field("id", "string"))));
        types.put("Line", recordDef("Line", List.of(field("sku", "string"))));
        types.put("Summary", recordDef("Summary", List.of(field("count", "int32"))));
        return new PipelineContractDescriptor(
            3, "polyglot-unit", "v1", "hash1", "COMPUTE", "LOCAL", null, false, "MONOLITH",
            List.of(
                new PipelineBundleStepDescriptor(0, "Validate Order", "internal", "ONE_TO_ONE", "Order", "Order", null, null, Map.of()),
                new PipelineBundleStepDescriptor(1, "Split Lines", "internal", "ONE_TO_MANY", "Order", "Line", null, null, Map.of()),
                new PipelineBundleStepDescriptor(2, "Summarize", "internal", "MANY_TO_ONE", "Line", "Summary", null, null, Map.of())),
            PipelineBundleCapabilities.defaults(),
            types,
            "catalog-fp-1",
            null,
            List.of(),
            List.of(),
            List.of(),
            Map.of("schemaVersion", 1, "inputCanonicalTypeId", "Order", "outputCanonicalTypeId", "Summary"));
    }

    private static Map<String, Object> recordDef(String id, List<Map<String, Object>> fields) {
        Map<String, Object> def = new LinkedHashMap<>();
        def.put("kind", "record");
        def.put("id", id);
        def.put("fields", fields);
        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("definitionFingerprint", "fp-" + id);
        wrapper.put("definition", def);
        return wrapper;
    }

    private static Map<String, Object> field(String name, String scalarId) {
        Map<String, Object> type = new LinkedHashMap<>();
        type.put("kind", "scalar");
        type.put("id", scalarId);
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("name", name);
        f.put("presence", "REQUIRED");
        f.put("nullability", "NON_NULL");
        f.put("type", type);
        return f;
    }

    private static PolyglotRuntimeConfig config(
        boolean enabled,
        Optional<String> functionHostUri,
        Map<String, String> stepUris,
        Optional<String> sharedSecret,
        boolean allowLoopbackHttp,
        String intentStore,
        Optional<String> connectorHostUri,
        Map<String, String> connectorSecrets
    ) {
        return new PolyglotRuntimeConfig() {
            @Override public boolean enabled() { return enabled; }
            @Override public Optional<String> functionHostUri() { return functionHostUri; }
            @Override public Map<String, String> stepUris() { return stepUris; }
            @Override public Optional<String> sharedSecret() { return sharedSecret; }
            @Override public boolean allowLoopbackHttp() { return allowLoopbackHttp; }
            @Override public Duration requestTimeout() { return Duration.ofSeconds(5); }
            @Override public Duration streamIdleTimeout() { return Duration.ofSeconds(5); }
            @Override public int maxResponseBytes() { return 4 * 1024 * 1024; }
            @Override public String dispatchIntentStore() { return intentStore; }
            @Override public String dispatchIntentTable() { return "tpf_foreign_dispatch_intent"; }
            @Override public ConnectorHostConfig connector() {
                return new ConnectorHostConfig() {
                    @Override public Optional<String> hostUri() { return connectorHostUri; }
                    @Override public Optional<String> sharedSecret() { return Optional.empty(); }
                    @Override public Map<String, String> secrets() { return connectorSecrets; }
                    @Override public Map<String, NamedConnectionConfig> connections() { return Map.of(); }
                };
            }
        };
    }
}

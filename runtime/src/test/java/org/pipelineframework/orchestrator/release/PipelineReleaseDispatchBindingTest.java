package org.pipelineframework.orchestrator.release;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PipelineReleaseDispatchBindingTest {
    private static final PipelineReleaseDispatchBinding TARGET = new PipelineReleaseDispatchBinding(
        "rest", "https://worker.example.test", "env:TPF_WORKER_SECRET", "worker-a1");

    @Test
    void originalConstructorAndRetainedJsonKeepAbsentDispatchBinding() throws Exception {
        var old = new PipelineReleaseEntryBinding("tenant", "pipeline", "local-contract", "release-a1", "entry-a1",
            "archive", "sha256:archive", "process", "runtime-a1", "sha256:code", "deployment-a1");
        String wire = PipelineJson.mapper().writeValueAsString(old);
        assertFalse(wire.contains("dispatchBinding"));
        assertEquals(old, PipelineJson.mapper().readValue(wire, PipelineReleaseEntryBinding.class));
        assertTrue(old.dispatchBinding().isEmpty());
    }

    @Test
    void dispatchBindingRoundTripsAsReferenceIdentityWithoutResolvingCredentials() throws Exception {
        var expected = binding(TARGET);
        String wire = PipelineJson.mapper().writeValueAsString(expected);
        assertEquals(expected, PipelineJson.mapper().readValue(wire, PipelineReleaseEntryBinding.class));
        assertTrue(wire.contains("env:TPF_WORKER_SECRET"));
        assertEquals(expected.key(), binding(new PipelineReleaseDispatchBinding(
            "rest", "https://other.example.test", "env:OTHER_SECRET", "other-worker")).key());
    }

    @Test
    void memoryRegistryRetainsEveryDispatchIdentityFieldImmutably() {
        exercise(new InMemoryPipelineReleaseRegistry());
    }

    @Test
    void durableRegistryRetainsDispatchAcrossFreshInstancesAndRejectsFieldDrift() {
        var client = mock(DynamoDbClient.class);
        var rows = new ConcurrentHashMap<Map<String, AttributeValue>, Map<String, AttributeValue>>();
        when(client.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            GetItemRequest request = call.getArgument(0);
            assertTrue(request.consistentRead());
            return GetItemResponse.builder().item(rows.getOrDefault(request.key(), Map.of())).build();
        });
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(List.of()).build());
        when(client.putItem(any(PutItemRequest.class))).thenAnswer(call -> {
            PutItemRequest request = call.getArgument(0);
            assertTrue(request.conditionExpression().contains("attribute_not_exists"));
            Map<String, AttributeValue> key = Map.of("registry_key", request.item().get("registry_key"),
                "registry_sort", request.item().get("registry_sort"));
            if (rows.putIfAbsent(key, request.item()) != null) {
                throw ConditionalCheckFailedException.builder().message("exists").build();
            }
            return PutItemResponse.builder().build();
        });
        var config = mock(PipelineOrchestratorConfig.class);
        var storage = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(storage);
        when(storage.releaseTable()).thenReturn("releases");
        exercise(new DynamoPipelineReleaseRegistry(client, config));
        var recovered = new DynamoPipelineReleaseRegistry(client, config);
        assertEquals(binding(TARGET), recovered.getEntryBinding("tenant", "pipeline", "local-contract", "release-a1", "entry-a1")
            .await().indefinitely().orElseThrow());
    }

    @Test
    void dispatchIdentityRejectsBlankOrNoncanonicalFields() {
        for (String invalid : List.of("", " ", " worker", "worker\nother")) {
            assertThrows(IllegalArgumentException.class, () -> new PipelineReleaseDispatchBinding(invalid, TARGET.endpoint(), TARGET.credentialRef(), TARGET.workerId()));
            assertThrows(IllegalArgumentException.class, () -> new PipelineReleaseDispatchBinding(TARGET.protocol(), invalid, TARGET.credentialRef(), TARGET.workerId()));
            assertThrows(IllegalArgumentException.class, () -> new PipelineReleaseDispatchBinding(TARGET.protocol(), TARGET.endpoint(), invalid, TARGET.workerId()));
            assertThrows(IllegalArgumentException.class, () -> new PipelineReleaseDispatchBinding(TARGET.protocol(), TARGET.endpoint(), TARGET.credentialRef(), invalid));
        }
    }

    private static void exercise(PipelineReleaseRegistry registry) {
        registry.register(release()).await().indefinitely();
        var original = binding(TARGET);
        assertEquals(original, registry.registerEntryBinding(original).await().indefinitely());
        assertEquals(original, registry.registerEntryBinding(original).await().indefinitely());
        for (var changed : List.of(
            new PipelineReleaseDispatchBinding("grpc", TARGET.endpoint(), TARGET.credentialRef(), TARGET.workerId()),
            new PipelineReleaseDispatchBinding(TARGET.protocol(), "https://replacement.example.test", TARGET.credentialRef(), TARGET.workerId()),
            new PipelineReleaseDispatchBinding(TARGET.protocol(), TARGET.endpoint(), "env:REPLACEMENT_SECRET", TARGET.workerId()),
            new PipelineReleaseDispatchBinding(TARGET.protocol(), TARGET.endpoint(), TARGET.credentialRef(), "replacement-worker"))) {
            assertThrows(IllegalStateException.class, () -> registry.registerEntryBinding(binding(changed)).await().indefinitely());
        }
        assertEquals(original, registry.getEntryBinding("tenant", "pipeline", "local-contract", "release-a1", "entry-a1")
            .await().indefinitely().orElseThrow());
        assertTrue(registry.getEntryBinding("other", "pipeline", "local-contract", "release-a1", "entry-a1")
            .await().indefinitely().isEmpty());
    }

    private static PipelineReleaseEntryBinding binding(PipelineReleaseDispatchBinding dispatch) {
        return new PipelineReleaseEntryBinding("tenant", "pipeline", "local-contract", "release-a1", "entry-a1", "archive",
            "sha256:archive", "process", "runtime-a1", "sha256:code", "deployment-a1", Optional.of(dispatch));
    }

    private static PipelineReleaseRecord release() {
        var contract = PipelineContractDescriptor.localFallback();
        var artifact = new PipelineReleaseArtifactDescriptor("archive", "application-archive", "file:/releases/application-a1.jar",
            "sha256:archive", List.of(), List.of());
        var deployment = new PipelineReleaseDeployment("process", List.of(new PipelineReleaseDeployment.Entry(
            "entry-a1", "archive", contract.module(), ".", "example.Application", "rest", List.of())), List.of(),
            new PipelineReleaseDeployment.Check("entry-a1", Map.of("payloadTypeId", "java.lang.String", "payloadEncoding", "json", "payload", "\"input\""),
                Map.of("payloadTypeId", "java.lang.String", "payloadEncoding", "json", "payload", "\"output\""), 1000));
        var descriptor = new PipelineReleaseDescriptor(2, "pipeline", contract.contractVersion(), "release-a1", "archive", List.of(artifact), deployment);
        return new PipelineReleaseRecord("tenant", "pipeline", contract.contractVersion(), "release-a1", PipelineReleaseStatus.REGISTERED,
            descriptor, "archive", artifact.digest(), artifact.uri(), 0, "", contract, 1, 1, 0);
    }
}

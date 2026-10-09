package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

@Testcontainers(disabledWithoutDocker = true)
class DynamoActivationSnapshotIT {
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8")).withServices("dynamodb");
    private DynamoDbClient client;
    private PipelineOrchestratorConfig config;
    private String table;

    @BeforeEach
    void createTable() {
        client = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
            .region(Region.of(LOCALSTACK.getRegion())).credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))).build();
        table = "activation-snapshot-" + UUID.randomUUID();
        client.createTable(CreateTableRequest.builder().tableName(table)
            .attributeDefinitions(AttributeDefinition.builder().attributeName("registry_key").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("registry_sort").attributeType(ScalarAttributeType.S).build())
            .keySchema(KeySchemaElement.builder().attributeName("registry_key").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("registry_sort").keyType(KeyType.RANGE).build())
            .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build()).build());
        client.waiter().waitUntilTableExists(request -> request.tableName(table));
        config = mock(PipelineOrchestratorConfig.class);
        var dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.releaseTable()).thenReturn(table);
    }

    @AfterEach
    void closeClient() { client.close(); }

    @Test
    void legacyActivationAcceptsEquivalentStoredJsonWithoutRewritingRelease() throws Exception {
        fresh().register(release()).await().indefinitely();
        var row = representedRow();
        client.putItem(request -> request.tableName(table).item(row));
        fresh().activate("tenant", "pipeline", "release", 2000).await().indefinitely().orElseThrow();
        assertEquals(row, registeredRow());
        assertEquals("release", fresh().active("tenant", "pipeline").await().indefinitely().orElseThrow().releaseVersion());
        assertEquals(3, rows().size());
    }

    @Test
    void strictActivationAcceptsEquivalentMapOrderAndHistoricalReplayPreservesAllRows() throws Exception {
        var release = release();
        fresh().register(release).await().indefinitely();
        var row = representedRow();
        client.putItem(request -> request.tableName(table).item(row));
        var receipt = fresh().activateOnce(new ActivationOperationCommand("operation", release, 2000)).await().indefinitely();
        assertEquals(row, registeredRow());
        assertEquals(PipelineReleaseEvidence.from(release), receipt.immutableReleaseIdentity());
        var complete = rows();
        assertEquals(receipt, fresh().activateOnce(new ActivationOperationCommand("operation", release, 3000)).await().indefinitely());
        assertEquals(receipt, fresh().getActivationOperation("tenant", "pipeline", "operation").await().indefinitely().orElseThrow());
        assertEquals(complete, rows());
        assertEquals(4, complete.size());
    }

    @Test
    void legacyUriOnlySnapshotRemainsSupportedAndUnchanged() {
        fresh().register(release()).await().indefinitely();
        var row = new HashMap<>(registeredRow());
        row.put("primary_artifact_path", row.remove("primary_artifact_uri"));
        client.putItem(request -> request.tableName(table).item(row));
        fresh().activate("tenant", "pipeline", "release", 2000).await().indefinitely().orElseThrow();
        assertEquals(row, registeredRow());
        assertEquals(3, rows().size());
    }

    @Test
    void everyObservedImmutableFieldChangeAfterReadFailsBothWritersWithoutActivationRows() {
        fresh().register(release()).await().indefinitely();
        var original = registeredRow();
        var fields = List.of("descriptor_json", "contract_json", "primary_artifact_id", "primary_artifact_digest",
            "primary_artifact_uri", "primary_artifact_size_bytes", "primary_artifact_checksum", "contract_version",
            "release_version", "tenant_id", "pipeline_id", "record_type", "registry_key", "registry_sort");
        for (boolean strict : List.of(false, true)) {
            for (String field : fields) {
                var mutated = new HashMap<>(original);
                var previous = original.get(field);
                mutated.put(field, previous.n() != null ? AttributeValue.builder().n("2").build()
                    : av(previous.s() + "-changed"));
                var registry = intercepted(request -> {
                    client.deleteItem(delete -> delete.tableName(table).key(key(original)));
                    client.putItem(put -> put.tableName(table).item(mutated));
                });
                assertThrows(TransactionCanceledException.class, () -> activate(registry, strict), field + " strict=" + strict);
                assertEquals(Set.of(mutated), rows(), field + " strict=" + strict);
                assertTrue(fresh().getActivationOperation("tenant", "pipeline", "operation").await().indefinitely().isEmpty());
                client.deleteItem(delete -> delete.tableName(table).key(key(mutated)));
                client.putItem(put -> put.tableName(table).item(original));
            }
        }
    }

    @Test
    void uriAttributeAbsenceAndShadowedLegacyPresenceAreFenced() {
        fresh().register(release()).await().indefinitely();
        var original = registeredRow();
        for (boolean strict : List.of(false, true)) {
            var added = new HashMap<>(original);
            added.put("primary_artifact_path", av("file:/shadowed.jar"));
            assertThrows(TransactionCanceledException.class, () -> activate(intercepted(request ->
                client.putItem(put -> put.tableName(table).item(added))), strict));
            assertEquals(Set.of(added), rows());
            client.putItem(put -> put.tableName(table).item(original));
            var legacy = new HashMap<>(original);
            legacy.put("primary_artifact_path", legacy.remove("primary_artifact_uri"));
            client.putItem(put -> put.tableName(table).item(legacy));
            assertThrows(TransactionCanceledException.class, () -> activate(intercepted(request ->
                client.putItem(put -> put.tableName(table).item(original))), strict));
            assertEquals(Set.of(original), rows());
        }
    }

    @Test
    void changedRawSnapshotCannotReplaceSelectedMetadataOrScopeBeforeTransaction() {
        fresh().register(release()).await().indefinitely();
        var original = registeredRow();
        for (boolean strict : List.of(false, true)) {
            for (String field : List.of("primary_artifact_digest", "contract_version", "release_version",
                "tenant_id", "pipeline_id", "record_type")) {
                var mutated = new HashMap<>(original);
                mutated.put(field, av(original.get(field).s() + "-changed"));
                var reads = new java.util.concurrent.atomic.AtomicInteger();
                var transactions = new java.util.concurrent.atomic.AtomicInteger();
                var boundary = (DynamoDbClient) Proxy.newProxyInstance(DynamoDbClient.class.getClassLoader(),
                    new Class<?>[]{DynamoDbClient.class}, (proxy, method, args) -> {
                        if (method.getName().equals("getItem") && args[0] instanceof GetItemRequest request
                            && "release:release".equals(request.key().get("registry_sort").s())
                            && reads.incrementAndGet() == 2) {
                            assertTrue(request.consistentRead());
                            client.putItem(put -> put.tableName(table).item(mutated));
                        }
                        if (method.getName().equals("transactWriteItems")) transactions.incrementAndGet();
                        try { return method.invoke(client, args); }
                        catch (InvocationTargetException error) { throw error.getCause(); }
                    });
                assertThrows(IllegalStateException.class,
                    () -> activate(new DynamoPipelineReleaseRegistry(boundary, config), strict), field + " strict=" + strict);
                assertEquals(0, transactions.get(), field + " strict=" + strict);
                assertEquals(Set.of(mutated), rows(), field + " strict=" + strict);
                client.putItem(put -> put.tableName(table).item(original));
            }
        }
    }

    private void activate(DynamoPipelineReleaseRegistry registry, boolean strict) {
        if (strict) registry.activateOnce(new ActivationOperationCommand("operation", release(), 2000)).await().indefinitely();
        else registry.activate("tenant", "pipeline", "release", 2000).await().indefinitely();
    }

    private Map<String, AttributeValue> representedRow() throws Exception {
        var row = new HashMap<>(registeredRow());
        var mapper = PipelineJson.mapper();
        var original = mapper.readTree(row.get("contract_json").s());
        var changed = original.deepCopy();
        var types = mapper.createObjectNode();
        var names = new java.util.ArrayList<String>();
        original.path("canonicalTypes").fieldNames().forEachRemaining(names::add);
        java.util.Collections.reverse(names);
        names.forEach(name -> types.set(name, original.path("canonicalTypes").get(name)));
        ((com.fasterxml.jackson.databind.node.ObjectNode) changed).set("canonicalTypes", types);
        assertEquals(original, changed);
        row.put("contract_json", av(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(changed)));
        var descriptor = mapper.readTree(row.get("descriptor_json").s());
        row.put("descriptor_json", av(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(descriptor)));
        return Map.copyOf(row);
    }

    private DynamoPipelineReleaseRegistry intercepted(Consumer<TransactWriteItemsRequest> before) {
        var boundary = (DynamoDbClient) Proxy.newProxyInstance(DynamoDbClient.class.getClassLoader(), new Class<?>[]{DynamoDbClient.class},
            (proxy, method, args) -> {
                if (method.getName().equals("transactWriteItems") && args[0] instanceof TransactWriteItemsRequest request) before.accept(request);
                try { return method.invoke(client, args); } catch (InvocationTargetException error) { throw error.getCause(); }
            });
        return new DynamoPipelineReleaseRegistry(boundary, config);
    }
    private DynamoPipelineReleaseRegistry fresh() { return new DynamoPipelineReleaseRegistry(client, config); }
    private Map<String, AttributeValue> registeredRow() {
        return client.getItem(request -> request.tableName(table).key(Map.of("registry_key", av("tenant#pipeline"),
            "registry_sort", av("release:release"))).consistentRead(true)).item();
    }
    private Set<Map<String, AttributeValue>> rows() { return Set.copyOf(client.scan(request -> request.tableName(table).consistentRead(true)).items()); }
    private static Map<String, AttributeValue> key(Map<String, AttributeValue> row) {
        return Map.of("registry_key", row.get("registry_key"), "registry_sort", row.get("registry_sort"));
    }
    private static AttributeValue av(String value) { return AttributeValue.builder().s(value).build(); }
    private static PipelineReleaseRecord release() {
        var descriptor = new PipelineReleaseDescriptor(1, "pipeline", "contract", "release", "artifact", List.of());
        var contract = new PipelineContractDescriptor(3, "pipeline", "contract", "hash", "quarkus", "grpc", "runtime", false,
            "modular", List.of(), PipelineBundleCapabilities.defaults(),
            Map.of("Input", Map.of("type", "record"), "Output", Map.of("type", "record")), "catalog");
        return new PipelineReleaseRecord("tenant", "pipeline", "contract", "release", PipelineReleaseStatus.REGISTERED,
            descriptor, "artifact", "sha256:artifact", "file:/artifact.jar", 1, "artifact", contract, 1000, 1000, 0);
    }
}

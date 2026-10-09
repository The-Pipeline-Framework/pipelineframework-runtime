package org.pipelineframework.orchestrator.worker;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
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
class DynamoWorkerPaginationIT {
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
        .withServices("dynamodb");

    @Test
    void laterHeartbeatDrainAndOtherWorkerMustSurviveRealQueryPages() {
        try (var client = client()) {
            var config = config(client);
            var writer = new DynamoPipelineWorkerRegistry(client, config);
            writer.register(registration("tenant", "pipeline", "z-worker"), 1000).await().indefinitely();
            writer.register(registration("tenant", "pipeline", "a-worker"), 1000).await().indefinitely();
            writer.heartbeat("tenant", "pipeline", "a-worker", 2000, Duration.ofSeconds(10)).await().indefinitely();
            writer.markDraining("tenant", "pipeline", "a-worker", 3000, Duration.ofSeconds(10)).await().indefinitely();
            writer.register(registration("other", "pipeline", "wrong-tenant"), 1000).await().indefinitely();
            writer.register(registration("tenant", "other", "wrong-pipeline"), 1000).await().indefinitely();
            var continuedPages = new AtomicInteger();
            var queries = new AtomicInteger();
            var limited = (DynamoDbClient) Proxy.newProxyInstance(DynamoDbClient.class.getClassLoader(), new Class<?>[] {DynamoDbClient.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("query") && args[0] instanceof QueryRequest request) {
                        queries.incrementAndGet();
                        var response = client.query(request.toBuilder().limit(1).build());
                        if (!response.lastEvaluatedKey().isEmpty()) continuedPages.incrementAndGet();
                        return response;
                    }
                    try { return method.invoke(client, args); }
                    catch (InvocationTargetException error) { throw error.getCause(); }
                });
            var actual = new DynamoPipelineWorkerRegistry(limited, config).list("tenant", "pipeline", 4000, Duration.ofSeconds(10))
                .await().indefinitely();
            assertTrue(continuedPages.get() > 0, "real provider must supply continuation evidence");
            var expected = writer.list("tenant", "pipeline", 4000, Duration.ofSeconds(10)).await().indefinitely();
            assertAll(
                () -> assertEquals(List.of("a-worker", "z-worker"), actual.stream().map(PipelineWorkerRecord::workerId).toList()),
                () -> assertEquals(expected, actual),
                () -> assertEquals(PipelineWorkerState.DRAINING, actual.getFirst().state()),
                () -> assertEquals(2000L, actual.getFirst().lastHeartbeatAtEpochMs()),
                () -> assertEquals(3000L, actual.getFirst().drainingSinceEpochMs()),
                () -> assertTrue(queries.get() >= 4, "all event pages must be consumed"));
            var workerSpecific = new DynamoPipelineWorkerRegistry(limited, config)
                .heartbeat("tenant", "pipeline", "a-worker", 5000, Duration.ofSeconds(10)).await().indefinitely().orElseThrow();
            assertAll(
                () -> assertEquals("a-worker", workerSpecific.workerId()),
                () -> assertEquals(PipelineWorkerState.DRAINING, workerSpecific.state()),
                () -> assertEquals(5000L, workerSpecific.lastHeartbeatAtEpochMs()),
                () -> assertEquals(3000L, workerSpecific.drainingSinceEpochMs()));
            assertEquals(writer.list("tenant", "pipeline", 5000, Duration.ofSeconds(10)).await().indefinitely(),
                new DynamoPipelineWorkerRegistry(limited, config).list("tenant", "pipeline", 5000, Duration.ofSeconds(10))
                    .await().indefinitely());
        }
    }

    @Test
    void emptyAndTerminalSinglePageRemainSupported() {
        try (var client = client()) {
            var registry = new DynamoPipelineWorkerRegistry(client, config(client));
            assertTrue(registry.list("tenant", "pipeline", 1000, Duration.ZERO).await().indefinitely().isEmpty());
            var original = registry.register(registration("tenant", "pipeline", "worker"), 1000).await().indefinitely();
            assertEquals(List.of(original), registry.list("tenant", "pipeline", 1000, Duration.ZERO).await().indefinitely());
            assertTrue(registry.list("other", "pipeline", 1000, Duration.ZERO).await().indefinitely().isEmpty());
        }
    }

    private static PipelineWorkerRegistration registration(String tenant, String pipeline, String worker) {
        return new PipelineWorkerRegistration(tenant, pipeline, "contract", "release", worker, "rest", "http://localhost",
            "artifact", "sha256:artifact");
    }

    private static PipelineOrchestratorConfig config(DynamoDbClient client) {
        String table = "worker-pages-" + UUID.randomUUID();
        client.createTable(CreateTableRequest.builder().tableName(table)
            .attributeDefinitions(AttributeDefinition.builder().attributeName("registry_key").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("registry_sort").attributeType(ScalarAttributeType.S).build())
            .keySchema(KeySchemaElement.builder().attributeName("registry_key").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("registry_sort").keyType(KeyType.RANGE).build())
            .billingMode(BillingMode.PAY_PER_REQUEST).build());
        client.waiter().waitUntilTableExists(request -> request.tableName(table));
        var config = mock(PipelineOrchestratorConfig.class);
        var dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.workerTable()).thenReturn(table);
        return config;
    }

    private static DynamoDbClient client() {
        return DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint()).region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
            .build();
    }
}

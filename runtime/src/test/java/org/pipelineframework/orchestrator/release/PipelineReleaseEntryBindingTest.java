package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

class PipelineReleaseEntryBindingTest {
    @Test
    void bindingSurvivesActivationAndRollbackAndRejectsConflictingEvidence() {
        var registry = new InMemoryPipelineReleaseRegistry();
        exercise(registry);
        registry.activate("tenant", "pipeline", "release-b", 2).await().indefinitely();
        registry.activate("tenant", "pipeline", "release-a", 3).await().indefinitely();
        assertEquals(binding("release-a", "1"), registry.getEntryBinding("tenant", "pipeline", "local-contract", "release-a", "transition")
            .await().indefinitely().orElseThrow());
        assertTrue(registry.getEntryBinding("other", "pipeline", "local-contract", "release-a", "transition")
            .await().indefinitely().isEmpty());
    }

    @Test
    void dynamoUsesConditionalWritesAndConsistentScopedReads() {
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
            if (rows.putIfAbsent(key, request.item()) != null) throw ConditionalCheckFailedException.builder().message("exists").build();
            return PutItemResponse.builder().build();
        });
        var config = mock(PipelineOrchestratorConfig.class);
        var dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.releaseTable()).thenReturn("releases");
        exercise(new DynamoPipelineReleaseRegistry(client, config));
    }

    @Test
    void schemaOneRemainsRegisterableButCannotAcquirePhysicalBindings() {
        var original = release("release-a");
        var descriptor = new PipelineReleaseDescriptor(1, original.pipelineId(), original.contractVersion(), original.releaseVersion(),
            "archive", original.descriptor().artifacts());
        var legacy = new PipelineReleaseRecord(original.tenantId(), original.pipelineId(), original.contractVersion(), original.releaseVersion(),
            original.status(), descriptor, "archive", original.primaryArtifactDigest(), original.primaryArtifactUri(), 0, "", original.contract(), 1, 1, 0);
        var registry = new InMemoryPipelineReleaseRegistry();
        assertEquals(legacy, registry.register(legacy).await().indefinitely());
        assertTrue(assertThrows(IllegalArgumentException.class, () -> registry.registerEntryBinding(binding("release-a", "1"))
            .await().indefinitely()).getMessage().contains("declared entry"));
    }

    @Test
    void physicalBindingMustNameDeclaredEntryAndItsSpecificArtifact() {
        var registry = new InMemoryPipelineReleaseRegistry();
        registry.register(release("release-a")).await().indefinitely();
        var unknown = new PipelineReleaseEntryBinding("tenant", "pipeline", "local-contract", "release-a", "unknown", "archive",
            "sha256:release-a", "aws-lambda", "arn:aws:lambda:eu-west-1:123456789012:function:transition:1", "code", "operation");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> registry.registerEntryBinding(unknown)
            .await().indefinitely()).getMessage().contains("declared entry"));
        var wrong = new PipelineReleaseEntryBinding("tenant", "pipeline", "local-contract", "release-a", "transition", "other",
            "sha256:other", "aws-lambda", "arn:aws:lambda:eu-west-1:123456789012:function:transition:1", "code", "operation");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> registry.registerEntryBinding(wrong)
            .await().indefinitely()).getMessage().contains("declared entry"));
    }

    private static void exercise(PipelineReleaseRegistry registry) {
        registry.register(release("release-a")).await().indefinitely();
        registry.register(release("release-b")).await().indefinitely();
        var first = binding("release-a", "1");
        assertEquals(first, registry.registerEntryBinding(first).await().indefinitely());
        assertEquals(first, registry.registerEntryBinding(first).await().indefinitely());
        assertTrue(assertThrows(IllegalStateException.class,
            () -> registry.registerEntryBinding(binding("release-a", "2")).await().indefinitely()).getMessage().contains("already bound"));
        var invalid = new PipelineReleaseEntryBinding("tenant", "pipeline", "local-contract", "release-a", "work",
            "archive", "sha256:foreign", "aws-lambda", "arn:aws:lambda:eu-west-1:123456789012:function:work:1", "code", "operation");
        assertTrue(assertThrows(IllegalArgumentException.class,
            () -> registry.registerEntryBinding(invalid).await().indefinitely()).getMessage().contains("original registered Release artifact"));
        assertTrue(assertThrows(IllegalStateException.class,
            () -> registry.registerEntryBinding(binding("missing", "1")).await().indefinitely()).getMessage().contains("registered Release"));
        assertEquals(first, registry.getEntryBinding("tenant", "pipeline", "local-contract", "release-a", "transition")
            .await().indefinitely().orElseThrow());
    }

    private static PipelineReleaseEntryBinding binding(String release, String version) {
        return new PipelineReleaseEntryBinding("tenant", "pipeline", "local-contract", release, "transition", "archive",
            "sha256:" + release, "aws-lambda", "arn:aws:lambda:eu-west-1:123456789012:function:transition:" + version,
            "observed-code-" + version, "operation-" + release);
    }

    private static PipelineReleaseRecord release(String version) {
        var contract = PipelineContractDescriptor.localFallback();
        var artifact = new PipelineReleaseArtifactDescriptor("archive", "lambda-zip", "s3://releases/" + version,
            "sha256:" + version, List.of(), List.of());
        var deployment = new PipelineReleaseDeployment("aws-durable", List.of(
            new PipelineReleaseDeployment.Entry("transition", "archive", contract.module(), ".",
                "org.pipelineframework.aws.durable.AwsDurableTransitionWorkerHandler", "sqs", List.of()),
            new PipelineReleaseDeployment.Entry("work", "archive", contract.module(), ".",
                "org.pipelineframework.aws.durable.AwsDurableWorkItemHandler", "sqs", List.of()),
            new PipelineReleaseDeployment.Entry("other", "other", contract.module(), ".",
                "org.pipelineframework.aws.durable.AwsDurableWorkItemHandler", "sqs", List.of())), List.of(),
            new PipelineReleaseDeployment.Check("transition", Map.of("payloadTypeId", "java.lang.String", "payloadEncoding", "json", "payload", "\"input\""),
                Map.of("payloadTypeId", "java.lang.String", "payloadEncoding", "json", "payload", "\"output\""), 1000));
        var descriptor = new PipelineReleaseDescriptor(2, "pipeline", contract.contractVersion(), version, "archive", List.of(artifact, new PipelineReleaseArtifactDescriptor("other", "lambda-zip", "s3://releases/other", "sha256:other", List.of(), List.of())), deployment);
        return new PipelineReleaseRecord("tenant", "pipeline", contract.contractVersion(), version, PipelineReleaseStatus.REGISTERED,
            descriptor, "archive", artifact.digest(), artifact.uri(), 0, "", contract, 1, 1, 0);
    }
}

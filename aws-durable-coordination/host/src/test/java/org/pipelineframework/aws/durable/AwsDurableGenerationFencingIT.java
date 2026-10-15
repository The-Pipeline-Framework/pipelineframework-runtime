package org.pipelineframework.aws.durable;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Local Dynamo transactions, without an AWS deployment or proof repository substitute. */
@Testcontainers
class AwsDurableGenerationFencingIT {
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8")).withServices("dynamodb");
    private static DynamoDbClient dynamo;
    private static final String TABLE = "generation-fencing";

    @BeforeAll
    static void createTable() {
        dynamo = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))).build();
        dynamo.createTable(request -> request.tableName(TABLE).billingMode(BillingMode.PAY_PER_REQUEST)
            .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("provider_execution_arn").attributeType(ScalarAttributeType.S).build())
            .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
            .globalSecondaryIndexes(index -> index.indexName(AwsDurableCallbackBindingRepository.PROVIDER_REGISTRATION_INDEX)
                .keySchema(KeySchemaElement.builder().attributeName("provider_execution_arn").keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
                .projection(projection -> projection.projectionType(software.amazon.awssdk.services.dynamodb.model.ProjectionType.ALL))));
        dynamo.waiter().waitUntilTableExists(request -> request.tableName(TABLE));
    }

    @AfterAll
    static void closeClient() {
        dynamo.close();
    }

    @Test
    void recoveryLocatorSurvivesCallbackDeletionAndRejectsConflictingOrStaleAuthority() {
        var repository = new AwsDurableCallbackBindingRepository(dynamo, TABLE);
        var registration = registration(UUID.randomUUID().toString(), 1);
        var locator = new org.pipelineframework.aws.durable.model.AwsDurableRecoveryLocator(registration.checkpoint(),
            registration.providerExecutionName(), registration.providerExecutionArn());
        assertThat(repository.registerRecovery(locator)).isTrue();
        assertThat(repository.registerRecovery(locator)).isFalse();
        assertThatThrownBy(() -> repository.registerRecovery(new org.pipelineframework.aws.durable.model.AwsDurableRecoveryLocator(
            locator.checkpoint(), "conflicting", "arn:conflicting"))).isInstanceOf(IllegalStateException.class);
        repository.register(registration);
        dynamo.deleteItem(request -> request.tableName(TABLE).key(java.util.Map.of(
            "pk", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(registration.partitionKey()).build(),
            "sk", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(registration.sortKey()).build())));
        assertThat(repository.findRegistration(registration.checkpoint().tenantId(), registration.checkpoint().executionId(), 1)).isEmpty();
        assertThat(repository.findRecoveryByProviderExecutionArn(locator.providerExecutionArn())).contains(locator);
        var item = dynamo.getItem(request -> request.tableName(TABLE).consistentRead(true).key(java.util.Map.of(
            "pk", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(locator.partitionKey()).build(),
            "sk", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(locator.sortKey()).build()))).item();
        assertThat(item).doesNotContainKey("expires_at_epoch_s");
        repository.claimGeneration(new AwsDurableDriverCheckpoint(locator.checkpoint().execution(), 2));
        assertThatThrownBy(() -> repository.registerRecovery(locator)).isInstanceOf(AwsDurableStaleGenerationException.class);
    }

    @Test
    void admissionWithoutExecutionLocatorRecoversWithActualGenerationTransactionConditions() {
        var repository = new AwsDurableCallbackBindingRepository(dynamo, TABLE);
        String executionId = UUID.randomUUID().toString();
        String arn = "arn:aws:lambda:eu-west-1:123456789012:function:driver:7/durable-execution/original/" + executionId;
        var admission = new org.pipelineframework.aws.durable.model.AwsDurableAdmissionLocator("tenant", "key-" + executionId,
            "pipeline", "contract", "release", java.util.Optional.empty(), 1, "provider", arn);
        repository.registerAdmission(admission);
        var semantic = new org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint("tenant", executionId,
            "pipeline", "contract", "release", org.pipelineframework.orchestrator.ExecutionStatus.RUNNING);
        var controlPlane = org.mockito.Mockito.mock(org.pipelineframework.orchestrator.PipelineControlPlane.class);
        org.mockito.Mockito.when(controlPlane.getExecutionSemanticCheckpointByClientKey("tenant", "key-" + executionId))
            .thenReturn(io.smallrye.mutiny.Uni.createFrom().item(java.util.Optional.of(semantic)));
        var starter = org.mockito.Mockito.mock(AwsDurableExecutionStarter.class);
        var pinned = org.mockito.Mockito.mock(AwsDurableExecutionStarter.class);
        org.mockito.Mockito.when(starter.pinnedToExecution(arn)).thenReturn(pinned);
        org.mockito.Mockito.when(pinned.start(org.mockito.ArgumentMatchers.any()))
            .thenReturn(new org.pipelineframework.aws.durable.model.AwsDurableStartResponse("replacement", 202));
        var repair = new AwsDurableTerminalReconciler(repository, controlPlane, starter,
            ignored -> AwsDurableProviderExecutionState.MISSING, java.time.Duration.ofSeconds(2));

        // No registerRecovery or callback registration occurred before provider/history loss.
        assertThat(repository.findRecoveryByProviderExecutionArn(arn)).isEmpty();
        assertThat(repair.reconcile(arn)).isTrue();
        assertThat(repository.isSuperseded("tenant", executionId, 1)).isTrue();
        assertThat(repository.isCurrentGeneration("tenant", executionId, 2)).isTrue();
        org.mockito.Mockito.verify(pinned).start(org.mockito.ArgumentMatchers.argThat(input ->
            input.resumeExecutionId().equals(java.util.Optional.of(executionId)) && input.generation() == 2
                && input.releaseVersion().equals("release")));
        org.mockito.Mockito.verify(controlPlane, org.mockito.Mockito.never()).executePipelineAsync(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.anyBoolean(), org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void preSubmissionLocatorIsImmutableRetainedAndContainsNoSemanticAdmissionOrInput() {
        var repository = new AwsDurableCallbackBindingRepository(dynamo, TABLE);
        String providerArn = "arn:provider:" + UUID.randomUUID();
        var locator = new org.pipelineframework.aws.durable.model.AwsDurableAdmissionLocator("tenant", "client-key",
            "pipeline", "contract", "release", java.util.Optional.empty(), 1, "provider-name", providerArn);
        assertThat(repository.registerAdmission(locator)).isTrue();
        assertThat(repository.registerAdmission(locator)).isFalse();
        var conflict = new org.pipelineframework.aws.durable.model.AwsDurableAdmissionLocator("tenant", "another-key",
            "pipeline", "contract", "release", java.util.Optional.empty(), 1, "provider-name", providerArn);
        assertThatThrownBy(() -> repository.registerAdmission(conflict)).isInstanceOf(IllegalStateException.class);
        assertThat(repository.findAdmissionByProviderExecutionArn(providerArn)).contains(locator);
        var item = dynamo.getItem(request -> request.tableName(TABLE).consistentRead(true).key(Map.of(
            "pk", AttributeValue.fromS(locator.partitionKey()), "sk", AttributeValue.fromS(locator.sortKey())))).item();
        assertThat(item).doesNotContainKeys("expires_at_epoch_s", "execution_id", "input_json", "status");
        assertThat(item.get("record_type").s()).isEqualTo("ADMISSION");
    }

    @Test
    void repeatedCallbacksPinSeparateInteractionsAndRejectCrossBinding() {
        var repository = new AwsDurableCallbackBindingRepository(dynamo, TABLE);
        String execution = UUID.randomUUID().toString();
        var checkpoint = registration(execution, 1).checkpoint();
        var first = new AwsDurableAwaitIdentity("tenant", execution, "first-" + execution, "correlation-first", 1, 1);
        var second = new AwsDurableAwaitIdentity("tenant", execution, "second-" + execution, "correlation-second", 1, 2);
        var r1 = new AwsDurableCallbackRegistration(checkpoint, "provider", "arn:" + execution, "callback-1", 100, 1000,
            java.util.Optional.of(first));
        var r2 = new AwsDurableCallbackRegistration(checkpoint, "provider", "arn:" + execution, "callback-2", 100, 1000,
            java.util.Optional.of(second));
        assertThat(repository.register(r1)).isTrue();
        assertThat(repository.register(r2)).isTrue();
        assertThat(repository.register(new AwsDurableCallbackRegistration(checkpoint, "provider", "arn:" + execution,
            "callback-2", 200, 2000, java.util.Optional.of(second)))).isFalse();
        assertThat(repository.findLatestRegistration("tenant", execution)).contains(r2);
        assertThat(repository.bind(r1.bind(first))).isTrue();
        assertThat(repository.bind(r2.bind(second))).isTrue();
        assertThat(repository.find("tenant", second.interactionId(), 1, 2)).contains(r2.bind(second));
        assertThatThrownBy(() -> r2.bind(first)).isInstanceOf(IllegalArgumentException.class);
        var forged = new org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding(
            new AwsDurableAwaitIdentity("tenant", execution, "forged", "forged", 1, 2), "provider", "arn:" + execution,
            "callback-2", org.pipelineframework.aws.durable.model.AwsDurableBindingStatus.OPEN, 100, 1000);
        assertThatThrownBy(() -> repository.bind(forged)).isInstanceOf(TransactionCanceledException.class);
        repository.recordDelivered(r1.bind(first), "accepted");
        assertThat(repository.delivered(r2.bind(second))).isFalse();
        repository.claimGeneration(new AwsDurableDriverCheckpoint(checkpoint.execution(), 2));
        assertThatThrownBy(() -> repository.register(r2)).isInstanceOf(AwsDurableStaleGenerationException.class);
        assertThatThrownBy(() -> repository.bind(r2.bind(second))).isInstanceOf(AwsDurableStaleGenerationException.class);
    }

    @Test
    void deliveryOfAnEarlierOccurrenceDoesNotRetireASecondWaitOnTheSameInteraction() {
        var repository = new AwsDurableCallbackBindingRepository(dynamo, TABLE);
        String execution = UUID.randomUUID().toString();
        var checkpoint = registration(execution, 1).checkpoint();
        var first = new AwsDurableAwaitIdentity("tenant", execution, "same-" + execution, "correlation", 1, 1);
        var second = new AwsDurableAwaitIdentity("tenant", execution, first.interactionId(), first.correlationId(), 1, 2);
        var r1 = new AwsDurableCallbackRegistration(checkpoint, "provider", "arn:" + execution, "first", 100, 1000,
            java.util.Optional.of(first));
        var r2 = new AwsDurableCallbackRegistration(checkpoint, "provider", "arn:" + execution, "second", 100, 1000,
            java.util.Optional.of(second));
        repository.register(r1);
        repository.register(r2);
        repository.bind(r1.bind(first));
        repository.bind(r2.bind(second));
        repository.recordDelivered(r1.bind(first), "accepted");
        assertThat(repository.delivered(r1.bind(first))).isTrue();
        assertThat(repository.delivered(r2.bind(second))).isFalse();
        assertThat(repository.findLatest("tenant", first.interactionId())).contains(r2.bind(second));
    }

    @Test
    void replayAndConflictAreClassifiedFromStoredAuthority() {
        var repository = new AwsDurableCallbackBindingRepository(dynamo, TABLE);
        String execution = UUID.randomUUID().toString();
        var registration = registration(execution, 1);
        assertThat(repository.register(registration)).isTrue();
        assertThat(repository.register(registration)).isFalse();
        var binding = registration.bind(identity(execution, 1));
        assertThat(repository.bind(binding)).isTrue();
        assertThat(repository.bind(binding)).isFalse();
        var conflicting = new AwsDurableCallbackRegistration(registration.checkpoint(), registration.providerExecutionName(),
            registration.providerExecutionArn(), "other-callback", 200, 2000);
        assertThatThrownBy(() -> repository.register(conflicting)).isInstanceOf(IllegalStateException.class);
        var wrongRelease = new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint(
            "tenant", execution, "pipeline", "contract", "other-release"), 2);
        assertThatThrownBy(() -> repository.claimGeneration(wrongRelease))
            .isInstanceOf(TransactionCanceledException.class);
        assertThat(repository.isCurrentGeneration("tenant", execution, 1)).isTrue();
    }

    @Test
    void concurrentBindingAndSuccessorAdmissionNeverReauthoriseAnOldGenerationAfterCallbackExpiry() {
        var repository = new AwsDurableCallbackBindingRepository(dynamo, TABLE);
        // Exercise both transaction orderings; real-AWS seeded race conformance remains a separate gate.
        for (int race = 0; race < 20; race++) {
            String execution = UUID.randomUUID().toString();
            var original = registration(execution, 1);
            var binding = original.bind(identity(execution, 1));
            repository.register(original);
            var bind = CompletableFuture.runAsync(() -> {
                try {
                    repository.bind(binding);
                } catch (AwsDurableStaleGenerationException | TransactionCanceledException retryable) {
                    // Replacement may win admission or cause transaction contention.
                }
            });
            var successor = CompletableFuture.runAsync(() -> {
                try {
                    repository.claimGeneration(registration(execution, 2).checkpoint());
                } catch (TransactionCanceledException retryable) {
                    // The durable caller retries transaction contention.
                }
            });
            CompletableFuture.allOf(bind, successor).join();
            repository.claimGeneration(registration(execution, 2).checkpoint());
            delete(original.partitionKey(), original.sortKey());
            delete(binding.partitionKey(), binding.sortKey());

            assertThat(repository.isSuperseded("tenant", execution, 1)).isTrue();
            assertThatThrownBy(() -> repository.register(original)).isInstanceOf(AwsDurableStaleGenerationException.class);
            assertThatThrownBy(() -> repository.bind(binding)).isInstanceOf(AwsDurableStaleGenerationException.class);
            var replacement = registration(execution, 2);
            assertThat(repository.register(replacement)).isTrue();
            assertThat(repository.bind(replacement.bind(identity(execution, 2)))).isTrue();
        }
    }

    private static void delete(String partition, String sort) {
        dynamo.deleteItem(request -> request.tableName(TABLE).key(Map.of(
            "pk", AttributeValue.fromS(partition), "sk", AttributeValue.fromS(sort))));
    }

    private static AwsDurableAwaitIdentity identity(String execution, long generation) {
        return new AwsDurableAwaitIdentity("tenant", execution, "interaction-" + execution, "correlation", generation);
    }

    private static AwsDurableCallbackRegistration registration(String execution, long generation) {
        return new AwsDurableCallbackRegistration(new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint(
            "tenant", execution, "pipeline", "contract", "release"), generation),
            "provider-" + execution + "-" + generation, "arn:" + execution + ":" + generation,
            "callback-" + generation, 100, 1000);
    }
}

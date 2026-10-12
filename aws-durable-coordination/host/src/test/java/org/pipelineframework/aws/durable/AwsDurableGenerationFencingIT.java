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
                AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build())
            .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build()));
        dynamo.waiter().waitUntilTableExists(request -> request.tableName(TABLE));
    }

    @AfterAll
    static void closeClient() {
        dynamo.close();
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

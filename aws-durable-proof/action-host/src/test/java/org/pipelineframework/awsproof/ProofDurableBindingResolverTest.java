package org.pipelineframework.awsproof;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.Execution;
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionHistoryRequest;
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionHistoryResponse;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;

class ProofDurableBindingResolverTest {

    @Test
    void semanticAwaitLookupReadsLaterDynamoPages() {
        DynamoDbClient dynamo = mock(DynamoDbClient.class);
        Map<String, AttributeValue> nextKey = Map.of(
            "tenant_id", AttributeValue.fromS("tenant"),
            "interaction_id", AttributeValue.fromS("before"));
        when(dynamo.query(any(QueryRequest.class))).thenReturn(
            QueryResponse.builder().items(List.of()).lastEvaluatedKey(nextKey).build(),
            QueryResponse.builder().items(List.of(Map.of(
                "tenant_id", AttributeValue.fromS("tenant"),
                "execution_id", AttributeValue.fromS("execution"),
                "interaction_id", AttributeValue.fromS("interaction"),
                "correlation_id", AttributeValue.fromS("correlation")))).build());
        ProofDurableBindingResolver resolver = new ProofDurableBindingResolver();
        resolver.dynamo = dynamo;

        var resolved = resolver.findSemanticAwait(
            new AwsDurableDriverCheckpoint(
                new AwsDurableExecutionCheckpoint(
                    "tenant", "execution", "pipeline", "contract", "release"),
                2),
            "await-table");

        assertThat(resolved).hasValueSatisfying(identity -> {
            assertThat(identity.interactionId()).isEqualTo("interaction");
            assertThat(identity.correlationId()).isEqualTo("correlation");
            assertThat(identity.generation()).isEqualTo(2);
        });
    }

    @Test
    void openBindingReconstructionIsolatesOneDisappearingExecution() {
        LambdaClient lambda = mock(LambdaClient.class);
        when(lambda.getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class)))
            .thenThrow(ResourceNotFoundException.builder().message("history expired").build())
            .thenReturn(GetDurableExecutionHistoryResponse.builder().events(List.of()).build());
        ProofDurableBindingResolver resolver = new ProofDurableBindingResolver();
        resolver.lambda = lambda;
        List<Execution> executions = List.of(
            Execution.builder().durableExecutionArn("arn:first").build(),
            Execution.builder().durableExecutionArn("arn:second").build());

        assertThat(resolver.reconstructOpenBindings(executions)).isEmpty();
        verify(lambda, times(2)).getDurableExecutionHistory(any(GetDurableExecutionHistoryRequest.class));
    }
}

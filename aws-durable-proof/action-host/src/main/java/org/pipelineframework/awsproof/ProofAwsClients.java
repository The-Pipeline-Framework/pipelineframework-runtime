package org.pipelineframework.awsproof;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.lambda.LambdaClient;

@ApplicationScoped
final class ProofAwsClients {
    @Produces
    @Singleton
    DynamoDbClient dynamoDbClient() {
        return DynamoDbClient.create();
    }

    @Produces
    @Singleton
    LambdaClient lambdaClient() {
        return LambdaClient.create();
    }
}

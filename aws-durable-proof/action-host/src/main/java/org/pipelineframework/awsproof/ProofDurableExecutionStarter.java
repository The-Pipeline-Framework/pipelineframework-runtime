package org.pipelineframework.awsproof;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionNames;
import org.pipelineframework.aws.durable.model.AwsDurableStartResponse;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.DurableExecutionAlreadyStartedException;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

@ApplicationScoped
final class ProofDurableExecutionStarter {
    @Inject
    LambdaClient lambda;

    @Inject
    ObjectMapper mapper;

    AwsDurableStartResponse start(AwsDurableExecutionInput input) {
        String executionName = AwsDurableExecutionNames.durableExecutionName(
            input.tenantId(), input.idempotencyKey(), input.generation());
        try {
            var response = lambda.invoke(InvokeRequest.builder()
                .functionName(requiredEnvironment("TPF_PROOF_DURABLE_FUNCTION"))
                .qualifier(requiredEnvironment("TPF_PROOF_DURABLE_QUALIFIER"))
                .invocationType(InvocationType.EVENT)
                .durableExecutionName(executionName)
                .payload(SdkBytes.fromUtf8String(mapper.writeValueAsString(input)))
                .build());
            return new AwsDurableStartResponse(executionName, response.statusCode());
        } catch (DurableExecutionAlreadyStartedException duplicate) {
            return new AwsDurableStartResponse(executionName, 202);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("proof execution input could not be serialized", exception);
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required");
        }
        return value;
    }
}

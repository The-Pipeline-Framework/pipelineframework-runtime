package org.pipelineframework.aws.durable;

import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionNames;
import org.pipelineframework.aws.durable.model.AwsDurableStartResponse;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.DurableExecutionAlreadyStartedException;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

/** Starts one named Durable execution; duplicate named starts are successful idempotent admission. */
public final class AwsDurableExecutionStarter {
    private final LambdaClient lambda;
    private final ObjectMapper mapper;
    private final String functionName;
    private final String qualifier;

    public AwsDurableExecutionStarter(
        LambdaClient lambda,
        ObjectMapper mapper,
        String functionName,
        String qualifier
    ) {
        this.lambda = Objects.requireNonNull(lambda, "lambda");
        this.mapper = Objects.requireNonNull(mapper, "mapper").copy()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());
        this.functionName = required(functionName, "functionName");
        this.qualifier = required(qualifier, "qualifier");
    }

    public AwsDurableStartResponse start(AwsDurableExecutionInput input) {
        Objects.requireNonNull(input, "input");
        String executionName = AwsDurableExecutionNames.durableExecutionName(
            input.tenantId(), input.idempotencyKey(), input.generation());
        try {
            var response = lambda.invoke(InvokeRequest.builder()
                .functionName(functionName)
                .qualifier(qualifier)
                .invocationType(InvocationType.EVENT)
                .durableExecutionName(executionName)
                .payload(SdkBytes.fromUtf8String(mapper.writeValueAsString(input)))
                .build());
            return new AwsDurableStartResponse(executionName, response.statusCode());
        } catch (DurableExecutionAlreadyStartedException duplicate) {
            return new AwsDurableStartResponse(executionName, 202);
        } catch (JsonProcessingException malformed) {
            throw new IllegalArgumentException("durable execution input could not be serialized", malformed);
        }
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

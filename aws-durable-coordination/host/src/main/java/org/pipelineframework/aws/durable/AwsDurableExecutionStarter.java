package org.pipelineframework.aws.durable;

import java.time.Duration;
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

    public AwsDurableExecutionStarter pinnedToExecution(String providerExecutionArn) {
        String arn = required(providerExecutionArn, "providerExecutionArn");
        int execution = arn.indexOf("/durable-execution/");
        if (execution < 0) throw new IllegalArgumentException("provider execution ARN has no qualified function");
        String qualifiedFunction = arn.substring(0, execution);
        int versionSeparator = qualifiedFunction.lastIndexOf(':');
        String version = qualifiedFunction.substring(versionSeparator + 1);
        if (!version.matches("[1-9][0-9]*")) throw new IllegalArgumentException("recovery requires a numbered coordinator version");
        return new AwsDurableExecutionStarter(lambda, mapper, qualifiedFunction.substring(0, versionSeparator), version);
    }

    public AwsDurableStartResponse start(AwsDurableExecutionInput input) {
        return invoke(input, null).response();
    }

    boolean startForHttpAdmission(AwsDurableExecutionInput input, Duration timeout) {
        var outcome = invoke(input, Objects.requireNonNull(timeout, "timeout"));
        if (outcome.response().statusCode() != 202) {
            throw new IllegalStateException("Durable coordinator did not accept asynchronous invocation");
        }
        return outcome.duplicate();
    }

    private StartOutcome invoke(AwsDurableExecutionInput input, Duration timeout) {
        Objects.requireNonNull(input, "input");
        String executionName = AwsDurableExecutionNames.durableExecutionName(
            input.tenantId(), input.idempotencyKey(), input.generation());
        try {
            var request = InvokeRequest.builder()
                .functionName(functionName)
                .qualifier(qualifier)
                .invocationType(InvocationType.EVENT)
                .durableExecutionName(executionName)
                .payload(SdkBytes.fromUtf8String(mapper.writeValueAsString(input)));
            if (timeout != null) {
                request.overrideConfiguration(override -> override.apiCallTimeout(timeout));
            }
            var response = lambda.invoke(request.build());
            return new StartOutcome(new AwsDurableStartResponse(executionName, response.statusCode()), false);
        } catch (DurableExecutionAlreadyStartedException duplicate) {
            return new StartOutcome(new AwsDurableStartResponse(executionName, 202), true);
        } catch (JsonProcessingException malformed) {
            throw new IllegalArgumentException("durable execution input could not be serialized", malformed);
        }
    }

    private record StartOutcome(AwsDurableStartResponse response, boolean duplicate) {}

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

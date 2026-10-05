package org.pipelineframework.awsproof;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.pipelineframework.awsproof.model.ProofActionRequest;
import org.pipelineframework.awsproof.model.ProofActionResponse;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.InvocationType;

final class LambdaProofActionInvoker implements ProofActionInvoker, AutoCloseable {
    private final LambdaClient lambdaClient;
    private final String functionName;
    private final ObjectMapper mapper;

    LambdaProofActionInvoker(LambdaClient lambdaClient, String functionName) {
        this.lambdaClient = java.util.Objects.requireNonNull(lambdaClient, "lambdaClient");
        this.functionName = required(functionName, "functionName");
        this.mapper = new ObjectMapper().registerModule(new Jdk8Module());
    }

    @Override
    public ProofActionResponse invoke(ProofActionRequest request) {
        try {
            InvokeResponse response = lambdaClient.invoke(InvokeRequest.builder()
                .functionName(functionName)
                .invocationType(InvocationType.REQUEST_RESPONSE)
                .payload(SdkBytes.fromUtf8String(mapper.writeValueAsString(request)))
                .build());
            if (response.functionError() != null && !response.functionError().isBlank()) {
                throw new IllegalStateException("proof action Lambda failed: " + response.functionError());
            }
            return mapper.readValue(response.payload().asString(StandardCharsets.UTF_8), ProofActionResponse.class);
        } catch (IOException exception) {
            throw new IllegalStateException("proof action response was not valid JSON", exception);
        }
    }

    @Override
    public void close() {
        lambdaClient.close();
    }

    private static String required(String value, String name) {
        java.util.Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

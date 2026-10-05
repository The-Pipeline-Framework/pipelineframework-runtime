package org.pipelineframework.aws.durable;

import java.util.Objects;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableStartResponse;
import software.amazon.awssdk.services.lambda.LambdaClient;

/** Generic ingress for already-typed Durable execution input. HTTP/API adaptation remains generated glue. */
public final class AwsDurableIngressHandler
    implements RequestHandler<AwsDurableExecutionInput, AwsDurableStartResponse> {

    private final AwsDurableExecutionStarter starter;

    public AwsDurableIngressHandler() {
        this(new AwsDurableExecutionStarter(
            LambdaClient.create(),
            new ObjectMapper(),
            requiredEnvironment("TPF_AWS_DURABLE_FUNCTION"),
            requiredEnvironment("TPF_AWS_DURABLE_QUALIFIER")));
    }

    AwsDurableIngressHandler(AwsDurableExecutionStarter starter) {
        this.starter = Objects.requireNonNull(starter, "starter");
    }

    @Override
    public AwsDurableStartResponse handleRequest(AwsDurableExecutionInput input, Context context) {
        return starter.start(Objects.requireNonNull(input, "input"));
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required");
        }
        return value;
    }
}

package org.pipelineframework.aws.durable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Duration;
import java.util.Objects;

import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackSignal;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionNames;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionOutput;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionStatusPoll;
import software.amazon.lambda.durable.DurableContext;
import software.amazon.lambda.durable.DurableConfig;
import software.amazon.lambda.durable.DurableHandler;
import software.amazon.lambda.durable.model.WaitForConditionResult;
import software.amazon.lambda.durable.serde.JacksonSerDes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.lambda.LambdaClient;

/**
 * AWS Durable coordination driver. Every side effect crosses a bounded TPF action boundary.
 */
public final class AwsDurableCoordinatorHandler
    extends DurableHandler<AwsDurableExecutionInput, AwsDurableExecutionOutput> {

    private static final String ACTION_FUNCTION_ENV = "TPF_AWS_DURABLE_ACTION_FUNCTION";
    private final AwsDurableActionInvoker actions;

    public AwsDurableCoordinatorHandler() {
        this(new LambdaAwsDurableActionInvoker(
            LambdaClient.builder()
                .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(90)))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                    .apiCallAttemptTimeout(Duration.ofSeconds(90))
                    .build())
                .build(),
            requiredEnvironment(ACTION_FUNCTION_ENV)));
    }

    AwsDurableCoordinatorHandler(AwsDurableActionInvoker actions) {
        this.actions = Objects.requireNonNull(actions, "actions");
    }

    @Override
    protected DurableConfig createConfiguration() {
        ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return DurableConfig.builder()
            .withSerDes(new JacksonSerDes(objectMapper))
            .build();
    }

    @Override
    public AwsDurableExecutionOutput handleRequest(AwsDurableExecutionInput input, DurableContext context) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(context, "context");

        AwsDurableDriverCheckpoint checkpoint = input.resumeExecutionId()
            .map(executionId -> context.step(
                "resume-tpf-execution",
                AwsDurableDriverCheckpoint.class,
                ignored -> new AwsDurableDriverCheckpoint(
                    new AwsDurableExecutionCheckpoint(
                        input.tenantId(),
                        executionId,
                        input.pipelineId(),
                        input.contractVersion(),
                        input.releaseVersion()),
                    input.generation())))
            .orElseGet(() -> context.step(
                "submit-tpf-execution",
                AwsDurableDriverCheckpoint.class,
                ignored -> new AwsDurableDriverCheckpoint(
                    requiredCheckpoint(actions.invoke(AwsDurableActionRequest.submit(input))),
                    input.generation())));

        int awaitIndex = 0;
        AwsDurableExecutionStatusPoll terminal = waitForBlockedOrTerminal(checkpoint, context, awaitIndex);
        while (!terminal.terminal()) {
            if (!"WAITING_EXTERNAL".equals(terminal.status())) {
                throw new IllegalStateException("TPF execution cannot be resumed from status " + terminal.status());
            }
            waitForAwaitWakeup(input, checkpoint, context, awaitIndex);
            awaitIndex++;
            terminal = waitForBlockedOrTerminal(checkpoint, context, awaitIndex);
        }
        if (!terminal.succeeded()) {
            throw new IllegalStateException("TPF execution ended with status " + terminal.status());
        }

        String resultJson = context.step(
            "read-tpf-result",
            String.class,
            ignored -> actions.invoke(AwsDurableActionRequest.result(checkpoint)).resultJson()
                .orElseThrow(() -> new IllegalStateException("TPF result action returned no payload")));

        return new AwsDurableExecutionOutput(
            checkpoint.tenantId(),
            checkpoint.executionId(),
            checkpoint.pipelineId(),
            checkpoint.contractVersion(),
            checkpoint.releaseVersion(),
            checkpoint.generation(),
            resultJson);
    }

    private void waitForAwaitWakeup(
        AwsDurableExecutionInput input,
        AwsDurableDriverCheckpoint checkpoint,
        DurableContext context,
        int awaitIndex
    ) {
        String providerExecutionName = AwsDurableExecutionNames.durableExecutionName(
            input.tenantId(), input.idempotencyKey(), input.generation());
        AwsDurableCallbackSignal signal = context.waitForCallback(
            "await-completion-" + awaitIndex,
            AwsDurableCallbackSignal.class,
            (callbackId, ignored) -> actions.invoke(AwsDurableActionRequest.register(
                checkpoint, providerExecutionName, context.getExecutionArn(), callbackId)));
        verifySignal(checkpoint, signal);
    }

    private AwsDurableExecutionStatusPoll waitForBlockedOrTerminal(
        AwsDurableDriverCheckpoint checkpoint,
        DurableContext context,
        int awaitIndex
    ) {
        return context.waitForCondition(
            "wait-for-tpf-state-" + awaitIndex,
            AwsDurableExecutionStatusPoll.class,
            (previous, ignored) -> {
                actions.invoke(AwsDurableActionRequest.sweep(checkpoint));
                AwsDurableActionResponse current = actions.invoke(AwsDurableActionRequest.status(checkpoint));
                AwsDurableExecutionStatusPoll poll = new AwsDurableExecutionStatusPoll(
                    current.executionStatus().orElseThrow(() ->
                        new IllegalStateException("TPF status action returned no status")));
                return poll.terminal() || "WAITING_EXTERNAL".equals(poll.status())
                    ? WaitForConditionResult.stopPolling(poll)
                    : WaitForConditionResult.continuePolling(poll);
            });
    }

    private static AwsDurableExecutionCheckpoint requiredCheckpoint(AwsDurableActionResponse response) {
        return response.checkpoint()
            .orElseThrow(() -> new IllegalStateException("TPF submit action returned no checkpoint"));
    }

    private static void verifySignal(AwsDurableDriverCheckpoint expected, AwsDurableCallbackSignal actual) {
        if (!expected.tenantId().equals(actual.tenantId())
            || !expected.executionId().equals(actual.executionId())
            || expected.generation() != actual.generation()) {
            throw new IllegalStateException("provider callback identity does not match the TPF execution checkpoint");
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

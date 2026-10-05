package org.pipelineframework.awsproof;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Duration;
import java.util.Objects;

import org.pipelineframework.awsproof.model.ProofActionRequest;
import org.pipelineframework.awsproof.model.ProofActionResponse;
import org.pipelineframework.awsproof.model.ProofCallbackSignal;
import org.pipelineframework.awsproof.model.ProofExecutionCheckpoint;
import org.pipelineframework.awsproof.model.ProofExecutionInput;
import org.pipelineframework.awsproof.model.ProofExecutionNames;
import org.pipelineframework.awsproof.model.ProofExecutionOutput;
import org.pipelineframework.awsproof.model.ProofExecutionStatusPoll;
import software.amazon.lambda.durable.DurableContext;
import software.amazon.lambda.durable.DurableConfig;
import software.amazon.lambda.durable.DurableHandler;
import software.amazon.lambda.durable.config.CallbackConfig;
import software.amazon.lambda.durable.config.WaitForCallbackConfig;
import software.amazon.lambda.durable.exception.CallbackTimeoutException;
import software.amazon.lambda.durable.model.WaitForConditionResult;
import software.amazon.lambda.durable.serde.JacksonSerDes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.lambda.LambdaClient;

/**
 * Proof-only durable driver. Every side effect crosses the existing bounded TPF action boundary.
 */
public final class AwsDurableCoordinatorHandler
    extends DurableHandler<ProofExecutionInput, ProofExecutionOutput> {

    private static final String ACTION_FUNCTION_ENV = "TPF_PROOF_ACTION_FUNCTION";
    private final ProofActionInvoker actions;

    public AwsDurableCoordinatorHandler() {
        this(new LambdaProofActionInvoker(
            LambdaClient.builder()
                .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(90)))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                    .apiCallAttemptTimeout(Duration.ofSeconds(90))
                    .build())
                .build(),
            requiredEnvironment(ACTION_FUNCTION_ENV)));
    }

    AwsDurableCoordinatorHandler(ProofActionInvoker actions) {
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
    public ProofExecutionOutput handleRequest(ProofExecutionInput input, DurableContext context) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(context, "context");

        ProofExecutionCheckpoint checkpoint = input.resumeExecutionId()
            .map(executionId -> context.step(
                "resume-tpf-execution",
                ProofExecutionCheckpoint.class,
                ignored -> new ProofExecutionCheckpoint(
                    input.tenantId(),
                    executionId,
                    true,
                    input.pipelineId(),
                    input.contractVersion(),
                    input.releaseVersion(),
                    input.generation())))
            .orElseGet(() -> context.step(
                "submit-tpf-execution",
                ProofExecutionCheckpoint.class,
                ignored -> requiredCheckpoint(actions.invoke(ProofActionRequest.submit(input)))));

        boolean requiresAwaitWakeup = input.resumeExecutionId().isEmpty()
            || !new ProofExecutionStatusPoll(context.step(
                "read-resume-status",
                String.class,
                ignored -> actions.invoke(ProofActionRequest.status(checkpoint)).executionStatus()
                    .orElseThrow(() -> new IllegalStateException("TPF status action returned no status"))))
                .terminal();

        if (requiresAwaitWakeup) {
            waitForAwaitWakeup(input, checkpoint, context);
        }

        ProofExecutionStatusPoll terminal = waitForTerminal(checkpoint, context);
        if (!terminal.succeeded()) {
            throw new IllegalStateException("TPF execution ended with status " + terminal.status());
        }

        String resultJson = context.step(
            "read-tpf-result",
            String.class,
            ignored -> actions.invoke(ProofActionRequest.result(checkpoint)).resultJson()
                .orElseThrow(() -> new IllegalStateException("TPF result action returned no payload")));

        return new ProofExecutionOutput(
            checkpoint.tenantId(),
            checkpoint.executionId(),
            checkpoint.pipelineId(),
            checkpoint.contractVersion(),
            checkpoint.releaseVersion(),
            checkpoint.generation(),
            resultJson);
    }

    private void waitForAwaitWakeup(
        ProofExecutionInput input,
        ProofExecutionCheckpoint checkpoint,
        DurableContext context
    ) {
        String providerExecutionName = ProofExecutionNames.durableExecutionName(
            input.tenantId(), input.idempotencyKey(), input.generation());
        try {
            ProofCallbackSignal signal = shortCallbackProbe(input)
                ? context.waitForCallback(
                    "await-completion",
                    ProofCallbackSignal.class,
                    (callbackId, ignored) -> actions.invoke(ProofActionRequest.register(
                        checkpoint, providerExecutionName, context.getExecutionArn(), callbackId)),
                    WaitForCallbackConfig.builder()
                        .callbackConfig(CallbackConfig.builder().timeout(Duration.ofSeconds(20)).build())
                        .build())
                : context.waitForCallback(
                    "await-completion",
                    ProofCallbackSignal.class,
                    (callbackId, ignored) -> actions.invoke(ProofActionRequest.register(
                        checkpoint, providerExecutionName, context.getExecutionArn(), callbackId)));
            verifySignal(checkpoint, signal);
        } catch (CallbackTimeoutException timeout) {
            if (!deadlineProbe(input)) {
                throw timeout;
            }
            context.step(
                "deadline-probe-sweep",
                String.class,
                ignored -> actions.invoke(ProofActionRequest.sweep(checkpoint))
                    .executionStatus().orElse("SWEEPED"));
        }
    }

    private ProofExecutionStatusPoll waitForTerminal(
        ProofExecutionCheckpoint checkpoint,
        DurableContext context
    ) {
        return context.waitForCondition(
            "wait-for-tpf-terminal",
            ProofExecutionStatusPoll.class,
            (previous, ignored) -> {
                actions.invoke(ProofActionRequest.sweep(checkpoint));
                ProofActionResponse current = actions.invoke(ProofActionRequest.status(checkpoint));
                ProofExecutionStatusPoll poll = new ProofExecutionStatusPoll(
                    current.executionStatus().orElseThrow(() ->
                        new IllegalStateException("TPF status action returned no status")));
                return poll.terminal()
                    ? WaitForConditionResult.stopPolling(poll)
                    : WaitForConditionResult.continuePolling(poll);
            });
    }

    private static ProofExecutionCheckpoint requiredCheckpoint(ProofActionResponse response) {
        return response.checkpoint()
            .orElseThrow(() -> new IllegalStateException("TPF submit action returned no checkpoint"));
    }

    private static boolean deadlineProbe(ProofExecutionInput input) {
        return input.inputJson().contains("\"deadlineProbe\":true");
    }

    private static boolean shortCallbackProbe(ProofExecutionInput input) {
        return deadlineProbe(input) || input.inputJson().contains("\"callbackExpiryProbe\":true");
    }

    private static void verifySignal(ProofExecutionCheckpoint expected, ProofCallbackSignal actual) {
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

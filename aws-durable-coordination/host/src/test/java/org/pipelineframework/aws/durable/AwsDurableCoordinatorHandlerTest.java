package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackSignal;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionOutput;
import software.amazon.lambda.durable.TypeToken;
import software.amazon.lambda.durable.model.ExecutionStatus;
import software.amazon.lambda.durable.testing.LocalDurableTestRunner;

class AwsDurableCoordinatorHandlerTest {
    private static final ObjectMapper JSON = new ObjectMapper().registerModule(new Jdk8Module());

    private static AwsDurableActionResponse waiting(AwsDurableExecutionCheckpoint execution, String interaction, String correlation) {
        return AwsDurableActionResponse.observation("WAITING_EXTERNAL", Optional.of(
            new org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint(execution.tenantId(), execution.executionId(),
                interaction, correlation, "unit-" + interaction, "step", "WAITING", execution.pipelineId(),
                execution.contractVersion(), execution.releaseVersion())));
    }

    @Test
    void durableConfigurationRoundTripsResumeIdentityForReplay() {
        AwsDurableExecutionInput input = AwsDurableExecutionInput.resume(
            new AwsDurableAwaitIdentity("tenant-a", "execution-a", "interaction-a", "correlation-a", 1),
            "pipeline-a",
            "contract-a",
            "release-a");
        AwsDurableCoordinatorHandler handler = new AwsDurableCoordinatorHandler(request -> {
            throw new AssertionError("serialization must not invoke an action");
        });

        String serialized = handler.getConfiguration().getSerDes().serialize(input);
        AwsDurableExecutionInput replayed = handler.getConfiguration().getSerDes()
            .deserialize(serialized, TypeToken.get(AwsDurableExecutionInput.class));

        assertThat(replayed).isEqualTo(input);
        assertThat(replayed.resumeExecutionId()).contains("execution-a");
    }

    @Test
    void callbackWakeupCarriesTheTpfIdentityAndPinnedRelease() throws Exception {
        AwsDurableExecutionCheckpoint checkpoint = new AwsDurableExecutionCheckpoint(
            "tenant-a", "execution-a", "pipeline-a", "contract-a", "release-a");
        AwsDurableAwaitIdentity await = new AwsDurableAwaitIdentity(
            "tenant-a", "execution-a", "interaction-a", "correlation-a", 1);
        AtomicReference<String> callbackId = new AtomicReference<>();
        AtomicBoolean resumed = new AtomicBoolean();
        AwsDurableActionInvoker actions = request -> switch (request.operation()) {
            case SUBMIT -> AwsDurableActionResponse.submitted(checkpoint, false);
            case REGISTER_CALLBACK -> {
                callbackId.set(request.providerCallbackId().orElseThrow());
                assertThat(request.awaitIdentity()).isPresent();
                yield AwsDurableActionResponse.bound(true);
            }
            case STATUS -> resumed.get() ? AwsDurableActionResponse.status("SUCCEEDED") : waiting(checkpoint, "interaction-a", "correlation-a");
            case RESULT -> AwsDurableActionResponse.result("{\"approved\":true}");
            case SWEEP -> AwsDurableActionResponse.status("SWEEPED");
            case REDRIVE -> throw new AssertionError("driver must not re-drive implicitly");
            case QUERY_PENDING_AWAIT, READ_AWAIT_CHECKPOINT, READ_EXECUTION_AWAITS, BIND_CALLBACK ->
                throw new AssertionError("driver must register before resolving the TPF Await identity");
        };
        AwsDurableCoordinatorHandler handler = new AwsDurableCoordinatorHandler(actions);
        LocalDurableTestRunner<AwsDurableExecutionInput, AwsDurableExecutionOutput> runner =
            LocalDurableTestRunner.create(AwsDurableExecutionInput.class, handler)
                .withOutputType(AwsDurableExecutionOutput.class);
        AwsDurableExecutionInput input = new AwsDurableExecutionInput(
            "tenant-a", "stable-key", "pipeline-a", "contract-a", "release-a", "{}", Optional.empty(), 1);

        var pending = runner.run(input);

        assertThat(pending.getStatus())
            .withFailMessage("durable runner failed: %s", pending.getError()
                .map(error -> error.errorType() + ": " + error.errorMessage() + " " + error.stackTrace())
                .orElse("missing error"))
            .isEqualTo(ExecutionStatus.PENDING);
        assertThat(callbackId.get()).isNotBlank();
        runner.completeCallback(callbackId.get(), JSON.writeValueAsString(new AwsDurableCallbackSignal(
            "tenant-a", "execution-a", "interaction-a", "correlation-a", 1, "COMPLETED")));
        resumed.set(true);
        var completed = runner.runUntilComplete(input);

        assertThat(completed.isSucceeded()).isTrue();
        assertThat(completed.getResult()).isEqualTo(new AwsDurableExecutionOutput(
            "tenant-a", "execution-a", "pipeline-a", "contract-a", "release-a", 1,
            "{\"approved\":true}"));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void rejectsCallbackForWrongInteractionOrOccurrence(boolean wrongOccurrence) throws Exception {
        var checkpoint = new AwsDurableExecutionCheckpoint("tenant-a", "execution-a", "pipeline-a", "contract-a", "release-a");
        var callback = new AtomicReference<String>();
        AwsDurableActionInvoker actions = request -> switch (request.operation()) {
            case SUBMIT -> AwsDurableActionResponse.submitted(checkpoint, false);
            case STATUS -> waiting(checkpoint, "interaction-a", "correlation-a");
            case SWEEP -> AwsDurableActionResponse.status("SWEEPED");
            case REGISTER_CALLBACK -> {
                callback.set(request.providerCallbackId().orElseThrow());
                yield AwsDurableActionResponse.bound(true);
            }
            default -> throw new AssertionError("invalid callback must not advance execution");
        };
        var runner = LocalDurableTestRunner.create(AwsDurableExecutionInput.class, new AwsDurableCoordinatorHandler(actions))
            .withOutputType(AwsDurableExecutionOutput.class);
        var input = new AwsDurableExecutionInput("tenant-a", "key", "pipeline-a", "contract-a", "release-a", "{}", Optional.empty(), 1);
        assertThat(runner.run(input).getStatus()).isEqualTo(ExecutionStatus.PENDING);
        runner.completeCallback(callback.get(), JSON.writeValueAsString(new AwsDurableCallbackSignal(
            "tenant-a", "execution-a", wrongOccurrence ? "interaction-a" : "old-interaction", "correlation-a", 1,
            "COMPLETED", wrongOccurrence ? 2 : 1)));
        assertThat(runner.runUntilComplete(input).getStatus()).isEqualTo(ExecutionStatus.FAILED);
    }

    @Test
    void completionBetweenStatusAndCurrentAwaitReadDoesNotParkWithoutAnIdentity() {
        var checkpoint = new AwsDurableExecutionCheckpoint("tenant-a", "execution-a", "pipeline-a", "contract-a", "release-a");
        var polls = new java.util.concurrent.atomic.AtomicInteger();
        AwsDurableActionInvoker actions = request -> switch (request.operation()) {
            case SUBMIT -> AwsDurableActionResponse.submitted(checkpoint, false);
            case STATUS -> AwsDurableActionResponse.status(polls.getAndIncrement() == 0 ? "WAITING_EXTERNAL" : "SUCCEEDED");
            case SWEEP -> AwsDurableActionResponse.status("SWEEPED");
            case RESULT -> AwsDurableActionResponse.result("{}");
            default -> throw new AssertionError("missing current Await must not register a callback");
        };
        var runner = LocalDurableTestRunner.create(AwsDurableExecutionInput.class, new AwsDurableCoordinatorHandler(actions))
            .withOutputType(AwsDurableExecutionOutput.class);
        var input = new AwsDurableExecutionInput("tenant-a", "key", "pipeline-a", "contract-a", "release-a", "{}", Optional.empty(), 1);
        assertThat(runner.runUntilComplete(input).isSucceeded()).isTrue();
        assertThat(polls).hasValue(2);
    }

    @Test
    void terminalTpfFailureStopsPollingAndFailsTheDurableExecution() throws Exception {
        AwsDurableExecutionCheckpoint checkpoint = new AwsDurableExecutionCheckpoint(
            "tenant-a", "execution-failed", "pipeline-a", "contract-a", "release-a");
        AtomicBoolean resultRead = new AtomicBoolean();
        AwsDurableActionInvoker actions = request -> switch (request.operation()) {
            case SUBMIT -> AwsDurableActionResponse.submitted(checkpoint, false);
            case REGISTER_CALLBACK -> throw new AssertionError("terminal state must not register a callback");
            case STATUS -> AwsDurableActionResponse.status("FAILED");
            case RESULT -> {
                resultRead.set(true);
                yield AwsDurableActionResponse.result("{}");
            }
            case SWEEP -> AwsDurableActionResponse.status("SWEEPED");
            case REDRIVE -> throw new AssertionError("driver must not re-drive implicitly");
            case QUERY_PENDING_AWAIT, READ_AWAIT_CHECKPOINT, READ_EXECUTION_AWAITS, BIND_CALLBACK ->
                throw new AssertionError("driver must register before resolving the TPF Await identity");
        };
        AwsDurableCoordinatorHandler handler = new AwsDurableCoordinatorHandler(actions);
        LocalDurableTestRunner<AwsDurableExecutionInput, AwsDurableExecutionOutput> runner =
            LocalDurableTestRunner.create(AwsDurableExecutionInput.class, handler)
                .withOutputType(AwsDurableExecutionOutput.class);
        AwsDurableExecutionInput input = new AwsDurableExecutionInput(
            "tenant-a", "failed-key", "pipeline-a", "contract-a", "release-a", "{}", Optional.empty(), 1);

        var failed = runner.runUntilComplete(input);

        assertThat(failed.getStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(resultRead).isFalse();
    }

    @Test
    void supportsMoreThanOneTpfAwaitWithoutChangingExecutionIdentity() throws Exception {
        AwsDurableExecutionCheckpoint checkpoint = new AwsDurableExecutionCheckpoint(
            "tenant-a", "execution-many", "pipeline-a", "contract-a", "release-a");
        AtomicReference<String> callbackId = new AtomicReference<>();
        var registrations = new java.util.ArrayList<AwsDurableAwaitIdentity>();
        java.util.concurrent.atomic.AtomicInteger completedAwaits = new java.util.concurrent.atomic.AtomicInteger();
        AwsDurableActionInvoker actions = request -> switch (request.operation()) {
            case SUBMIT -> AwsDurableActionResponse.submitted(checkpoint, false);
            case REGISTER_CALLBACK -> {
                callbackId.set(request.providerCallbackId().orElseThrow());
                registrations.add(request.awaitIdentity().orElseThrow());
                yield AwsDurableActionResponse.bound(true);
            }
            case STATUS -> completedAwaits.get() < 2
                ? waiting(checkpoint, "interaction-" + (completedAwaits.get() + 1), "correlation-" + (completedAwaits.get() + 1))
                : AwsDurableActionResponse.status("SUCCEEDED");
            case RESULT -> AwsDurableActionResponse.result("{\"awaits\":2}");
            case SWEEP -> AwsDurableActionResponse.status("SWEEPED");
            case REDRIVE, QUERY_PENDING_AWAIT, READ_AWAIT_CHECKPOINT, READ_EXECUTION_AWAITS, BIND_CALLBACK ->
                throw new AssertionError("unexpected driver action " + request.operation());
        };
        AwsDurableExecutionInput input = new AwsDurableExecutionInput(
            "tenant-a", "many-key", "pipeline-a", "contract-a", "release-a", "{}", Optional.empty(), 1);
        LocalDurableTestRunner<AwsDurableExecutionInput, AwsDurableExecutionOutput> runner =
            LocalDurableTestRunner.create(AwsDurableExecutionInput.class, new AwsDurableCoordinatorHandler(actions))
                .withOutputType(AwsDurableExecutionOutput.class);

        assertThat(runner.run(input).getStatus()).isEqualTo(ExecutionStatus.PENDING);
        String firstCallback = callbackId.get();
        completedAwaits.incrementAndGet();
        runner.completeCallback(firstCallback, JSON.writeValueAsString(new AwsDurableCallbackSignal(
            "tenant-a", "execution-many", "interaction-1", "correlation-1", 1, "COMPLETED")));
        assertThat(runner.run(input).getStatus()).isEqualTo(ExecutionStatus.PENDING);
        String secondCallback = callbackId.get();
        assertThat(secondCallback).isNotEqualTo(firstCallback);
        completedAwaits.incrementAndGet();
        runner.completeCallback(secondCallback, JSON.writeValueAsString(new AwsDurableCallbackSignal(
            "tenant-a", "execution-many", "interaction-2", "correlation-2", 1, "COMPLETED", 2)));

        var completed = runner.runUntilComplete(input);

        assertThat(completed.isSucceeded()).isTrue();
        assertThat(completed.getResult().executionId()).isEqualTo("execution-many");
        assertThat(completed.getResult().resultJson()).isEqualTo("{\"awaits\":2}");
        assertThat(registrations).extracting(AwsDurableAwaitIdentity::occurrence).containsExactly(1L, 2L);
        assertThat(registrations).extracting(AwsDurableAwaitIdentity::interactionId).containsExactly("interaction-1", "interaction-2");
    }

    @Test
    void replacementGenerationResumesFromTheTpfCheckpointWithoutResubmitting() {
        AwsDurableAwaitIdentity previous = new AwsDurableAwaitIdentity(
            "tenant-a", "execution-a", "interaction-a", "correlation-a", 1);
        AtomicBoolean submitted = new AtomicBoolean();
        AtomicBoolean callbackBound = new AtomicBoolean();
        AwsDurableActionInvoker actions = request -> switch (request.operation()) {
            case SUBMIT -> {
                submitted.set(true);
                throw new AssertionError("replacement driver must not submit again");
            }
            case STATUS -> AwsDurableActionResponse.status("SUCCEEDED");
            case RESULT -> AwsDurableActionResponse.result("{\"approved\":true}");
            case SWEEP -> AwsDurableActionResponse.status("SWEEPED");
            case REGISTER_CALLBACK, BIND_CALLBACK -> {
                callbackBound.set(true);
                throw new AssertionError("terminal TPF state must not register a replacement callback");
            }
            case QUERY_PENDING_AWAIT, READ_AWAIT_CHECKPOINT, READ_EXECUTION_AWAITS ->
                throw new AssertionError("terminal TPF state has no pending Await");
            case REDRIVE -> throw new AssertionError("driver must not re-drive implicitly");
        };
        AwsDurableExecutionInput input = AwsDurableExecutionInput.resume(
            previous, "pipeline-a", "contract-a", "release-a");
        AwsDurableCoordinatorHandler handler = new AwsDurableCoordinatorHandler(actions);
        LocalDurableTestRunner<AwsDurableExecutionInput, AwsDurableExecutionOutput> runner =
            LocalDurableTestRunner.create(AwsDurableExecutionInput.class, handler)
                .withOutputType(AwsDurableExecutionOutput.class);

        var completed = runner.runUntilComplete(input);

        assertThat(completed.isSucceeded()).isTrue();
        assertThat(completed.getResult().executionId()).isEqualTo("execution-a");
        assertThat(completed.getResult().generation()).isEqualTo(2);
        assertThat(submitted).isFalse();
        assertThat(callbackBound).isFalse();
    }
}

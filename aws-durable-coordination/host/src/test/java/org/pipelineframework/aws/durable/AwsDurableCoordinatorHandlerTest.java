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
                yield AwsDurableActionResponse.bound(true);
            }
            case STATUS -> AwsDurableActionResponse.status(resumed.get() ? "SUCCEEDED" : "WAITING_EXTERNAL");
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
        java.util.concurrent.atomic.AtomicInteger completedAwaits = new java.util.concurrent.atomic.AtomicInteger();
        AwsDurableActionInvoker actions = request -> switch (request.operation()) {
            case SUBMIT -> AwsDurableActionResponse.submitted(checkpoint, false);
            case REGISTER_CALLBACK -> {
                callbackId.set(request.providerCallbackId().orElseThrow());
                yield AwsDurableActionResponse.bound(true);
            }
            case STATUS -> AwsDurableActionResponse.status(
                completedAwaits.get() < 2 ? "WAITING_EXTERNAL" : "SUCCEEDED");
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
            "tenant-a", "execution-many", "interaction-2", "correlation-2", 1, "COMPLETED")));

        var completed = runner.runUntilComplete(input);

        assertThat(completed.isSucceeded()).isTrue();
        assertThat(completed.getResult().executionId()).isEqualTo("execution-many");
        assertThat(completed.getResult().resultJson()).isEqualTo("{\"awaits\":2}");
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

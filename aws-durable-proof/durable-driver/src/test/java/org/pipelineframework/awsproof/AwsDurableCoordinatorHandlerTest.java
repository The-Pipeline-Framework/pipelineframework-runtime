package org.pipelineframework.awsproof;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awsproof.model.ProofActionResponse;
import org.pipelineframework.awsproof.model.ProofAwaitIdentity;
import org.pipelineframework.awsproof.model.ProofCallbackSignal;
import org.pipelineframework.awsproof.model.ProofExecutionCheckpoint;
import org.pipelineframework.awsproof.model.ProofExecutionInput;
import org.pipelineframework.awsproof.model.ProofExecutionOutput;
import software.amazon.lambda.durable.TypeToken;
import software.amazon.lambda.durable.model.ExecutionStatus;
import software.amazon.lambda.durable.testing.LocalDurableTestRunner;

class AwsDurableCoordinatorHandlerTest {
    private static final ObjectMapper JSON = new ObjectMapper().registerModule(new Jdk8Module());

    @Test
    void durableConfigurationRoundTripsResumeIdentityForReplay() {
        ProofExecutionInput input = ProofExecutionInput.resume(
            new ProofAwaitIdentity("tenant-a", "execution-a", "interaction-a", "correlation-a", 1),
            "pipeline-a",
            "contract-a",
            "release-a");
        AwsDurableCoordinatorHandler handler = new AwsDurableCoordinatorHandler(request -> {
            throw new AssertionError("serialization must not invoke an action");
        });

        String serialized = handler.getConfiguration().getSerDes().serialize(input);
        ProofExecutionInput replayed = handler.getConfiguration().getSerDes()
            .deserialize(serialized, TypeToken.get(ProofExecutionInput.class));

        assertThat(replayed).isEqualTo(input);
        assertThat(replayed.resumeExecutionId()).contains("execution-a");
    }

    @Test
    void callbackWakeupCarriesTheTpfIdentityAndPinnedRelease() throws Exception {
        ProofExecutionCheckpoint checkpoint = new ProofExecutionCheckpoint(
            "tenant-a", "execution-a", false, "pipeline-a", "contract-a", "release-a", 1);
        ProofAwaitIdentity await = new ProofAwaitIdentity(
            "tenant-a", "execution-a", "interaction-a", "correlation-a", 1);
        AtomicReference<String> callbackId = new AtomicReference<>();
        ProofActionInvoker actions = request -> switch (request.operation()) {
            case SUBMIT -> ProofActionResponse.submitted(checkpoint);
            case REGISTER_CALLBACK -> {
                callbackId.set(request.providerCallbackId().orElseThrow());
                yield ProofActionResponse.bound(true);
            }
            case STATUS -> ProofActionResponse.status("SUCCEEDED");
            case RESULT -> ProofActionResponse.result("{\"approved\":true}");
            case SWEEP -> ProofActionResponse.status("SWEEPED");
            case REDRIVE -> throw new AssertionError("driver must not re-drive implicitly");
            case QUERY_PENDING_AWAIT, BIND_CALLBACK ->
                throw new AssertionError("driver must register before resolving the TPF Await identity");
        };
        AwsDurableCoordinatorHandler handler = new AwsDurableCoordinatorHandler(actions);
        LocalDurableTestRunner<ProofExecutionInput, ProofExecutionOutput> runner =
            LocalDurableTestRunner.create(ProofExecutionInput.class, handler::handleRequest)
                .withOutputType(ProofExecutionOutput.class);
        ProofExecutionInput input = new ProofExecutionInput(
            "tenant-a", "stable-key", "pipeline-a", "contract-a", "release-a", "{}", Optional.empty(), 1);

        var pending = runner.run(input);

        assertThat(pending.getStatus())
            .withFailMessage("durable runner failed: %s", pending.getError()
                .map(error -> error.errorType() + ": " + error.errorMessage() + " " + error.stackTrace())
                .orElse("missing error"))
            .isEqualTo(ExecutionStatus.PENDING);
        assertThat(callbackId.get()).isNotBlank();
        runner.completeCallback(callbackId.get(), JSON.writeValueAsString(new ProofCallbackSignal(
            "tenant-a", "execution-a", "interaction-a", "correlation-a", 1, "COMPLETED")));
        var completed = runner.runUntilComplete(input);

        assertThat(completed.isSucceeded()).isTrue();
        assertThat(completed.getResult()).isEqualTo(new ProofExecutionOutput(
            "tenant-a", "execution-a", "pipeline-a", "contract-a", "release-a", 1,
            "{\"approved\":true}"));
    }

    @Test
    void terminalTpfFailureStopsPollingAndFailsTheDurableExecution() throws Exception {
        ProofExecutionCheckpoint checkpoint = new ProofExecutionCheckpoint(
            "tenant-a", "execution-failed", false, "pipeline-a", "contract-a", "release-a", 1);
        ProofAwaitIdentity await = new ProofAwaitIdentity(
            "tenant-a", "execution-failed", "interaction-a", "correlation-a", 1);
        AtomicReference<String> callbackId = new AtomicReference<>();
        AtomicBoolean resultRead = new AtomicBoolean();
        ProofActionInvoker actions = request -> switch (request.operation()) {
            case SUBMIT -> ProofActionResponse.submitted(checkpoint);
            case REGISTER_CALLBACK -> {
                callbackId.set(request.providerCallbackId().orElseThrow());
                yield ProofActionResponse.bound(true);
            }
            case STATUS -> ProofActionResponse.status("FAILED");
            case RESULT -> {
                resultRead.set(true);
                yield ProofActionResponse.result("{}");
            }
            case SWEEP -> ProofActionResponse.status("SWEEPED");
            case REDRIVE -> throw new AssertionError("driver must not re-drive implicitly");
            case QUERY_PENDING_AWAIT, BIND_CALLBACK ->
                throw new AssertionError("driver must register before resolving the TPF Await identity");
        };
        LocalDurableTestRunner<ProofExecutionInput, ProofExecutionOutput> runner =
            LocalDurableTestRunner.create(
                    ProofExecutionInput.class,
                    new AwsDurableCoordinatorHandler(actions)::handleRequest)
                .withOutputType(ProofExecutionOutput.class);
        ProofExecutionInput input = new ProofExecutionInput(
            "tenant-a", "failed-key", "pipeline-a", "contract-a", "release-a", "{}", Optional.empty(), 1);

        runner.run(input);
        runner.completeCallback(callbackId.get(), JSON.writeValueAsString(new ProofCallbackSignal(
            "tenant-a", "execution-failed", "interaction-a", "correlation-a", 1, "COMPLETED")));
        var failed = runner.runUntilComplete(input);

        assertThat(failed.getStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(resultRead).isFalse();
    }

    @Test
    void replacementGenerationResumesFromTheTpfCheckpointWithoutResubmitting() {
        ProofAwaitIdentity previous = new ProofAwaitIdentity(
            "tenant-a", "execution-a", "interaction-a", "correlation-a", 1);
        AtomicBoolean submitted = new AtomicBoolean();
        AtomicBoolean callbackBound = new AtomicBoolean();
        ProofActionInvoker actions = request -> switch (request.operation()) {
            case SUBMIT -> {
                submitted.set(true);
                throw new AssertionError("replacement driver must not submit again");
            }
            case STATUS -> ProofActionResponse.status("SUCCEEDED");
            case RESULT -> ProofActionResponse.result("{\"approved\":true}");
            case SWEEP -> ProofActionResponse.status("SWEEPED");
            case REGISTER_CALLBACK, BIND_CALLBACK -> {
                callbackBound.set(true);
                throw new AssertionError("terminal TPF state must not register a replacement callback");
            }
            case QUERY_PENDING_AWAIT -> throw new AssertionError("terminal TPF state has no pending Await");
            case REDRIVE -> throw new AssertionError("driver must not re-drive implicitly");
        };
        ProofExecutionInput input = ProofExecutionInput.resume(
            previous, "pipeline-a", "contract-a", "release-a");
        LocalDurableTestRunner<ProofExecutionInput, ProofExecutionOutput> runner =
            LocalDurableTestRunner.create(
                    ProofExecutionInput.class,
                    new AwsDurableCoordinatorHandler(actions)::handleRequest)
                .withOutputType(ProofExecutionOutput.class);

        var completed = runner.runUntilComplete(input);

        assertThat(completed.isSucceeded()).isTrue();
        assertThat(completed.getResult().executionId()).isEqualTo("execution-a");
        assertThat(completed.getResult().generation()).isEqualTo(2);
        assertThat(submitted).isFalse();
        assertThat(callbackBound).isFalse();
    }
}

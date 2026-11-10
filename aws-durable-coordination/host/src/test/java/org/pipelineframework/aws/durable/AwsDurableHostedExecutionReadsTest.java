package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableOperation;
import org.pipelineframework.aws.durable.model.AwsDurableRecoveryLocator;
import org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;

class AwsDurableHostedExecutionReadsTest {
    @Test
    void hostedReadsRequireTheExactExecutionAndReturnRetainedEvidenceWithoutACallback() throws Exception {
        var controlPlane = mock(PipelineControlPlane.class);
        var repository = mock(AwsDurableCallbackBindingRepository.class);
        var pin = driverCheckpoint();
        var semantic = new ExecutionSemanticCheckpoint("tenant-1", "execution-1", "pipeline-1", "contract-1",
            "release-1", ExecutionStatus.SUCCEEDED);
        when(controlPlane.getExecutionSemanticCheckpoint("tenant-1", "execution-1"))
            .thenReturn(Uni.createFrom().item(semantic));
        var locator = new AwsDurableRecoveryLocator(pin, "native-name", "arn:retained-execution");
        when(repository.findCurrentRecovery(pin)).thenReturn(Optional.of(locator));
        var payload = new SerializedTransitionPayload("app.Result", "application/tpf-transition+json", "{}");
        when(controlPlane.getExecutionTypedResult(semantic)).thenReturn(Uni.createFrom().item(payload));
        var mapper = new ObjectMapper();
        var actions = new AwsDurableControlPlaneActions(
            controlPlane, repository, input -> input, mapper, Duration.ofSeconds(5), Duration.ofDays(1));

        var lookup = pinnedRead(AwsDurableOperation.READ_EXECUTION_LOCATOR, "release-1");
        var typed = pinnedRead(AwsDurableOperation.TYPED_RESULT, "release-1");
        assertThat(mapper.readValue(actions.invoke(lookup).resultJson().orElseThrow(), AwsDurableRecoveryLocator.class))
            .isEqualTo(locator);
        assertThat(mapper.readValue(actions.invoke(typed).resultJson().orElseThrow(), SerializedTransitionPayload.class))
            .isEqualTo(payload);
        var wrongRelease = assertThrows(IllegalStateException.class,
            () -> actions.invoke(pinnedRead(AwsDurableOperation.READ_EXECUTION_LOCATOR, "foreign")));
        assertThat(wrongRelease).hasMessageContaining("requested Release pin");
        when(repository.findCurrentRecovery(pin)).thenReturn(Optional.empty());
        assertThat(actions.invoke(lookup).resultJson()).contains("null");
        var stale = assertThrows(IllegalStateException.class, () -> actions.invoke(typed));
        assertThat(stale).hasMessageContaining("current execution locator");
        verify(controlPlane, times(1)).getExecutionTypedResult(semantic);

        when(repository.findCurrentRecovery(pin)).thenReturn(Optional.of(locator)).thenReturn(Optional.empty());
        var supersededDuringRead = assertThrows(IllegalStateException.class, () -> actions.invoke(typed));
        assertThat(supersededDuringRead).hasMessageContaining("current execution locator");
        verify(controlPlane, times(2)).getExecutionTypedResult(semantic);
    }

    private static AwsDurableActionRequest pinnedRead(AwsDurableOperation operation, String release) {
        var base = AwsDurableActionRequest.result(driverCheckpoint());
        return new AwsDurableActionRequest(operation, base.tenantId(), base.executionId(), base.idempotencyKey(),
            base.pipelineId(), base.contractVersion(), Optional.of(release), base.inputJson(), base.awaitIdentity(),
            base.providerExecutionName(), base.providerExecutionArn(), base.providerCallbackId(), base.expectedVersion(),
            base.reason(), base.generation());
    }

    private static AwsDurableDriverCheckpoint driverCheckpoint() {
        return new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint(
            "tenant-1", "execution-1", "pipeline-1", "contract-1", "release-1"), 7);
    }
}

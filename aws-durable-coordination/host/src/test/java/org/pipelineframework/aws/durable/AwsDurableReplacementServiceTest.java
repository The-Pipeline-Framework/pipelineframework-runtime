package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableBindingStatus;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionInput;
import org.pipelineframework.aws.durable.model.AwsDurableStartResponse;

class AwsDurableReplacementServiceTest {
    @Test
    void startsNextGenerationFromSemanticCheckpoint() {
        AwsDurableExecutionStarter starter = mock(AwsDurableExecutionStarter.class);
        when(starter.pinnedToExecution(org.mockito.ArgumentMatchers.anyString())).thenReturn(starter);
        AwsDurableCallbackBinding binding = binding();
        AwsDurableAwaitCheckpointReader checkpoints = (tenant, interaction) -> Optional.of(
            new AwsDurableAwaitCheckpoint(
                tenant, "execution-a", interaction, "correlation-a", "unit-a", "await-a", "COMPLETED",
                "pipeline-a", "contract-a", "release-a"));
        AwsDurableExecutionInput expected = AwsDurableExecutionInput.resume(
            binding.awaitIdentity(), "pipeline-a", "contract-a", "release-a");
        when(starter.start(expected)).thenReturn(new AwsDurableStartResponse("replacement", 202));

        assertThat(new AwsDurableReplacementService(checkpoints, starter, mock(AwsDurableCallbackBindingRepository.class)).startReplacement(binding)).isTrue();

        verify(starter).start(expected);
    }

    @Test
    void missingSemanticCheckpointDoesNotStartReplacement() {
        AwsDurableExecutionStarter starter = mock(AwsDurableExecutionStarter.class);
        when(starter.pinnedToExecution(org.mockito.ArgumentMatchers.anyString())).thenReturn(starter);

        assertThat(new AwsDurableReplacementService((tenant, interaction) -> Optional.empty(), starter, mock(AwsDurableCallbackBindingRepository.class))
            .startReplacement(binding())).isFalse();
    }

    @Test
    void retriesProviderStartAfterTheGenerationClaimSurvivesALostAcknowledgement() {
        var repository = mock(AwsDurableCallbackBindingRepository.class);
        var starter = mock(AwsDurableExecutionStarter.class);
        when(starter.pinnedToExecution(org.mockito.ArgumentMatchers.anyString())).thenReturn(starter);
        AwsDurableAwaitCheckpointReader checkpoints = (tenant, interaction) -> Optional.of(
            new AwsDurableAwaitCheckpoint(tenant, "execution-a", interaction, "correlation-a", "unit-a", "await-a",
                "COMPLETED", "pipeline-a", "contract-a", "release-a"));
        when(repository.claimGeneration(org.mockito.ArgumentMatchers.any()))
            .thenReturn(AwsDurableGenerationAdmission.CREATED, AwsDurableGenerationAdmission.REPLAYED);
        when(starter.start(org.mockito.ArgumentMatchers.any()))
            .thenThrow(new IllegalStateException("lost start acknowledgement"))
            .thenReturn(new AwsDurableStartResponse("replacement", 202));
        var service = new AwsDurableReplacementService(checkpoints, starter, repository);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> service.startReplacement(binding()))
            .hasMessageContaining("lost start acknowledgement");
        assertThat(service.startReplacement(binding())).isTrue();

        var order = org.mockito.Mockito.inOrder(repository, starter);
        var expected = AwsDurableExecutionInput.resume(binding().awaitIdentity(), "pipeline-a", "contract-a", "release-a");
        order.verify(repository).claimGeneration(org.mockito.ArgumentMatchers.any());
        order.verify(starter).start(expected);
        order.verify(repository).claimGeneration(org.mockito.ArgumentMatchers.any());
        order.verify(starter).start(expected);
    }

    @Test
    void laterGenerationDoesNotStartAnObsoleteReplacement() {
        var repository = mock(AwsDurableCallbackBindingRepository.class);
        var starter = mock(AwsDurableExecutionStarter.class);
        when(starter.pinnedToExecution(org.mockito.ArgumentMatchers.anyString())).thenReturn(starter);
        when(repository.claimGeneration(org.mockito.ArgumentMatchers.any())).thenReturn(AwsDurableGenerationAdmission.STALE);
        AwsDurableAwaitCheckpointReader checkpoints = (tenant, interaction) -> Optional.of(
            new AwsDurableAwaitCheckpoint(tenant, "execution-a", interaction, "correlation-a", "unit-a", "await-a",
                "COMPLETED", "pipeline-a", "contract-a", "release-a"));

        assertThat(new AwsDurableReplacementService(checkpoints, starter, repository).startReplacement(binding())).isTrue();
        org.mockito.Mockito.verifyNoInteractions(starter);
    }

    private static AwsDurableCallbackBinding binding() {
        return new AwsDurableCallbackBinding(
            new AwsDurableAwaitIdentity("tenant-a", "execution-a", "interaction-a", "correlation-a", 2),
            "provider-a", "arn:provider-a", "callback-a", AwsDurableBindingStatus.OPEN, 100, 1_000);
    }
}

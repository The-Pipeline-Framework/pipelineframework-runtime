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
        AwsDurableCallbackBinding binding = binding();
        AwsDurableAwaitCheckpointReader checkpoints = (tenant, interaction) -> Optional.of(
            new AwsDurableAwaitCheckpoint(
                tenant, "execution-a", interaction, "correlation-a", "unit-a", "await-a", "COMPLETED",
                "pipeline-a", "contract-a", "release-a"));
        AwsDurableExecutionInput expected = AwsDurableExecutionInput.resume(
            binding.awaitIdentity(), "pipeline-a", "contract-a", "release-a");
        when(starter.start(expected)).thenReturn(new AwsDurableStartResponse("replacement", 202));

        assertThat(new AwsDurableReplacementService(checkpoints, starter).startReplacement(binding)).isTrue();

        verify(starter).start(expected);
    }

    @Test
    void missingSemanticCheckpointDoesNotStartReplacement() {
        AwsDurableExecutionStarter starter = mock(AwsDurableExecutionStarter.class);

        assertThat(new AwsDurableReplacementService((tenant, interaction) -> Optional.empty(), starter)
            .startReplacement(binding())).isFalse();
    }

    private static AwsDurableCallbackBinding binding() {
        return new AwsDurableCallbackBinding(
            new AwsDurableAwaitIdentity("tenant-a", "execution-a", "interaction-a", "correlation-a", 2),
            "provider-a", "arn:provider-a", "callback-a", AwsDurableBindingStatus.OPEN, 100, 1_000);
    }
}

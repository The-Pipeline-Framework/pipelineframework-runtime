package org.pipelineframework.aws.durable.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;

import org.junit.jupiter.api.Test;

class AwsDurableModelTest {

    @Test
    void durableNamesAreStableAndGenerationFenced() {
        String first = AwsDurableExecutionNames.durableExecutionName("tenant-a", "stable-key", 1);

        assertThat(first).isEqualTo(AwsDurableExecutionNames.durableExecutionName("tenant-a", "stable-key", 1));
        assertThat(first).isNotEqualTo(AwsDurableExecutionNames.durableExecutionName("tenant-a", "stable-key", 2));
        assertThat(first).matches("tpf-[0-9a-f]{48}");
    }

    @Test
    void actionRequestsRejectMissingOperationDataWithoutNullSentinels() {
        assertThatThrownBy(() -> new AwsDurableActionRequest(
            AwsDurableOperation.STATUS,
            "tenant-a",
            Optional.of(" "),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("executionId");
    }

    @Test
    void callbackBindingKeysIncludeAwaitIdentityAndGeneration() {
        AwsDurableAwaitIdentity identity = new AwsDurableAwaitIdentity(
            "tenant-a", "execution-a", "interaction-a", "correlation-a", 3);
        AwsDurableCallbackBinding binding = new AwsDurableCallbackBinding(
            identity, "provider-execution", "arn:aws:lambda:region:account:function:name:1/durable-execution/id",
            "callback", AwsDurableBindingStatus.OPEN, 10, 20);

        assertThat(binding.partitionKey()).isEqualTo("tenant-a#interaction-a");
        assertThat(binding.sortKey()).isEqualTo("GEN#00000000000000000003");
        assertThat(AwsDurableCallbackBinding.generationSortKey(10))
            .isGreaterThan(AwsDurableCallbackBinding.generationSortKey(9));
    }

    @Test
    void callbackRegistrationIsKeyedBeforeAwaitIdentityAndBindsOnlyToItsCheckpoint() {
        AwsDurableExecutionCheckpoint execution = new AwsDurableExecutionCheckpoint(
            "tenant-a", "execution-a", "pipeline-a", "1", "release-a");
        AwsDurableDriverCheckpoint checkpoint = new AwsDurableDriverCheckpoint(execution, 3);
        AwsDurableCallbackRegistration registration = new AwsDurableCallbackRegistration(
            checkpoint,
            "provider-execution",
            "arn:aws:lambda:region:account:function:name:1/durable-execution/id",
            "callback",
            10,
            20);
        AwsDurableAwaitIdentity matching = new AwsDurableAwaitIdentity(
            "tenant-a", "execution-a", "interaction-a", "correlation-a", 3);

        assertThat(registration.partitionKey()).isEqualTo("tenant-a#execution-a");
        assertThat(registration.sortKey()).isEqualTo("REGISTRATION#00000000000000000003");
        assertThat(registration.bind(matching).awaitIdentity()).isEqualTo(matching);
        assertThatThrownBy(() -> registration.bind(new AwsDurableAwaitIdentity(
            "tenant-a", "execution-b", "interaction-a", "correlation-a", 3)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("does not belong");
    }

    @Test
    void providerGenerationWrapsRatherThanPollutesTheTpfCheckpoint() {
        AwsDurableExecutionCheckpoint execution = new AwsDurableExecutionCheckpoint(
            "tenant-a", "execution-a", "pipeline-a", "1", "release-a");

        assertThat(new AwsDurableDriverCheckpoint(execution, 2).execution()).isSameAs(execution);
        assertThat(execution.getClass().getRecordComponents())
            .extracting(java.lang.reflect.RecordComponent::getName)
            .containsExactly("tenantId", "executionId", "pipelineId", "contractVersion", "releaseVersion");
    }

    @Test
    void executionPollingUsesTheTpfTerminalStatuses() {
        assertThat(new AwsDurableExecutionStatusPoll("SUCCEEDED").succeeded()).isTrue();
        assertThat(new AwsDurableExecutionStatusPoll("SUCCEEDED").terminal()).isTrue();
        assertThat(new AwsDurableExecutionStatusPoll("FAILED").terminal()).isTrue();
        assertThat(new AwsDurableExecutionStatusPoll("DLQ").terminal()).isTrue();
        assertThat(new AwsDurableExecutionStatusPoll("REMOTE_OUTCOME_UNKNOWN").terminal()).isTrue();
        assertThat(new AwsDurableExecutionStatusPoll("WAIT_RETRY").terminal()).isFalse();
        assertThat(new AwsDurableExecutionStatusPoll("WAITING_EXTERNAL").terminal()).isFalse();
    }
}

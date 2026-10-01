package org.pipelineframework.awsproof.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;

import org.junit.jupiter.api.Test;

class ProofModelTest {

    @Test
    void durableNamesAreStableAndGenerationFenced() {
        String first = ProofExecutionNames.durableExecutionName("tenant-a", "stable-key", 1);

        assertThat(first).isEqualTo(ProofExecutionNames.durableExecutionName("tenant-a", "stable-key", 1));
        assertThat(first).isNotEqualTo(ProofExecutionNames.durableExecutionName("tenant-a", "stable-key", 2));
        assertThat(first).matches("tpf-[0-9a-f]{48}");
    }

    @Test
    void actionRequestsRejectMissingOperationDataWithoutNullSentinels() {
        assertThatThrownBy(() -> new ProofActionRequest(
            ProofOperation.STATUS,
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
        ProofAwaitIdentity identity = new ProofAwaitIdentity(
            "tenant-a", "execution-a", "interaction-a", "correlation-a", 3);
        ProofCallbackBinding binding = new ProofCallbackBinding(
            identity, "provider-execution", "arn:aws:lambda:region:account:function:name:1/durable-execution/id",
            "callback", ProofBindingStatus.OPEN, 10, 20);

        assertThat(binding.partitionKey()).isEqualTo("tenant-a#interaction-a");
        assertThat(binding.sortKey()).isEqualTo("GEN#00000000000000000003");
        assertThat(ProofCallbackBinding.generationSortKey(10))
            .isGreaterThan(ProofCallbackBinding.generationSortKey(9));
    }

    @Test
    void callbackRegistrationIsKeyedBeforeAwaitIdentityAndBindsOnlyToItsCheckpoint() {
        ProofExecutionCheckpoint execution = new ProofExecutionCheckpoint(
            "tenant-a", "execution-a", "pipeline-a", "1", "release-a");
        ProofDriverCheckpoint checkpoint = new ProofDriverCheckpoint(execution, 3);
        ProofCallbackRegistration registration = new ProofCallbackRegistration(
            checkpoint,
            "provider-execution",
            "arn:aws:lambda:region:account:function:name:1/durable-execution/id",
            "callback",
            10,
            20);
        ProofAwaitIdentity matching = new ProofAwaitIdentity(
            "tenant-a", "execution-a", "interaction-a", "correlation-a", 3);

        assertThat(registration.partitionKey()).isEqualTo("tenant-a#execution-a");
        assertThat(registration.sortKey()).isEqualTo("REGISTRATION#00000000000000000003");
        assertThat(registration.bind(matching).awaitIdentity()).isEqualTo(matching);
        assertThatThrownBy(() -> registration.bind(new ProofAwaitIdentity(
            "tenant-a", "execution-b", "interaction-a", "correlation-a", 3)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("does not belong");
    }

    @Test
    void providerGenerationWrapsRatherThanPollutesTheTpfCheckpoint() {
        ProofExecutionCheckpoint execution = new ProofExecutionCheckpoint(
            "tenant-a", "execution-a", "pipeline-a", "1", "release-a");

        assertThat(new ProofDriverCheckpoint(execution, 2).execution()).isSameAs(execution);
        assertThat(execution.getClass().getRecordComponents())
            .extracting(java.lang.reflect.RecordComponent::getName)
            .containsExactly("tenantId", "executionId", "pipelineId", "contractVersion", "releaseVersion");
    }

    @Test
    void executionPollingUsesTheTpfTerminalStatuses() {
        assertThat(new ProofExecutionStatusPoll("SUCCEEDED").succeeded()).isTrue();
        assertThat(new ProofExecutionStatusPoll("SUCCEEDED").terminal()).isTrue();
        assertThat(new ProofExecutionStatusPoll("FAILED").terminal()).isTrue();
        assertThat(new ProofExecutionStatusPoll("DLQ").terminal()).isTrue();
        assertThat(new ProofExecutionStatusPoll("REMOTE_OUTCOME_UNKNOWN").terminal()).isTrue();
        assertThat(new ProofExecutionStatusPoll("WAIT_RETRY").terminal()).isFalse();
        assertThat(new ProofExecutionStatusPoll("WAITING_EXTERNAL").terminal()).isFalse();
    }
}

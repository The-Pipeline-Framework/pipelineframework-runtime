package org.pipelineframework.awsproof;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awsproof.model.ProofAwaitIdentity;
import org.pipelineframework.awsproof.model.ProofBindingStatus;
import org.pipelineframework.awsproof.model.ProofCallbackBinding;
import org.pipelineframework.awsproof.model.ProofCallbackRegistration;
import org.pipelineframework.awsproof.model.ProofExecutionCheckpoint;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.assertj.core.api.Assertions.assertThat;

class ProofCallbackBindingRepositoryTest {
    private static final ProofAwaitIdentity AWAIT = new ProofAwaitIdentity(
        "tenant-a", "execution-a", "interaction-a", "correlation-a", 1);

    @Test
    void replayedAuthorityIgnoresReconstructedHousekeepingTimestamps() {
        ProofCallbackBinding original = binding("callback-a", 100, 1_000);
        ProofCallbackBinding reconstructed = binding("callback-a", 200, 2_000);

        assertThat(ProofCallbackBindingRepository.sameBindingAuthority(original, reconstructed)).isTrue();
    }

    @Test
    void generationCannotBeReboundToAnotherProviderCallback() {
        assertThat(ProofCallbackBindingRepository.sameBindingAuthority(
            binding("callback-a", 100, 1_000),
            binding("callback-b", 100, 1_000))).isFalse();
    }

    @Test
    void replayedRegistrationAuthorityIgnoresHousekeepingTimestamps() {
        ProofCallbackRegistration original = registration("callback-a", 100, 1_000);
        ProofCallbackRegistration replayed = registration("callback-a", 200, 2_000);

        assertThat(ProofCallbackBindingRepository.sameRegistrationAuthority(original, replayed)).isTrue();
    }

    @Test
    void generationCannotBeRegisteredToAnotherProviderCallback() {
        assertThat(ProofCallbackBindingRepository.sameRegistrationAuthority(
            registration("callback-a", 100, 1_000),
            registration("callback-b", 100, 1_000))).isFalse();
    }

    @Test
    void malformedBindingIsIsolatedFromTheReconciliationBatch() {
        Map<String, AttributeValue> malformed = Map.of(
            "pk", AttributeValue.fromS("malformed"),
            "sk", AttributeValue.fromS("GEN#00000000000000000001"),
            "record_type", AttributeValue.fromS("BINDING"));

        assertThat(ProofCallbackBindingRepository.decodeBinding(malformed)).isEmpty();
    }

    private static ProofCallbackBinding binding(String callbackId, long createdAt, long expiresAt) {
        return new ProofCallbackBinding(
            AWAIT,
            "provider-execution-a",
            "arn:aws:lambda:us-east-2:123456789012:function:proof:1/durable-execution/execution-a",
            callbackId,
            ProofBindingStatus.OPEN,
            createdAt,
            expiresAt);
    }

    private static ProofCallbackRegistration registration(String callbackId, long createdAt, long expiresAt) {
        return new ProofCallbackRegistration(
            new ProofExecutionCheckpoint(
                "tenant-a", "execution-a", false, "pipeline-a", "1", "release-a", 1),
            "provider-execution-a",
            "arn:aws:lambda:us-east-2:123456789012:function:proof:1/durable-execution/execution-a",
            callbackId,
            createdAt,
            expiresAt);
    }
}

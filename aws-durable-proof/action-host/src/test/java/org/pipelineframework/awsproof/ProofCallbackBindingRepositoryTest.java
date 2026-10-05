package org.pipelineframework.awsproof;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableBindingStatus;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.assertj.core.api.Assertions.assertThat;

class ProofCallbackBindingRepositoryTest {
    private static final AwsDurableAwaitIdentity AWAIT = new AwsDurableAwaitIdentity(
        "tenant-a", "execution-a", "interaction-a", "correlation-a", 1);

    @Test
    void replayedAuthorityIgnoresReconstructedHousekeepingTimestamps() {
        AwsDurableCallbackBinding original = binding("callback-a", 100, 1_000);
        AwsDurableCallbackBinding reconstructed = binding("callback-a", 200, 2_000);

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
        AwsDurableCallbackRegistration original = registration("callback-a", 100, 1_000);
        AwsDurableCallbackRegistration replayed = registration("callback-a", 200, 2_000);

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

    private static AwsDurableCallbackBinding binding(String callbackId, long createdAt, long expiresAt) {
        return new AwsDurableCallbackBinding(
            AWAIT,
            "provider-execution-a",
            "arn:aws:lambda:us-east-2:123456789012:function:proof:1/durable-execution/execution-a",
            callbackId,
            AwsDurableBindingStatus.OPEN,
            createdAt,
            expiresAt);
    }

    private static AwsDurableCallbackRegistration registration(String callbackId, long createdAt, long expiresAt) {
        return new AwsDurableCallbackRegistration(
            new AwsDurableDriverCheckpoint(
                new AwsDurableExecutionCheckpoint(
                    "tenant-a", "execution-a", "pipeline-a", "1", "release-a"),
                1),
            "provider-execution-a",
            "arn:aws:lambda:us-east-2:123456789012:function:proof:1/durable-execution/execution-a",
            callbackId,
            createdAt,
            expiresAt);
    }
}

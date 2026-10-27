package org.pipelineframework.aws.durable;

import java.util.Map;
import java.util.Optional;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.orchestrator.AwaitSemanticCheckpoint;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint;
import org.pipelineframework.orchestrator.ExecutionStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AwsDurableHostServicesStreamTest {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value = ExecutionStatus.class,
        names = {"SUCCEEDED", "FAILED", "DLQ", "REMOTE_OUTCOME_UNKNOWN"})
    void unregisteredTerminalExecutionIsConfirmedByCurrentSemanticAuthority(ExecutionStatus status) {
        var services = services();
        when(services.controlPlane.getExecutionSemanticCheckpoint("tenant", "execution"))
            .thenReturn(Uni.createFrom().item(new ExecutionSemanticCheckpoint(
                "tenant", "execution", "pipeline", "contract", "release", status)));
        assertThat(services.terminalWithoutRegistration(providerCheckpoint())).isTrue();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value = ExecutionStatus.class, names = {"RUNNING", "WAITING_EXTERNAL"})
    void unregisteredNonterminalExecutionMustStillRetry(ExecutionStatus status) {
        var services = services();
        when(services.controlPlane.getExecutionSemanticCheckpoint("tenant", "execution"))
            .thenReturn(Uni.createFrom().item(new ExecutionSemanticCheckpoint(
                "tenant", "execution", "pipeline", "contract", "release", status)));
        assertThat(services.terminalWithoutRegistration(providerCheckpoint())).isFalse();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"tenant", "execution", "pipeline", "contract", "release"})
    void terminalObservationCannotAcknowledgeAnotherIdentityOrRelease(String mismatch) {
        var services = services();
        when(services.controlPlane.getExecutionSemanticCheckpoint("tenant", "execution"))
            .thenReturn(Uni.createFrom().item(new ExecutionSemanticCheckpoint(
                "tenant".equals(mismatch) ? "other" : "tenant", "execution".equals(mismatch) ? "other" : "execution",
                "pipeline".equals(mismatch) ? "other" : "pipeline", "contract".equals(mismatch) ? "other" : "contract",
                "release".equals(mismatch) ? "other" : "release", ExecutionStatus.SUCCEEDED)));
        assertThatThrownBy(() -> services.terminalWithoutRegistration(providerCheckpoint()))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("does not match");
    }

    private static org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint providerCheckpoint() {
        return new org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint(
            "tenant", "execution", "interaction", "correlation", "unit", "step", "COMPLETED", "pipeline", "contract", "release");
    }

    @Test
    void keysOnlyEventReadsCurrentSemanticStateThroughTheControlPlane() {
        var services = services();
        when(services.controlPlane.getAwaitSemanticCheckpoint("tenant", "interaction"))
            .thenReturn(Uni.createFrom().item(Optional.of(checkpoint("tenant"))));

        assertThat(services.observeAwaitStream(record(Map.of(
            "tenant_id", string("tenant"), "interaction_id", string("interaction")))))
            .hasValueSatisfying(checkpoint -> {
                assertThat(checkpoint.executionId()).isEqualTo("execution");
                assertThat(checkpoint.correlationId()).isEqualTo("correlation");
                assertThat(checkpoint.status()).isEqualTo("COMPLETED");
                assertThat(checkpoint.releaseVersion()).isEqualTo("release");
            });
        verify(services.controlPlane).getAwaitSemanticCheckpoint("tenant", "interaction");
    }

    @Test
    void malformedKeyDoesNotGuessIdentityFromAnImageOrReadOtherSemanticState() {
        var services = services();
        assertThatThrownBy(() -> services.observeAwaitStream(record(Map.of("pk", string("tenant#interaction")))))
            .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(services.controlPlane);
    }

    @Test
    void mismatchedSemanticIdentityStaysUnresolved() {
        var services = services();
        when(services.controlPlane.getAwaitSemanticCheckpoint("tenant", "interaction"))
            .thenReturn(Uni.createFrom().item(Optional.of(checkpoint("other-tenant"))));
        assertThat(services.observeAwaitStream(record(Map.of(
            "tenant_id", string("tenant"), "interaction_id", string("interaction"))))).isEmpty();
    }

    @Test
    void semanticReadFailureKeepsTheSameStreamSequenceRetryable() {
        var services = services();
        when(services.controlPlane.getAwaitSemanticCheckpoint("tenant", "interaction"))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("semantic store unavailable")));
        var processor = new AwsDurableAwaitStreamProcessor(mock(AwsDurableCallbackBindingRepository.class),
            mock(AwsDurableActionInvoker.class), mock(AwsDurableWakeupService.class),
            "arn:await-stream", "arn:binding-stream", services::observeAwaitStream, ignoredRegistration -> Optional.empty());
        var record = record(Map.of("tenant_id", string("tenant"), "interaction_id", string("interaction")));
        record.setEventSourceARN("arn:await-stream");
        record.setEventName("MODIFY");
        record.getDynamodb().setSequenceNumber("sequence");
        var event = new DynamodbEvent();
        event.setRecords(java.util.List.of(record));

        assertThat(new AwsDurableAwaitStreamBatchAdapter().handle(event, processor::process).getBatchItemFailures())
            .extracting(item -> item.getItemIdentifier()).containsExactly("sequence");
    }

    private static AwsDurableHostServices services() {
        var services = new AwsDurableHostServices();
        services.controlPlane = mock(PipelineControlPlane.class);
        return services;
    }

    private static AwaitSemanticCheckpoint checkpoint(String tenant) {
        return new AwaitSemanticCheckpoint(tenant, "execution", "interaction", "correlation", "unit", "step",
            AwaitInteractionStatus.COMPLETED, "pipeline", "contract", "release");
    }

    private static DynamodbEvent.DynamodbStreamRecord record(Map<String, AttributeValue> keys) {
        var record = new DynamodbEvent.DynamodbStreamRecord();
        var stream = new StreamRecord();
        stream.setKeys(keys);
        record.setDynamodb(stream);
        return record;
    }

    private static AttributeValue string(String value) {
        return new AttributeValue().withS(value);
    }
}

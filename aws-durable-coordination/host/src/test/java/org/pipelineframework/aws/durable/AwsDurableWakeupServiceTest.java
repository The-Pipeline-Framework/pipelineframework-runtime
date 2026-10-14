package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableBindingStatus;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding;

class AwsDurableWakeupServiceTest {
    private static final AwsDurableAwaitIdentity IDENTITY = new AwsDurableAwaitIdentity(
        "tenant-a", "execution-a", "interaction-a", "correlation-a", 2);

    @Test
    void doesNotWakeBeforeTpfHasAdmittedCompletion() {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableCallbackClient callbacks = mock(AwsDurableCallbackClient.class);
        AwsDurableWakeupService service = service(bindings, callbacks, "PENDING", binding(2));

        assertThat(service.wake(IDENTITY)).isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);

        verify(callbacks, never()).sendSuccessObserved(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void staleGenerationCannotWakeAfterReplacement() {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableCallbackClient callbacks = mock(AwsDurableCallbackClient.class);
        AwsDurableCallbackBinding current = binding(2);
        when(bindings.find(IDENTITY.tenantId(), IDENTITY.interactionId(), 2, 1)).thenReturn(Optional.of(current));
        when(bindings.findLatest(IDENTITY.tenantId(), IDENTITY.interactionId()))
            .thenReturn(Optional.of(binding(3)));
        AwsDurableWakeupService service = service(bindings, callbacks, "COMPLETED", current);

        assertThat(service.wake(IDENTITY)).isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);

        verify(callbacks, never()).sendSuccessObserved(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void completedSemanticCheckpointWakesCurrentGenerationOnce() {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableCallbackClient callbacks = mock(AwsDurableCallbackClient.class);
        AwsDurableCallbackBinding current = binding(2);
        when(bindings.find(IDENTITY.tenantId(), IDENTITY.interactionId(), 2, 1)).thenReturn(Optional.of(current));
        when(bindings.findLatest(IDENTITY.tenantId(), IDENTITY.interactionId()))
            .thenReturn(Optional.of(current));
        when(bindings.delivered(current)).thenReturn(false);
        when(callbacks.sendSuccessObserved(eq(current), any())).thenReturn(1_234L);
        AwsDurableWakeupService service = service(bindings, callbacks, "COMPLETED", current);

        assertThat(service.wake(IDENTITY)).isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);

        verify(callbacks).sendSuccessObserved(org.mockito.ArgumentMatchers.eq(current), org.mockito.ArgumentMatchers.any());
        verify(bindings).recordCallbackAccepted(current, 1_234L);
        verify(bindings).recordDelivered(current, "CALLBACK_ACCEPTED");
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void fencedWakeRetriesReplacementStartWithoutCallingTheOldCallback(boolean bindingMissing) {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var callbacks = mock(AwsDurableCallbackClient.class);
        var replacements = mock(AwsDurableReplacementStarter.class);
        var current = binding(2);
        when(bindings.isSuperseded("tenant-a", "execution-a", 2)).thenReturn(true);
        when(bindings.find("tenant-a", "interaction-a", 2, 1))
            .thenReturn(bindingMissing ? Optional.empty() : Optional.of(current));
        when(bindings.findRegistration("tenant-a", "execution-a", 2, 1)).thenReturn(Optional.of(
            new org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration(
                new org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint(
                    new org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint(
                        "tenant-a", "execution-a", "pipeline-a", "contract-a", "release-a"), 2),
                current.providerExecutionName(), current.providerExecutionArn(), current.providerCallbackId(),
                current.createdAtEpochMs(), current.expiresAtEpochS())));
        when(replacements.startReplacement(current)).thenReturn(false, true);
        AwsDurableAwaitCheckpointReader checkpoints = (tenant, interaction) -> Optional.of(
            new AwsDurableAwaitCheckpoint(tenant, "execution-a", interaction, "correlation-a", "unit-a", "await-a",
                "COMPLETED", "pipeline-a", "contract-a", "release-a"));
        var service = new AwsDurableWakeupService(checkpoints, bindings, callbacks, replacements);

        assertThat(service.wake(IDENTITY)).isEqualTo(AwsDurableWakeupDisposition.RETRY);
        assertThat(service.wake(IDENTITY)).isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        org.mockito.Mockito.verifyNoInteractions(callbacks);
    }

    private static AwsDurableWakeupService service(
        AwsDurableCallbackBindingRepository bindings,
        AwsDurableCallbackClient callbacks,
        String status,
        AwsDurableCallbackBinding current
    ) {
        AwsDurableAwaitCheckpointReader reader = (tenantId, interactionId) -> Optional.of(
            new AwsDurableAwaitCheckpoint(
                tenantId, "execution-a", interactionId, "correlation-a", "unit-a", "await-a", status,
                "pipeline-a", "contract-a", "release-a"));
        when(bindings.isCurrentGeneration(IDENTITY.tenantId(), IDENTITY.executionId(), 2)).thenReturn(true);
        when(bindings.find(IDENTITY.tenantId(), IDENTITY.interactionId(), 2, 1))
            .thenReturn(Optional.of(current));
        return new AwsDurableWakeupService(reader, bindings, callbacks, ignored -> true);
    }

    private static AwsDurableCallbackBinding binding(long generation) {
        return new AwsDurableCallbackBinding(
            new AwsDurableAwaitIdentity(
                "tenant-a", "execution-a", "interaction-a", "correlation-a", generation),
            "provider-execution-" + generation,
            "arn:aws:lambda:eu-west-1:123456789012:function:driver:1/durable-execution/execution-" + generation,
            "callback-" + generation,
            AwsDurableBindingStatus.OPEN,
            100,
            1_000);
    }
}

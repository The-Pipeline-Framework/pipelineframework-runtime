package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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

        verify(callbacks, never()).sendSuccess(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void staleGenerationCannotWakeAfterReplacement() {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableCallbackClient callbacks = mock(AwsDurableCallbackClient.class);
        AwsDurableCallbackBinding current = binding(2);
        when(bindings.find(IDENTITY.tenantId(), IDENTITY.interactionId(), 2)).thenReturn(Optional.of(current));
        when(bindings.findLatest(IDENTITY.tenantId(), IDENTITY.interactionId()))
            .thenReturn(Optional.of(binding(3)));
        AwsDurableWakeupService service = service(bindings, callbacks, "COMPLETED", current);

        assertThat(service.wake(IDENTITY)).isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);

        verify(callbacks, never()).sendSuccess(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void completedSemanticCheckpointWakesCurrentGenerationOnce() {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableCallbackClient callbacks = mock(AwsDurableCallbackClient.class);
        AwsDurableCallbackBinding current = binding(2);
        when(bindings.find(IDENTITY.tenantId(), IDENTITY.interactionId(), 2)).thenReturn(Optional.of(current));
        when(bindings.findLatest(IDENTITY.tenantId(), IDENTITY.interactionId()))
            .thenReturn(Optional.of(current));
        when(bindings.delivered(current)).thenReturn(false);
        AwsDurableWakeupService service = service(bindings, callbacks, "COMPLETED", current);

        assertThat(service.wake(IDENTITY)).isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);

        verify(callbacks).sendSuccess(org.mockito.ArgumentMatchers.eq(current), org.mockito.ArgumentMatchers.any());
        verify(bindings).recordDelivered(current, "CALLBACK_ACCEPTED");
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
        when(bindings.find(IDENTITY.tenantId(), IDENTITY.interactionId(), 2))
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

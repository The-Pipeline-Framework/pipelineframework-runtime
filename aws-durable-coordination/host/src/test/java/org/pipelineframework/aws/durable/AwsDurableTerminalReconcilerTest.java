package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;

class AwsDurableTerminalReconcilerTest {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void retriesReplacementAdmissionEvenAfterItsClaimFencesTheOldBinding(boolean superseded) {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableActionInvoker actions = mock(AwsDurableActionInvoker.class);
        AwsDurableReplacementStarter replacements = mock(AwsDurableReplacementStarter.class);
        AwsDurableProviderExecutionInspector providerExecutions = mock(AwsDurableProviderExecutionInspector.class);
        AwsDurableCallbackRegistration registration = registration();
        when(providerExecutions.inspect("arn:closed"))
            .thenReturn(AwsDurableProviderExecutionState.REPLACEABLE);
        when(bindings.findRegistrationByProviderExecutionArn("arn:closed"))
            .thenReturn(Optional.of(registration));
        when(actions.invoke(AwsDurableActionRequest.status(registration.checkpoint())))
            .thenReturn(AwsDurableActionResponse.status("WAITING_EXTERNAL"));
        when(actions.invoke(AwsDurableActionRequest.pendingAwait(registration.checkpoint())))
            .thenReturn(AwsDurableActionResponse.pendingAwait(registration.awaitIdentity().orElseThrow()));
        when(actions.invoke(AwsDurableActionRequest.awaitCheckpoint(registration.awaitIdentity().orElseThrow())))
            .thenReturn(AwsDurableActionResponse.awaitCheckpoint(
                new AwsDurableAwaitCheckpoint(
                    "tenant-a", "execution-a", "interaction-a", "correlation-a", "unit-a", "await-a",
                    "COMPLETED", "pipeline-a", "contract-a", "release-a")));
        if (superseded) {
            org.mockito.Mockito.doThrow(new AwsDurableStaleGenerationException())
                .when(bindings).bind(org.mockito.ArgumentMatchers.any());
        }
        when(replacements.startReplacement(org.mockito.ArgumentMatchers.any())).thenReturn(false, true);
        var reconciler = new AwsDurableTerminalReconciler(bindings, actions, replacements, providerExecutions);

        assertThatThrownBy(() -> reconciler.reconcile("arn:closed"))
            .hasMessageContaining("replacement admission is unresolved");
        assertThat(reconciler.reconcile("arn:closed")).isTrue();

        verify(bindings, org.mockito.Mockito.times(2)).bind(org.mockito.ArgumentMatchers.any());
        verify(replacements, org.mockito.Mockito.times(2)).startReplacement(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void providerLossBeforeSecondRegistrationRecoversCurrentAwaitWithoutRebindingOldCallback() {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var replacements = mock(AwsDurableReplacementStarter.class);
        var registration = registration();
        when(bindings.findRegistrationByProviderExecutionArn("arn:closed")).thenReturn(Optional.of(registration));
        var current = new org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity(
            "tenant-a", "execution-a", "interaction-2", "correlation-2", 1);
        AwsDurableActionInvoker actions = request -> switch (request.operation()) {
            case STATUS -> AwsDurableActionResponse.status("WAITING_EXTERNAL");
            case QUERY_PENDING_AWAIT -> AwsDurableActionResponse.pendingAwait(current);
            case READ_AWAIT_CHECKPOINT -> AwsDurableActionResponse.awaitCheckpoint(new AwsDurableAwaitCheckpoint(
                "tenant-a", "execution-a", "interaction-2", "correlation-2", "unit-2", "step-2", "WAITING",
                "pipeline-a", "contract-a", "release-a"));
            default -> throw new AssertionError("repair must not list historical Await records");
        };
        when(replacements.startReplacement(org.mockito.ArgumentMatchers.any())).thenReturn(true);
        assertThat(new AwsDurableTerminalReconciler(bindings, actions, replacements,
            arn -> AwsDurableProviderExecutionState.MISSING).reconcile("arn:closed")).isTrue();
        verify(bindings, org.mockito.Mockito.never()).bind(org.mockito.ArgumentMatchers.any());
        var captured = org.mockito.ArgumentCaptor.forClass(org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding.class);
        verify(replacements).startReplacement(captured.capture());
        assertThat(captured.getValue().awaitIdentity().interactionId()).isEqualTo("interaction-2");
        assertThat(captured.getValue().awaitIdentity().occurrence()).isEqualTo(2);
    }

    @Test
    void activeProviderExecutionIsNotReplaced() {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableProviderExecutionInspector providerExecutions = arn -> AwsDurableProviderExecutionState.ACTIVE;

        assertThat(new AwsDurableTerminalReconciler(
            bindings, mock(AwsDurableActionInvoker.class), mock(AwsDurableReplacementStarter.class),
            providerExecutions).reconcile("arn:running")).isFalse();

        org.mockito.Mockito.verifyNoInteractions(bindings);
    }

    @Test
    void terminalProviderDoesNotReplaceTerminalTpfExecution() {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableActionInvoker actions = mock(AwsDurableActionInvoker.class);
        AwsDurableReplacementStarter replacements = mock(AwsDurableReplacementStarter.class);
        AwsDurableCallbackRegistration registration = registration();
        when(bindings.findRegistrationByProviderExecutionArn("arn:closed"))
            .thenReturn(Optional.of(registration));
        when(actions.invoke(AwsDurableActionRequest.status(registration.checkpoint())))
            .thenReturn(AwsDurableActionResponse.status("SUCCEEDED"));

        assertThat(new AwsDurableTerminalReconciler(
            bindings, actions, replacements, arn -> AwsDurableProviderExecutionState.MISSING)
            .reconcile("arn:closed")).isFalse();

        org.mockito.Mockito.verifyNoInteractions(replacements);
    }

    @Test
    void indexLagKeepsTerminalRepairRetryable() {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableActionInvoker actions = mock(AwsDurableActionInvoker.class);
        AwsDurableReplacementStarter replacements = mock(AwsDurableReplacementStarter.class);
        when(bindings.findRegistrationByProviderExecutionArn("arn:closed")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> new AwsDurableTerminalReconciler(
            bindings, actions, replacements, arn -> AwsDurableProviderExecutionState.REPLACEABLE)
            .reconcile("arn:closed"))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("retry targeted reconciliation");
        org.mockito.Mockito.verifyNoInteractions(actions, replacements);
    }

    @Test
    void missingSemanticCheckpointDoesNotRetireRepair() {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableActionInvoker actions = mock(AwsDurableActionInvoker.class);
        AwsDurableReplacementStarter replacements = mock(AwsDurableReplacementStarter.class);
        when(bindings.findRegistrationByProviderExecutionArn("arn:closed")).thenReturn(Optional.of(registration()));
        when(actions.invoke(AwsDurableActionRequest.status(registration().checkpoint())))
            .thenReturn(AwsDurableActionResponse.status("WAITING_EXTERNAL"));
        when(actions.invoke(AwsDurableActionRequest.pendingAwait(registration().checkpoint())))
            .thenReturn(AwsDurableActionResponse.awaitCheckpoints(java.util.List.of()));

        assertThatThrownBy(() -> new AwsDurableTerminalReconciler(
            bindings, actions, replacements, arn -> AwsDurableProviderExecutionState.MISSING)
            .reconcile("arn:closed"))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("retry reconciliation");
        org.mockito.Mockito.verifyNoInteractions(replacements);
    }

    private static AwsDurableCallbackRegistration registration() {
        return new AwsDurableCallbackRegistration(
            new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint(
                "tenant-a", "execution-a", "pipeline-a", "contract-a", "release-a"), 1),
            "provider-a", "arn:closed", "callback-a", 100, 1_000, Optional.of(new org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity(
                "tenant-a", "execution-a", "interaction-a", "correlation-a", 1)));
    }
}

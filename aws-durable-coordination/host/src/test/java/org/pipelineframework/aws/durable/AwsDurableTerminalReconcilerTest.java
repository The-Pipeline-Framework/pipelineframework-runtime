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
    @Test
    void reconstructsOneBindingAndStartsOneReplacement() {
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
        when(actions.invoke(AwsDurableActionRequest.executionAwaits(registration.checkpoint())))
            .thenReturn(AwsDurableActionResponse.awaitCheckpoints(java.util.List.of(
                new AwsDurableAwaitCheckpoint(
                    "tenant-a", "execution-a", "interaction-a", "correlation-a", "unit-a", "await-a",
                    "COMPLETED", "pipeline-a", "contract-a", "release-a"))));
        when(replacements.startReplacement(org.mockito.ArgumentMatchers.any())).thenReturn(true);

        assertThat(new AwsDurableTerminalReconciler(bindings, actions, replacements, providerExecutions)
            .reconcile("arn:closed")).isTrue();

        verify(bindings).bind(org.mockito.ArgumentMatchers.any());
        verify(replacements).startReplacement(org.mockito.ArgumentMatchers.any());
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
        when(actions.invoke(AwsDurableActionRequest.executionAwaits(registration().checkpoint())))
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
            "provider-a", "arn:closed", "callback-a", 100, 1_000);
    }
}

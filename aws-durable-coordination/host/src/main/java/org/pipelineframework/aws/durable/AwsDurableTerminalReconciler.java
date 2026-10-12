package org.pipelineframework.aws.durable;

import java.util.Objects;

import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;

/** Bounded repair for one terminal provider execution event. */
public final class AwsDurableTerminalReconciler {
    private final AwsDurableCallbackBindingRepository bindings;
    private final AwsDurableActionInvoker actions;
    private final AwsDurableReplacementStarter replacements;
    private final AwsDurableProviderExecutionInspector providerExecutions;

    public AwsDurableTerminalReconciler(
        AwsDurableCallbackBindingRepository bindings,
        AwsDurableActionInvoker actions,
        AwsDurableReplacementStarter replacements,
        AwsDurableProviderExecutionInspector providerExecutions
    ) {
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.actions = Objects.requireNonNull(actions, "actions");
        this.replacements = Objects.requireNonNull(replacements, "replacements");
        this.providerExecutions = Objects.requireNonNull(providerExecutions, "providerExecutions");
    }

    public boolean reconcile(String providerExecutionArn) {
        AwsDurableProviderExecutionState providerState = providerExecutions.inspect(providerExecutionArn);
        if (providerState == AwsDurableProviderExecutionState.ACTIVE
            || providerState == AwsDurableProviderExecutionState.SUCCEEDED) {
            return false;
        }
        var registration = bindings.findRegistrationByProviderExecutionArn(providerExecutionArn)
            .orElseThrow(() -> new IllegalStateException(
                "provider registration is not visible; retry targeted reconciliation"));
        String semanticStatus = actions.invoke(AwsDurableActionRequest.status(registration.checkpoint()))
            .executionStatus().orElseThrow(() -> new IllegalStateException("TPF execution status is unavailable"));
        if (!"WAITING_EXTERNAL".equals(semanticStatus)) {
            return false;
        }
        registration.awaitIdentity()
            .orElseThrow(() -> new IllegalStateException("legacy registration requires a validated protocol upgrade"));
        var current = actions.invoke(AwsDurableActionRequest.pendingAwait(registration.checkpoint()))
            .awaitIdentity().orElseThrow(() -> new IllegalStateException("current Await checkpoint is unavailable; retry reconciliation"));
        AwsDurableAwaitCheckpoint checkpoint = actions.invoke(AwsDurableActionRequest.awaitCheckpoint(current)).awaitCheckpoint()
            .orElseThrow(() -> new IllegalStateException("current Await checkpoint is unavailable; retry reconciliation"));
        boolean sameInteraction = registration.awaitIdentity().map(identity ->
            identity.interactionId().equals(checkpoint.interactionId())
                && identity.correlationId().equals(checkpoint.correlationId())).orElse(true);
        var identity = new AwsDurableAwaitIdentity(checkpoint.tenantId(), checkpoint.executionId(),
            checkpoint.interactionId(), checkpoint.correlationId(), registration.checkpoint().generation(),
            sameInteraction ? registration.occurrence() : Math.addExact(registration.occurrence(), 1));
        var binding = sameInteraction ? registration.bind(identity)
            : new org.pipelineframework.aws.durable.model.AwsDurableCallbackBinding(identity,
                registration.providerExecutionName(), registration.providerExecutionArn(), registration.providerCallbackId(),
                org.pipelineframework.aws.durable.model.AwsDurableBindingStatus.OPEN,
                registration.createdAtEpochMs(), registration.expiresAtEpochS());
        if (sameInteraction) {
            try {
                bindings.bind(binding);
            } catch (AwsDurableStaleGenerationException stale) {
                // The successor claim fences this binding, but its provider start may still need replay.
            }
        }
        // A later semantic Await is a reconstruction locator, never authority to bind the old callback to it.
        if (!replacements.startReplacement(binding)) {
            throw new IllegalStateException("replacement admission is unresolved; retry reconciliation");
        }
        return true;
    }
}

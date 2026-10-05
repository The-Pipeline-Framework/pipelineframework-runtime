package org.pipelineframework.aws.durable;

import java.util.Objects;
import java.util.Optional;

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
        return bindings.findRegistrationByProviderExecutionArn(providerExecutionArn)
            .flatMap(registration -> {
                String semanticStatus = actions.invoke(AwsDurableActionRequest.status(registration.checkpoint()))
                    .executionStatus().orElse("");
                if (!"WAITING_EXTERNAL".equals(semanticStatus)) {
                    return Optional.empty();
                }
                Optional<AwsDurableAwaitCheckpoint> semantic = actions.invoke(
                    AwsDurableActionRequest.executionAwaits(registration.checkpoint()))
                    .awaitCheckpoints().stream().findFirst();
                return semantic.map(checkpoint -> registration.bind(new AwsDurableAwaitIdentity(
                    checkpoint.tenantId(), checkpoint.executionId(), checkpoint.interactionId(),
                    checkpoint.correlationId(), registration.checkpoint().generation())));
            })
            .map(binding -> {
                bindings.bind(binding);
                return replacements.startReplacement(binding);
            })
            .orElse(false);
    }
}

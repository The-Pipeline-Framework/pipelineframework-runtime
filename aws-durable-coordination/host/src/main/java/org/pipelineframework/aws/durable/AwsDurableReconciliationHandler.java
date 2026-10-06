package org.pipelineframework.aws.durable;

import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

/** Targeted terminal-event repair; it never scans TPF or provider state. */
@Named("aws-durable-reconcile")
@ApplicationScoped
public final class AwsDurableReconciliationHandler implements RequestHandler<Map<String, Object>, Boolean> {
    @Inject
    AwsDurableHostServices services;

    @Override
    public Boolean handleRequest(Map<String, Object> event, Context context) {
        return providerExecutionArn(event)
            .map(services.terminalReconciler()::reconcile)
            .orElse(false);
    }

    private static Optional<String> providerExecutionArn(Map<String, Object> event) {
        Object detail = event == null ? Map.of() : event.getOrDefault("detail", Map.of());
        if (!(detail instanceof Map<?, ?> values)) {
            return Optional.empty();
        }
        Object arn = values.get("durableExecutionArn");
        return arn instanceof String text && !text.isBlank() ? Optional.of(text) : Optional.empty();
    }
}

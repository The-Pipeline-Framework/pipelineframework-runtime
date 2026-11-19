package org.pipelineframework.orchestrator;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Caches immutable durable codec plans by pinned release and root canonical type expression. */
public final class DurablePayloadPlanRegistry {
    private final ConcurrentHashMap<DurablePayloadReleaseCoordinate, Map<String, CompiledDurablePayloadPlan>> plans = new ConcurrentHashMap<>();

    public void activate(DurablePayloadReleaseCoordinate release, Map<String, CanonicalPayloadBinding> bindings) {
        activate(release, bindings, Map.of());
    }

    /** Activates plans with the canonical variant bindings from the pinned release. */
    public void activate(DurablePayloadReleaseCoordinate release, Map<String, CanonicalPayloadBinding> bindings,
                         Map<String, Map<String, Object>> definitions) {
        plans.compute(release, (ignored, existing) -> {
            Map<String, CompiledDurablePayloadPlan> merged = new java.util.HashMap<>(
                existing == null ? Map.of() : existing);
            bindings.forEach((expression, binding) -> {
                CompiledDurablePayloadPlan prior = merged.get(expression);
                if (prior != null && !prior.binding().equals(binding)) {
                    throw new IllegalStateException("Conflicting durable payload binding for release " + release
                        + " and type expression " + expression);
                }
                if (prior == null) {
                    merged.put(expression, CompiledDurablePayloadPlan.compile(binding, definitions));
                }
            });
            return Map.copyOf(merged);
        });
    }

    public Optional<CompiledDurablePayloadPlan> find(
        DurablePayloadReleaseCoordinate release,
        String typeExpressionFingerprint
    ) {
        return Optional.ofNullable(plans.getOrDefault(release, Map.of()).get(typeExpressionFingerprint));
    }

    public CompiledDurablePayloadPlan plan(DurablePayloadReleaseCoordinate release, String typeExpressionFingerprint) {
        return find(release, typeExpressionFingerprint).orElseThrow(() ->
            new IllegalStateException("No durable payload plan for release " + release
                + " and type expression " + typeExpressionFingerprint));
    }
}

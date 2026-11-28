/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.runtime.spring.polyglot;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.pipelineframework.runtime.core.PipelineUnaryStep;
import org.pipelineframework.runtime.core.TpfExecutionContext;

/**
 * Cardinality-aware executor for Spring-hosted polyglot step chains covering {@code ONE_TO_ONE},
 * {@code ONE_TO_MANY}, {@code MANY_TO_MANY}, and {@code MANY_TO_ONE} transitions.
 *
 * <p>Validates stream/unary shape compatibility across adjacent steps at construction time so
 * mixed-cardinality pipelines are either executed faithfully through every step or rejected
 * explicitly at startup.</p>
 */
public final class SpringPolyglotPipelineExecutor
    implements PipelineUnaryStep<SpringCanonicalPayload, SpringCanonicalPayload> {

    private final List<SpringCanonicalFunctionHostStep> steps;

    public SpringPolyglotPipelineExecutor(List<SpringCanonicalFunctionHostStep> steps) {
        this.steps = List.copyOf(Objects.requireNonNull(steps, "steps"));
        if (this.steps.isEmpty()) {
            throw new IllegalArgumentException("SpringPolyglotPipelineExecutor requires at least one step");
        }
        validateUnaryBoundedShape(this.steps);
    }

    public List<SpringCanonicalFunctionHostStep> steps() {
        return steps;
    }

    /**
     * Validates that the ordered steps form a well-typed unary-in to unary-out pipeline
     * (supporting unary chains as well as fan-out / duplex / fan-in streaming segments).
     */
    public static void validateUnaryBoundedShape(List<SpringCanonicalFunctionHostStep> steps) {
        boolean streaming = false;
        for (SpringCanonicalFunctionHostStep step : steps) {
            String card = step.spec().cardinality();
            String id = step.spec().stepId();
            if (!streaming) {
                switch (card) {
                    case "ONE_TO_ONE" -> streaming = false;
                    case "ONE_TO_MANY" -> streaming = true;
                    case "MANY_TO_ONE", "MANY_TO_MANY" -> throw new IllegalStateException(
                        "Polyglot step '" + id + "' has cardinality " + card
                            + " which expects a streaming upstream, but the preceding stage produces a single item");
                    default -> throw new IllegalStateException("Unsupported cardinality '" + card + "' on step '" + id + "'");
                }
            } else {
                switch (card) {
                    case "MANY_TO_MANY" -> streaming = true;
                    case "MANY_TO_ONE" -> streaming = false;
                    case "ONE_TO_ONE", "ONE_TO_MANY" -> throw new IllegalStateException(
                        "Polyglot step '" + id + "' has cardinality " + card
                            + " which expects a single input, but the preceding stage produces a stream");
                    default -> throw new IllegalStateException("Unsupported cardinality '" + card + "' on step '" + id + "'");
                }
            }
        }
        if (streaming) {
            SpringCanonicalFunctionHostStep last = steps.get(steps.size() - 1);
            throw new IllegalStateException(
                "SpringPipelineRunner requires a unary final output, but the last polyglot step '"
                    + last.spec().stepId() + "' has streaming cardinality " + last.spec().cardinality());
        }
    }

    @Override
    public CompletionStage<SpringCanonicalPayload> apply(SpringCanonicalPayload input) {
        Objects.requireNonNull(input, "input");
        TpfExecutionContext.Snapshot context = TpfExecutionContext.capture();
        Optional<SpringTransitionDispatchScope> dispatchScope = SpringTransitionDispatchScope.current();
        return executeFromIndex(0, input, context, dispatchScope);
    }

    private CompletionStage<SpringCanonicalPayload> executeFromIndex(
        int startIndex,
        SpringCanonicalPayload currentInput,
        TpfExecutionContext.Snapshot context,
        Optional<SpringTransitionDispatchScope> dispatchScope
    ) {
        if (startIndex >= steps.size()) {
            return CompletableFuture.completedFuture(currentInput);
        }
        SpringCanonicalFunctionHostStep step = steps.get(startIndex);
        String card = step.spec().cardinality();
        if ("ONE_TO_ONE".equals(card)) {
            return runInContext(context, dispatchScope, () -> step.apply(currentInput))
                .thenCompose(next -> executeFromIndex(startIndex + 1, next, context, dispatchScope));
        }
        if ("ONE_TO_MANY".equals(card)) {
            Flow.Publisher<SpringCanonicalPayload> stream = runInContext(
                context, dispatchScope, () -> step.applyOneToMany(currentInput));
            int idx = startIndex + 1;
            while (idx < steps.size() && "MANY_TO_MANY".equals(steps.get(idx).spec().cardinality())) {
                SpringCanonicalFunctionHostStep duplexStep = steps.get(idx);
                Flow.Publisher<SpringCanonicalPayload> upstream = stream;
                stream = runInContext(context, dispatchScope, () -> duplexStep.applyManyToMany(upstream));
                idx++;
            }
            if (idx >= steps.size() || !"MANY_TO_ONE".equals(steps.get(idx).spec().cardinality())) {
                throw new IllegalStateException("Streaming segment did not terminate in a MANY_TO_ONE step");
            }
            SpringCanonicalFunctionHostStep collectStep = steps.get(idx);
            Flow.Publisher<SpringCanonicalPayload> finalStream = stream;
            int nextIndex = idx + 1;
            return runInContext(context, dispatchScope, () -> collectStep.applyManyToOne(finalStream))
                .thenCompose(collected -> executeFromIndex(nextIndex, collected, context, dispatchScope));
        }
        throw new IllegalStateException(
            "Unexpected cardinality '" + card + "' at unary boundary for step '" + step.spec().stepId() + "'");
    }

    private static <T> T runInContext(
        TpfExecutionContext.Snapshot context,
        Optional<SpringTransitionDispatchScope> dispatchScope,
        java.util.function.Supplier<T> action
    ) {
        TpfExecutionContext.Snapshot previous = TpfExecutionContext.capture();
        try (SpringTransitionDispatchScope.Scope ignored = SpringTransitionDispatchScope.withOptional(dispatchScope)) {
            TpfExecutionContext.restore(context);
            return action.get();
        } finally {
            TpfExecutionContext.restore(previous);
        }
    }
}

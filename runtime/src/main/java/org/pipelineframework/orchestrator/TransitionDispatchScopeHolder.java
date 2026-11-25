package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;

/**
 * Thread-local carrier for {@link TransitionDispatchScope} during standard transition execution.
 */
public final class TransitionDispatchScopeHolder {

    private static final ThreadLocal<TransitionDispatchScope> CURRENT = new ThreadLocal<>();

    private TransitionDispatchScopeHolder() {
    }

    public static Optional<TransitionDispatchScope> get() {
        return Optional.ofNullable(CURRENT.get());
    }

    public static void set(TransitionDispatchScope scope) {
        CURRENT.set(Objects.requireNonNull(scope, "scope must not be null"));
    }

    public static void clear() {
        CURRENT.remove();
    }
}

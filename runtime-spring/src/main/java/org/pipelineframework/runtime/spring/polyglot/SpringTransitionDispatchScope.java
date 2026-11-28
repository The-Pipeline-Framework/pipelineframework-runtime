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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pipelineframework.runtime.core.RuntimeAdapters;

/**
 * Durable attempt scope carried through {@link RuntimeAdapters} execution context for
 * Spring-hosted pre-send dispatch-intent reservation.
 */
public record SpringTransitionDispatchScope(
    String tenantId,
    String executionId,
    long executionVersion,
    int attempt,
    String transitionKey
) {
    private static final String CONTEXT_KEY = SpringTransitionDispatchScope.class.getName();

    public SpringTransitionDispatchScope {
        tenantId = requireNonBlank(tenantId, "tenantId");
        executionId = requireNonBlank(executionId, "executionId");
        if (executionVersion < 0L) {
            throw new IllegalArgumentException("executionVersion must be non-negative");
        }
        if (attempt < 0) {
            throw new IllegalArgumentException("attempt must be non-negative");
        }
        transitionKey = requireNonBlank(transitionKey, "transitionKey");
    }

    public static Optional<SpringTransitionDispatchScope> current() {
        return Optional.ofNullable(RuntimeAdapters.executionContext(CONTEXT_KEY, SpringTransitionDispatchScope.class));
    }

    public static Scope open(SpringTransitionDispatchScope scope) {
        Objects.requireNonNull(scope, "scope");
        return withOptional(Optional.of(scope));
    }

    public static Scope withOptional(Optional<SpringTransitionDispatchScope> scope) {
        Objects.requireNonNull(scope, "scope");
        Optional<SpringTransitionDispatchScope> previous = current();
        scope.ifPresentOrElse(
            value -> RuntimeAdapters.setExecutionContext(CONTEXT_KEY, value),
            () -> RuntimeAdapters.clearExecutionContext(CONTEXT_KEY));
        return new Scope(previous);
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    public static final class Scope implements AutoCloseable {
        private final Optional<SpringTransitionDispatchScope> previous;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Scope(Optional<SpringTransitionDispatchScope> previous) {
            this.previous = Objects.requireNonNull(previous, "previous");
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                previous.ifPresentOrElse(
                    value -> RuntimeAdapters.setExecutionContext(CONTEXT_KEY, value),
                    () -> RuntimeAdapters.clearExecutionContext(CONTEXT_KEY));
            }
        }
    }
}

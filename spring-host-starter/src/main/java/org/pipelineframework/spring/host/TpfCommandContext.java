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

package org.pipelineframework.spring.host;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Authorized dispatch context passed to {@link TpfCommandConnector} methods, including
 * TPF-owned dispatch identity and scoped secrets from TPF's host {@code ConnectionResolver}.
 */
public record TpfCommandContext(
    String operation,
    String commandId,
    String occurrenceId,
    String attemptId,
    String providerIdempotencyKey,
    Optional<String> scope,
    Map<String, String> secrets
) {
    public TpfCommandContext {
        if (operation == null || operation.isBlank()) {
            throw new IllegalArgumentException("operation must not be blank");
        }
        if (commandId == null || commandId.isBlank()) {
            throw new IllegalArgumentException("commandId must not be blank");
        }
        if (occurrenceId == null || occurrenceId.isBlank()) {
            throw new IllegalArgumentException("occurrenceId must not be blank");
        }
        if (attemptId == null || attemptId.isBlank()) {
            throw new IllegalArgumentException("attemptId must not be blank");
        }
        if (providerIdempotencyKey == null || providerIdempotencyKey.isBlank()) {
            throw new IllegalArgumentException("providerIdempotencyKey must not be blank");
        }
        scope = Objects.requireNonNull(scope, "scope must not be null").filter(v -> !v.isBlank());
        secrets = Map.copyOf(Objects.requireNonNull(secrets, "secrets must not be null"));
    }

    public Optional<String> secret(String key) {
        Objects.requireNonNull(key, "key must not be null");
        return Optional.ofNullable(secrets.get(key));
    }
}

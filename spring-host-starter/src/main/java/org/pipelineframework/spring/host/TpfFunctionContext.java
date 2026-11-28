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
 * Non-secret execution context passed to {@link TpfFunction} methods.
 *
 * <p>Per ADR-0092 secret isolation, business functions never receive connector credentials
 * or host secrets through this context.</p>
 */
public record TpfFunctionContext(
    String stepId,
    int stepIndex,
    String inputTypeId,
    String outputTypeId,
    Map<String, String> attributes
) {
    public TpfFunctionContext {
        if (stepId == null || stepId.isBlank()) {
            throw new IllegalArgumentException("stepId must not be blank");
        }
        if (stepIndex < 0) {
            throw new IllegalArgumentException("stepIndex must be non-negative");
        }
        inputTypeId = inputTypeId == null ? "" : inputTypeId;
        outputTypeId = outputTypeId == null ? "" : outputTypeId;
        attributes = Map.copyOf(Objects.requireNonNull(attributes, "attributes must not be null"));
    }

    public Optional<String> versionTag() {
        return Optional.ofNullable(attributes.get("versionTag")).filter(v -> !v.isBlank());
    }
}

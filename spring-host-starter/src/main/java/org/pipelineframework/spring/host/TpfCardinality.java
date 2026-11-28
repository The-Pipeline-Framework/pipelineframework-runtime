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

/** Cardinality of a Spring-hosted TPF foreign business function. */
public enum TpfCardinality {
    /** Infer cardinality from the annotated method's parameter and return types. */
    AUTO,
    /** Unary request/response ({@code POST /tpf/functions/{stepId}}). */
    ONE_TO_ONE,
    /** Streaming fan-out ({@code POST /tpf/functions/{stepId}/stream}). */
    ONE_TO_MANY,
    /** Streaming reducer ({@code POST /tpf/functions/{stepId}/reduce}). */
    MANY_TO_ONE,
    /** Full-duplex streaming transform ({@code POST /tpf/functions/{stepId}/duplex}). */
    MANY_TO_MANY
}

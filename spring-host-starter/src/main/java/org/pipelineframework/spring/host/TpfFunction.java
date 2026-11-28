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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Exposes a method on an ordinary Spring bean (such as {@code @Service} or {@code @Component})
 * as an ADR-0092 foreign business function step.
 *
 * <p>Supported method shapes:</p>
 * <ul>
 *   <li>{@code ONE_TO_ONE}: {@code Out(In)}, {@code Mono<Out>(In)}, or {@code CompletionStage<Out>(In)}</li>
 *   <li>{@code ONE_TO_MANY}: {@code Flux<Out>(In)}, {@code Stream<Out>(In)}, or {@code Iterable<Out>(In)}</li>
 *   <li>{@code MANY_TO_ONE}: {@code Out(Flux<In>)}, {@code Mono<Out>(Flux<In>)}, {@code CompletionStage<Out>(Flux<In>)}, or {@code Out(Stream<In>)}</li>
 *   <li>{@code MANY_TO_MANY}: {@code Flux<Out>(Flux<In>)}</li>
 * </ul>
 *
 * <p>An optional trailing {@link TpfFunctionContext} parameter may be declared to receive
 * non-secret step metadata (such as {@code stepId}, {@code stepIndex}, and {@code versionTag}).
 * Connector secrets are never delivered to business functions.</p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TpfFunction {

    /**
     * Step identifier matching {@code /tpf/functions/{stepId}}.
     * When blank, inferred by converting the Java method name from camelCase to kebab-case.
     */
    String stepId() default "";

    /**
     * Explicit step cardinality, or {@link TpfCardinality#AUTO} to infer from the method signature.
     */
    TpfCardinality cardinality() default TpfCardinality.AUTO;

    /**
     * Canonical output type identifier emitted in {@code outputTypeId} response envelopes and NDJSON frames.
     * When blank, falls back to {@code tpf.host.step-output-types}, the request envelope's {@code outputTypeId}
     * (for {@code ONE_TO_ONE}/{@code ONE_TO_MANY}), or the Java output element class simple name.
     */
    String outputTypeId() default "";
}

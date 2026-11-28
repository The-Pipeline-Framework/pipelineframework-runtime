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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Registry of Spring-hosted ADR-0092 foreign business functions and Tier-2 custom connector
 * operations.
 */
public final class TpfHostRegistry {

    /** Registered unary ({@code ONE_TO_ONE}) function entry. */
    public record OneToOneHandler(
        String stepId,
        Optional<String> defaultOutputTypeId,
        BiFunction<JsonNode, TpfFunctionContext, Mono<JsonNode>> handler
    ) {
        public OneToOneHandler {
            requireNonBlank(stepId, "stepId");
            defaultOutputTypeId = Objects.requireNonNull(defaultOutputTypeId, "defaultOutputTypeId")
                .filter(v -> !v.isBlank());
            Objects.requireNonNull(handler, "handler must not be null");
        }
    }

    /** Registered streaming fan-out ({@code ONE_TO_MANY}) function entry. */
    public record OneToManyHandler(
        String stepId,
        Optional<String> defaultOutputTypeId,
        BiFunction<JsonNode, TpfFunctionContext, Flux<JsonNode>> handler
    ) {
        public OneToManyHandler {
            requireNonBlank(stepId, "stepId");
            defaultOutputTypeId = Objects.requireNonNull(defaultOutputTypeId, "defaultOutputTypeId")
                .filter(v -> !v.isBlank());
            Objects.requireNonNull(handler, "handler must not be null");
        }
    }

    /** Registered streaming reducer ({@code MANY_TO_ONE}) function entry. */
    public record ManyToOneHandler(
        String stepId,
        String outputTypeId,
        BiFunction<Flux<JsonNode>, TpfFunctionContext, Mono<JsonNode>> handler
    ) {
        public ManyToOneHandler {
            requireNonBlank(stepId, "stepId");
            requireNonBlank(outputTypeId, "outputTypeId");
            Objects.requireNonNull(handler, "handler must not be null");
        }
    }

    /** Registered full-duplex streaming ({@code MANY_TO_MANY}) function entry. */
    public record ManyToManyHandler(
        String stepId,
        String outputTypeId,
        BiFunction<Flux<JsonNode>, TpfFunctionContext, Flux<JsonNode>> handler
    ) {
        public ManyToManyHandler {
            requireNonBlank(stepId, "stepId");
            requireNonBlank(outputTypeId, "outputTypeId");
            Objects.requireNonNull(handler, "handler must not be null");
        }
    }

    /** Registered Tier-2 Query Connector operation entry. */
    public record QueryHandler(
        String operation,
        BiFunction<JsonNode, TpfConnectorContext, Mono<TpfQueryResult<JsonNode>>> handler
    ) {
        public QueryHandler {
            requireNonBlank(operation, "operation");
            Objects.requireNonNull(handler, "handler must not be null");
        }
    }

    /** Registered Tier-2 Command Connector dispatch operation entry. */
    public record CommandHandler(
        String operation,
        BiFunction<JsonNode, TpfCommandContext, Mono<TpfCommandResult<JsonNode>>> handler
    ) {
        public CommandHandler {
            requireNonBlank(operation, "operation");
            Objects.requireNonNull(handler, "handler must not be null");
        }
    }

    /** Registered Tier-2 Command Connector reconciliation operation entry. */
    public record ReconcilerHandler(
        String operation,
        Function<TpfReconcileContext, Mono<TpfReconcileResult<JsonNode>>> handler
    ) {
        public ReconcilerHandler {
            requireNonBlank(operation, "operation");
            Objects.requireNonNull(handler, "handler must not be null");
        }
    }

    private final Map<String, OneToOneHandler> oneToOne = new LinkedHashMap<>();
    private final Map<String, OneToManyHandler> oneToMany = new LinkedHashMap<>();
    private final Map<String, ManyToOneHandler> manyToOne = new LinkedHashMap<>();
    private final Map<String, ManyToManyHandler> manyToMany = new LinkedHashMap<>();
    private final Map<String, QueryHandler> queries = new LinkedHashMap<>();
    private final Map<String, CommandHandler> commands = new LinkedHashMap<>();
    private final Map<String, ReconcilerHandler> reconcilers = new LinkedHashMap<>();

    public synchronized void registerOneToOne(OneToOneHandler entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        if (oneToOne.putIfAbsent(entry.stepId(), entry) != null) {
            throw new IllegalStateException("Duplicate ONE_TO_ONE TPF function registered for stepId: " + entry.stepId());
        }
    }

    public synchronized void registerOneToMany(OneToManyHandler entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        if (oneToMany.putIfAbsent(entry.stepId(), entry) != null) {
            throw new IllegalStateException("Duplicate ONE_TO_MANY TPF function registered for stepId: " + entry.stepId());
        }
    }

    public synchronized void registerManyToOne(ManyToOneHandler entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        if (manyToOne.putIfAbsent(entry.stepId(), entry) != null) {
            throw new IllegalStateException("Duplicate MANY_TO_ONE TPF function registered for stepId: " + entry.stepId());
        }
    }

    public synchronized void registerManyToMany(ManyToManyHandler entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        if (manyToMany.putIfAbsent(entry.stepId(), entry) != null) {
            throw new IllegalStateException("Duplicate MANY_TO_MANY TPF function registered for stepId: " + entry.stepId());
        }
    }

    public synchronized void registerQuery(QueryHandler entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        if (queries.putIfAbsent(entry.operation(), entry) != null) {
            throw new IllegalStateException("Duplicate TPF Query Connector registered for operation: " + entry.operation());
        }
    }

    public synchronized void registerCommand(CommandHandler entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        if (commands.putIfAbsent(entry.operation(), entry) != null) {
            throw new IllegalStateException("Duplicate TPF Command Connector registered for operation: " + entry.operation());
        }
    }

    public synchronized void registerReconciler(ReconcilerHandler entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        if (reconcilers.putIfAbsent(entry.operation(), entry) != null) {
            throw new IllegalStateException("Duplicate TPF Command Reconciler registered for operation: " + entry.operation());
        }
    }

    public synchronized Optional<OneToOneHandler> findOneToOne(String stepId) {
        return Optional.ofNullable(oneToOne.get(stepId));
    }

    public synchronized Optional<OneToManyHandler> findOneToMany(String stepId) {
        return Optional.ofNullable(oneToMany.get(stepId));
    }

    public synchronized Optional<ManyToOneHandler> findManyToOne(String stepId) {
        return Optional.ofNullable(manyToOne.get(stepId));
    }

    public synchronized Optional<ManyToManyHandler> findManyToMany(String stepId) {
        return Optional.ofNullable(manyToMany.get(stepId));
    }

    public synchronized Optional<QueryHandler> findQuery(String operation) {
        return Optional.ofNullable(queries.get(operation));
    }

    public synchronized Optional<CommandHandler> findCommand(String operation) {
        return Optional.ofNullable(commands.get(operation));
    }

    public synchronized Optional<ReconcilerHandler> findReconciler(String operation) {
        return Optional.ofNullable(reconcilers.get(operation));
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}

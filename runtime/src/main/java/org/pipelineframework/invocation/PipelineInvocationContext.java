/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.pipelineframework.invocation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.pipelineframework.invocation.PipelineInvocationPath.Frame;

/** Immutable framework-neutral identity and bound for structured recursive invocation. */
public record PipelineInvocationContext(PipelineInvocationPath path, int maxRecursiveDepth) {

    public PipelineInvocationContext {
        Objects.requireNonNull(path, "path");
        if (maxRecursiveDepth < 0) {
            throw new IllegalArgumentException("maxRecursiveDepth must be non-negative");
        }
    }

    public PipelineInvocationContext(List<Frame> recursivePath, int maxRecursiveDepth) {
        this(new PipelineInvocationPath(recursivePath), maxRecursiveDepth);
    }

    public List<Frame> recursivePath() {
        return path.frames();
    }

    public static PipelineInvocationContext root(int maxRecursiveDepth) {
        return new PipelineInvocationContext(List.of(), maxRecursiveDepth);
    }

    public PipelineInvocationContext enterRecursive(String definitionId, String callsiteId) {
        String definition = required(definitionId, "definitionId");
        String callsite = required(callsiteId, "callsiteId");
        int nextDepth = recursivePath().size() + 1;
        if (nextDepth > maxRecursiveDepth) {
            throw new PipelineRecursionLimitExceededException(
                definition, callsite, nextDepth, maxRecursiveDepth, recursivePath());
        }
        long occurrence = recursivePath().stream()
            .filter(frame -> frame.definitionId().equals(definition) && frame.callsiteId().equals(callsite))
            .count() + 1;
        List<Frame> childPath = new ArrayList<>(recursivePath());
        childPath.add(new Frame(definition, callsite, occurrence));
        return new PipelineInvocationContext(childPath, maxRecursiveDepth);
    }

    public int recursiveDepth() {
        return recursivePath().size();
    }

    private static String required(String value, String field) {
        String normalized = Objects.requireNonNull(value, field + " must not be null").strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return normalized;
    }
}

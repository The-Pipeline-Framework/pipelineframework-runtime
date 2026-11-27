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

import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.runtime.core.TpfExecutionContext;
import org.pipelineframework.runtime.spring.SpringPipelineRunner;

/**
 * Durable Await suspension and idempotent resume coordinator for Spring-hosted polyglot pipelines.
 *
 * <p>Uses {@link AwaitUnitRecord}, {@link AwaitUnitStatus}, and {@link TransitionAwaitSuspension}
 * from {@code pipelineframework-runtime-core}/{@code pipelineframework-runtime-protocol} so
 * pre-Await and post-Await polyglot {@link SpringCanonicalFunctionHostStep} stages preserve
 * durable wait state, correlation, duplicate-completion suppression, timeout handling,
 * {@link TpfExecutionContext}, and {@link SpringTransitionDispatchScope}.</p>
 */
public final class SpringAwaitResumeCoordinator {

    /** Durable state persisted while a Spring-hosted polyglot pipeline awaits external completion. */
    public record SuspendedAwaitState(
        AwaitUnitRecord unit,
        TransitionAwaitSuspension suspension,
        SpringCanonicalPayload suspendedPayload,
        TpfExecutionContext.Snapshot capturedContext,
        Optional<SpringCanonicalPayload> completionPayload
    ) {
        public SuspendedAwaitState {
            Objects.requireNonNull(unit, "unit");
            Objects.requireNonNull(suspension, "suspension");
            Objects.requireNonNull(suspendedPayload, "suspendedPayload");
            Objects.requireNonNull(capturedContext, "capturedContext");
            Objects.requireNonNull(completionPayload, "completionPayload");
        }
    }

    /** Outcome of starting a pipeline up to its durable Await boundary. */
    public record StartResult(
        AwaitUnitRecord unit,
        TransitionAwaitSuspension suspension,
        SpringCanonicalPayload preAwaitOutput
    ) {
        public StartResult {
            Objects.requireNonNull(unit, "unit");
            Objects.requireNonNull(suspension, "suspension");
            Objects.requireNonNull(preAwaitOutput, "preAwaitOutput");
        }
    }

    /** Classification of a completion admission and resume attempt. */
    public enum ResumeStatus {
        RESUMED,
        DUPLICATE_IGNORED,
        TERMINAL_REJECTED,
        UNKNOWN_UNIT
    }

    /** Result of admitting an external completion and resuming post-Await steps. */
    public record ResumeResult(
        ResumeStatus status,
        Optional<AwaitUnitRecord> unit,
        Optional<SpringCanonicalPayload> resumedOutput
    ) {
        public ResumeResult {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(unit, "unit");
            Objects.requireNonNull(resumedOutput, "resumedOutput");
        }
    }

    private final Clock clock;
    private final ConcurrentMap<String, SuspendedAwaitState> units = new ConcurrentHashMap<>();

    public SpringAwaitResumeCoordinator() {
        this(Clock.systemUTC());
    }

    public SpringAwaitResumeCoordinator(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Runs pre-Await steps through {@code preAwaitRunner} and durably suspends execution at
     * the specified Await step boundary.
     */
    public CompletionStage<StartResult> startAndSuspend(
        String tenantId,
        String executionId,
        String awaitStepId,
        int awaitStepIndex,
        Duration ttl,
        SpringPipelineRunner preAwaitRunner,
        SpringCanonicalPayload initialInput
    ) {
        requireNonBlank(tenantId, "tenantId");
        requireNonBlank(executionId, "executionId");
        requireNonBlank(awaitStepId, "awaitStepId");
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be positive");
        }
        Objects.requireNonNull(preAwaitRunner, "preAwaitRunner");
        Objects.requireNonNull(initialInput, "initialInput");

        TpfExecutionContext.Snapshot capturedContext = TpfExecutionContext.capture();
        return preAwaitRunner.run(initialInput).thenApply(rawOutput -> {
            if (!(rawOutput instanceof SpringCanonicalPayload suspendedPayload)) {
                throw new IllegalStateException(
                    "Pre-Await runner must produce SpringCanonicalPayload, got "
                        + (rawOutput == null ? "null" : rawOutput.getClass().getName()));
            }
            long nowMs = clock.millis();
            long ttlEpochS = (nowMs + ttl.toMillis()) / 1000L;
            String unitId = "await-" + UUID.randomUUID();
            String interactionId = "interaction-" + UUID.randomUUID();
            AwaitUnitRecord unit = new AwaitUnitRecord(
                tenantId,
                unitId,
                executionId,
                awaitStepId,
                awaitStepIndex,
                "ONE_TO_ONE",
                1L,
                AwaitUnitStatus.WAITING_EXTERNAL,
                interactionId,
                1,
                0,
                Set.of(),
                true,
                nowMs,
                nowMs,
                ttlEpochS);
            TransitionAwaitSuspension suspension = new TransitionAwaitSuspension(
                tenantId, executionId, unitId, awaitStepIndex, unit, java.util.List.of());
            SuspendedAwaitState state = new SuspendedAwaitState(
                unit, suspension, suspendedPayload, capturedContext, Optional.empty());
            units.put(unitKey(tenantId, unitId), state);
            return new StartResult(unit, suspension, suspendedPayload);
        });
    }

    /**
     * Admits an external completion idempotently and resumes {@code postAwaitRunner} with the
     * restored {@link TpfExecutionContext} and a fresh {@link SpringTransitionDispatchScope}.
     */
    public CompletionStage<ResumeResult> admitCompletionAndResume(
        String tenantId,
        String unitId,
        String completionKey,
        SpringCanonicalPayload completionPayload,
        SpringPipelineRunner postAwaitRunner
    ) {
        requireNonBlank(tenantId, "tenantId");
        requireNonBlank(unitId, "unitId");
        requireNonBlank(completionKey, "completionKey");
        Objects.requireNonNull(completionPayload, "completionPayload");
        Objects.requireNonNull(postAwaitRunner, "postAwaitRunner");

        String key = unitKey(tenantId, unitId);
        AdmissionOutcome admission = admitAtomically(key, completionKey, completionPayload);
        if (admission.status() != ResumeStatus.RESUMED) {
            return CompletableFuture.completedFuture(
                new ResumeResult(admission.status(), admission.updatedUnit(), Optional.empty()));
        }

        SuspendedAwaitState admittedState = admission.state().orElseThrow();
        AwaitUnitRecord completedUnit = admittedState.unit();
        TpfExecutionContext.Snapshot previous = TpfExecutionContext.capture();
        SpringTransitionDispatchScope resumeScope = new SpringTransitionDispatchScope(
            tenantId,
            completedUnit.executionId(),
            completedUnit.version(),
            1,
            "await-resume:" + unitId + ":" + completionKey);
        try {
            TpfExecutionContext.restore(admittedState.capturedContext());
            try (SpringTransitionDispatchScope.Scope ignored = SpringTransitionDispatchScope.open(resumeScope)) {
                return postAwaitRunner.run(completionPayload).thenApply(rawOutput -> {
                    if (!(rawOutput instanceof SpringCanonicalPayload output)) {
                        throw new IllegalStateException(
                            "Post-Await runner must produce SpringCanonicalPayload, got "
                                + (rawOutput == null ? "null" : rawOutput.getClass().getName()));
                    }
                    return new ResumeResult(ResumeStatus.RESUMED, Optional.of(completedUnit), Optional.of(output));
                });
            }
        } finally {
            TpfExecutionContext.restore(previous);
        }
    }

    /** Transitions an active {@code WAITING_EXTERNAL} await unit to {@code TIMED_OUT}. */
    public Optional<AwaitUnitRecord> timeoutUnit(String tenantId, String unitId) {
        requireNonBlank(tenantId, "tenantId");
        requireNonBlank(unitId, "unitId");
        String key = unitKey(tenantId, unitId);
        SuspendedAwaitState updated = units.computeIfPresent(key, (k, existing) -> {
            if (existing.unit().status().terminal()) {
                return existing;
            }
            AwaitUnitRecord u = existing.unit();
            AwaitUnitRecord timedOut = new AwaitUnitRecord(
                u.tenantId(), u.unitId(), u.executionId(), u.stepId(), u.stepIndex(), u.cardinality(),
                u.version() + 1L, AwaitUnitStatus.TIMED_OUT, u.primaryInteractionId(),
                u.expectedItemCount(), u.completedItemCount(), u.completedItemKeys(),
                u.dispatchComplete(), u.createdAtEpochMs(), clock.millis(), u.ttlEpochS());
            TransitionAwaitSuspension updatedSuspension = new TransitionAwaitSuspension(
                u.tenantId(), u.executionId(), u.unitId(), u.stepIndex(), timedOut, existing.suspension().interactions());
            return new SuspendedAwaitState(
                timedOut, updatedSuspension, existing.suspendedPayload(), existing.capturedContext(), existing.completionPayload());
        });
        return Optional.ofNullable(updated).map(SuspendedAwaitState::unit);
    }

    public Optional<SuspendedAwaitState> findUnit(String tenantId, String unitId) {
        requireNonBlank(tenantId, "tenantId");
        requireNonBlank(unitId, "unitId");
        return Optional.ofNullable(units.get(unitKey(tenantId, unitId)));
    }

    private AdmissionOutcome admitAtomically(String key, String completionKey, SpringCanonicalPayload completionPayload) {
        final AdmissionOutcome[] holder = new AdmissionOutcome[] {
            new AdmissionOutcome(ResumeStatus.UNKNOWN_UNIT, Optional.empty(), Optional.empty())
        };
        units.computeIfPresent(key, (k, existing) -> {
            AwaitUnitRecord current = existing.unit();
            if (current.completedItemKeys().contains(completionKey)) {
                holder[0] = new AdmissionOutcome(
                    ResumeStatus.DUPLICATE_IGNORED, Optional.of(current), Optional.of(existing));
                return existing;
            }
            if (current.status().terminal()) {
                holder[0] = new AdmissionOutcome(
                    ResumeStatus.TERMINAL_REJECTED, Optional.of(current), Optional.of(existing));
                return existing;
            }
            Set<String> nextKeys = new HashSet<>(current.completedItemKeys());
            nextKeys.add(completionKey);
            AwaitUnitRecord completed = new AwaitUnitRecord(
                current.tenantId(),
                current.unitId(),
                current.executionId(),
                current.stepId(),
                current.stepIndex(),
                current.cardinality(),
                current.version() + 1L,
                AwaitUnitStatus.COMPLETED,
                current.primaryInteractionId(),
                current.expectedItemCount(),
                current.completedItemCount() + 1,
                nextKeys,
                current.dispatchComplete(),
                current.createdAtEpochMs(),
                clock.millis(),
                current.ttlEpochS());
            TransitionAwaitSuspension updatedSuspension = new TransitionAwaitSuspension(
                completed.tenantId(), completed.executionId(), completed.unitId(), completed.stepIndex(),
                completed, existing.suspension().interactions());
            SuspendedAwaitState nextState = new SuspendedAwaitState(
                completed, updatedSuspension, existing.suspendedPayload(),
                existing.capturedContext(), Optional.of(completionPayload));
            holder[0] = new AdmissionOutcome(ResumeStatus.RESUMED, Optional.of(completed), Optional.of(nextState));
            return nextState;
        });
        return holder[0];
    }

    private record AdmissionOutcome(
        ResumeStatus status,
        Optional<AwaitUnitRecord> updatedUnit,
        Optional<SuspendedAwaitState> state
    ) {
    }

    private static String unitKey(String tenantId, String unitId) {
        return tenantId + "::" + unitId;
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}

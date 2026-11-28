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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.runtime.core.TpfExecutionContext;
import org.pipelineframework.runtime.spring.SpringPipelineRunner;

/**
 * Durable Await suspension and two-phase recoverable resume coordinator for Spring-hosted polyglot pipelines.
 *
 * <p>Persists all suspended, resuming, completed, and timed-out Await state through a pluggable
 * {@link SpringAwaitStateStore} so units survive process restarts. Admission transitions a unit to
 * {@link ResumePhase#RESUMING} (non-terminal) with its admitted payload and stable replay identity
 * before executing {@code postAwaitRunner}, and marks the unit {@link AwaitUnitStatus#COMPLETED}
 * only after post-Await execution succeeds. If post-Await execution fails or crashes mid-flight,
 * {@link #retryPendingResume(String, String, SpringPipelineRunner)} (or retrying the same
 * {@code completionKey}) recovers using the persisted payload and stable dispatch scope.</p>
 */
public final class SpringAwaitResumeCoordinator {

    /** Two-phase durability lifecycle of a suspended Spring Await unit. */
    public enum ResumePhase {
        WAITING,
        RESUMING,
        COMPLETED,
        TIMED_OUT
    }

    /** Durable state persisted while a Spring-hosted polyglot pipeline awaits or resumes completion. */
    public record SuspendedAwaitState(
        AwaitUnitRecord unit,
        TransitionAwaitSuspension suspension,
        SpringCanonicalPayload suspendedPayload,
        TpfExecutionContext.Snapshot capturedContext,
        Optional<SpringCanonicalPayload> completionPayload,
        ResumePhase phase,
        Optional<String> admittedCompletionKey
    ) {
        public SuspendedAwaitState {
            Objects.requireNonNull(unit, "unit");
            Objects.requireNonNull(suspension, "suspension");
            Objects.requireNonNull(suspendedPayload, "suspendedPayload");
            Objects.requireNonNull(capturedContext, "capturedContext");
            Objects.requireNonNull(completionPayload, "completionPayload");
            Objects.requireNonNull(phase, "phase");
            Objects.requireNonNull(admittedCompletionKey, "admittedCompletionKey");
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
        UNKNOWN_UNIT,
        RESUME_IN_PROGRESS
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

    private final SpringAwaitStateStore store;
    private final Clock clock;
    private final Set<String> activeResumes = ConcurrentHashMap.newKeySet();

    public SpringAwaitResumeCoordinator(SpringAwaitStateStore store) {
        this(store, Clock.systemUTC());
    }

    public SpringAwaitResumeCoordinator(SpringAwaitStateStore store, Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
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
                tenantId, executionId, unitId, awaitStepIndex, unit, List.of());
            SuspendedAwaitState state = new SuspendedAwaitState(
                unit,
                suspension,
                suspendedPayload,
                capturedContext,
                Optional.empty(),
                ResumePhase.WAITING,
                Optional.empty());
            store.saveNew(unitKey(tenantId, unitId), state);
            return new StartResult(unit, suspension, suspendedPayload);
        });
    }

    /**
     * Admits an external completion durably into {@link ResumePhase#RESUMING} and executes
     * {@code postAwaitRunner} with the restored {@link TpfExecutionContext} and a deterministic
     * {@link SpringTransitionDispatchScope}. Transitions to {@link AwaitUnitStatus#COMPLETED}
     * only after {@code postAwaitRunner} succeeds; failures leave the unit recoverable.
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
        AdmissionOutcome admission = admitForResumeAtomically(key, completionKey, completionPayload);
        if (admission.status() != ResumeStatus.RESUMED) {
            return CompletableFuture.completedFuture(
                new ResumeResult(admission.status(), admission.updatedUnit(), Optional.empty()));
        }
        return executePostAwaitAndFinalize(key, admission.state().orElseThrow(), postAwaitRunner);
    }

    /**
     * Recovers a unit that was durably admitted into {@link ResumePhase#RESUMING} (for example
     * after a post-Await failure or process restart), replaying with the persisted completion
     * payload, captured context, and identical {@link SpringTransitionDispatchScope} identity.
     */
    public CompletionStage<ResumeResult> retryPendingResume(
        String tenantId,
        String unitId,
        SpringPipelineRunner postAwaitRunner
    ) {
        requireNonBlank(tenantId, "tenantId");
        requireNonBlank(unitId, "unitId");
        Objects.requireNonNull(postAwaitRunner, "postAwaitRunner");

        String key = unitKey(tenantId, unitId);
        Optional<SuspendedAwaitState> existingOpt = store.find(key);
        if (existingOpt.isEmpty()) {
            return CompletableFuture.completedFuture(
                new ResumeResult(ResumeStatus.UNKNOWN_UNIT, Optional.empty(), Optional.empty()));
        }
        SuspendedAwaitState state = existingOpt.orElseThrow();
        if (state.phase() == ResumePhase.COMPLETED) {
            return CompletableFuture.completedFuture(
                new ResumeResult(ResumeStatus.DUPLICATE_IGNORED, Optional.of(state.unit()), Optional.empty()));
        }
        if (state.phase() == ResumePhase.TIMED_OUT || state.unit().status().terminal()) {
            return CompletableFuture.completedFuture(
                new ResumeResult(ResumeStatus.TERMINAL_REJECTED, Optional.of(state.unit()), Optional.empty()));
        }
        if (state.phase() != ResumePhase.RESUMING
            || state.completionPayload().isEmpty()
            || state.admittedCompletionKey().isEmpty()) {
            throw new IllegalStateException(
                "Await unit '" + unitId + "' has no durably admitted completion pending resume");
        }
        return executePostAwaitAndFinalize(key, state, postAwaitRunner);
    }

    /** Transitions an active {@link ResumePhase#WAITING} await unit to {@link AwaitUnitStatus#TIMED_OUT}. */
    public Optional<AwaitUnitRecord> timeoutUnit(String tenantId, String unitId) {
        requireNonBlank(tenantId, "tenantId");
        requireNonBlank(unitId, "unitId");
        String key = unitKey(tenantId, unitId);
        Optional<SuspendedAwaitState> updated = store.updateAtomically(key, existing -> {
            if (existing.phase() != ResumePhase.WAITING || existing.unit().status().terminal()) {
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
                timedOut,
                updatedSuspension,
                existing.suspendedPayload(),
                existing.capturedContext(),
                existing.completionPayload(),
                ResumePhase.TIMED_OUT,
                existing.admittedCompletionKey());
        });
        return updated.map(SuspendedAwaitState::unit);
    }

    public Optional<SuspendedAwaitState> findUnit(String tenantId, String unitId) {
        requireNonBlank(tenantId, "tenantId");
        requireNonBlank(unitId, "unitId");
        return store.find(unitKey(tenantId, unitId));
    }

    private CompletionStage<ResumeResult> executePostAwaitAndFinalize(
        String key,
        SuspendedAwaitState admittedState,
        SpringPipelineRunner postAwaitRunner
    ) {
        if (!activeResumes.add(key)) {
            return CompletableFuture.completedFuture(
                new ResumeResult(ResumeStatus.RESUME_IN_PROGRESS, Optional.of(admittedState.unit()), Optional.empty()));
        }
        AwaitUnitRecord resumingUnit = admittedState.unit();
        String completionKey = admittedState.admittedCompletionKey().orElseThrow();
        SpringCanonicalPayload payloadToRun = admittedState.completionPayload().orElseThrow();
        SpringTransitionDispatchScope resumeScope = new SpringTransitionDispatchScope(
            resumingUnit.tenantId(),
            resumingUnit.executionId(),
            resumingUnit.version(),
            1,
            "await-resume:" + resumingUnit.unitId() + ":" + completionKey);

        CompletionStage<Object> runStage;
        TpfExecutionContext.Snapshot previous = TpfExecutionContext.capture();
        try {
            TpfExecutionContext.restore(admittedState.capturedContext());
            try (SpringTransitionDispatchScope.Scope ignored = SpringTransitionDispatchScope.open(resumeScope)) {
                runStage = postAwaitRunner.run(payloadToRun);
            }
        } catch (Throwable synchronousFailure) {
            activeResumes.remove(key);
            CompletableFuture<ResumeResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(synchronousFailure);
            return failed;
        } finally {
            TpfExecutionContext.restore(previous);
        }

        CompletableFuture<ResumeResult> outcome = new CompletableFuture<>();
        runStage.whenComplete((rawOutput, failure) -> {
            try {
                if (failure != null) {
                    outcome.completeExceptionally(failure);
                    return;
                }
                if (!(rawOutput instanceof SpringCanonicalPayload output)) {
                    outcome.completeExceptionally(new IllegalStateException(
                        "Post-Await runner must produce SpringCanonicalPayload, got "
                            + (rawOutput == null ? "null" : rawOutput.getClass().getName())));
                    return;
                }
                AwaitUnitRecord completedUnit = markCompletedAtomically(key, completionKey);
                outcome.complete(new ResumeResult(ResumeStatus.RESUMED, Optional.of(completedUnit), Optional.of(output)));
            } catch (Throwable finalizationFailure) {
                outcome.completeExceptionally(finalizationFailure);
            } finally {
                activeResumes.remove(key);
            }
        });
        return outcome;
    }

    private AdmissionOutcome admitForResumeAtomically(
        String key,
        String completionKey,
        SpringCanonicalPayload completionPayload
    ) {
        AtomicReference<AdmissionOutcome> holder = new AtomicReference<>(
            new AdmissionOutcome(ResumeStatus.UNKNOWN_UNIT, Optional.empty(), Optional.empty()));
        store.updateAtomically(key, existing -> {
            AwaitUnitRecord current = existing.unit();
            if (existing.phase() == ResumePhase.COMPLETED) {
                if (current.completedItemKeys().contains(completionKey)
                    || existing.admittedCompletionKey().map(completionKey::equals).orElse(false)) {
                    holder.set(new AdmissionOutcome(
                        ResumeStatus.DUPLICATE_IGNORED, Optional.of(current), Optional.of(existing)));
                } else {
                    holder.set(new AdmissionOutcome(
                        ResumeStatus.TERMINAL_REJECTED, Optional.of(current), Optional.of(existing)));
                }
                return existing;
            }
            if (existing.phase() == ResumePhase.TIMED_OUT || current.status().terminal()) {
                holder.set(new AdmissionOutcome(
                    ResumeStatus.TERMINAL_REJECTED, Optional.of(current), Optional.of(existing)));
                return existing;
            }
            if (existing.phase() == ResumePhase.RESUMING) {
                if (existing.admittedCompletionKey().map(completionKey::equals).orElse(false)) {
                    // Recoverable retry of the same admitted completion key with the persisted payload & version.
                    holder.set(new AdmissionOutcome(
                        ResumeStatus.RESUMED, Optional.of(current), Optional.of(existing)));
                } else {
                    holder.set(new AdmissionOutcome(
                        ResumeStatus.TERMINAL_REJECTED, Optional.of(current), Optional.of(existing)));
                }
                return existing;
            }
            // WAITING -> RESUMING (non-terminal; preserves admitted payload and stable replay version)
            AwaitUnitRecord resuming = new AwaitUnitRecord(
                current.tenantId(),
                current.unitId(),
                current.executionId(),
                current.stepId(),
                current.stepIndex(),
                current.cardinality(),
                current.version() + 1L,
                AwaitUnitStatus.WAITING_EXTERNAL,
                current.primaryInteractionId(),
                current.expectedItemCount(),
                current.completedItemCount(),
                current.completedItemKeys(),
                current.dispatchComplete(),
                current.createdAtEpochMs(),
                clock.millis(),
                current.ttlEpochS());
            TransitionAwaitSuspension updatedSuspension = new TransitionAwaitSuspension(
                resuming.tenantId(), resuming.executionId(), resuming.unitId(), resuming.stepIndex(),
                resuming, existing.suspension().interactions());
            SuspendedAwaitState nextState = new SuspendedAwaitState(
                resuming,
                updatedSuspension,
                existing.suspendedPayload(),
                existing.capturedContext(),
                Optional.of(completionPayload),
                ResumePhase.RESUMING,
                Optional.of(completionKey));
            holder.set(new AdmissionOutcome(ResumeStatus.RESUMED, Optional.of(resuming), Optional.of(nextState)));
            return nextState;
        });
        return holder.get();
    }

    private AwaitUnitRecord markCompletedAtomically(String key, String completionKey) {
        Optional<SuspendedAwaitState> updated = store.updateAtomically(key, existing -> {
            if (existing.phase() == ResumePhase.COMPLETED) {
                return existing;
            }
            AwaitUnitRecord current = existing.unit();
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
            return new SuspendedAwaitState(
                completed,
                updatedSuspension,
                existing.suspendedPayload(),
                existing.capturedContext(),
                existing.completionPayload(),
                ResumePhase.COMPLETED,
                Optional.of(completionKey));
        });
        return updated.map(SuspendedAwaitState::unit)
            .orElseThrow(() -> new IllegalStateException("Persisted Await unit disappeared before completion: " + key));
    }

    private record AdmissionOutcome(
        ResumeStatus status,
        Optional<AwaitUnitRecord> updatedUnit,
        Optional<SuspendedAwaitState> state
    ) {
    }

    private static String unitKey(String tenantId, String unitId) {
        return tenantId.length() + ":" + tenantId + ":" + unitId;
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}

package org.pipelineframework.orchestrator;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.pipelineframework.command.CommandRetryableEffectException;
import org.pipelineframework.step.NonRetryableException;

/** Runtime-local mapping between portable failure data and executable exceptions. */
final class TransitionFailureRuntimeAdapter {
    private TransitionFailureRuntimeAdapter() {
    }

    static TransitionFailureEnvelope from(Throwable failure, int failedStepIndex) {
        Objects.requireNonNull(failure, "failure");
        Throwable classifiedFailure = findRemoteUnknown(failure)
            .<Throwable>map(unknown -> unknown).orElseGet(() -> findNonRetryable(failure)
            .<Throwable>map(nonRetryable -> nonRetryable)
            .orElse(failure));
        return new TransitionFailureEnvelope(
            classifiedFailure.getClass().getName(),
            Optional.ofNullable(classifiedFailure.getMessage()).orElse(""),
            failedStepIndex,
            CommandRetryableEffectException.find(failure)
                .map(CommandRetryableEffectException::commandId));
    }

    private static Optional<RemoteTransitionOutcomeUnknownException> findRemoteUnknown(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable current = failure; current != null && seen.add(current); current = current.getCause()) {
            if (current instanceof RemoteTransitionOutcomeUnknownException unknown) return Optional.of(unknown);
        }
        return Optional.empty();
    }

    private static Optional<NonRetryableException> findNonRetryable(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = failure;
        while (current != null && seen.add(current)) {
            if (current instanceof NonRetryableException nonRetryable) {
                return Optional.of(nonRetryable);
            }
            current = current.getCause();
        }
        return Optional.empty();
    }

    static RuntimeException toException(TransitionFailureEnvelope failure) {
        Objects.requireNonNull(failure, "failure");
        if (RemoteTransitionOutcomeUnknownException.class.getName().equals(failure.failureClass())) {
            // This portable failure preserves disposition, not transport measurements.
            return new RemoteTransitionOutcomeUnknownException("worker-reported", "pinned-worker", -1, -1,
                new IllegalStateException(failure.message()));
        }
        if (isNonRetryableFailureClass(failure.failureClass())) {
            return new NonRetryableException(
                failure.message() == null || failure.message().isBlank()
                    ? failure.failureClass()
                    : failure.message());
        }
        String suffix = failure.message() == null || failure.message().isBlank()
            ? ""
            : ": " + failure.message();
        return new TransitionWorkerFailureException(
            failure.failureClass() + suffix,
            failure.failedStepIndex(),
            failure.failedCommandId());
    }

    private static boolean isNonRetryableFailureClass(String failureClass) {
        if (NonRetryableException.class.getName().equals(failureClass)) {
            return true;
        }
        try {
            ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
            ClassLoader loader = contextLoader == null
                ? TransitionFailureRuntimeAdapter.class.getClassLoader()
                : contextLoader;
            Class<?> failureType = Class.forName(failureClass, false, loader);
            return NonRetryableException.class.isAssignableFrom(failureType);
        } catch (ClassNotFoundException | LinkageError ignored) {
            return false;
        }
    }
}

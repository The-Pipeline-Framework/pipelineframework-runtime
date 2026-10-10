package org.pipelineframework.orchestrator;

/** Test-only access to the native portable transition-failure boundary. */
public final class TransitionFailureTestAccess {
    private TransitionFailureTestAccess() {}

    public static TransitionFailureEnvelope from(Throwable failure, int failedStepIndex) {
        return TransitionFailureRuntimeAdapter.from(failure, failedStepIndex);
    }

    public static RuntimeException restore(TransitionFailureEnvelope envelope) {
        return TransitionFailureRuntimeAdapter.toException(envelope);
    }
}

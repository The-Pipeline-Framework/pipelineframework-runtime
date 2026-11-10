package org.pipelineframework.orchestrator.release;

import java.util.Objects;

/** Internal immutable operation intent: null means unconditional, empty means observed absence. */
final class ActivationPrecondition {
    private ActivationPrecondition() {}

    static String from(ActivationOperationCommand command, CurrentActivationObservation observation) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(observation, "expected current activation");
        if (!command.release().tenantId().equals(observation.tenantId())
            || !command.release().pipelineId().equals(observation.pipelineId())) {
            throw new IllegalArgumentException("Activation expectation has a different scope");
        }
        return switch (observation.state()) {
            case NONE -> "";
            case IDENTIFIED -> observation.identifiedEvent().orElseThrow().activationId();
            case LEGACY_UNKNOWN -> throw new IllegalArgumentException("Unknown activation cannot authorize a conditional change");
        };
    }

    static boolean matches(String expected, String actual, boolean legacyUnknown) {
        return expected == null || (!legacyUnknown && expected.equals(actual));
    }

    static void requireMatch(String expected, String actual, boolean legacyUnknown) {
        if (!matches(expected, actual, legacyUnknown)) {
            throw new IllegalStateException("Current activation no longer matches the operation's expected event");
        }
    }

    static void requireReplay(String stored, String expected) {
        if (!Objects.equals(stored, expected)) {
            throw new IllegalStateException("Activation operation key has a different expected head");
        }
    }
}

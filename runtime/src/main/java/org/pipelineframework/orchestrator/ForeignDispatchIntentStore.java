package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Durable pre-send dispatch-intent store for foreign function invocations on the standard path.
 *
 * <p>Implementations must use atomic conditional writes (never blind upserts) so a duplicate
 * reservation for an unresolved attempt always returns
 * {@link ReservationOutcome#ALREADY_DISPATCHED_UNCERTAIN}.</p>
 */
public interface ForeignDispatchIntentStore {

    /** Result of attempting to reserve pre-send authority for one foreign step invocation. */
    enum ReservationOutcome {
        /** Newly committed reservation; caller is authorized to send the request. */
        RESERVED,
        /** Prior reservation already exists for this attempt/step; caller must not resend. */
        ALREADY_DISPATCHED_UNCERTAIN
    }

    /** Append-only settlement classification recorded after the HTTP response (or failure) arrives. */
    enum SettlementStatus {
        VALIDATED_SUCCEEDED,
        VALIDATED_RETRYABLE,
        VALIDATED_TERMINAL,
        UNCERTAIN
    }

    /** Atomically records pre-send dispatch intent before sending bytes over the wire. */
    ReservationOutcome reserve(ForeignDispatchIntent intent);

    /** Records an immutable append-only settlement event for a previously reserved intent. */
    void recordSettlement(ForeignDispatchIntent intent, SettlementStatus status, long settledAtEpochMs);

    /** No-op store for non-durable direct step invocations and unit tests. */
    static ForeignDispatchIntentStore noop() {
        return NoopForeignDispatchIntentStore.INSTANCE;
    }

    final class NoopForeignDispatchIntentStore implements ForeignDispatchIntentStore {
        private static final NoopForeignDispatchIntentStore INSTANCE = new NoopForeignDispatchIntentStore();

        private NoopForeignDispatchIntentStore() {
        }

        @Override
        public ReservationOutcome reserve(ForeignDispatchIntent intent) {
            Objects.requireNonNull(intent, "intent must not be null");
            return ReservationOutcome.RESERVED;
        }

        @Override
        public void recordSettlement(ForeignDispatchIntent intent, SettlementStatus status, long settledAtEpochMs) {
            Objects.requireNonNull(intent, "intent must not be null");
            Objects.requireNonNull(status, "status must not be null");
        }
    }
}

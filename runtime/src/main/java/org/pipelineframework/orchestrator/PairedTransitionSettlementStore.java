package org.pipelineframework.orchestrator;

import io.smallrye.mutiny.Uni;
import java.util.Optional;
import java.util.OptionalLong;
import org.pipelineframework.orchestrator.TransitionResultEnvelope.PortableFailure;

/** Store capability for the explicit native pairing. Dispatch and response require the same current lease. */
public interface PairedTransitionSettlementStore {
    /** Only a newly committed intent authorizes a send, up to the observed lease expiry in epoch milliseconds. */
    Uni<OptionalLong> beginPairedDispatch(PairedDispatchIntent intent);

    Uni<Optional<ExecutionRecord<Object, Object>>> settlePairedSuccess(PairedDispatchIntent intent,
        SerializedTransitionPayload output);

    Uni<Optional<ExecutionRecord<Object, Object>>> settlePairedFailure(PairedDispatchIntent intent,
        PortableFailure failure);

    /** Records a coordinator rejection of a validated paired result. */
    Uni<Optional<ExecutionRecord<Object, Object>>> settlePairedRejection(PairedDispatchIntent intent,
        String errorCode, String diagnostic);

    Uni<Optional<ExecutionRecord<Object, Object>>> settlePairedUnknown(PairedDispatchIntent intent);
}

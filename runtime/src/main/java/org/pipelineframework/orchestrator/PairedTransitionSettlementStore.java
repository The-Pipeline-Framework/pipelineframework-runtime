package org.pipelineframework.orchestrator;

import io.smallrye.mutiny.Uni;
import java.util.Optional;
import org.pipelineframework.orchestrator.TransitionResultEnvelope.PortableFailure;

/** Store capability for the explicit native pairing. Each write requires a current RUNNING lease. */
public interface PairedTransitionSettlementStore {
    record Lease(String tenantId, String executionId, long version, String owner, String transitionKey) {
        public Lease {
            if (version < 0 || blank(tenantId) || blank(executionId) || blank(owner) || blank(transitionKey)) {
                throw new IllegalArgumentException("Paired settlement requires a complete claimed lease");
            }
        }

        private static boolean blank(String value) {
            return value == null || value.isBlank();
        }
    }

    Uni<Optional<ExecutionRecord<Object, Object>>> settlePairedSuccess(Lease lease, SerializedTransitionPayload output);

    Uni<Optional<ExecutionRecord<Object, Object>>> settlePairedFailure(Lease lease, PortableFailure failure);
}

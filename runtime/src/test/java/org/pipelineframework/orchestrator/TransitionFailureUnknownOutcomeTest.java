package org.pipelineframework.orchestrator;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TransitionFailureUnknownOutcomeTest {
    @Test
    void nestedAuthorityLostReplyKeepsUnknownDispositionAcrossPortableResult() {
        var unknown = new RemoteTransitionOutcomeUnknownException("await-rest", "https://coordinator.test", 50, 50,
            new java.io.IOException("reply lost after grant"));
        var encoded = TransitionResultEnvelope.failed(new IllegalStateException("step wrapper", unknown));
        var decoded = TransitionResultEnvelope.fromWireResult(encoded.toWireResult());
        assertInstanceOf(RemoteTransitionOutcomeUnknownException.class, decoded.failureException());
    }
}

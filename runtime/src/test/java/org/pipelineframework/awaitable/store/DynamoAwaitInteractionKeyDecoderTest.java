package org.pipelineframework.awaitable.store;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DynamoAwaitInteractionKeyDecoderTest {
    private final DynamoAwaitInteractionKeyDecoder decoder = new DynamoAwaitInteractionKeyDecoder();

    @Test
    void interactionPrimaryKeyIsEnoughWithoutAnExecutionOrCompletionImage() {
        var key = decoder.decode(Map.of("tenant_id", "tenant", "interaction_id", "interaction"));
        assertEquals("tenant", key.tenantId());
        assertEquals("interaction", key.interactionId());
    }

    @Test
    void unrelatedMechanicalKeysCannotBeDecodedAsAnInteraction() {
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(Map.of("pk", "tenant#interaction", "sk", "GEN#1")));
    }

    @Test
    void missingOrBlankSemanticKeysAreFailuresRatherThanIgnoredEvents() {
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(Map.of("tenant_id", "tenant")));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(Map.of("tenant_id", " ", "interaction_id", "interaction")));
    }
}

package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DynamoExecutionKeyDecoderTest {
    @Test
    void decodesOnlyStorageOwnedPrimaryKeys() {
        var decoder = new DynamoExecutionKeyDecoder();
        assertEquals(new DynamoExecutionKeyDecoder.ExecutionKey("tenant", "execution"),
            decoder.decode(Map.of("tenant_id", "tenant", "execution_id", "execution")));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(Map.of("pk", "tenant#execution")));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(Map.of("tenant_id", " ", "execution_id", "execution")));
    }
}

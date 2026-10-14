package org.pipelineframework.orchestrator;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Storage-owned decoding of an execution locator, never an observation of completion. */
public final class DynamoExecutionKeyDecoder {
    public ExecutionKey decode(Map<String, String> keys) {
        Objects.requireNonNull(keys, "keys");
        return new ExecutionKey(requiredKey(keys, DynamoExecutionStateStore.TENANT_ID),
            requiredKey(keys, DynamoExecutionStateStore.EXECUTION_ID));
    }

    private String requiredKey(Map<String, String> keys, String attribute) {
        return Optional.ofNullable(keys.get(attribute)).filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("Execution key is missing " + attribute));
    }

    /** Provider-neutral identifiers used with the existing semantic status read. */
    public record ExecutionKey(String tenantId, String executionId) {
        public ExecutionKey {
            tenantId = required(tenantId, "tenantId");
            executionId = required(executionId, "executionId");
        }

        private static String required(String value, String name) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Execution key is missing " + name);
            }
            return value;
        }
    }
}

package org.pipelineframework.awaitable.store;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Storage-owned decoding of an interaction locator, never an observation of completion. */
public final class DynamoAwaitInteractionKeyDecoder {
    public InteractionKey decode(Map<String, String> keys) {
        Objects.requireNonNull(keys, "keys");
        return new InteractionKey(requiredKey(keys, DynamoAwaitInteractionStore.TENANT_ID),
            requiredKey(keys, DynamoAwaitInteractionStore.INTERACTION_ID));
    }

    private String requiredKey(Map<String, String> keys, String attribute) {
        return Optional.ofNullable(keys.get(attribute)).filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("Await interaction key is missing " + attribute));
    }

    /** Provider-neutral identifiers used with the existing semantic checkpoint read. */
    public record InteractionKey(String tenantId, String interactionId) {
        public InteractionKey {
            tenantId = required(tenantId, "tenantId");
            interactionId = required(interactionId, "interactionId");
        }

        private static String required(String value, String name) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Await interaction key is missing " + name);
            }
            return value;
        }
    }
}

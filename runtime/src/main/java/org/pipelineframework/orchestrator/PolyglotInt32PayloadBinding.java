package org.pipelineframework.orchestrator;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.pipelineframework.serialization.PolyglotInt32JsonCodec;

/**
 * Opt-in application payload binding for the experimental four-type native worker profile.
 * Construct the catalog from the verified compiler projection. The caller selects the expected
 * type and inner encoding from its trusted profile before interpreting the envelope.
 */
public final class PolyglotInt32PayloadBinding {

    private static final Pattern LOGICAL_TYPE_ID = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

    private final Set<String> typeIds;
    private final PolyglotInt32JsonCodec codec = new PolyglotInt32JsonCodec();

    public PolyglotInt32PayloadBinding(String... typeIds) {
        if (typeIds == null || typeIds.length != 4) {
            throw new IllegalArgumentException("INVALID_PAYLOAD_CATALOG");
        }
        Set<String> catalog = new HashSet<>();
        for (String id : typeIds) {
            if (id == null || !LOGICAL_TYPE_ID.matcher(id).matches() || !catalog.add(id)) {
                throw new IllegalArgumentException("INVALID_PAYLOAD_CATALOG");
            }
        }
        this.typeIds = Set.copyOf(catalog);
    }

    public SerializedTransitionPayload encode(String typeId, int value) {
        requireKnownType(typeId);
        return new SerializedTransitionPayload(typeId, PolyglotInt32JsonCodec.ENCODING, codec.encode(value));
    }

    public int decode(String expectedTypeId, SerializedTransitionPayload envelope) {
        requireKnownType(expectedTypeId);
        if (envelope == null) {
            throw new IllegalArgumentException("INVALID_PAYLOAD_ENVELOPE");
        }
        if (!expectedTypeId.equals(envelope.payloadTypeId())) {
            throw new IllegalArgumentException("PAYLOAD_TYPE_MISMATCH");
        }
        if (!PolyglotInt32JsonCodec.ENCODING.equals(envelope.payloadEncoding())) {
            throw new IllegalArgumentException("PAYLOAD_ENCODING_MISMATCH");
        }
        return codec.decode(envelope.payload());
    }

    private void requireKnownType(String typeId) {
        if (typeId == null || !typeIds.contains(typeId)) {
            throw new IllegalArgumentException("UNKNOWN_PAYLOAD_TYPE");
        }
    }
}

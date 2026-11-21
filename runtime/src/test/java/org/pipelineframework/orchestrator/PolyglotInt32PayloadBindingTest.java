package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.serialization.PolyglotInt32JsonCodec;

class PolyglotInt32PayloadBindingTest {

    private final PolyglotInt32PayloadBinding binding =
        new PolyglotInt32PayloadBinding("InputValue", "FirstValue", "SecondValue", "OutputValue");

    @Test
    void roundTripsEveryNominalTypeUsingExistingCarrier() {
        for (String typeId : List.of("InputValue", "FirstValue", "SecondValue", "OutputValue")) {
            SerializedTransitionPayload envelope = binding.encode(typeId, Integer.MIN_VALUE);
            assertEquals(new SerializedTransitionPayload(typeId, PolyglotInt32JsonCodec.ENCODING,
                "{\"value\":-2147483648}"), envelope);
            assertEquals(Integer.MIN_VALUE, binding.decode(typeId, envelope));
        }
    }

    @Test
    void rejectsUnexpectedTypesAndEncodingsBeforeParsing() {
        var malformed = new SerializedTransitionPayload("FirstValue", "wrong", "invalid JSON");
        assertEquals("UNKNOWN_PAYLOAD_TYPE", assertThrows(IllegalArgumentException.class,
            () -> binding.decode("Unknown", malformed)).getMessage());
        assertEquals("PAYLOAD_TYPE_MISMATCH", assertThrows(IllegalArgumentException.class,
            () -> binding.decode("InputValue", malformed)).getMessage());
        assertEquals("PAYLOAD_ENCODING_MISMATCH", assertThrows(IllegalArgumentException.class,
            () -> binding.decode("FirstValue", malformed)).getMessage());
        var malformedValue = new SerializedTransitionPayload("FirstValue", PolyglotInt32JsonCodec.ENCODING, "{}");
        assertEquals("INVALID_PAYLOAD_VALUE", assertThrows(IllegalArgumentException.class,
            () -> binding.decode("FirstValue", malformedValue)).getMessage());
        assertThrows(IllegalArgumentException.class, () -> binding.encode("java.lang.Integer", 1));
    }

    @Test
    void validatesAndCopiesCatalog() {
        assertThrows(IllegalArgumentException.class, () -> new PolyglotInt32PayloadBinding("A", "B", "C"));
        assertThrows(IllegalArgumentException.class, () -> new PolyglotInt32PayloadBinding("A", "A", "C", "D"));
        assertThrows(IllegalArgumentException.class, () -> new PolyglotInt32PayloadBinding("A", "B", "C", "java.Integer"));
        String[] catalog = {"A", "B", "C", "D"};
        var copied = new PolyglotInt32PayloadBinding(catalog);
        catalog[0] = "E";
        assertEquals("A", copied.encode("A", 1).payloadTypeId());
        assertThrows(IllegalArgumentException.class, () -> copied.encode("E", 1));
    }

    @Test
    void legacyCodecKeepsItsEncodingAndRejectsPortableValues() {
        var legacy = new JsonTransitionPayloadCodec();
        var legacyPayload = legacy.encode(7);
        assertEquals(JsonTransitionPayloadCodec.ENCODING, legacyPayload.payloadEncoding());
        assertEquals(7, legacy.decode(legacyPayload));
        assertThrows(IllegalArgumentException.class, () -> legacy.decode(binding.encode("InputValue", 7)));
        assertEquals("PAYLOAD_ENCODING_MISMATCH", assertThrows(IllegalArgumentException.class,
            () -> binding.decode("InputValue", new SerializedTransitionPayload("InputValue",
                legacyPayload.payloadEncoding(), legacyPayload.payload()))).getMessage());
    }
}

package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CompiledCanonicalTransitionPayloadCodecTest {
    private final JsonTransitionPayloadCodec framing = new JsonTransitionPayloadCodec();

    @Test
    void explicitAliasesRemainDistinctEvenWhenTheirJavaRealizationIsIdentical() {
        var codec = codec("Primary", "Secondary");
        var encoded = codec.encode(new Value("test"));
        var envelope = (TypedDurablePayload) framing.decode(encoded);
        assertEquals("Secondary", envelope.canonicalTypeId());
        assertTrue(codec.containsCanonicalPayload(encoded));
        assertThrows(IllegalArgumentException.class, () -> codec.decode(encoded));
        assertEquals(new Value("test"), codec("Secondary", "Primary").decode(encoded));
        assertThrows(IllegalArgumentException.class, () -> codec(Value.class.getName(), "Primary"));
    }

    @Test
    void uniqueHistoricalJavaBoundaryMetadataRetainsCanonicalIdentity() {
        var contract = contract(Map.of("Primary", definition("primary")));
        var codec = new CompiledCanonicalTransitionPayloadCodec(framing, contract, Value.class.getName(), Value.class.getName());
        var encoded = codec.encode(new Value("test"));
        assertEquals("Primary", ((TypedDurablePayload) framing.decode(encoded)).canonicalTypeId());
        assertEquals(new Value("test"), codec.decode(encoded));
    }

    @Test
    void preservesSingleBusinessCollectionVersusMultiInputAndRequestPolicy() {
        var codec = codec("Primary", "Primary");
        var policy = new PipelineContext("release-a", "REPLAY", "BYPASS");
        var items = List.of(new Value("one"), new Value("two"));
        for (var shape : ExecutionInputShape.values()) {
            var path = org.pipelineframework.invocation.PipelineInvocationContext.root(3).atStep(0)
                .enterRecursive("$root", "Continue").atStep(1).path();
            var snapshot = new ExecutionInputSnapshot(shape, items, Optional.of(policy), Optional.of(path));
            var encoded = codec.encode(snapshot);
            var framed = (ExecutionInputSnapshot) framing.decode(encoded);
            if (shape == ExecutionInputShape.MULTI) {
                assertInstanceOf(List.class, framed.payload());
                assertEquals(2, ((List<?>) framed.payload()).size());
            } else {
                assertEquals("List<Primary>", ((TypedDurablePayload) framed.payload()).canonicalTypeId());
            }
            assertEquals(snapshot, codec.decode(encoded));
        }
        assertEquals(List.of(), codec.decode(codec.encode(List.of())));
        assertThrows(IllegalArgumentException.class, () -> codec.encode(List.of(new Value("one"), "foreign")));
    }

    @Test
    void rejectsMixedUnframedValuesAndMalformedCanonicalBytes() {
        var codec = codec("Primary", "Primary");
        var typed = (TypedDurablePayload) framing.decode(codec.encode(new Value("one")));
        assertThrows(IllegalArgumentException.class,
            () -> codec.decode(framing.encode(List.of(typed, new Value("unframed")))));
        assertThrows(IllegalArgumentException.class, () -> codec.decode(framing.encode(new Value("unframed"))));
        for (String bytes : List.of("not-json", "null", "{\"text\":\"one\"} {}", "[1]")) {
            var invalid = new TypedDurablePayload(typed.canonicalTypeId(), typed.typeExpressionFingerprint(),
                typed.catalogFingerprint(), typed.encoding(), typed.encodingVersion(), bytes.getBytes(StandardCharsets.UTF_8));
            assertThrows(IllegalArgumentException.class, () -> codec.decode(framing.encode(invalid)));
        }
    }

    @Test
    void validatesCatalogFingerprintEncodingAndVersionBeforeMaterializing() {
        var codec = codec("Primary", "Primary");
        var typed = (TypedDurablePayload) framing.decode(codec.encode(new Value("one")));
        for (var invalid : List.of(
            new TypedDurablePayload("Primary", "foreign", "catalog", typed.encoding(), 1, typed.payload()),
            new TypedDurablePayload("Primary", "primary", "foreign", typed.encoding(), 1, typed.payload()),
            new TypedDurablePayload("Primary", "primary", "catalog", "foreign", 1, typed.payload()),
            new TypedDurablePayload("Primary", "primary", "catalog", typed.encoding(), 2, typed.payload()))) {
            assertThrows(IllegalArgumentException.class, () -> codec.decode(framing.encode(invalid)));
        }
        assertThrows(IllegalArgumentException.class,
            () -> codec.decode(new SerializedTransitionPayload("x", "invalid", "{}")));
    }

    @Test
    void preselectionNeedsNoCanonicalBoundaryAndDoesNotHideMalformedEnvelope() {
        assertFalse(CompiledCanonicalTransitionPayloadCodec.containsCanonicalPayload(framing, framing.encode("legacy")));
        var malformed = Map.of("canonicalTypeId", "Primary", "encodingVersion", 0);
        assertTrue(CompiledCanonicalTransitionPayloadCodec.containsCanonicalPayload(framing, framing.encode(malformed)));
        assertThrows(IllegalArgumentException.class, () -> codec("Primary", "Primary").decode(framing.encode(malformed)));
    }

    private CompiledCanonicalTransitionPayloadCodec codec(String input, String output) {
        return new CompiledCanonicalTransitionPayloadCodec(framing,
            contract(Map.of("Primary", definition("primary"), "Secondary", definition("secondary"))), input, output);
    }

    private static PipelineContractDescriptor contract(Map<String, Map<String, Object>> definitions) {
        var contract = mock(PipelineContractDescriptor.class);
        when(contract.canonicalTypes()).thenReturn(definitions);
        when(contract.canonicalCatalogFingerprint()).thenReturn("catalog");
        return contract;
    }

    private static Map<String, Object> definition(String fingerprint) {
        return Map.of("runtimeClass", Value.class.getName(), "definitionFingerprint", fingerprint,
            "definition", Map.of("kind", "record", "id", fingerprint.equals("primary") ? "Primary" : "Secondary", "fields",
                List.of(Map.of("name", "text", "presence", "REQUIRED", "nullability", "NON_NULL", "type", Map.of("kind", "scalar", "id", "string")))));
    }

    public record Value(String text) { }
}

package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class JsonDurablePayloadCodecTest {

    private final JsonDurablePayloadCodec codec = new JsonDurablePayloadCodec();
    private final CanonicalPayloadBinding binding = new CanonicalPayloadBinding(
        Decision.class.getSimpleName(), "expression-fingerprint", "catalog-fingerprint", Decision.class);

    @Test
    void roundTripsTheBoundCanonicalType() {
        TypedDurablePayload encoded = codec.encode(new Decision("approved"), binding);

        assertEquals(new Decision("approved"), codec.decode(encoded, binding));
    }

    @Test
    void rejectsATypeOrFingerprintMismatchBeforeDecoding() {
        TypedDurablePayload encoded = codec.encode(new Decision("approved"), binding);
        CanonicalPayloadBinding incompatible = new CanonicalPayloadBinding(
            Decision.class.getSimpleName(), "other-expression", "catalog-fingerprint", Decision.class);

        assertThrows(IllegalArgumentException.class, () -> codec.decode(encoded, incompatible));
    }

    @Test
    void roundTripsASealedCanonicalUnionCaseWithoutJavaClassMetadata() {
        CanonicalPayloadBinding unionBinding = new CanonicalPayloadBinding(
            DecisionStatus.class.getSimpleName(), "union-expression", "catalog-fingerprint", DecisionStatus.class);

        TypedDurablePayload encoded = codec.encode(new DecisionStatus.Approved(new Decision("approved")), unionBinding);
        Object decoded = codec.decode(encoded, unionBinding);

        assertEquals(new DecisionStatus.Approved(new Decision("approved")), decoded);
        assertFalse(new String(encoded.payload(), java.nio.charset.StandardCharsets.UTF_8)
            .contains("_tpf_java_class"));
    }

    @Test
    void protectsTheEncodedBytesFromCallerMutation() {
        byte[] bytes = new byte[] {1, 2, 3};
        TypedDurablePayload payload = new TypedDurablePayload(
            "Decision", "expression", "catalog", "application/test", 1, bytes);

        bytes[0] = 9;
        byte[] exposed = payload.payload();
        exposed[1] = 9;

        assertEquals(1, payload.payload()[0]);
        assertEquals(2, payload.payload()[1]);
    }

    @Test
    void rejectsNonIntegralOrOutOfRangeEncodingVersionsInDurableMaps() {
        for (Number invalidVersion : java.util.List.of(0, -1, 1.5d, Long.MAX_VALUE, Double.NaN)) {
            assertTrue(TypedDurablePayload.fromDurableValue(java.util.Map.of(
                "canonicalTypeId", "Decision",
                "typeExpressionFingerprint", "expression",
                "catalogFingerprint", "catalog",
                "encoding", "application/test",
                "encodingVersion", invalidVersion,
                "payload", "e30=")).isEmpty());
        }
    }

    @Test
    void roundTripsMaterializedResultsContainingMarkerUnions() {
        var type = com.fasterxml.jackson.databind.type.TypeFactory.defaultInstance()
            .constructCollectionType(java.util.List.class, ApplicationResult.class);
        var resultBinding = binding(type);
        var results = java.util.List.of(new ApplicationResult("application-7", new ApplicationCompletion.Completed("ok")));

        TypedDurablePayload encoded = codec.encode(results, resultBinding);

        assertEquals(results, codec.decode(encoded, resultBinding));
        assertTrue(json(encoded).contains("\"discriminator\":\"Completed\",\"case\":\"Completed\",\"value\""));
        assertFalse(json(encoded).contains(ApplicationCompletion.class.getName()));
    }

    @Test
    void roundTripsUnionsThroughNestedGenericCollectionsMapsArraysAndRecords() {
        var value = new NestedResults(java.util.Map.of("first", java.util.List.of(
            new Box<ApplicationCompletion>(new ApplicationCompletion.Completed("ok")),
            new Box<ApplicationCompletion>(new ApplicationCompletion.Failed("no")))),
            new ApplicationCompletion[] {new ApplicationCompletion.Completed("array"), null},
            new DecisionStatus.Approved(new Decision("generated")));
        var resultBinding = new CanonicalPayloadBinding("NestedResults", "expression", "catalog", NestedResults.class);

        NestedResults decoded = (NestedResults) codec.decode(codec.encode(value, resultBinding), resultBinding);

        assertEquals(value.values(), decoded.values());
        org.junit.jupiter.api.Assertions.assertArrayEquals(value.array(), decoded.array());
        assertEquals(value.status(), decoded.status());
    }

    @Test
    void preservesOrdinaryJsonAndNullOrMissingOptionalFields() {
        var resultBinding = new CanonicalPayloadBinding("ApplicationResult", "expression", "catalog", ApplicationResult.class);
        assertEquals(new ApplicationResult("empty", null), codec.decode(
            payload("{\"id\":\"empty\",\"completion\":null}", resultBinding), resultBinding));
        assertEquals(new ApplicationResult("missing", null), codec.decode(
            payload("{\"id\":\"missing\"}", resultBinding), resultBinding));
        assertEquals("{\"result\":\"approved\"}", json(codec.encode(new Decision("approved"), binding)));
        var scalarBinding = new CanonicalPayloadBinding("String", "expression", "catalog", String.class);
        assertEquals("plain", codec.decode(codec.encode("plain", scalarBinding), scalarBinding));
        var concreteBinding = new CanonicalPayloadBinding("Completed", "expression", "catalog", ApplicationCompletion.Completed.class);
        assertEquals("{\"result\":\"plain\"}", json(codec.encode(new ApplicationCompletion.Completed("plain"), concreteBinding)));
    }

    @Test
    void decodesPreviouslyWrittenRootUnionEnvelopeAndKeepsItsWireShape() {
        var unionBinding = new CanonicalPayloadBinding("DecisionStatus", "expression", "catalog", DecisionStatus.class);
        String oldPayload = "{\"discriminator\":\"approved\",\"case\":\"Approved\",\"value\":{\"value\":{\"result\":\"approved\"}}}";
        var value = new DecisionStatus.Approved(new Decision("approved"));

        assertEquals(value, codec.decode(payload(oldPayload, unionBinding), unionBinding));
        assertEquals(oldPayload, json(codec.encode(value, unionBinding)));
    }

    @Test
    void roundTripsRootMarkerUnionsAndRejectsUnsupportedParameterizedUnionBases() {
        var markerBinding = new CanonicalPayloadBinding("ApplicationCompletion", "expression", "catalog", ApplicationCompletion.class);
        var value = new ApplicationCompletion.Completed("done");
        assertEquals(value, codec.decode(codec.encode(value, markerBinding), markerBinding));
        var genericType = com.fasterxml.jackson.databind.type.TypeFactory.defaultInstance()
            .constructParametricType(GenericStatus.class, ApplicationCompletion.class);
        var genericBinding = binding(genericType);
        GenericStatus<ApplicationCompletion> generic = new GenericStatus.Present<>(value);
        assertThrows(IllegalArgumentException.class, () -> codec.encode(generic, genericBinding));
        var erasedBinding = new CanonicalPayloadBinding("GenericStatus", "expression", "catalog", GenericStatus.class);
        assertThrows(IllegalArgumentException.class, () -> codec.encode(generic, erasedBinding));
        var genericCaseBinding = new CanonicalPayloadBinding("GenericCaseUnion", "expression", "catalog", GenericCaseUnion.class);
        assertThrows(IllegalArgumentException.class, () -> codec.encode(new GenericCase<>(value), genericCaseBinding));
    }

    @Test
    void retainsCanonicalPresenceAndPublishedMapperModules() {
        var presenceBinding = new CanonicalPayloadBinding("Presence", "expression", "catalog", Presence.class);
        var value = new Presence(org.pipelineframework.type.CanonicalFieldValue.of("present"),
            org.pipelineframework.type.CanonicalFieldValue.nullValue(),
            org.pipelineframework.type.CanonicalFieldValue.absent(), java.util.Optional.of("optional"),
            java.time.LocalDate.of(2026, 10, 29));

        assertEquals(value, codec.decode(codec.encode(value, presenceBinding), presenceBinding));
        Presence omitted = (Presence) codec.decode(payload(
            "{\"present\":\"present\",\"nullable\":null,\"date\":\"2026-10-29\"}", presenceBinding), presenceBinding);
        assertEquals(org.pipelineframework.type.CanonicalFieldValue.absent(), omitted.absent());
        assertEquals(org.pipelineframework.type.CanonicalFieldValue.nullValue(), omitted.nullable());
        assertEquals(java.util.Optional.empty(), omitted.optional());
        assertEquals(java.time.LocalDate.of(2026, 10, 29), omitted.date());
    }

    @Test
    void rejectsMalformedUnknownOrTamperedNestedUnionsWithoutInferringOldUntaggedValues() {
        var resultBinding = new CanonicalPayloadBinding("ApplicationResult", "expression", "catalog", ApplicationResult.class);
        for (String invalid : java.util.List.of(
            "{}", "[]", "42", "{\"result\":\"old-untagged\"}",
            "{\"case\":\"Completed\",\"value\":{\"result\":\"ok\"}}",
            "{\"case\":\"Completed\",\"discriminator\":null,\"value\":{}}",
            "{\"case\":\"Unknown\",\"discriminator\":\"Unknown\",\"value\":{}}",
            "{\"case\":\"java.lang.String\",\"discriminator\":\"String\",\"value\":{}}",
            "{\"case\":\"Completed\",\"discriminator\":\"Failed\",\"value\":{\"result\":\"ok\"}}",
            "{\"case\":\"Completed\",\"discriminator\":\"Completed\",\"value\":null}",
            "{\"case\":\"Completed\",\"case\":\"Failed\",\"discriminator\":\"Failed\",\"value\":{}}",
            "{\"case\":\"Completed\",\"discriminator\":\"Completed\",\"value\":{},\"extra\":true}")) {
            assertThrows(IllegalArgumentException.class, () -> codec.decode(
                payload("{\"id\":\"bad\",\"completion\":" + invalid + "}", resultBinding), resultBinding), invalid);
        }
    }

    @Test
    void rejectsGeneratedUnionDiscriminatorTamperingAndTrailingData() {
        var unionBinding = new CanonicalPayloadBinding("DecisionStatus", "expression", "catalog", DecisionStatus.class);
        assertThrows(IllegalArgumentException.class, () -> codec.decode(payload(
            "{\"discriminator\":\"rejected\",\"case\":\"Approved\",\"value\":{\"value\":{\"result\":\"approved\"}}}", unionBinding), unionBinding));
        assertThrows(IllegalArgumentException.class, () -> codec.decode(payload(
            "{\"result\":\"approved\"} {}", binding), binding));
    }

    @Test
    void rejectsUndeclaredSubclassesOfNonSealedCases() {
        var unionBinding = new CanonicalPayloadBinding("OpenUnion", "expression", "catalog", OpenUnion.class);
        assertThrows(IllegalArgumentException.class, () -> codec.encode(new Undeclared(), unionBinding));
    }

    private static CanonicalPayloadBinding binding(com.fasterxml.jackson.databind.JavaType type) {
        return new CanonicalPayloadBinding("Result", "expression", "catalog", type.getRawClass(), type);
    }

    private static TypedDurablePayload payload(String json, CanonicalPayloadBinding binding) {
        return new TypedDurablePayload(binding.canonicalTypeId(), binding.typeExpressionFingerprint(),
            binding.catalogFingerprint(), JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
            json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String json(TypedDurablePayload payload) {
        return new String(payload.payload(), java.nio.charset.StandardCharsets.UTF_8);
    }

    record ApplicationResult(String id, ApplicationCompletion completion) {
    }

    sealed interface ApplicationCompletion {
        record Completed(String result) implements ApplicationCompletion { }
        record Failed(String reason) implements ApplicationCompletion { }
    }

    record Presence(org.pipelineframework.type.CanonicalFieldValue<String> present,
                    org.pipelineframework.type.CanonicalFieldValue<String> nullable,
                    org.pipelineframework.type.CanonicalFieldValue<String> absent,
                    java.util.Optional<String> optional, java.time.LocalDate date) { }

    record Box<T>(T value) { }
    enum OrdinaryMode {
        FIRST { @Override String label() { return "first"; } },
        SECOND { @Override String label() { return "second"; } };
        abstract String label();
    }

    @Test
    void retainsOrdinaryEnumsWithConstantSpecificBodiesAtRootAndNestedPositions() {
        var enumBinding = new CanonicalPayloadBinding("Mode", "expression", "catalog", OrdinaryMode.class);
        assertEquals(OrdinaryMode.FIRST, codec.decode(codec.encode(OrdinaryMode.FIRST, enumBinding), enumBinding));
        var holderType = com.fasterxml.jackson.databind.type.TypeFactory.defaultInstance()
            .constructParametricType(Box.class, OrdinaryMode.class);
        var holderBinding = binding(holderType);
        var value = new Box<>(OrdinaryMode.SECOND);
        assertEquals(value, codec.decode(codec.encode(value, holderBinding), holderBinding));
        assertEquals("{\"value\":\"SECOND\"}", json(codec.encode(value, holderBinding)));
    }
    record NestedResults(java.util.Map<String, java.util.List<Box<ApplicationCompletion>>> values,
                         ApplicationCompletion[] array, DecisionStatus status) { }
    sealed interface GenericStatus<T> {
        record Present<T>(T value) implements GenericStatus<T> { }
    }
    sealed interface GenericCaseUnion permits GenericCase { }
    record GenericCase<T>(T value) implements GenericCaseUnion { }
    sealed interface OpenUnion permits OpenCase { }
    static non-sealed class OpenCase implements OpenUnion { public String getValue() { return "open"; } }
    static final class Undeclared extends OpenCase { }

    record Decision(String result) {
    }

    sealed interface DecisionStatus permits DecisionStatus.Approved, DecisionStatus.Rejected {
        String discriminator();

        record Approved(Decision value) implements DecisionStatus {
            @Override public String discriminator() { return "approved"; }
        }

        record Rejected(Decision value) implements DecisionStatus {
            @Override public String discriminator() { return "rejected"; }
        }
    }
}

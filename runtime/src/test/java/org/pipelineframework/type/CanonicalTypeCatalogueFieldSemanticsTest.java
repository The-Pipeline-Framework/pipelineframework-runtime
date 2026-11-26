package org.pipelineframework.type;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;

import static org.junit.jupiter.api.Assertions.*;

class CanonicalTypeCatalogueFieldSemanticsTest {

    private final CanonicalTypeCatalogue catalogue = CanonicalTypeCatalogue.fromCanonicalTypes(Map.of(
        "Customer", Map.of("definition", Map.of("id", "Customer", "kind", "record", "fields", List.of(
            field("name", "REQUIRED", "NON_NULL"),
            field("nickname", "OPTIONAL", "NON_NULL"),
            field("middleName", "REQUIRED", "NULLABLE"),
            field("note", "OPTIONAL", "NULLABLE"))))));

    @Test
    void validatesAbsentNullAndPresentIndependently() {
        // required/non-null
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("Customer", "{\"middleName\":null}"));
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("Customer", "{\"name\":null,\"middleName\":null}"));
        assertEquals("{\"middleName\":null,\"name\":\"Ada\"}",
            catalogue.validateAndCanonicalize("Customer", "{\"name\":\"Ada\",\"middleName\":null}"));

        // required/nullable
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("Customer", "{\"name\":\"Ada\"}"));
        assertEquals("{\"middleName\":null,\"name\":\"Ada\"}",
            catalogue.validateAndCanonicalize("Customer", "{\"name\":\"Ada\",\"middleName\":null}"));
        assertEquals("{\"middleName\":\"L\",\"name\":\"Ada\"}",
            catalogue.validateAndCanonicalize("Customer", "{\"name\":\"Ada\",\"middleName\":\"L\"}"));

        // optional/non-null
        assertEquals("{\"middleName\":\"L\",\"name\":\"Ada\"}",
            catalogue.validateAndCanonicalize("Customer", "{\"name\":\"Ada\",\"middleName\":\"L\"}"));
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("Customer",
                "{\"name\":\"Ada\",\"nickname\":null,\"middleName\":\"L\"}"));
        assertEquals("{\"middleName\":\"L\",\"name\":\"Ada\",\"nickname\":\"A\"}",
            catalogue.validateAndCanonicalize("Customer",
                "{\"name\":\"Ada\",\"nickname\":\"A\",\"middleName\":\"L\"}"));

        // optional/nullable
        assertEquals("{\"middleName\":\"L\",\"name\":\"Ada\",\"note\":null}",
            catalogue.validateAndCanonicalize("Customer",
                "{\"name\":\"Ada\",\"middleName\":\"L\",\"note\":null}"));
        assertEquals("{\"middleName\":\"L\",\"name\":\"Ada\",\"note\":\"memo\"}",
            catalogue.validateAndCanonicalize("Customer",
                "{\"name\":\"Ada\",\"middleName\":\"L\",\"note\":\"memo\"}"));
    }

    @Test
    void schemaMapsTheFourStates() throws Exception {
        JsonNode schema = PipelineJson.mapper().readTree(catalogue.schema("Customer"));
        assertEquals(List.of("middleName", "name"),
            java.util.stream.StreamSupport.stream(schema.path("required").spliterator(), false)
                .map(JsonNode::textValue).sorted().toList());
        assertEquals("string", schema.path("properties").path("name").path("type").textValue());
        assertEquals("null", schema.path("properties").path("middleName").path("oneOf").get(1).path("type").textValue());
        assertEquals("null", schema.path("properties").path("note").path("oneOf").get(1).path("type").textValue());
    }

    @Test
    void retainsCompiledRuntimeClassBindingWithoutChangingCanonicalValidation() {
        var bound = CanonicalTypeCatalogue.fromCanonicalTypes(Map.of(
            "Customer", Map.of(
                "runtimeClass", "org.example.shared.Customer",
                "definition", Map.of("id", "Customer", "kind", "record", "fields", List.of(
                    field("name", "REQUIRED", "NON_NULL"))))));

        assertEquals(java.util.Optional.of("org.example.shared.Customer"), bound.runtimeClass("Customer"));
        assertEquals(java.util.Optional.empty(), catalogue.runtimeClass("Customer"));
        assertEquals("{\"name\":\"Ada\"}", bound.validateAndCanonicalize("Customer", "{\"name\":\"Ada\"}"));
    }

    @Test
    void preservesArbitraryPrecisionDecimalsDuringValidationAndCanonicalization() {
        var decimalCatalogue = CanonicalTypeCatalogue.fromCanonicalTypes(Map.of(
            "BoundedAmount", Map.of("definition", Map.of(
                "id", "BoundedAmount",
                "kind", "wrapper",
                "wraps", Map.of("kind", "scalar", "id", "decimal"),
                "minimum", new java.math.BigDecimal("99999999999999999999.000000001"),
                "maximum", new java.math.BigDecimal("99999999999999999999.500000000"))),
            "Invoice", Map.of("definition", Map.of(
                "id", "Invoice",
                "kind", "record",
                "fields", List.of(
                    Map.of("name", "amount", "type", Map.of("kind", "named", "id", "BoundedAmount"),
                        "presence", "REQUIRED", "nullability", "NON_NULL"),
                    Map.of("name", "seq", "type", Map.of("kind", "scalar", "id", "int64"),
                        "presence", "REQUIRED", "nullability", "NON_NULL"))))));

        String canonical = decimalCatalogue.validateAndCanonicalize(
            "Invoice",
            "{\"seq\":9007199254740993,\"amount\":99999999999999999999.123456789}");
        assertEquals("{\"amount\":99999999999999999999.123456789,\"seq\":9007199254740993}", canonical);

        // Values differing only beyond IEEE-754 double precision must still be caught by bounds.
        assertThrows(IllegalArgumentException.class, () -> decimalCatalogue.validateAndCanonicalize(
            "Invoice",
            "{\"seq\":1,\"amount\":99999999999999999999.500000001}"));
    }

    private static Map<String, Object> field(String name, String presence, String nullability) {
        return Map.of("name", name, "type", Map.of("kind", "scalar", "id", "string"),
            "presence", presence, "nullability", nullability);
    }
}

/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.spring.host;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Exact-decimal JSON codec for the Spring Boot ADR-0092 Foreign Function &amp; Connector Host.
 *
 * <p>Preserves full arbitrary-precision {@link java.math.BigDecimal} values (including trailing
 * zeroes and plain-string representation) and 64-bit {@code long} precision beyond IEEE-754's
 * {@code 2^53} safe-integer ceiling.</p>
 */
public final class TpfHostJson {

    private static final ObjectMapper MAPPER = createExactMapper();
    private static final ObjectReader STRICT_READER = MAPPER.reader()
        .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private TpfHostJson() {
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static ObjectReader strictReader() {
        return STRICT_READER;
    }

    public static ObjectMapper createExactMapper() {
        return new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
            .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
    }

    public static JsonNode readStrictObject(byte[] bytes) throws IOException {
        JsonNode root = STRICT_READER.readTree(bytes);
        if (root == null || !root.isObject()) {
            throw new IOException("Expected JSON object payload");
        }
        return root;
    }

    public static JsonNode readStrictObject(String line) throws IOException {
        return readStrictObject(line.getBytes(StandardCharsets.UTF_8));
    }

    public static Object decodeValue(JsonNode node, JavaType targetType) throws IOException {
        if (JsonNode.class.isAssignableFrom(targetType.getRawClass())) {
            return node;
        }
        // Re-serialize via exact JSON bytes so BigDecimal scale and int64 values bind cleanly to Java types.
        byte[] exactBytes = MAPPER.writeValueAsBytes(node);
        return MAPPER.readValue(exactBytes, targetType);
    }

    public static JsonNode encodeValue(Object value) {
        if (value == null) {
            return MAPPER.nullNode();
        }
        if (value instanceof JsonNode node) {
            return node;
        }
        try {
            byte[] exactBytes = MAPPER.writeValueAsBytes(value);
            return MAPPER.readTree(exactBytes);
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding Java value to canonical JSON", e);
        }
    }

    public static byte[] writeBytes(Object value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new IllegalStateException("Failed serializing JSON response", e);
        }
    }

    public static byte[] writeNdjsonLine(Object value) {
        try {
            return (MAPPER.writeValueAsString(value) + "\n").getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed serializing NDJSON frame", e);
        }
    }
}

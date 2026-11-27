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

package org.pipelineframework.runtime.spring.polyglot;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;

/**
 * Shared exact-decimal JSON mapper for Spring-hosted polyglot canonical payloads.
 */
public final class SpringPolyglotJson {

    private static final ObjectMapper EXACT_MAPPER = new ObjectMapper()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);

    private static final ObjectReader STRICT_READER = EXACT_MAPPER.reader()
        .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private SpringPolyglotJson() {
    }

    public static ObjectMapper exactMapper() {
        return EXACT_MAPPER;
    }

    public static ObjectReader strictReader() {
        return STRICT_READER;
    }
}

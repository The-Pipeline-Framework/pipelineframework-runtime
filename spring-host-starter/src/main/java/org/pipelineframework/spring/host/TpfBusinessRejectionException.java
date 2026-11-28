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

import java.util.Objects;

/**
 * Portable non-retryable business rejection raised by a Spring-hosted foreign function.
 * Encoded over ADR-0092 as {@code disposition = "BUSINESS_REJECTION"}.
 */
public class TpfBusinessRejectionException extends RuntimeException {

    private final String code;

    public TpfBusinessRejectionException(String code, String message) {
        super(Objects.requireNonNull(message, "message must not be null"));
        this.code = requireNonBlank(code, "code");
    }

    public TpfBusinessRejectionException(String code, String message, Throwable cause) {
        super(Objects.requireNonNull(message, "message must not be null"), cause);
        this.code = requireNonBlank(code, "code");
    }

    public String code() {
        return code;
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}

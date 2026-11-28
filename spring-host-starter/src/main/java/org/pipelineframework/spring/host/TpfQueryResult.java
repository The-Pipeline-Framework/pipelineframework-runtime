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
 * Outcome returned by a {@link TpfQueryConnector} operation.
 */
public sealed interface TpfQueryResult<T>
    permits TpfQueryResult.Found,
            TpfQueryResult.NotFound,
            TpfQueryResult.AuthRequired,
            TpfQueryResult.TemporarilyUnavailable,
            TpfQueryResult.TerminalFailure {

    record Found<T>(T payload) implements TpfQueryResult<T> {
        public Found {
            Objects.requireNonNull(payload, "payload must not be null");
        }
    }

    record NotFound<T>(String code) implements TpfQueryResult<T> {
        public NotFound {
            code = requireCode(code, "not-found");
        }
    }

    record AuthRequired<T>(String code) implements TpfQueryResult<T> {
        public AuthRequired {
            code = requireCode(code, "auth-required");
        }
    }

    record TemporarilyUnavailable<T>(String code) implements TpfQueryResult<T> {
        public TemporarilyUnavailable {
            code = requireCode(code, "temporarily-unavailable");
        }
    }

    record TerminalFailure<T>(String code) implements TpfQueryResult<T> {
        public TerminalFailure {
            code = requireCode(code, "terminal-failure");
        }
    }

    static <T> TpfQueryResult<T> found(T payload) {
        return new Found<>(payload);
    }

    static <T> TpfQueryResult<T> notFound(String code) {
        return new NotFound<>(code);
    }

    static <T> TpfQueryResult<T> authRequired(String code) {
        return new AuthRequired<>(code);
    }

    static <T> TpfQueryResult<T> temporarilyUnavailable(String code) {
        return new TemporarilyUnavailable<>(code);
    }

    static <T> TpfQueryResult<T> terminalFailure(String code) {
        return new TerminalFailure<>(code);
    }

    private static String requireCode(String code, String fallback) {
        return (code == null || code.isBlank()) ? fallback : code;
    }
}

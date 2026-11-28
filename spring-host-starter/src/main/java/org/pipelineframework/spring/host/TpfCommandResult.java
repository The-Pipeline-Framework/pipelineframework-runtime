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
import java.util.Optional;

/**
 * Outcome returned by a {@link TpfCommandConnector} dispatch operation.
 */
public sealed interface TpfCommandResult<T>
    permits TpfCommandResult.Succeeded,
            TpfCommandResult.RetryableFailure,
            TpfCommandResult.TerminalFailure,
            TpfCommandResult.Ambiguous {

    record Succeeded<T>(T payload, Optional<String> receiptReference) implements TpfCommandResult<T> {
        public Succeeded {
            Objects.requireNonNull(payload, "payload must not be null");
            receiptReference = Objects.requireNonNull(receiptReference, "receiptReference must not be null")
                .filter(v -> !v.isBlank());
        }
    }

    record RetryableFailure<T>(String code) implements TpfCommandResult<T> {
        public RetryableFailure {
            code = requireCode(code, "retryable-failure");
        }
    }

    record TerminalFailure<T>(String code) implements TpfCommandResult<T> {
        public TerminalFailure {
            code = requireCode(code, "terminal-failure");
        }
    }

    record Ambiguous<T>(String code) implements TpfCommandResult<T> {
        public Ambiguous {
            code = requireCode(code, "ambiguous");
        }
    }

    static <T> TpfCommandResult<T> succeeded(T payload) {
        return new Succeeded<>(payload, Optional.empty());
    }

    static <T> TpfCommandResult<T> succeeded(T payload, String receiptReference) {
        return new Succeeded<>(payload, Optional.ofNullable(receiptReference));
    }

    static <T> TpfCommandResult<T> retryableFailure(String code) {
        return new RetryableFailure<>(code);
    }

    static <T> TpfCommandResult<T> terminalFailure(String code) {
        return new TerminalFailure<>(code);
    }

    static <T> TpfCommandResult<T> ambiguous(String code) {
        return new Ambiguous<>(code);
    }

    private static String requireCode(String code, String fallback) {
        return (code == null || code.isBlank()) ? fallback : code;
    }
}

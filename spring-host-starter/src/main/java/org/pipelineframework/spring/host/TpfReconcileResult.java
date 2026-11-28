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
 * Outcome returned by a {@link TpfCommandReconciler} reconciliation operation.
 */
public sealed interface TpfReconcileResult<T>
    permits TpfReconcileResult.ConfirmedSucceeded,
            TpfReconcileResult.Unresolved {

    record ConfirmedSucceeded<T>(T payload, String receiptReference) implements TpfReconcileResult<T> {
        public ConfirmedSucceeded {
            Objects.requireNonNull(payload, "payload must not be null");
            if (receiptReference == null || receiptReference.isBlank()) {
                throw new IllegalArgumentException("receiptReference must not be blank");
            }
        }
    }

    record Unresolved<T>(String code) implements TpfReconcileResult<T> {
        public Unresolved {
            code = (code == null || code.isBlank()) ? "reconcile-unresolved" : code;
        }
    }

    static <T> TpfReconcileResult<T> confirmedSucceeded(T payload, String receiptReference) {
        return new ConfirmedSucceeded<>(payload, receiptReference);
    }

    static <T> TpfReconcileResult<T> unresolved(String code) {
        return new Unresolved<>(code);
    }
}

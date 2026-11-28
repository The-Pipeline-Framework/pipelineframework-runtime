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

import java.util.Objects;
import java.util.Optional;

/**
 * Classified exception hierarchy for Spring-hosted foreign business-function steps.
 */
public sealed class SpringForeignStepException extends RuntimeException
    permits SpringForeignStepException.BusinessRejection,
            SpringForeignStepException.TransientFailure,
            SpringForeignStepException.OutcomeUnknown,
            SpringForeignStepException.AuthenticationFailure {

    public enum Disposition {
        TRANSIENT,
        BUSINESS_REJECTION,
        UNCLASSIFIED,
        OUTCOME_UNKNOWN,
        AUTHENTICATION_REJECTED
    }

    private final Disposition disposition;
    private final String stepId;
    private final int stepIndex;
    private final boolean retryable;

    protected SpringForeignStepException(
        Disposition disposition,
        String stepId,
        int stepIndex,
        boolean retryable,
        String message,
        Optional<Throwable> cause
    ) {
        super(message, cause.orElse(null));
        this.disposition = Objects.requireNonNull(disposition, "disposition");
        this.stepId = Objects.requireNonNull(stepId, "stepId");
        this.stepIndex = stepIndex;
        this.retryable = retryable;
    }

    public Disposition disposition() {
        return disposition;
    }

    public String stepId() {
        return stepId;
    }

    public int stepIndex() {
        return stepIndex;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public boolean isOutcomeUnknown() {
        return disposition == Disposition.OUTCOME_UNKNOWN;
    }

    public static final class BusinessRejection extends SpringForeignStepException {
        public BusinessRejection(String stepId, int stepIndex, Disposition disposition, String message) {
            super(disposition, stepId, stepIndex, false, message, Optional.empty());
        }
    }

    public static final class TransientFailure extends SpringForeignStepException {
        public TransientFailure(String stepId, int stepIndex, String message) {
            super(Disposition.TRANSIENT, stepId, stepIndex, true, message, Optional.empty());
        }
    }

    public static final class OutcomeUnknown extends SpringForeignStepException {
        private final String targetUri;

        public OutcomeUnknown(String stepId, int stepIndex, String targetUri, String message, Optional<Throwable> cause) {
            super(Disposition.OUTCOME_UNKNOWN, stepId, stepIndex, false, message, cause);
            this.targetUri = Objects.requireNonNull(targetUri, "targetUri");
        }

        public String targetUri() {
            return targetUri;
        }
    }

    public static final class AuthenticationFailure extends SpringForeignStepException {
        private final int statusCode;

        public AuthenticationFailure(String stepId, int stepIndex, int statusCode, String message) {
            super(Disposition.AUTHENTICATION_REJECTED, stepId, stepIndex, false, message, Optional.empty());
            this.statusCode = statusCode;
        }

        public int statusCode() {
            return statusCode;
        }
    }
}

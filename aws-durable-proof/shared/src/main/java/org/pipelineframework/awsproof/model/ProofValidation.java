package org.pipelineframework.awsproof.model;

import java.util.Objects;
import java.util.Optional;

final class ProofValidation {
    private ProofValidation() {
    }

    static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    static Optional<String> optional(Optional<String> value, String name) {
        Objects.requireNonNull(value, name);
        value.ifPresent(item -> required(item, name));
        return value;
    }
}

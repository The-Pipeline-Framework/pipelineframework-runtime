package org.pipelineframework.deployment.release;

import java.io.IOException;

public final class ReleaseVerificationException extends IOException {
    public enum Category { DESCRIPTOR, CLOSURE, RESOLUTION }

    private final Category category;

    public ReleaseVerificationException(Category category, String message, Throwable cause) {
        super(message, cause);
        this.category = category;
    }

    public Category category() {
        return category;
    }
}

package org.pipelineframework.deployment.release;

/** Environment-owned logical credential name. */
public record CredentialReference(String name) {
    public CredentialReference {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Credential reference name is required");
        }
        name = name.trim();
    }
}

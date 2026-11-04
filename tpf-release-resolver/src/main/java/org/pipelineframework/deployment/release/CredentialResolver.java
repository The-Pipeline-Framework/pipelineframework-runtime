package org.pipelineframework.deployment.release;

import java.util.Optional;

@FunctionalInterface
public interface CredentialResolver {
    CredentialResolver NONE = reference -> Optional.empty();

    Optional<Credential> resolve(CredentialReference reference);
}

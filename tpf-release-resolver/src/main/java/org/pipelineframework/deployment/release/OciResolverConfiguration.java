package org.pipelineframework.deployment.release;

import java.util.Map;
import java.util.Set;

public record OciResolverConfiguration(
    String credentialSource,
    Map<String, CredentialReference> registryCredentials,
    Set<String> insecureRegistries
) {
    public OciResolverConfiguration {
        credentialSource = credentialSource == null ? "" : credentialSource;
        registryCredentials = registryCredentials == null ? Map.of() : Map.copyOf(registryCredentials);
        insecureRegistries = insecureRegistries == null ? Set.of() : Set.copyOf(insecureRegistries);
    }
}

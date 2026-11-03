package org.pipelineframework.deployment.release;

import java.util.Optional;

public record ResolverProfile(
    boolean fileEnabled,
    Optional<MavenResolverConfiguration> maven,
    Optional<OciResolverConfiguration> oci
) {
    public ResolverProfile {
        maven = maven == null ? Optional.empty() : maven;
        oci = oci == null ? Optional.empty() : oci;
    }

    public static ResolverProfile localOnly() {
        return new ResolverProfile(true, Optional.empty(), Optional.empty());
    }
}

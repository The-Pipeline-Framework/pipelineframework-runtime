package org.pipelineframework.orchestrator.release;

import java.util.List;

/** Immutable worker dispatch identity; credentialRef names a credential, never its resolved value. */
public record PipelineReleaseDispatchBinding(
    String protocol, String endpoint, String credentialRef, String workerId
) {
    public PipelineReleaseDispatchBinding {
        for (String field : List.of(protocol, endpoint, credentialRef, workerId)) {
            if (field.isBlank() || !field.equals(field.trim()) || field.indexOf('\r') >= 0 || field.indexOf('\n') >= 0) {
                throw new IllegalArgumentException("Dispatch binding fields must be nonblank canonical values");
            }
        }
    }
}

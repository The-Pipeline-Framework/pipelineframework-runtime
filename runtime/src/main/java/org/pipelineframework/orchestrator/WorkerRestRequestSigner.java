package org.pipelineframework.orchestrator;

import java.net.http.HttpRequest;
import jakarta.enterprise.inject.Instance;

/** Optional deployment authentication applied after the TPF request signature. */
public interface WorkerRestRequestSigner {
    void sign(HttpRequest.Builder request, byte[] body);

    static HttpRequest finish(Instance<WorkerRestRequestSigner> signer, HttpRequest.Builder request, byte[] body) {
        if (signer != null && !signer.isUnsatisfied()) {
            signer.get().sign(request, body);
        }
        return request.build();
    }
}

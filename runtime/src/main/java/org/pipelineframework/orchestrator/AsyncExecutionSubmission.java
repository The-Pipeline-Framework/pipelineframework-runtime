package org.pipelineframework.orchestrator;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/** Optional ingress adapter for the Quarkus execution facade. Native host actions bypass this adapter. */
public interface AsyncExecutionSubmission {
    Uni<RunAsyncAcceptedDto> submit(Object input, String tenantId, String idempotencyKey, boolean outputStreaming);
}

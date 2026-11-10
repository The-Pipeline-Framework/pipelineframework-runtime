package org.pipelineframework.aws.durable.model;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonInclude;

public record AwsDurableExecutionInput(
    String tenantId,
    String idempotencyKey,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    String inputJson,
    Optional<String> resumeExecutionId,
    long generation,
    @JsonInclude(JsonInclude.Include.NON_ABSENT) Optional<String> submissionAttemptId
) {
    public AwsDurableExecutionInput(String tenantId, String idempotencyKey, String pipelineId,
        String contractVersion, String releaseVersion, String inputJson, Optional<String> resumeExecutionId, long generation) {
        this(tenantId, idempotencyKey, pipelineId, contractVersion, releaseVersion, inputJson, resumeExecutionId,
            generation, Optional.empty());
    }

    public AwsDurableExecutionInput {
        tenantId = AwsDurableValidation.required(tenantId, "tenantId");
        idempotencyKey = AwsDurableValidation.required(idempotencyKey, "idempotencyKey");
        pipelineId = AwsDurableValidation.required(pipelineId, "pipelineId");
        contractVersion = AwsDurableValidation.required(contractVersion, "contractVersion");
        releaseVersion = AwsDurableValidation.required(releaseVersion, "releaseVersion");
        inputJson = AwsDurableValidation.required(inputJson, "inputJson");
        resumeExecutionId = AwsDurableValidation.optional(resumeExecutionId, "resumeExecutionId");
        submissionAttemptId = submissionAttemptId == null ? Optional.empty()
            : AwsDurableValidation.optional(submissionAttemptId, "submissionAttemptId");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }

    public static AwsDurableExecutionInput resume(
        AwsDurableAwaitIdentity identity,
        String pipelineId,
        String contractVersion,
        String releaseVersion
    ) {
        return new AwsDurableExecutionInput(
            identity.tenantId(),
            identity.executionId(),
            pipelineId,
            contractVersion,
            releaseVersion,
            "{}",
            Optional.of(identity.executionId()),
            Math.addExact(identity.generation(), 1));
    }
}

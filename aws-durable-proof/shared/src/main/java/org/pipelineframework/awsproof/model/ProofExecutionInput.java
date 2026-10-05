package org.pipelineframework.awsproof.model;

import java.util.Optional;

public record ProofExecutionInput(
    String tenantId,
    String idempotencyKey,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    String inputJson,
    Optional<String> resumeExecutionId,
    long generation
) {
    public ProofExecutionInput {
        tenantId = ProofValidation.required(tenantId, "tenantId");
        idempotencyKey = ProofValidation.required(idempotencyKey, "idempotencyKey");
        pipelineId = ProofValidation.required(pipelineId, "pipelineId");
        contractVersion = ProofValidation.required(contractVersion, "contractVersion");
        releaseVersion = ProofValidation.required(releaseVersion, "releaseVersion");
        inputJson = ProofValidation.required(inputJson, "inputJson");
        resumeExecutionId = ProofValidation.optional(resumeExecutionId, "resumeExecutionId");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }

    public static ProofExecutionInput resume(
        ProofAwaitIdentity identity,
        String pipelineId,
        String contractVersion,
        String releaseVersion
    ) {
        return new ProofExecutionInput(
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

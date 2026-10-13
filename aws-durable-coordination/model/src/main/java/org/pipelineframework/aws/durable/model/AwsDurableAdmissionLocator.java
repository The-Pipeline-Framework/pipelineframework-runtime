package org.pipelineframework.aws.durable.model;

import java.util.Objects;
import java.util.Optional;

/** Hosting correlation committed before submission; it is never semantic admission authority. */
public record AwsDurableAdmissionLocator(String tenantId, String clientKey, String pipelineId,
    String contractVersion, String releaseVersion, Optional<String> executionId, long generation,
    String providerExecutionName, String providerExecutionArn) {
    public AwsDurableAdmissionLocator {
        tenantId = AwsDurableValidation.required(tenantId, "tenantId");
        clientKey = AwsDurableValidation.required(clientKey, "clientKey");
        pipelineId = AwsDurableValidation.required(pipelineId, "pipelineId");
        contractVersion = AwsDurableValidation.required(contractVersion, "contractVersion");
        releaseVersion = AwsDurableValidation.required(releaseVersion, "releaseVersion");
        executionId = Objects.requireNonNull(executionId, "executionId");
        executionId.ifPresent(id -> AwsDurableValidation.required(id, "executionId"));
        if (generation < 1) throw new IllegalArgumentException("generation must be positive");
        providerExecutionName = AwsDurableValidation.required(providerExecutionName, "providerExecutionName");
        providerExecutionArn = AwsDurableValidation.required(providerExecutionArn, "providerExecutionArn");
    }

    public String partitionKey() { return "ADMISSION#" + providerExecutionArn; }
    public String sortKey() { return "ADMISSION#" + String.format(java.util.Locale.ROOT, "%020d", generation); }
}

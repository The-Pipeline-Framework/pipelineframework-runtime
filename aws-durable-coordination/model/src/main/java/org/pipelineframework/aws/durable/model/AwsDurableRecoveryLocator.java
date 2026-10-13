package org.pipelineframework.aws.durable.model;

import java.util.Objects;

/** Retained hosting correlation, independent of disposable callback registration. */
public record AwsDurableRecoveryLocator(AwsDurableDriverCheckpoint checkpoint,
    String providerExecutionName, String providerExecutionArn) {
    public AwsDurableRecoveryLocator {
        checkpoint = Objects.requireNonNull(checkpoint, "checkpoint");
        providerExecutionName = AwsDurableValidation.required(providerExecutionName, "providerExecutionName");
        providerExecutionArn = AwsDurableValidation.required(providerExecutionArn, "providerExecutionArn");
    }
    public String partitionKey() { return checkpoint.tenantId() + "#" + checkpoint.executionId(); }
    public String sortKey() { return "RECOVERY#" + String.format(java.util.Locale.ROOT, "%020d", checkpoint.generation()); }
}

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
    public String sortKey() { return sortKey(checkpoint.generation()); }

    /** Storage key for the retained locator of a particular generation. */
    public static String sortKey(long generation) {
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
        return "RECOVERY#" + String.format(java.util.Locale.ROOT, "%020d", generation);
    }
}

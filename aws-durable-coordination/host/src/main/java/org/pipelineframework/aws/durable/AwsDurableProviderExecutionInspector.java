package org.pipelineframework.aws.durable;

@FunctionalInterface
interface AwsDurableProviderExecutionInspector {
    AwsDurableProviderExecutionState inspect(String providerExecutionArn);
}

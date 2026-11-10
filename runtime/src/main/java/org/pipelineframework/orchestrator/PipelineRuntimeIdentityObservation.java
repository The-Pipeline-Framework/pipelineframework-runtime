package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** Actual loaded compiled identity after strict queue-provider and generated-step validation. */
public record PipelineRuntimeIdentityObservation(String pipelineId, String contractVersion, String releaseVersion,
    boolean ready, int loadedStepCount, PipelineContractDescriptor compiledContract,
    String commandEffectStoreProvider, boolean commandEffectStoreRecoverySupported, String commandEffectStoreTable,
    String queryCaptureStoreProvider, String queryCaptureStoreTable) {
    public PipelineRuntimeIdentityObservation(String pipelineId, String contractVersion, String releaseVersion,
        boolean ready, int loadedStepCount, PipelineContractDescriptor compiledContract,
        String commandEffectStoreProvider, boolean commandEffectStoreRecoverySupported, String commandEffectStoreTable) {
        this(pipelineId, contractVersion, releaseVersion, ready, loadedStepCount, compiledContract,
            commandEffectStoreProvider, commandEffectStoreRecoverySupported, commandEffectStoreTable, "", "");
    }

    public PipelineRuntimeIdentityObservation(String pipelineId, String contractVersion, String releaseVersion,
            boolean ready, int loadedStepCount, PipelineContractDescriptor compiledContract) {
        this(pipelineId, contractVersion, releaseVersion, ready, loadedStepCount, compiledContract, "", false, "");
    }
}

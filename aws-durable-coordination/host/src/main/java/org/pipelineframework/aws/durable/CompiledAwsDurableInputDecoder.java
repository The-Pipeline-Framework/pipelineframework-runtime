package org.pipelineframework.aws.durable;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.quarkus.arc.DefaultBean;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;

/** Generic input boundary: compiler metadata, never application-specific decoder source, selects the type. */
@DefaultBean
@ApplicationScoped
public class CompiledAwsDurableInputDecoder implements AwsDurableInputDecoder {
    @Inject PipelineContractDescriptorLoader contracts;
    @Inject TransitionPayloadCodec codec;
    /** Decodes a typed payload only when it matches the compiled pipeline entry contract. */
    @Override public Object decode(String inputJson) {
        var contract = contracts.load().orElseThrow(() -> new IllegalStateException("Compiled input Contract is unavailable"));
        String entryType = entryType(contract);
        try {
            var payload = PipelineJson.mapper().readValue(inputJson, SerializedTransitionPayload.class);
            if (!entryType.equals(payload.payloadTypeId())) {
                throw new IllegalArgumentException("Input type does not match the compiled entry type");
            }
            return codec.decode(payload);
        } catch (java.io.IOException malformed) { throw new IllegalArgumentException("Input is not a typed TPF payload", malformed); }
    }

    /** Resolves the root input's Java type, with a first-step fallback for flat contracts. */
    private static String entryType(PipelineContractDescriptor contract) {
        var composition = contract.composition();
        if (composition.present()) {
            String inputContract = composition.definition(composition.rootDefinitionId()).inputContractId();
            var binding = contract.canonicalTypes().getOrDefault(inputContract, Map.of());
            return binding.get("runtimeClass") instanceof String runtimeClass ? runtimeClass : inputContract;
        }
        if (contract.steps().isEmpty()) {
            throw new IllegalStateException("Compiled input entry is unavailable");
        }
        return contract.steps().getFirst().inputTypeId();
    }
}

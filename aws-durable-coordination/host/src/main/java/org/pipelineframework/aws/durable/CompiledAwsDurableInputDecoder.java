package org.pipelineframework.aws.durable;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.quarkus.arc.DefaultBean;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;

/** Generic input boundary: compiler metadata, never application-specific decoder source, selects the type. */
@DefaultBean
@ApplicationScoped
public class CompiledAwsDurableInputDecoder implements AwsDurableInputDecoder {
    @Inject PipelineContractDescriptorLoader contracts;
    @Inject TransitionPayloadCodec codec;
    @Override public Object decode(String inputJson) {
        var contract = contracts.load().orElseThrow(() -> new IllegalStateException("Compiled input Contract is unavailable"));
        if (contract.steps().isEmpty()) throw new IllegalStateException("Compiled input entry is unavailable");
        try {
            var payload = PipelineJson.mapper().readValue(inputJson, SerializedTransitionPayload.class);
            if (!contract.steps().getFirst().inputTypeId().equals(payload.payloadTypeId())) {
                throw new IllegalArgumentException("Input type does not match the compiled entry type");
            }
            return codec.decode(payload);
        } catch (java.io.IOException malformed) { throw new IllegalArgumentException("Input is not a typed TPF payload", malformed); }
    }
}

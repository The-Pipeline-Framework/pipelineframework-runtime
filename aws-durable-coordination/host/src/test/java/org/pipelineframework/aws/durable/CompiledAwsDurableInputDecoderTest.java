package org.pipelineframework.aws.durable;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class CompiledAwsDurableInputDecoderTest {
    @Test void compilerSelectsActualInputTypeAndCodecRejectsWrongEncoding() {
        var decoder = new CompiledAwsDurableInputDecoder();
        decoder.contracts = mock(PipelineContractDescriptorLoader.class);
        decoder.codec = new JsonTransitionPayloadCodec();
        var contract = new PipelineContractDescriptorLoader().load(new ByteArrayInputStream("""
            {"schemaVersion":1,"pipelineId":"arbitrary.app","contractVersion":"sha","contractHash":"sha","steps":[
            {"index":0,"authoredName":"one","runtimeClass":"arbitrary.One","clientClass":"arbitrary.OneClient","cardinality":"ONE_TO_ONE","inputTypeId":"java.lang.String","outputTypeId":"java.lang.String"}]}
            """.getBytes(StandardCharsets.UTF_8)));
        when(decoder.contracts.load()).thenReturn(Optional.of(contract));
        assertThat(decoder.decode("""
            {"payloadTypeId":"java.lang.String","payloadEncoding":"application/tpf-transition+json","payload":"\\\"hello\\\""}
            """)).isEqualTo("hello");
        assertThatThrownBy(() -> decoder.decode("""
            {"payloadTypeId":"java.lang.Integer","payloadEncoding":"application/tpf-transition+json","payload":"1"}
            """)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("compiled entry type");
        assertThatThrownBy(() -> decoder.decode("""
            {"payloadTypeId":"java.lang.String","payloadEncoding":"json","payload":"\\\"hello\\\""}
            """)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("encoding");
        when(decoder.contracts.load()).thenReturn(Optional.empty());
        assertThatThrownBy(() -> decoder.decode("{}")).isInstanceOf(IllegalStateException.class).hasMessageContaining("Contract");
    }
}

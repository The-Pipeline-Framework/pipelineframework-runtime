package org.pipelineframework.orchestrator;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class CompiledContractPayloadCodecTest {
 @Test void acceptsExactCompiledCustomerTypeWithoutPermittingPrefixNeighbors() {
  var codec = new JsonTransitionPayloadCodec();
  codec.orchestratorConfig = mock(PipelineOrchestratorConfig.class);
  when(codec.orchestratorConfig.coordinationHost()).thenReturn(CoordinationHost.AWS_DURABLE);
  codec.releaseIdentityResolver = mock(PipelineReleaseIdentityResolver.class);
  var compiled = new PipelineContractDescriptorLoader().load(new ByteArrayInputStream("""
   {"schemaVersion":1,"pipelineId":"customer","contractVersion":"sha","contractHash":"sha","steps":[
   {"index":0,"authoredName":"one","inputTypeId":"com.example.CustomerPayload","outputTypeId":"com.example.CustomerPayload"}]}
   """.getBytes(StandardCharsets.UTF_8)));
  when(codec.releaseIdentityResolver.contract()).thenReturn(compiled);
  var value = new com.example.CustomerPayload("customer value");
  assertEquals(value, codec.decode(codec.encode(value)));
  assertThrows(IllegalArgumentException.class, () -> codec.encode(new com.example.CustomerPayloadExtra("forbidden")));
  assertThrows(IllegalArgumentException.class, () -> codec.decode(new SerializedTransitionPayload("com.example.CustomerPayloadExtra", JsonTransitionPayloadCodec.ENCODING, "{}")));
 }
}

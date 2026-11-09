package org.pipelineframework.extension;

import java.util.List;
import io.quarkus.arc.processor.BeanInfo;
import org.jboss.jandex.ClassType;
import org.junit.jupiter.api.Test;
import org.pipelineframework.command.DynamoCommandEffectStore;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.orchestrator.release.PipelineReleaseRuntimeCapabilities;
import org.pipelineframework.query.DynamoQueryCaptureStore;
import org.pipelineframework.query.InMemoryQueryCaptureStore;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CommandStoreCapabilityBuildStepTest {
    @Test void producerMetadataMustNameAConcreteSupportedStore() {
        var producer = mock(BeanInfo.class);
        when(producer.isProducer()).thenReturn(true);
        // Even a producer declared on this class must not advertise its unknown interface return as Dynamo.
        when(producer.getBeanClass()).thenReturn(org.jboss.jandex.DotName.createSimple(DynamoCommandEffectStore.class));
        when(producer.getProviderType()).thenReturn(ClassType.create(org.pipelineframework.command.CommandEffectStore.class));
        String unknown = CommandStoreCapabilityBuildStep.selectedType(producer);
        assertEquals(org.pipelineframework.command.CommandEffectStore.class.getName(), unknown);
        assertThrows(IllegalArgumentException.class, () -> CommandStoreCapabilityBuildStep.facts(List.of(unknown), List.of())
            .requireRecoverable(PipelineReleaseRuntimeCapabilities.COMMAND_EFFECT_DYNAMODB));
        when(producer.getProviderType()).thenReturn(ClassType.create(DynamoCommandEffectStore.class));
        assertEquals(DynamoCommandEffectStore.class.getName(), CommandStoreCapabilityBuildStep.selectedType(producer));
    }

    @Test void exportsOnlyExactlySelectedDurableStore() throws Exception {
        String capability = PipelineReleaseRuntimeCapabilities.COMMAND_EFFECT_DYNAMODB;
        var durable = CommandStoreCapabilityBuildStep.facts(List.of(DynamoCommandEffectStore.class.getName()), List.of());
        assertEquals("dynamo", PipelineReleaseRuntimeCapabilities.parse(durable.encode()).requireRecoverable(capability).provider());
        for (var beans : List.of(List.<String>of(), List.of(InMemoryCommandEffectStore.class.getName()),
                List.of(InMemoryCommandEffectStore.class.getName(), DynamoCommandEffectStore.class.getName()))) {
            assertThrows(IllegalArgumentException.class, () -> CommandStoreCapabilityBuildStep.facts(beans, List.of()).requireRecoverable(capability));
        }
    }

    @Test void queryFactsRequireExactlyOneSelectedDynamoCaptureStore() throws Exception {
        String capability = PipelineReleaseRuntimeCapabilities.QUERY_CAPTURE_DYNAMODB;
        String dynamo = DynamoQueryCaptureStore.class.getName();
        String memory = InMemoryQueryCaptureStore.class.getName();
        var durable = CommandStoreCapabilityBuildStep.facts(List.of(), List.of(dynamo));
        assertEquals(List.of(dynamo), PipelineReleaseRuntimeCapabilities.parse(durable.encode()).requireRecoverable(capability).selectedBeans());
        for (var beans : List.of(List.<String>of(), List.of(memory), List.of(memory, dynamo))) {
            assertThrows(IllegalArgumentException.class,
                () -> CommandStoreCapabilityBuildStep.facts(List.of(), beans).requireRecoverable(capability));
        }
    }
}

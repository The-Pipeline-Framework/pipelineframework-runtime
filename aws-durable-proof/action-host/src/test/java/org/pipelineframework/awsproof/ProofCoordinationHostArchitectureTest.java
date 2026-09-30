package org.pipelineframework.awsproof;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PipelineControlPlane;

class ProofCoordinationHostArchitectureTest {

    @Test
    void gatewaySeparatesTpfActionsFromAwsDurableMechanics() {
        assertThat(fieldTypes(ProofActionGatewayHandler.class))
            .contains(ProofControlPlaneActionAdapter.class, ProofDurableHostActionAdapter.class)
            .doesNotContain(PipelineControlPlane.class, ProofCallbackBindingRepository.class);
        assertThat(fieldTypes(ProofDurableHostActionAdapter.class))
            .doesNotContain(PipelineControlPlane.class);
        assertThat(fieldTypes(ProofControlPlaneActionAdapter.class))
            .contains(PipelineControlPlane.class)
            .doesNotContain(ProofCallbackBindingRepository.class, ProofWakeupService.class);
    }

    private static Class<?>[] fieldTypes(Class<?> type) {
        return Arrays.stream(type.getDeclaredFields()).map(Field::getType).toArray(Class<?>[]::new);
    }
}

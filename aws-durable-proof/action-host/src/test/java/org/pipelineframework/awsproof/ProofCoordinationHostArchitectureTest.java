package org.pipelineframework.awsproof;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
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
            .contains(org.pipelineframework.aws.durable.AwsDurableActionInvoker.class)
            .doesNotContain(PipelineControlPlane.class, ProofCallbackBindingRepository.class, ProofWakeupService.class);
    }

    @Test
    void actionHostDisablesProcessLoopsWithoutQuarkusTypeExclusions() throws Exception {
        String properties = Files.readString(Path.of("src/main/resources/application.properties"));

        assertThat(properties).contains("pipeline.orchestrator.process-loops-disabled=true");
        assertThat(properties).doesNotContain("quarkus.arc.exclude-types");
    }

    private static Class<?>[] fieldTypes(Class<?> type) {
        return Arrays.stream(type.getDeclaredFields()).map(Field::getType).toArray(Class<?>[]::new);
    }
}

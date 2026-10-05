package org.pipelineframework.awsproof;

import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProofFaultScenarioCatalogTest {
    @Test
    void catalogContainsEveryAcceptanceScenarioExactlyOnce() {
        var scenarios = ProofFaultScenarioCatalog.all();

        assertThat(scenarios).hasSize(22);
        assertThat(scenarios.stream().map(ProofFaultScenarioCatalog.Scenario::id))
            .containsExactlyElementsOf(IntStream.rangeClosed(1, 22).boxed().toList());
        assertThat(scenarios)
            .allMatch(scenario -> scenario.lane() == ProofFaultScenarioCatalog.Lane.QUICK);
    }
}

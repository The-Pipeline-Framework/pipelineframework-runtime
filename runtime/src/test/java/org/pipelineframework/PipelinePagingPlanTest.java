package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

class PipelinePagingPlanTest {

    @Test
    void rejectsFractionalMaxRecordsBeforeNarrowing() {
        assertThrows(IllegalStateException.class, () -> PipelinePagingPlan.from(contractWith(1.5d)));
    }

    @Test
    void rejectsMaxRecordsOutsidePositiveIntRange() {
        assertThrows(IllegalStateException.class,
            () -> PipelinePagingPlan.from(contractWith((long) Integer.MAX_VALUE + 1L)));
    }

    @Test
    void acceptsIntegralNumericValue() {
        assertEquals(1_000, PipelinePagingPlan.from(contractWith(1_000.0d)).orElseThrow().maxRecords());
    }

    private PipelineContractDescriptor contractWith(Number maxRecords) {
        PipelineBundleStepDescriptor source = mock(PipelineBundleStepDescriptor.class);
        when(source.index()).thenReturn(0);
        when(source.cardinality()).thenReturn("ONE_TO_MANY");
        when(source.paging()).thenReturn(Map.of("maxRecords", maxRecords));
        PipelineContractDescriptor contract = mock(PipelineContractDescriptor.class);
        when(contract.steps()).thenReturn(List.of(source));
        return contract;
    }
}

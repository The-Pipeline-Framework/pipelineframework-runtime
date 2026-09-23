package org.pipelineframework;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** Release-pinned paging declaration projected from generated contract metadata. */
record PipelinePagingPlan(int maxRecords) {
  PipelinePagingPlan {
    if (maxRecords < 1) {
      throw new IllegalArgumentException("paging.maxRecords must be positive");
    }
  }

  static Optional<PipelinePagingPlan> from(PipelineContractDescriptor contract) {
    if (contract == null) {
      return Optional.empty();
    }
    List<PipelineBundleStepDescriptor> steps = contract.steps();
    if (steps == null || steps.isEmpty()) {
      return Optional.empty();
    }
    PipelineBundleStepDescriptor source = steps.getFirst();
    Map<String, Object> paging = source.paging();
    if (paging == null || paging.isEmpty()) {
      return Optional.empty();
    }
    if (source.index() != 0
        || CardinalitySemantics.fromString(source.cardinality()) != CardinalitySemantics.ONE_TO_MANY) {
      throw new IllegalStateException("paging requires the first step to be ONE_TO_MANY");
    }
    Object configured = paging.get("maxRecords");
    if (!(configured instanceof Number number) || number.intValue() < 1) {
      throw new IllegalStateException("generated paging.maxRecords must be a positive integer");
    }
    return Optional.of(new PipelinePagingPlan(number.intValue()));
  }
}

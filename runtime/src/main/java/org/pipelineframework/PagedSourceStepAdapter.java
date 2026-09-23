package org.pipelineframework;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Multi;
import org.pipelineframework.orchestrator.PagedTransitionContext;
import org.pipelineframework.paging.PagedSourceCompletion;
import org.pipelineframework.paging.PagedSourceOperation;
import org.pipelineframework.paging.PagedSourceRequest;
import org.pipelineframework.paging.PagedSourceStream;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.telemetry.PageExecutionTelemetry;

/** Adapts one provider-owned page to the ordinary ONE_TO_MANY runtime path. */
final class PagedSourceStepAdapter extends ConfigurableStep implements StepOneToMany<Object, Object> {
  private final PagedSourceOperation<Object, Object> operation;
  private final PagedTransitionContext context;
  private final AtomicReference<CompletionStage<PagedSourceCompletion>> completion;
  private final PageExecutionTelemetry telemetry;
  private final boolean replay;

  @SuppressWarnings("unchecked")
  PagedSourceStepAdapter(
      Object sourceStep,
      PagedTransitionContext context,
      AtomicReference<CompletionStage<PagedSourceCompletion>> completion,
      PageExecutionTelemetry telemetry,
      boolean replay) {
    if (!(sourceStep instanceof PagedSourceOperation<?, ?> paged)) {
      throw new IllegalStateException(
          "paging requires the first ONE_TO_MANY step to implement PagedSourceOperation");
    }
    this.operation = (PagedSourceOperation<Object, Object>) paged;
    this.context = Objects.requireNonNull(context, "context must not be null");
    this.completion = Objects.requireNonNull(completion, "completion must not be null");
    this.telemetry = Objects.requireNonNull(telemetry, "telemetry must not be null");
    this.replay = replay;
  }

  @Override
  public Multi<Object> applyOneToMany(Object input) {
    PagedSourceStream<Object> opened = operation.openPage(new PagedSourceRequest<>(
        input,
        context.sourceIdentity(),
        context.startCheckpoint(),
        context.maxRecords()));
    PageExecutionTelemetry.PageObservation observation = telemetry.open(replay);
    CompletionStage<PagedSourceCompletion> observedCompletion = opened.completion()
        .whenComplete((result, failure) -> {
          if (failure == null) {
            observation.complete(result);
          }
        });
    if (!completion.compareAndSet(null, observedCompletion)) {
      return Multi.createFrom().failure(new IllegalStateException(
          "a paged source step may open only one page per transition"));
    }
    return Multi.createFrom().publisher(observation.observeDemand(opened.items()));
  }
}

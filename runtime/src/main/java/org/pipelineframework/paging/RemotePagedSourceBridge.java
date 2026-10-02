package org.pipelineframework.paging;

import io.smallrye.mutiny.Multi;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.function.Function;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;

/** Preserves provider item demand while appending one completion frame after resource release. */
@ApplicationScoped
public class RemotePagedSourceBridge {

  @Inject PipelineReleaseIdentityResolver releaseIdentity;
  @Inject PipelineOrchestratorConfig orchestratorConfig;

  public String catalogFingerprint() {
    return releaseIdentity.contract().canonicalCatalogFingerprint();
  }

  public void validateRelease(String pipelineId, String contractVersion,
      String releaseVersion, String catalogFingerprint) {
    if (!releaseIdentity.pipelineId(orchestratorConfig).equals(pipelineId)
        || contractVersion == null || contractVersion.isBlank()
        || !releaseIdentity.releaseVersion(orchestratorConfig).equals(releaseVersion)) {
      throw new IllegalArgumentException("remote page targets a different pipeline release");
    }
    if (catalogFingerprint == null || catalogFingerprint.isBlank()
        || !catalogFingerprint().equals(catalogFingerprint)) {
      throw new IllegalArgumentException("remote page uses a different canonical type catalogue");
    }
  }

  public <I, O, F> Multi<F> serve(
      PagedSourceStream<O> opened,
      PagedSourceRequest<I> request,
      Function<O, F> itemFrame,
      Function<PagedSourceCompletion, F> completionFrame) {
    Objects.requireNonNull(opened, "opened must not be null");
    Objects.requireNonNull(request, "request must not be null");
    Objects.requireNonNull(itemFrame, "itemFrame must not be null");
    Objects.requireNonNull(completionFrame, "completionFrame must not be null");
    Multi<F> items = Multi.createFrom().publisher(opened.items()).onItem().transform(itemFrame);
    Multi<F> end = Multi.createFrom().completionStage(opened.completion())
        .onItem().transform(completion -> {
          completion.validateAgainst(request);
          return completionFrame.apply(completion);
        });
    return Multi.createBy().concatenating().streams(items, end);
  }

  public <I, O, F> PagedSourceStream<O> open(
      PagedSourceRequest<I> request,
      Flow.Publisher<F> remoteFrames,
      Function<F, RemotePageFrame<O>> decode) {
    return new RemotePagedSourceClient<>(request, remoteFrames, decode).stream();
  }
}

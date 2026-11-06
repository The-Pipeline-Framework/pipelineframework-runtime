package org.pipelineframework.release.count;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
@PipelineStep
public final class CountService implements ReactiveService<NormalizedDocument,WordCount> {
    @Override public Uni<WordCount> process(NormalizedDocument input) { return Uni.createFrom().item(new WordCount(input.content().isEmpty() ? 0 : input.content().split("\\s+").length)); }
}

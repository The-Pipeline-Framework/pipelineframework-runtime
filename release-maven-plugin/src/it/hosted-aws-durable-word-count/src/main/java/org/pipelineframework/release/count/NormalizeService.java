package org.pipelineframework.release.count;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
@PipelineStep
public final class NormalizeService implements ReactiveService<Document,NormalizedDocument> {
    @Override public Uni<NormalizedDocument> process(Document input) { return Uni.createFrom().item(new NormalizedDocument(input.content().trim())); }
}

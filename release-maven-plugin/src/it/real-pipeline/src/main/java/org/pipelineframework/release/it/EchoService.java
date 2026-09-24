package org.pipelineframework.release.it;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;

@PipelineStep
public final class EchoService implements ReactiveService<EchoInput, EchoOutput> {
    @Override
    public Uni<EchoOutput> process(EchoInput input) {
        return Uni.createFrom().item(new EchoOutput(input.value()));
    }
}

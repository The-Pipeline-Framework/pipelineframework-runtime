package com.example.echo;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
@Unremovable
@PipelineStep
public final class EchoService implements ReactiveService<EchoInput, EchoOutput> {
    @Override
    public Uni<EchoOutput> process(EchoInput input) {
        return Uni.createFrom().item(new EchoOutput(input.value()));
    }
}

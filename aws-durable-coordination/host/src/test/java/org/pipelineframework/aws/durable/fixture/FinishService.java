package org.pipelineframework.aws.durable.fixture;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

@ApplicationScoped @Unremovable
public class FinishService extends ConfigurableStep implements StepOneToOne<Prepared, Result> {
    @Override public Uni<Result> applyOneToOne(Prepared prepared) {
        return Uni.createFrom().item(new Result(prepared.id(), prepared.value()));
    }
}

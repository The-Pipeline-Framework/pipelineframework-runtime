package org.pipelineframework.awsproof;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionSupport;
import org.pipelineframework.service.ReactiveService;

/** Minimal generated-shape Await step used only by the deployed proof pipeline. */
@Unremovable
@ApplicationScoped
public final class ProofAwaitStep implements ReactiveService<ProofPipelineInput, Object> {
    @Inject
    AwaitCompletionSupport awaits;

    @Inject
    ProofFaultInjector faults;

    @Inject
    ProofAwaitDescriptorFactory descriptors;

    @Override
    public Uni<Object> process(ProofPipelineInput input) {
        faults.failIfArmed("pipeline-transition", input.request());
        return awaits.awaitOneToOne(
            descriptors.create(), input);
    }
}

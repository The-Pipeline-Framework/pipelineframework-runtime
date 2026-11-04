package org.pipelineframework.aws.durable.fixture;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.WorkDispatcher;

/** Replaces external queue delivery only; the native runtime owns every transition. */
@ApplicationScoped @Unremovable
public class CapturedDelivery implements WorkDispatcher {
    private final LinkedBlockingQueue<ExecutionWorkItem> queue = new LinkedBlockingQueue<>();
    @Override public String providerName() { return "native-await-test"; }
    @Override public Optional<String> startupValidationError() { return Optional.empty(); }
    @Override public Uni<Void> enqueueNow(ExecutionWorkItem item) {
        return Uni.createFrom().voidItem().invoke(() -> queue.add(item));
    }
    @Override public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
        return Uni.createFrom().failure(new AssertionError("Fixture must not schedule retry work"));
    }
    public ExecutionWorkItem receive() throws InterruptedException {
        return Optional.ofNullable(queue.poll(3, TimeUnit.SECONDS)).orElseThrow(() -> new AssertionError("No native delivery"));
    }
    public int pending() { return queue.size(); }
}

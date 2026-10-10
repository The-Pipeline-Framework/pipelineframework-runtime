package org.pipelineframework.orchestrator.worker;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WorkerReregistrationDrainTest {
    @Test
    void reregistrationAndHeartbeatCannotUndrainWorker() {
        var registry = new InMemoryPipelineWorkerRegistry();
        var registration = new PipelineWorkerRegistration("tenant", "pipeline", "contract", "release",
            "worker", "rest", "https://worker.example", "app", "sha256:artifact");
        registry.register(registration, 1000).await().indefinitely();
        registry.markDraining("tenant", "pipeline", "worker", 2000, Duration.ofMinutes(2)).await().indefinitely();
        var repeated = registry.register(registration, 3000).await().indefinitely();
        assertEquals(PipelineWorkerState.DRAINING, repeated.state());
        assertEquals(2000, repeated.drainingSinceEpochMs());
        assertEquals(3000, repeated.lastHeartbeatAtEpochMs());
        var heartbeat = registry.heartbeat("tenant", "pipeline", "worker", 4000, Duration.ofMinutes(2))
            .await().indefinitely().orElseThrow();
        assertEquals(PipelineWorkerState.DRAINING, heartbeat.state());
        assertEquals(2000, heartbeat.drainingSinceEpochMs());
    }
}

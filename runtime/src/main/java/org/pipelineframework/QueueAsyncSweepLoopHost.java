package org.pipelineframework;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import io.quarkus.runtime.StartupEvent;
import org.jboss.logging.Logger;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

/**
 * Compute-first lifecycle host for periodic queue-async sweeps.
 */
@ApplicationScoped
class QueueAsyncSweepLoopHost {

  private static final Logger LOG = Logger.getLogger(QueueAsyncSweepLoopHost.class);

  @Inject
  PipelineOrchestratorConfig orchestratorConfig;

  @Inject
  PipelineControlPlane controlPlane;

  private final ScheduledExecutorService sweepExecutor;
  private final LongSupplier currentTimeMillis;
  private volatile ScheduledFuture<?> sweepFuture;
  private volatile boolean started;

  QueueAsyncSweepLoopHost() {
    this(
        Executors.newSingleThreadScheduledExecutor(runnable -> {
          Thread thread = new Thread(runnable, "tpf-queue-sweeper");
          thread.setDaemon(true);
          return thread;
        }),
        System::currentTimeMillis);
  }

  QueueAsyncSweepLoopHost(
      ScheduledExecutorService sweepExecutor,
      LongSupplier currentTimeMillis) {
    this.sweepExecutor = sweepExecutor;
    this.currentTimeMillis = currentTimeMillis;
  }

  void onStartup(@Observes StartupEvent event) {
    start();
  }

  synchronized void start() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC || started) {
      return;
    }
    controlPlane.initializeQueueMode();
    Duration interval = orchestratorConfig.sweepInterval();
    long intervalMs = Math.max(1000L, interval == null ? 30000L : interval.toMillis());
    sweepFuture = sweepExecutor.scheduleAtFixedRate(
        this::sweepOnce,
        intervalMs,
        intervalMs,
        TimeUnit.MILLISECONDS);
    started = true;
    LOG.infof("Queue async periodic sweep host started with interval=%s", Duration.ofMillis(intervalMs));
  }

  void sweepOnce() {
    try {
      controlPlane.sweepOnce(currentTimeMillis.getAsLong())
          .subscribe()
          .with(
              ignored -> {
              },
              failure -> LOG.errorf(failure, "Failed sweeping due async executions"));
    } catch (RuntimeException failure) {
      LOG.errorf(failure, "Failed sweeping due async executions");
    }
  }

  @PreDestroy
  synchronized void shutdown() {
    started = false;
    ScheduledFuture<?> activeSweepFuture = sweepFuture;
    if (activeSweepFuture != null) {
      activeSweepFuture.cancel(false);
    }
    sweepFuture = null;
    sweepExecutor.shutdownNow();
  }

  boolean started() {
    return started;
  }
}

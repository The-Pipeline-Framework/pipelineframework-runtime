package org.pipelineframework;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.CoordinatorSweepResult;
import org.pipelineframework.orchestrator.CoordinationHost;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class QueueAsyncSweepLoopHostTest {

  @Test
  void serviceLifecycleStartsExactlyOnePeriodicSweep() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> future = mock(ScheduledFuture.class);
    doReturn(future).when(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(2000L), eq(2000L), eq(TimeUnit.MILLISECONDS));
    PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
    QueueAsyncSweepLoopHost host = host(executor, controlPlane, OrchestratorMode.QUEUE_ASYNC, 123L);

    host.start();
    host.start();

    assertTrue(host.started());
    verify(controlPlane, times(1)).initializeQueueMode();
    verify(executor, times(1)).scheduleAtFixedRate(
        any(Runnable.class), eq(2000L), eq(2000L), eq(TimeUnit.MILLISECONDS));

    host.shutdown();
    assertFalse(host.started());
    verify(future).cancel(false);
    verify(executor).shutdownNow();
  }

  @Test
  void disabledModeDoesNotStartScheduler() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
    QueueAsyncSweepLoopHost host = host(executor, controlPlane, OrchestratorMode.SYNC, 123L);

    host.start();

    assertFalse(host.started());
    verify(controlPlane, never()).initializeQueueMode();
    verify(executor, never()).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
  }

  @Test
  void awsDurableHostDoesNotStartNativeSweepScheduler() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(config.coordinationHost()).thenReturn(CoordinationHost.AWS_DURABLE);
    QueueAsyncSweepLoopHost host = new QueueAsyncSweepLoopHost(executor, () -> 123L);
    host.orchestratorConfig = config;
    host.controlPlane = controlPlane;

    host.start();

    assertFalse(host.started());
    verify(controlPlane, never()).initializeQueueMode();
    verify(executor, never()).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
  }

  @Test
  void eventSourceArtifactDoesNotStartProcessSweepScheduler() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(config.coordinationHost()).thenReturn(CoordinationHost.NATIVE);
    when(config.processLoopsDisabled()).thenReturn(true);
    QueueAsyncSweepLoopHost host = new QueueAsyncSweepLoopHost(executor, () -> 123L);
    host.orchestratorConfig = config;
    host.controlPlane = controlPlane;

    host.start();

    assertFalse(host.started());
    verify(controlPlane, never()).initializeQueueMode();
    verify(executor, never()).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
  }

  @Test
  void oneTickUsesCurrentTimeAndFailureDoesNotPreventLaterTick() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
    when(controlPlane.sweepOnce(456L)).thenReturn(
        Uni.createFrom().failure(new IllegalStateException("store down")),
        Uni.createFrom().item(new CoordinatorSweepResult(456L, 100, 0, 0)));
    QueueAsyncSweepLoopHost host = host(executor, controlPlane, OrchestratorMode.QUEUE_ASYNC, 456L);

    assertDoesNotThrow(host::sweepOnce);
    assertDoesNotThrow(host::sweepOnce);

    verify(controlPlane, times(2)).sweepOnce(456L);
  }

  @Test
  void synchronousClockAndSweepFailuresDoNotPreventLaterTicks() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
    LongSupplier clock = mock(LongSupplier.class);
    when(controlPlane.sweepOnce(789L))
        .thenThrow(new IllegalStateException("sweep setup failed"))
        .thenReturn(Uni.createFrom().item(new CoordinatorSweepResult(789L, 100, 0, 0)));
    when(clock.getAsLong()).thenThrow(new IllegalStateException("clock unavailable"))
        .thenReturn(789L, 789L);
    QueueAsyncSweepLoopHost host = host(executor, controlPlane, OrchestratorMode.QUEUE_ASYNC, clock);

    assertDoesNotThrow(host::sweepOnce);
    assertDoesNotThrow(host::sweepOnce);
    assertDoesNotThrow(host::sweepOnce);

    verify(controlPlane, times(2)).sweepOnce(789L);
  }

  private static QueueAsyncSweepLoopHost host(
      ScheduledExecutorService executor,
      PipelineControlPlane controlPlane,
      OrchestratorMode mode,
      long nowEpochMs) {
    return host(executor, controlPlane, mode, () -> nowEpochMs);
  }

  private static QueueAsyncSweepLoopHost host(
      ScheduledExecutorService executor,
      PipelineControlPlane controlPlane,
      OrchestratorMode mode,
      LongSupplier currentTimeMillis) {
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    when(config.mode()).thenReturn(mode);
    when(config.sweepInterval()).thenReturn(Duration.ofSeconds(2));
    QueueAsyncSweepLoopHost host = new QueueAsyncSweepLoopHost(executor, currentTimeMillis);
    host.orchestratorConfig = config;
    host.controlPlane = controlPlane;
    return host;
  }
}

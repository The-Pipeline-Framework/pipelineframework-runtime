package org.pipelineframework.orchestrator;

/**
 * Summary of one bounded queue-async coordinator sweep.
 *
 * @param nowEpochMs timestamp supplied to the sweep action
 * @param limit configured maximum batch size used by the sweep
 * @param timedOutAwaitCount successfully admitted await timeout transitions
 * @param dispatchedExecutionCount successfully dispatched due executions
 */
public record CoordinatorSweepResult(
    long nowEpochMs,
    int limit,
    int timedOutAwaitCount,
    int dispatchedExecutionCount) {

  public CoordinatorSweepResult {
    if (nowEpochMs < 0) {
      throw new IllegalArgumentException("nowEpochMs must not be negative");
    }
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be positive");
    }
    if (timedOutAwaitCount < 0) {
      throw new IllegalArgumentException("timedOutAwaitCount must not be negative");
    }
    if (dispatchedExecutionCount < 0) {
      throw new IllegalArgumentException("dispatchedExecutionCount must not be negative");
    }
  }
}

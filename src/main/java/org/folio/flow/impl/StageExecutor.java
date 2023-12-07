package org.folio.flow.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.folio.flow.api.StageContext;
import org.folio.flow.model.StageExecutionResult;

public interface StageExecutor {

  /**
   * Executes stage using upstream {@link StageExecutionResult} and {@link Executor}.
   *
   * @param upstreamResult - upstream stage result.
   * @param executor - root executor from {@link org.folio.flow.api.FlowEngine}
   * @return {@link CompletableFuture} with {@link StageExecutionResult}
   */
  CompletableFuture<StageExecutionResult> execute(StageExecutionResult upstreamResult, Executor executor);

  /**
   * Skips stage using upstream {@link StageExecutionResult} and {@link Executor}.
   *
   * @param upstreamResult - upstream stage result.
   * @param executor - root executor from {@link org.folio.flow.api.FlowEngine}
   * @return {@link CompletableFuture} with {@link StageExecutionResult}
   */
  CompletableFuture<StageExecutionResult> skip(StageExecutionResult upstreamResult, Executor executor);

  /**
   * Cancels stage using upstream {@link StageExecutionResult} and {@link Executor}.
   *
   * <p>
   * If cancellation is not support, upstream stage result can be returned with status
   * {@link org.folio.flow.model.ExecutionStatus#CANCELLATION_IGNORED}
   * </p>
   *
   * @param upstreamResult - upstream stage result.
   * @param executor - root executor from {@link org.folio.flow.api.FlowEngine}
   * @return {@link CompletableFuture} with {@link StageExecutionResult}
   */
  CompletableFuture<StageExecutionResult> cancel(StageExecutionResult upstreamResult, Executor executor);

  /**
   * Returns stage name.
   *
   * @return stage name as {@link String} object
   */
  default String getStageId() {
    return toString();
  }

  /**
   * Defines if stage with {@link org.folio.flow.model.ExecutionStatus#FAILED} must be attempted to cancel.
   *
   * @return true - if stage should be attempted to cancel, false - otherwise.
   */
  default boolean shouldCancelIfFailed(StageContext stageContext) {
    return true;
  }
}

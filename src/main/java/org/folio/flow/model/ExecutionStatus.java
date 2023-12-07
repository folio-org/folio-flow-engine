package org.folio.flow.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.folio.flow.api.Cancellable;
import org.folio.flow.api.StageContext;
import org.folio.flow.impl.StageExecutor;

@Getter
@RequiredArgsConstructor
public enum ExecutionStatus {

  /**
   * Indicates that {@link org.folio.flow.api.Stage} or {@link StageExecutor} is in progress.
   *
   * <p>{@link org.folio.flow.api.Flow} will be continued after stage with that status.</p>
   */
  IN_PROGRESS("in_progress"),

  /**
   * Indicates that {@link org.folio.flow.api.Stage} or {@link StageExecutor} was executed successfully.
   *
   * <p>{@link org.folio.flow.api.Flow} will be continued after stage with that status.</p>
   */
  SUCCESS("success"),

  /**
   * Indicates that {@link org.folio.flow.api.Stage} or {@link StageExecutor} was recovered successfully after error.
   *
   * <p>{@link org.folio.flow.api.Flow} will be continued after stage with that status.</p>
   */
  RECOVERED("recovered"),

  /**
   * Indicates that {@link org.folio.flow.api.Stage} or {@link StageExecutor} executed with error.
   *
   * <p>{@link org.folio.flow.api.Flow} will be failed or cancelled, depends on {@link FlowExecutionStrategy}.</p>
   */
  FAILED("failed"),

  /**
   * Indicates that {@link org.folio.flow.api.Stage} or {@link StageExecutor} was cancelled after cancellation signal,
   * stage must implement {@link Cancellable} interface to be able to cancel work.
   *
   * <p>
   * {@link FlowExecutionStrategy} must be set to {@link FlowExecutionStrategy#CANCEL_ON_ERROR}.
   * </p>
   */
  CANCELLED("cancelled"),

  /**
   * Indicates that {@link org.folio.flow.api.Stage} or {@link StageExecutor} was skipped, because upstream stage has
   * failed.
   */
  SKIPPED("skipped"),

  /**
   * Indicates that {@link org.folio.flow.api.Stage} or {@link StageExecutor} cancellation was failed due to exception
   * in {@link Cancellable#cancel(StageContext)} method.
   *
   * <p>
   * {@link FlowExecutionStrategy} must be set to {@link FlowExecutionStrategy#CANCEL_ON_ERROR}.
   * </p>
   */
  CANCELLATION_FAILED("cancellation_failed"),

  /**
   * Indicates that{@link org.folio.flow.api.Stage} or {@link StageExecutor} cancellation was ignored because it does
   * not implement {@link Cancellable} interface.
   *
   * <p>
   * {@link FlowExecutionStrategy} must be set to {@link FlowExecutionStrategy#CANCEL_ON_ERROR}.
   * </p>
   */
  CANCELLATION_IGNORED("cancellation_ignored"),

  /**
   * Indicates that{@link org.folio.flow.api.Stage} or {@link StageExecutor} has unknown status.
   */
  UNKNOWN("unknown");

  /**
   * String representation of enum value.
   */
  private final String value;
}

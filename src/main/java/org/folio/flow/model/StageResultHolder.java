package org.folio.flow.model;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.folio.flow.impl.FlowExecutor;
import org.folio.flow.impl.StageExecutor;

@Data
@RequiredArgsConstructor
public final class StageResultHolder {

  private final StageExecutionResult result;
  private final StageExecutor stage;
  private final boolean cancel;

  /**
   * Checks if {@link StageResultHolder} contains flow as a stage.
   *
   * @return true if this is a flow {@link StageResultHolder}, false - otherwise.
   */
  public boolean isFlow() {
    return stage instanceof FlowExecutor;
  }

  /**
   * Provides underlying stage identifier.
   *
   * @return stage identifier as {@link String}.
   */
  public String getStageId() {
    return stage.getStageId();
  }

  /**
   * Provides stage result error, nullable.
   *
   * @return stage result error {@link Exception} object, null - if stage is skipped or succeed
   */
  public Exception getError() {
    return result.getError();
  }

  /**
   * Provides underlying stage execution status.
   *
   * @return status as {@link ExecutionStatus} object.
   */
  public ExecutionStatus getStatus() {
    return result.getStatus();
  }
}

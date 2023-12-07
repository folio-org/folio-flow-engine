package org.folio.flow.exception;

import java.io.Serial;
import java.util.Collections;
import java.util.List;
import org.folio.flow.model.StageResult;

public class FlowExecutionException extends StageExecutionException {

  @Serial private static final long serialVersionUID = -3080840591624731726L;

  /**
   * Creates an exception object.
   *
   * @param flowId - flow identifier as {@link String} object
   * @param failedStage - failed stage name as {@link String} object
   * @param stageResults - list of corresponding stage results
   * @param cause - error cause as {@link Throwable}
   */
  public FlowExecutionException(String flowId, String failedStage, List<StageResult> stageResults, Throwable cause) {
    super(getErrorMessage(flowId, failedStage), flowId, stageResults, cause);
  }

  /**
   * Creates an exception object.
   *
   * @param message - error message as {@link String} object
   * @param flowId - flow identifier as {@link String} object
   * @param cause - error cause as {@link Throwable}
   */
  public FlowExecutionException(String message, String flowId, Throwable cause) {
    super(message, flowId, Collections.emptyList(), cause);
  }

  private static String getErrorMessage(String flowId, String failedStage) {
    return String.format("Failed to execute flow %s, stage '%s' failed", flowId, failedStage);
  }
}

package org.folio.flow.exception;

import java.io.Serial;
import java.util.List;
import org.folio.flow.model.StageResult;

public class FlowCancelledException extends StageExecutionException {

  @Serial private static final long serialVersionUID = 6401078029030305706L;

  /**
   * Creates an exception object.
   *
   * @param flowId - flow identifier as {@link String} object
   * @param failedStage - failed stage name as {@link String} object
   * @param stageResults - list of corresponding stage results
   * @param cause - error cause as {@link Throwable}
   */
  public FlowCancelledException(String flowId, String failedStage, List<StageResult> stageResults, Throwable cause) {
    super(getErrorMessage(flowId, failedStage), flowId, stageResults, cause);
  }

  private static String getErrorMessage(String flowId, String failedStage) {
    return String.format("Flow %s is cancelled, stage '%s' failed", flowId, failedStage);
  }
}

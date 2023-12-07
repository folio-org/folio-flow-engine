package org.folio.flow.exception;

import static java.lang.String.format;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_FAILED;
import static org.folio.flow.utils.CollectionUtils.isEmpty;

import java.io.Serial;
import java.util.List;
import java.util.stream.Collectors;
import org.folio.flow.model.StageResult;

public class FlowCancellationException extends StageExecutionException {

  @Serial private static final long serialVersionUID = -7037563669292518232L;

  /**
   * Creates an exception object.
   *
   * @param flowId - flow identifier as {@link String} object
   * @param failedStage - failed stage name
   * @param stageResults - list of corresponding stage results
   * @param cause - error cause as {@link Throwable}
   */
  public FlowCancellationException(String flowId, String failedStage, List<StageResult> stageResults, Throwable cause) {
    super(getErrorMessage(flowId, failedStage, stageResults), flowId, stageResults, cause);
  }

  private static String getErrorMessage(String id, String stage, List<StageResult> stageResults) {
    var notCancelledStages = getNotCancelledStagesNames(stageResults);
    if (isEmpty(notCancelledStages)) {
      return format("Failed to cancel flow %s, stage '%s' failed", id, stage);
    }

    return format("Failed to cancel flow %s. Stage '%s' failed and %s not cancelled", id, stage, notCancelledStages);
  }

  private static List<String> getNotCancelledStagesNames(List<StageResult> stageResults) {
    return stageResults.stream()
      .filter(stageResult -> stageResult.getStatus() == CANCELLATION_FAILED)
      .map(StageResult::getStageName)
      .collect(Collectors.toList());
  }
}

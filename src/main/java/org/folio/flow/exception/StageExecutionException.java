package org.folio.flow.exception;

import static org.folio.flow.utils.FlowUtils.FLOW_ENGINE_LOGGER_NAME;

import java.io.Serial;
import java.util.List;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.folio.flow.model.StageResult;

@Getter
@Log4j2(topic = FLOW_ENGINE_LOGGER_NAME)
public class StageExecutionException extends RuntimeException {

  @Serial private static final long serialVersionUID = 5632861972885430223L;

  private final String flowId;
  private final List<StageResult> stageResults;

  /**
   * Creates an exception object.
   *
   * @param message - error message as {@link String} object
   * @param flowId - flow identifier as {@link String} object
   * @param stageResults - list of corresponding stage results
   * @param cause - error cause as {@link Throwable}
   */
  public StageExecutionException(String message, String flowId, List<StageResult> stageResults, Throwable cause) {
    super(message, cause);
    this.flowId = flowId;
    this.stageResults = stageResults;
  }
}

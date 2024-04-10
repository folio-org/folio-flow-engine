package org.folio.flow.model;

import static java.util.Collections.emptyList;
import static org.folio.flow.utils.FlowUtils.convertToStageResults;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@Data
@Builder
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class StageResult implements Serializable {

  @Serial private static final long serialVersionUID = -2351840685666223047L;

  /**
   * Flow identifier.
   */
  private final String flowId;

  /**
   * Stage name.
   */
  private final String stageId;

  /**
   * Stage type.
   */
  private final String stageType;

  /**
   * Stage execution status.
   */
  private final ExecutionStatus status;

  /**
   * Stage execution error, nullable.
   */
  @EqualsAndHashCode.Exclude
  private final Exception error;

  /**
   * A list of internal stage results (can be empty).
   */
  @Builder.Default
  private final List<StageResult> subStageResults = emptyList();

  /**
   * Creates {@link StageResult} from {@link StageExecutionResult} object.
   *
   * @param stageExecutionResult - stage execution result as source for {@link StageResult} object
   * @return created {@link StageResult} object
   */
  public static StageResult from(StageExecutionResult stageExecutionResult) {
    return new StageResult(
      stageExecutionResult.getFlowId(),
      stageExecutionResult.getStageName(),
      stageExecutionResult.getStageType(),
      stageExecutionResult.getStatus(),
      stageExecutionResult.getError(),
      convertToStageResults(stageExecutionResult.getExecutedStages())
    );
  }
}

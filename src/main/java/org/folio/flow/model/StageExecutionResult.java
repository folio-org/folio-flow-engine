package org.folio.flow.model;

import static java.util.Collections.emptyList;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_FAILED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.RECOVERED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;

import java.util.List;
import java.util.Objects;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.folio.flow.api.StageContext;

@Data
@Builder
public class StageExecutionResult {

  /**
   * Name of executed stage as {@link String} object.
   */
  private final String stageName;

  /**
   * Type of executed stage as {@link String} object.
   */
  private final String stageType;

  /**
   * Stage specific {@link StageContext} object.
   */
  private final StageContext context;

  /**
   * Stage execution status.
   */
  private final ExecutionStatus status;

  /**
   * Error for executed stage, null if stage was executed successfully.
   */
  @EqualsAndHashCode.Exclude
  private final Exception error;

  /**
   * List with the results of composite tasks - flow, parallel executing task.
   */
  @Builder.Default
  private final List<StageResultHolder> executedStages = emptyList();

  /**
   * Checks if {@link StageExecutionResult} can be considered as failed.
   *
   * @return true if stage result can be considered as failed, false - otherwise
   */
  public boolean isFailed() {
    return status == FAILED || status == CANCELLATION_FAILED;
  }

  /**
   * Checks if {@link StageExecutionResult} can be considered as finished.
   *
   * @return true if stage result can be considered as failed, false - otherwise
   */
  public boolean isSucceed() {
    return status == SUCCESS || status == RECOVERED;
  }

  /**
   * Returns stage name.
   *
   * @return a stage name as {@link String}
   */
  public String getFlowId() {
    return this.context.flowId();
  }

  /**
   * Creates a new {@link StageExecutionResult} object with updated {@link ExecutionStatus} value.
   *
   * @param status - stage status to update in {@link StageExecutionResult}.
   * @return a new {@link StageExecutionResult} object with updated {@link ExecutionStatus} value.
   */
  public StageExecutionResult withStatus(ExecutionStatus status) {
    return new StageExecutionResult(stageName, stageType, context, status, error, executedStages);
  }

  /**
   * Creates a new {@link StageExecutionResult} object with updated {@link StageContext} value.
   *
   * <p>
   * Flow id is updated after sub-flow execution, it is required to update stage to the correct flow id after sub-flow
   * execution to have properly formed results.
   * </p>
   *
   * @param flowId - flow identifier.
   * @return a new {@link StageExecutionResult} object with updated {@link StageContext} value.
   */
  public StageExecutionResult withFlowId(String flowId) {
    return Objects.equals(this.context.flowId(), flowId)
      ? this
      : new StageExecutionResult(stageName, stageType, context.withFlowId(flowId), status, error, executedStages);
  }
}

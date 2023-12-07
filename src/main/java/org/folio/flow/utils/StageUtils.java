package org.folio.flow.utils;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.flow.model.ExecutionStatus.SKIPPED;
import static org.folio.flow.utils.FlowUtils.FLOW_ENGINE_LOGGER_NAME;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.flow.api.StageContext;
import org.folio.flow.impl.StageExecutor;
import org.folio.flow.model.StageExecutionResult;
import org.folio.flow.model.StageResultHolder;

@Log4j2(topic = FLOW_ENGINE_LOGGER_NAME)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StageUtils {

  /**
   * Executes cancel() method for {@link StageResultHolder} value if necessary.
   *
   * @param flowId - flow identifier for stage cancellation
   * @param srh - cancellation context as {@link StageResultHolder}
   * @param upstreamResult - upstream {@link StageExecutionResult} object
   * @param executor - flow engine executor as {@link Executor} object
   * @return cancellation result as {@link CompletableFuture}
   */
  public static CompletableFuture<StageExecutionResult> cancelStageAsync(String flowId,
    StageResultHolder srh, StageExecutionResult upstreamResult, Executor executor) {
    var stage = srh.getStage();
    var executedStageStatus = srh.getResult().getStatus();

    if (executedStageStatus != SKIPPED && shouldCancelStage(srh, stage)) {
      return stage.cancel(getInitialCancellationResult(flowId, srh, upstreamResult), executor);
    }

    var notCancelledStageResult = createNotCancelledStageResult(flowId, upstreamResult, srh.getResult(), stage);
    return completedFuture(notCancelledStageResult);
  }

  private static StageExecutionResult getInitialCancellationResult(
    String flowId, StageResultHolder srh, StageExecutionResult ser) {
    var srhResult = srh.getResult();
    return StageExecutionResult.builder()
      .stageName(ser.getStageName())
      .context(ser.getContext().withFlowId(flowId))
      .status(srh.getStatus())
      .error(srhResult.getError())
      .executedStages(srhResult.getExecutedStages())
      .build();
  }

  private static StageExecutionResult createNotCancelledStageResult(String flowId,
    StageExecutionResult upstreamResult, StageExecutionResult ser, StageExecutor stage) {
    return StageExecutionResult.builder()
      .stageName(stage.getStageId())
      .context(upstreamResult.getContext().withFlowId(flowId))
      .status(ser.getStatus())
      .error(ser.getError())
      .executedStages(ser.getExecutedStages())
      .build();
  }

  private static boolean shouldCancelStage(StageResultHolder srh, StageExecutor stage) {
    var stageResult = srh.getResult();
    return stageResult.isSucceed() || stage.shouldCancelIfFailed(StageContext.copy(stageResult.getContext()));
  }
}

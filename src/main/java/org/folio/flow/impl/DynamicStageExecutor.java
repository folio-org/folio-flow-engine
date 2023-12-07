package org.folio.flow.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.SKIPPED;
import static org.folio.flow.model.StageExecutionResult.stageResult;
import static org.folio.flow.utils.FlowUtils.FLOW_ENGINE_LOGGER_NAME;
import static org.folio.flow.utils.FlowUtils.defaultIfNull;
import static org.folio.flow.utils.FlowUtils.findFirstValue;
import static org.folio.flow.utils.StageUtils.cancelStageAsync;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.flow.api.DynamicStage;
import org.folio.flow.api.NoOpStage;
import org.folio.flow.api.StageContext;
import org.folio.flow.model.StageExecutionResult;
import org.folio.flow.model.StageResultHolder;
import org.folio.flow.utils.FlowUtils;

@RequiredArgsConstructor
@Log4j2(topic = FLOW_ENGINE_LOGGER_NAME)
public final class DynamicStageExecutor implements StageExecutor {

  private final DynamicStage dynamicStage;

  @Override
  public String getStageId() {
    return dynamicStage.getName();
  }

  @Override
  public String toString() {
    return dynamicStage.getName();
  }

  @Override
  public CompletableFuture<StageExecutionResult> execute(StageExecutionResult upstreamResult, Executor executor) {
    log.debug("[{}] Initializing dynamic stage execution: {}", upstreamResult.getFlowId(), getStageId());
    return completedFuture(upstreamResult)
      .thenComposeAsync(ser -> executeDynamicStageAsync(ser, executor), executor)
      .thenApply(this::buildExecutionResult);
  }

  @Override
  public CompletableFuture<StageExecutionResult> skip(StageExecutionResult upstreamResult, Executor executor) {
    log.debug("[{}] Skipping dynamic stage: {}", upstreamResult.getFlowId(), getStageId());
    return completedFuture(buildSkippedStageResult(upstreamResult));
  }

  @Override
  public CompletableFuture<StageExecutionResult> cancel(StageExecutionResult upstreamResult, Executor executor) {
    return findFirstValue(upstreamResult.getExecutedStages())
      .map(srh -> completedFuture(upstreamResult)
        .thenComposeAsync(ser -> cancelStageAsync(ser.getFlowId(), srh, ser, executor))
        .thenApply(ser -> buildCancellationResult(ser, srh)))
      .orElseGet(() -> completedFuture(stageResult(getStageId(), upstreamResult.getContext(), CANCELLED)));
  }

  private CompletionStage<StageResultHolder> executeDynamicStageAsync(
    StageExecutionResult upstreamResult, Executor executor) {
    var context = upstreamResult.getContext();
    var stage = defaultIfNull(dynamicStage.getStageProvider().apply(context), NoOpStage::getInstance);
    var stageExecutor = FlowUtils.getStageExecutor(stage);
    return stageExecutor.execute(upstreamResult, executor)
      .thenApply(result -> new StageResultHolder(result, stageExecutor, true));
  }

  private StageExecutionResult buildCancellationResult(StageExecutionResult ser, StageResultHolder srh) {
    log.debug("[{}] Dynamic stage '{}' is cancelled with status: {}", ser.getFlowId(), getStageId(), ser.getStatus());
    return StageExecutionResult.builder()
      .stageName(getStageId())
      .context(StageContext.copy(ser.getContext()))
      .status(ser.getStatus())
      .error(ser.getError())
      .executedStages(List.of(new StageResultHolder(ser, srh.getStage(), false)))
      .build();
  }

  private StageExecutionResult buildExecutionResult(StageResultHolder srh) {
    var result = srh.getResult();
    log.debug("[{}] Dynamic stage '{}' is finished with status: {}", result.getFlowId(), getStageId(), srh.getStatus());
    return StageExecutionResult.builder()
      .stageName(getStageId())
      .context(StageContext.copy(result.getContext()))
      .status(srh.getStatus())
      .error(srh.getError())
      .executedStages(List.of(srh))
      .build();
  }

  private StageExecutionResult buildSkippedStageResult(StageExecutionResult upstreamResult) {
    log.debug("[{}] Dynamic stage is skipped: {}", upstreamResult.getFlowId(), getStageId());
    return StageExecutionResult.builder()
      .stageName(getStageId())
      .context(upstreamResult.getContext())
      .status(SKIPPED)
      .build();
  }
}

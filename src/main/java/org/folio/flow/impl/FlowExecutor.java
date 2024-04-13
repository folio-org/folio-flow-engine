package org.folio.flow.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_FAILED;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.SKIPPED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.folio.flow.model.FlowExecutionStrategy.CANCEL_ON_ERROR;
import static org.folio.flow.model.FlowExecutionStrategy.IGNORE_ON_ERROR;
import static org.folio.flow.utils.FlowUtils.FLOW_ENGINE_LOGGER_NAME;
import static org.folio.flow.utils.FlowUtils.getLastFailedStage;
import static org.folio.flow.utils.FlowUtils.getLastFailedStageWithAnyStatusOf;
import static org.folio.flow.utils.StageUtils.cancelStageAsync;

import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.flow.api.Flow;
import org.folio.flow.api.StageContext;
import org.folio.flow.model.ExecutionStatus;
import org.folio.flow.model.StageExecutionResult;
import org.folio.flow.model.StageResultHolder;

@RequiredArgsConstructor
@Log4j2(topic = FLOW_ENGINE_LOGGER_NAME)
public class FlowExecutor implements StageExecutor {

  /**
   * Flow object to be executed.
   */
  private final Flow flow;

  @Override
  public String getStageId() {
    return flow.getId();
  }

  @Override
  public String getStageType() {
    return "Flow";
  }

  @Override
  public boolean shouldCancelIfFailed(StageContext stageContext) {
    return flow.getFlowExecutionStrategy() == CANCEL_ON_ERROR;
  }

  @Override
  public CompletableFuture<StageExecutionResult> execute(StageExecutionResult upstreamResult, Executor executor) {
    log.debug("[{}] Initializing flow...", getStageId());
    var executionDeque = new ExecutionDeque();
    var executableStages = flow.getStages();

    var initialFlowResult = createInitialFlowResult(upstreamResult);
    var initialCompletableFuture = completedFuture(initialFlowResult);
    if (executableStages.isEmpty()) {
      log.debug("[{}] Nothing to execute, flow is empty", getStageId());
      return initialCompletableFuture;
    }

    for (var stage : executableStages) {
      initialCompletableFuture = initialCompletableFuture
        .thenApply(ser -> ser.withFlowId(getStageId()))
        .thenComposeAsync(ser -> tryExecuteStageOrSkip(stage, ser, executor), executor)
        .thenApply(ser -> executionDeque.addLast(ser, stage, ser.getStatus() != FAILED));
    }

    return initialCompletableFuture
      .thenCompose(ser -> finalizeFlowExecution(ser, executor, executionDeque))
      .thenApply(ser -> createFlowResult(ser, upstreamResult, executionDeque));
  }

  private static CompletableFuture<StageExecutionResult> tryExecuteStageOrSkip(
    StageExecutor stage, StageExecutionResult ser, Executor executor) {
    return ser.isSucceed() ? stage.execute(ser, executor) : stage.skip(ser, executor);
  }

  @Override
  public CompletableFuture<StageExecutionResult> skip(StageExecutionResult upstreamResult, Executor executor) {
    var skippedStagesDeque = new ExecutionDeque();
    var stageExecutors = flow.getStages();

    var future = completedFuture(createInitialFlowResult(upstreamResult));
    for (var stageExecutor : stageExecutors) {
      future = future
        .thenComposeAsync(ser -> stageExecutor.skip(ser.withFlowId(getStageId()), executor), executor)
        .thenApply(ser -> skippedStagesDeque.addLast(ser, stageExecutor, false));
    }

    return future
      .thenCompose(ser -> createSkippedFlowResult(ser, executor, upstreamResult.getFlowId(), skippedStagesDeque));
  }

  @Override
  public CompletableFuture<StageExecutionResult> cancel(StageExecutionResult upstreamResult, Executor executor) {
    log.debug("[{}] Initializing flow cancellation...", getStageId());

    var cancellationDeque = new ExecutionDeque();
    var executedStages = upstreamResult.getExecutedStages();

    var cancellationFuture = completedFuture(createInitialFlowResult(upstreamResult));
    var listIterator = executedStages.listIterator(executedStages.size());
    while (listIterator.hasPrevious()) {
      var currSrh = listIterator.previous();
      cancellationFuture = cancellationFuture
        .thenComposeAsync(ser -> cancelStageAsync(getStageId(), currSrh, ser, executor), executor)
        .thenApply(rs -> cancellationDeque.addFirst(rs, currSrh.getStage()));
    }

    return cancellationFuture
      .thenCompose(ser -> finalizeFlowCancellation(ser, executor, cancellationDeque))
      .thenApply(ser -> createCancelledFlowResult(upstreamResult, ser, cancellationDeque));
  }

  private StageExecutionResult createInitialFlowResult(StageExecutionResult ser) {
    if (Objects.equals(ser.getFlowId(), getStageId())) {
      return ser;
    }

    var stageName = getStageId();
    var upstreamContext = ser.getContext();
    var flowParameters = mergeFlowParameters(upstreamContext.flowParameters(), flow.getFlowParameters());
    var stageContext = StageContext.of(stageName, flowParameters, upstreamContext.data());
    return StageExecutionResult.builder()
      .stageName(stageName)
      .stageType("Flow")
      .context(stageContext)
      .status(ser.getStatus())
      .build();
  }

  private CompletableFuture<StageExecutionResult> finalizeFlowExecution(
    StageExecutionResult stageResult, Executor executor, ExecutionDeque executionDeque) {
    var onFlowErrorStage = flow.getOnFlowErrorFinalStage();
    if (stageResult.isSucceed() || onFlowErrorStage == null || flow.getFlowExecutionStrategy() != IGNORE_ON_ERROR) {
      return completedFuture(stageResult);
    }

    return getFinalStageExecutionFuture(onFlowErrorStage, executor, stageResult, executionDeque);
  }

  private CompletableFuture<StageExecutionResult> finalizeFlowCancellation(
    StageExecutionResult stageResult, Executor executor, ExecutionDeque executionDeque) {
    var onFlowCancellationErrorStage = flow.getOnFlowCancellationErrorFinalStage();
    var stagesList = executionDeque.getExecutedStagesAsList();
    var isCancellationFailed = getLastFailedStageWithAnyStatusOf(stagesList, Set.of(CANCELLATION_FAILED))
      .filter(stage -> stage.getStatus() != CANCELLED)
      .isPresent();

    if (isCancellationFailed) {
      return onFlowCancellationErrorStage != null
        ? getFinalStageExecutionFuture(onFlowCancellationErrorStage, executor, stageResult, executionDeque)
        : completedFuture(stageResult);
    }

    var onFlowCancellationStage = flow.getOnFlowCancellationFinalStage();
    if (onFlowCancellationStage == null) {
      return completedFuture(stageResult);
    }

    return getFinalStageExecutionFuture(onFlowCancellationStage, executor, stageResult, executionDeque);
  }

  private CompletableFuture<StageExecutionResult> getFinalStageExecutionFuture(StageExecutor stage, Executor executor,
    StageExecutionResult stageResult, ExecutionDeque executionDeque) {
    return stage.execute(stageResult, executor)
      .thenApply(ser -> executionDeque.addLast(ser, stage, false))
      .thenApply(ser -> ser.withStatus(stageResult.getStatus()));
  }

  private StageExecutionResult createFlowResult(StageExecutionResult ser,
    StageExecutionResult upstreamResult, ExecutionDeque deque) {
    var executedStages = deque.getExecutedStagesAsList();
    var lastFailedStageError = getLastFailedStage(executedStages).map(StageResultHolder::getError).orElse(null);
    var resultStatus = getFlowExecutionStatus(ser);
    var stageContextData = ser.getContext().data();
    var resolvedParams = upstreamResult.getContext().flowParameters();
    log.debug("[{}] Flow is finished with status: {}", getStageId(), resultStatus);

    return StageExecutionResult.builder()
      .stageName(getStageId())
      .stageType(getStageType())
      .context(StageContext.of(upstreamResult.getFlowId(), resolvedParams, stageContextData))
      .status(resultStatus)
      .error(lastFailedStageError)
      .executedStages(executedStages)
      .build();
  }

  private static ExecutionStatus getFlowExecutionStatus(StageExecutionResult ser) {
    var status = ser.getStatus();
    return switch (status) {
      case SKIPPED -> FAILED;
      case RECOVERED -> SUCCESS;
      default -> status;
    };
  }

  private CompletableFuture<StageExecutionResult> createSkippedFlowResult(StageExecutionResult ser, Executor executor,
    String parentFlowId, ExecutionDeque deque) {
    var stageContext = ser.getContext().withFlowId(parentFlowId);
    log.debug("[{}] Flow is skipped", getStageId());
    var onFlowSkipFinalStage = flow.getOnFlowSkipFinalStage();
    if (onFlowSkipFinalStage == null) {
      return completedFuture(getStageResult(deque, stageContext));
    }

    return getFinalStageExecutionFuture(onFlowSkipFinalStage, executor, ser, deque)
      .thenApply(result -> getStageResult(deque, stageContext));
  }

  private StageExecutionResult createCancelledFlowResult(StageExecutionResult upstreamResult, StageExecutionResult ser,
    ExecutionDeque cancellationDeque) {
    var executedStages = cancellationDeque.getExecutedStagesAsList();
    var lastFailedStage = getLastFailedStageWithAnyStatusOf(executedStages, Set.of(FAILED, CANCELLATION_FAILED));

    var statuses = executedStages.stream().map(StageResultHolder::getStatus).collect(toSet());

    var finalStatus = lastFailedStage
      .map(StageResultHolder::getStage)
      .filter(stage -> Objects.equals(stage, flow.getOnFlowCancellationFinalStage()))
      .map(onFlowCancellationFinalStageFailed -> CANCELLATION_FAILED)
      .orElseGet(() -> statuses.contains(CANCELLATION_FAILED) ? CANCELLATION_FAILED : CANCELLED);

    var lastFailedStageError = lastFailedStage
      .map(StageResultHolder::getResult)
      .map(StageExecutionResult::getError)
      .orElse(null);

    var contextData = ser.getContext().data();
    var upstreamFlowId = upstreamResult.getFlowId();
    var upstreamFlowParameters = upstreamResult.getContext().flowParameters();
    log.debug("[{}] Flow is cancelled with status: {}", getStageId(), finalStatus);

    return StageExecutionResult.builder()
      .stageName(getStageId())
      .stageType(getStageType())
      .context(StageContext.of(upstreamFlowId, upstreamFlowParameters, contextData))
      .status(finalStatus)
      .error(lastFailedStageError)
      .executedStages(executedStages)
      .build();
  }

  private static <K, V> Map<K, V> mergeFlowParameters(Map<K, V> oldParams, Map<K, V> newParams) {
    var mergeResult = new HashMap<>(oldParams);
    mergeResult.putAll(newParams);
    return Map.copyOf(mergeResult);
  }

  private StageExecutionResult getStageResult(ExecutionDeque deque, StageContext stageContext) {
    return StageExecutionResult.builder()
      .stageName(getStageId())
      .stageType(getStageType())
      .context(stageContext)
      .status(SKIPPED)
      .executedStages(deque.getExecutedStagesAsList())
      .build();
  }

  @RequiredArgsConstructor
  private static final class ExecutionDeque {

    private final Deque<StageResultHolder> executedStages = new LinkedBlockingDeque<>();

    private StageExecutionResult addLast(StageExecutionResult sr, StageExecutor stage, boolean cancel) {
      executedStages.addLast(new StageResultHolder(sr, stage, cancel));
      return sr;
    }

    private StageExecutionResult addFirst(StageExecutionResult sr, StageExecutor stage) {
      executedStages.addFirst(new StageResultHolder(sr, stage, false));
      return sr;
    }

    private List<StageResultHolder> getExecutedStagesAsList() {
      return List.copyOf(executedStages);
    }
  }
}

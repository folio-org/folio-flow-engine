package org.folio.flow.impl;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_FAILED;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.SKIPPED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.folio.flow.utils.FlowUtils.FLOW_ENGINE_LOGGER_NAME;
import static org.folio.flow.utils.FlowUtils.findFirstValue;
import static org.folio.flow.utils.StageUtils.cancelStageAsync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.flow.api.ParallelStage;
import org.folio.flow.api.StageContext;
import org.folio.flow.model.ExecutionStatus;
import org.folio.flow.model.StageExecutionResult;
import org.folio.flow.model.StageResultHolder;

@RequiredArgsConstructor
@Log4j2(topic = FLOW_ENGINE_LOGGER_NAME)
public final class ParallelStageExecutor implements StageExecutor {

  /**
   * Parallel stage to be executed.
   */
  private final ParallelStage parallelStage;

  @Override
  public String getStageId() {
    return parallelStage.getId();
  }

  @Override
  public String getStageType() {
    return "ParallelStage";
  }

  @Override
  public boolean shouldCancelIfFailed(StageContext stageContext) {
    return parallelStage.isShouldCancelIfFailed();
  }

  @Override
  public CompletableFuture<StageExecutionResult> execute(StageExecutionResult upstreamResult, Executor executor) {
    log.debug("[{}] Executing stages in parallel: {}", upstreamResult::getFlowId, this::getStageNames);

    var effectiveExecutor = getEffectiveExecutor(executor);
    var initialFuture = completedFuture(upstreamResult);
    var executors = parallelStage.getStages();
    var completableFutures = executors.stream()
      .map(stage -> initialFuture.thenComposeAsync(ser -> stage.execute(ser, effectiveExecutor), effectiveExecutor))
      .collect(toList());

    return executeInParallelAsync(completableFutures, executors)
      .thenApply(psr -> getStageExecutionResult(upstreamResult, psr, psr.getExecutionFinalStatus()));
  }

  @Override
  public CompletableFuture<StageExecutionResult> skip(StageExecutionResult upstreamResult, Executor executor) {
    log.debug("[{}] Skipping parallel stages: {}", upstreamResult::getFlowId, this::getStageNames);

    var effectiveExecutor = getEffectiveExecutor(executor);
    var initialFuture = completedFuture(upstreamResult);
    var stageExecutors = parallelStage.getStages();
    var skipFutures = stageExecutors.stream()
      .map(stage -> initialFuture.thenComposeAsync(ser -> stage.skip(ser, effectiveExecutor), effectiveExecutor))
      .collect(toList());

    return executeInParallelAsync(skipFutures, stageExecutors)
      .thenApply(psr -> getStageExecutionResult(upstreamResult, psr, SKIPPED));
  }

  @Override
  public CompletableFuture<StageExecutionResult> cancel(StageExecutionResult upstreamResult, Executor executor) {
    log.debug("[{}] Cancelling parallel stages: {}", upstreamResult::getFlowId, this::getStageNames);

    var effectiveExecutor = getEffectiveExecutor(executor);
    var flowId = upstreamResult.getFlowId();
    var initialFuture = completedFuture(upstreamResult);
    var executedStages = upstreamResult.getExecutedStages();
    var stageExecutors = executedStages.stream()
      .map(StageResultHolder::getStage)
      .collect(Collectors.toList());

    var cancellationCompletableFuture = executedStages.stream()
      .map(stage -> initialFuture.thenComposeAsync(ser -> cancelStageAsync(flowId, stage, ser, effectiveExecutor),
        effectiveExecutor))
      .collect(toList());

    return executeInParallelAsync(cancellationCompletableFuture, stageExecutors)
      .thenApply(psr -> getStageExecutionResult(upstreamResult, psr, psr.getCancellationFinalStatus()));
  }

  private static CompletableFuture<ParallelStageResult> executeInParallelAsync(
    List<CompletableFuture<StageExecutionResult>> stages, List<StageExecutor> executors) {
    return allOf(stages.toArray(CompletableFuture[]::new))
      .thenApply(unused -> composeToParallelStageResult(executors, stages));
  }

  private StageExecutionResult getStageExecutionResult(StageExecutionResult upstreamResult,
    ParallelStageResult psr, ExecutionStatus finalStatus) {
    var upstreamFlowId = upstreamResult.getFlowId();
    var upstreamFlowParameters = upstreamResult.getContext().flowParameters();
    return StageExecutionResult.builder()
      .stageName(getStageId())
      .stageType(getStageType())
      .context(StageContext.of(upstreamFlowId, upstreamFlowParameters, psr.contextData()))
      .status(finalStatus)
      .error(findFirstValue(psr.errors()).orElse(null))
      .executedStages(psr.executedStages())
      .build();
  }

  private static ParallelStageResult composeToParallelStageResult(List<StageExecutor> executors,
    List<CompletableFuture<StageExecutionResult>> futures) {
    var resultList = new ArrayList<StageResultHolder>();
    for (int i = 0; i < futures.size(); i++) {
      var future = futures.get(i);
      var stage = executors.get(i);
      resultList.add(new StageResultHolder(future.join(), stage, true));
    }

    return ParallelStageResult.fromExecutedStages(resultList);
  }

  private List<String> getStageNames() {
    return parallelStage.getStages().stream()
      .map(StageExecutor::getStageId)
      .collect(toList());
  }

  private Executor getEffectiveExecutor(Executor defaultExecutor) {
    var customExecutor = parallelStage.getCustomExecutor();
    return customExecutor != null ? customExecutor : defaultExecutor;
  }

  private record ParallelStageResult(
    Map<Object, Object> contextData,
    Set<ExecutionStatus> statuses,
    List<Exception> errors,
    List<StageResultHolder> executedStages) {

    private static ParallelStageResult fromExecutedStages(List<StageResultHolder> stageResultHolders) {
      var contextData = new HashMap<>();
      var executionStatuses = new HashSet<ExecutionStatus>();
      var errors = new ArrayList<Exception>();

      for (var stageResultHolder : stageResultHolders) {
        var stageExecutionResult = stageResultHolder.getResult();
        var context = stageExecutionResult.getContext();
        contextData.putAll(context.data());
        executionStatuses.add(stageExecutionResult.getStatus());
        var error = stageExecutionResult.getError();
        if (error != null) {
          errors.add(error);
        }
      }

      return new ParallelStageResult(contextData, executionStatuses, errors, stageResultHolders);
    }

    public ExecutionStatus getExecutionFinalStatus() {
      return statuses.contains(FAILED) ? FAILED : SUCCESS;
    }

    public ExecutionStatus getCancellationFinalStatus() {
      return statuses.contains(CANCELLATION_FAILED) ? CANCELLATION_FAILED : CANCELLED;
    }
  }
}

package org.folio.flow.impl;

import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyList;
import static java.util.Collections.synchronizedMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_FAILED;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.IN_PROGRESS;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.folio.flow.model.ExecutionStatus.UNKNOWN;
import static org.folio.flow.utils.FlowUtils.FLOW_ENGINE_LOGGER_NAME;
import static org.folio.flow.utils.FlowUtils.convertToStageResults;
import static org.folio.flow.utils.FlowUtils.getLastFailedStageWithAnyStatusOf;

import java.io.Serial;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.flow.api.Flow;
import org.folio.flow.api.FlowEngine;
import org.folio.flow.api.StageContext;
import org.folio.flow.exception.FlowCancellationException;
import org.folio.flow.exception.FlowCancelledException;
import org.folio.flow.exception.FlowExecutionException;
import org.folio.flow.exception.StageExecutionException;
import org.folio.flow.model.ExecutionStatus;
import org.folio.flow.model.StageExecutionResult;
import org.folio.flow.model.StageResult;
import org.folio.flow.model.StageResultHolder;
import org.folio.flow.utils.FlowUtils;
import org.folio.flow.utils.StageReportProvider;

@RequiredArgsConstructor
@Log4j2(topic = FLOW_ENGINE_LOGGER_NAME)
public final class FlowEngineImpl implements FlowEngine {

  private final String name;
  private final int cacheSize;
  private final Executor executor;
  private final Duration defaultExecutionTimeout;
  private final boolean printFlowExecutionResult;
  private final StageReportProvider stageReportProvider;
  private final Map<String, ExecutionStatus> flowExecutionStatuses = synchronizedMap(getEvictingLinkedHashMap());

  private final Map<ExecutionStatus, StageExecutionExceptionProvider> errorProvidersMap = Map.of(
    FAILED, FlowExecutionException::new,
    CANCELLED, FlowCancelledException::new,
    CANCELLATION_FAILED, FlowCancellationException::new);

  private final Map<ExecutionStatus, Set<ExecutionStatus>> errorFailedStatuses = Map.of(
    FAILED, Set.of(FAILED),
    CANCELLED, Set.of(FAILED, CANCELLED),
    CANCELLATION_FAILED, Set.of(FAILED, CANCELLATION_FAILED, CANCELLED));

  @Override
  public void execute(Flow flow) {
    execute(flow, defaultExecutionTimeout);
  }

  @Override
  public void execute(Flow flow, Duration executionTimeout) {
    var future = (CompletableFuture<Void>) executeAsync(flow);
    try {
      future.get(executionTimeout.toMillis(), MILLISECONDS);
    } catch (InterruptedException exception) {
      future.cancel(true);
      log.warn("[{}] Flow has been interrupted, interrupting current thread", flow.getId(), exception);
      flowExecutionStatuses.put(flow.getId(), FAILED);
      currentThread().interrupt();
    } catch (TimeoutException exception) {
      future.cancel(true);
      log.warn("[{}] Flow execution timed-out and it was stopped", flow.getId(), exception);
      flowExecutionStatuses.put(flow.getId(), FAILED);
      throw new FlowExecutionException("Failed to execute flow " + flow, flow.getId(), exception);
    } catch (ExecutionException exception) {
      handleExecutionException(flow.getId(), exception);
    }
  }

  @Override
  public Future<Void> executeAsync(Flow flow) {
    FlowUtils.requireNonNull(executor, "Executor must not be null");
    var lastFlowStatus = getFlowStatus(flow);
    var flowId = flow.getId();
    if (lastFlowStatus == IN_PROGRESS) {
      log.warn("[{}] Flow is already executing", flow.getId());
      return failedFuture(new IllegalStateException(String.format("Flow %s already executing", flowId)));
    }

    flowExecutionStatuses.put(flowId, IN_PROGRESS);
    var flowExec = new FlowExecutor(flow);
    var initialStageResult = getInitialStageExecutionResult(flow);
    return completedFuture(initialStageResult)
      .thenComposeAsync(rs -> flowExec.execute(rs, executor), executor)
      .thenCompose(ser -> shouldCancelFlow(flowExec, ser) ? flowExec.cancel(ser, executor) : completedFuture(ser))
      .thenApply(ser -> cacheExecutionStatus(ser, flowId))
      .thenCompose(ser -> finalizeFlow(flowId, ser));
  }

  @Override
  public ExecutionStatus getFlowStatus(Flow flow) {
    return flowExecutionStatuses.getOrDefault(flow.getId(), UNKNOWN);
  }

  @Override
  public String toString() {
    return this.name;
  }

  private StageExecutionResult cacheExecutionStatus(StageExecutionResult stageResult, String flowId) {
    flowExecutionStatuses.put(flowId, stageResult.getStatus());
    return stageResult;
  }

  private static boolean shouldCancelFlow(FlowExecutor flowExecutor, StageExecutionResult rs) {
    return rs.isFailed() && flowExecutor.shouldCancelIfFailed(rs.getContext());
  }

  private static StageExecutionResult getInitialStageExecutionResult(Flow flow) {
    var stageContext = StageContext.of(flow.getId(), flow.getFlowParameters(), new HashMap<>());
    return StageExecutionResult.builder()
      .stageName(flow.getId())
      .stageType("Flow")
      .context(stageContext)
      .status(SUCCESS)
      .build();
  }

  private void handleExecutionException(String flowId, ExecutionException exception) {
    var cause = exception.getCause();
    if (cause instanceof StageExecutionException stageExecutionResult) {
      throw stageExecutionResult;
    }

    throw new FlowExecutionException("Failed to execute flow: " + flowId, flowId, cause);
  }

  private CompletableFuture<Void> finalizeFlow(String flowId, StageExecutionResult ser) {
    var stages = ser.getExecutedStages();
    var executionStatus = ser.getStatus();

    if (printFlowExecutionResult) {
      log.info("Flow '{}' execution report:\n{}",
        () -> flowId,
        () -> stageReportProvider.create(convertToStageResults(stages)));
    }

    var errorProvider = errorProvidersMap.get(executionStatus);
    if (errorProvider != null) {
      var statuses = errorFailedStatuses.getOrDefault(executionStatus, Set.of(FAILED));
      return getLastFailedStageWithAnyStatusOf(stages, statuses)
        .map(h -> errorProvider.buildError(flowId, h.getStageId(), convertToStageResults(stages), h.getError()))
        .map(CompletableFuture::<Void>failedFuture)
        .orElseGet(() -> failedFuture(errorProvider.buildError(flowId, "unknown", emptyList(), null)));
    }

    return completedFuture(null);
  }

  private LinkedHashMap<String, ExecutionStatus> getEvictingLinkedHashMap() {
    return new LinkedHashMap<>() {

      @Serial private static final long serialVersionUID = 1638089448889343377L;

      @Override
      protected boolean removeEldestEntry(Entry<String, ExecutionStatus> eldest) {
        return size() > cacheSize;
      }
    };
  }

  /**
   * On demand holder for singleton, for lazy initialization.
   */
  @FunctionalInterface
  private interface StageExecutionExceptionProvider {

    /**
     * Provides a {@link StageExecutionException} object.
     *
     * @param flowId - flow identifier as {@link String} object
     * @param name - failed stage name as {@link String} object
     * @param stages - a {@link List} with executed {@link StageResultHolder} objects
     * @param cause - failed {@link StageResultHolder} object
     */
    StageExecutionException buildError(String flowId, String name, List<StageResult> stages, Throwable cause);
  }
}

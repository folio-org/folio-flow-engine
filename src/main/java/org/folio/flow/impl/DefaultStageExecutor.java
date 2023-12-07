package org.folio.flow.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_FAILED;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_IGNORED;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.RECOVERED;
import static org.folio.flow.model.ExecutionStatus.SKIPPED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.folio.flow.model.StageExecutionResult.stageResult;
import static org.folio.flow.utils.FlowUtils.FLOW_ENGINE_LOGGER_NAME;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import lombok.extern.log4j.Log4j2;
import org.folio.flow.api.Cancellable;
import org.folio.flow.api.Listenable;
import org.folio.flow.api.Recoverable;
import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;
import org.folio.flow.model.StageExecutionResult;

@Log4j2(topic = FLOW_ENGINE_LOGGER_NAME)
public final class DefaultStageExecutor implements StageExecutor {

  private final Stage stage;
  private final String stageName;

  /**
   * Creates a {@link DefaultStageExecutor} for simple stages.
   *
   * @param stage - stage to be executed as part of flow.
   */
  public DefaultStageExecutor(Stage stage) {
    this.stage = stage;
    this.stageName = stage.getId();
  }

  @Override
  public String getStageId() {
    return stageName;
  }

  @Override
  public boolean shouldCancelIfFailed(StageContext stageContext) {
    return stage instanceof Cancellable && ((Cancellable) stage).shouldCancelIfFailed(stageContext);
  }

  /**
   * Executes a stage.
   *
   * @param upstreamResult - {@link StageExecutionResult} for execution
   * @param executor - {@link Executor} object
   * @return {@link StageExecutionResult} object as a result of stage execution
   */
  @Override
  public CompletableFuture<StageExecutionResult> execute(StageExecutionResult upstreamResult, Executor executor) {
    return completedFuture(upstreamResult)
      .thenApply(result -> applyListenableMethod(result, Listenable::onStart))
      .thenApply(this::tryExecuteStage)
      .thenApply(result -> shouldRecover(result) ? tryRecoverStage(result) : result)
      .thenApply(this::applyTerminalListenableMethod);
  }

  @Override
  public CompletableFuture<StageExecutionResult> skip(StageExecutionResult upstreamResult, Executor executor) {
    log.debug("[{}] Stage '{}' is skipped", upstreamResult.getFlowId(), stageName);
    return completedFuture(stageResult(getStageId(), upstreamResult.getContext(), SKIPPED));
  }

  @Override
  public CompletableFuture<StageExecutionResult> cancel(StageExecutionResult upstreamResult, Executor executor) {
    if (stage instanceof Cancellable) {
      return completedFuture(upstreamResult)
        .thenApply(this::tryCancelStage)
        .thenApply(this::applyTerminalCancellationListenableMethod);
    }

    return completedFuture(stageResult(stageName, upstreamResult.getContext(), CANCELLATION_IGNORED));
  }

  private StageExecutionResult tryExecuteStage(StageExecutionResult rs) {
    var context = StageContext.copy(rs.getContext());
    var flowId = context.flowId();
    try {
      stage.execute(context);
      log.debug("[{}] Stage '{}' executed with status: {}", flowId, stageName, SUCCESS);
      return stageResult(stageName, context, SUCCESS);
    } catch (Exception exception) {
      log.debug("[{}] Stage '{}' executed with status: {}", flowId, stageName, FAILED);
      return stageResult(stageName, context, FAILED, exception);
    }
  }

  private boolean shouldRecover(StageExecutionResult result) {
    return result.isFailed() && stage instanceof Recoverable;
  }

  private StageExecutionResult tryRecoverStage(StageExecutionResult rs) {
    var context = StageContext.copy(rs.getContext());
    try {
      log.debug("[{}] Recovering stage '{}'", context.flowId(), stageName);
      ((Recoverable) stage).recover(context);
      log.debug("[{}] Stage '{}' is recovered", context.flowId(), stageName);
      return stageResult(stageName, context, RECOVERED);
    } catch (Exception exception) {
      log.debug("[{}] Stage '{}' recovery failed", context.flowId(), stageName);
      return stageResult(stageName, context, FAILED, exception);
    }
  }

  private StageExecutionResult tryCancelStage(StageExecutionResult result) {
    var context = result.getContext();
    try {
      ((Cancellable) stage).cancel(StageContext.copy(context));
      log.debug("[{}] Stage '{}' is cancelled", result.getFlowId(), stageName);
      return stageResult(stageName, context, CANCELLED, result.getError());
    } catch (Exception exception) {
      log.debug("[{}] Stage '{}' cancellation failed", result.getFlowId(), stageName, exception);
      return stageResult(stageName, context, CANCELLATION_FAILED, exception);
    }
  }

  private StageExecutionResult applyTerminalListenableMethod(StageExecutionResult rs) {
    return rs.isSucceed()
      ? applyListenableMethod(rs, Listenable::onSuccess)
      : applyListenableMethod(rs, (listenable, ctx) -> listenable.onError(ctx, rs.getError()));
  }

  private StageExecutionResult applyTerminalCancellationListenableMethod(StageExecutionResult rs) {
    return rs.getStatus() == CANCELLED
      ? applyListenableMethod(rs, Listenable::onCancel)
      : applyListenableMethod(rs, (listenable, ctx) -> listenable.onCancelError(ctx, rs.getError()));
  }

  private StageExecutionResult applyListenableMethod(StageExecutionResult ser, BiConsumer<Listenable, StageContext> c) {
    if (!(stage instanceof Listenable)) {
      return ser;
    }

    var context = StageContext.copy(ser.getContext());
    try {
      c.accept((Listenable) stage, context);
    } catch (Exception e) {
      log.warn("[{}] Listenable method failed in stage '{}'", ser.getFlowId(), stageName, e);
    }

    return stageResult(stageName, context, ser.getStatus(), ser.getError());
  }
}

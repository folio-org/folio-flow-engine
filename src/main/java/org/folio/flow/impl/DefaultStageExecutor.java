package org.folio.flow.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static lombok.AccessLevel.PACKAGE;
import static org.apache.commons.lang3.reflect.MethodUtils.getMatchingMethod;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_FAILED;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_IGNORED;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.RECOVERED;
import static org.folio.flow.model.ExecutionStatus.SKIPPED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.folio.flow.utils.FlowUtils.FLOW_ENGINE_LOGGER_NAME;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.folio.flow.api.AbstractStageContextWrapper;
import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;
import org.folio.flow.model.ExecutionStatus;
import org.folio.flow.model.StageExecutionResult;
import org.folio.flow.utils.types.Cancellable;
import org.folio.flow.utils.types.Listenable;
import org.folio.flow.utils.types.Recoverable;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.core.ResolvableType;

@Log4j2(topic = FLOW_ENGINE_LOGGER_NAME)
public final class DefaultStageExecutor<T extends StageContext> implements StageExecutor {

  private final Stage<T> stage;
  private final String stageId;
  private final String stageType;
  private final Constructor<T> stageContextWrapperConstructor;

  @Getter(PACKAGE) private final boolean isListenable;
  @Getter(PACKAGE) private final boolean isCancellable;
  @Getter(PACKAGE) private final boolean isRecoverable;

  /**
   * Creates a {@link DefaultStageExecutor} for simple stages.
   *
   * @param stage - stage to be executed as part of flow.
   */
  public DefaultStageExecutor(Stage<T> stage) {
    this.stage = stage;
    this.stageId = stage.getId();
    this.stageType = "Stage";
    this.stageContextWrapperConstructor = getStageContextWrapperConstructor();
    this.isCancellable = isAnyDefaultMethodOverwritten(stage, Cancellable.class);
    this.isRecoverable = isAnyDefaultMethodOverwritten(stage, Recoverable.class);
    this.isListenable = isAnyDefaultMethodOverwritten(stage, Listenable.class);
  }

  @Override
  public String getStageId() {
    return stageId;
  }

  @Override
  public boolean shouldCancelIfFailed(StageContext context) {
    if (!isCancellable) {
      return false;
    }

    try {
      T contextWrapper = createContextWrapper(context);
      return stage.shouldCancelIfFailed(contextWrapper);
    } catch (Exception e) {
      log.warn("[{}] Failed to check if stage '{}' should be cancelled if failed", context.flowId(), stageId, e);
      return false;
    }
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
      .thenApply(result -> applyListenableMethod(result, Stage::onStart))
      .thenApply(this::tryExecuteStage)
      .thenApply(result -> shouldRecover(result) ? tryRecoverStage(result) : result)
      .thenApply(this::applyTerminalListenableMethod);
  }

  @Override
  public CompletableFuture<StageExecutionResult> skip(StageExecutionResult upstreamResult, Executor executor) {
    log.debug("[{}] Stage '{}' is skipped", upstreamResult.getFlowId(), stageId);
    var stageResult = StageExecutionResult.builder()
      .stageName(stageId)
      .stageType(stageType)
      .context(upstreamResult.getContext())
      .status(SKIPPED)
      .build();

    return completedFuture(stageResult);
  }

  @Override
  public CompletableFuture<StageExecutionResult> cancel(StageExecutionResult upstreamResult, Executor executor) {
    if (isCancellable) {
      return completedFuture(upstreamResult)
        .thenApply(this::tryCancelStage)
        .thenApply(this::applyTerminalCancellationListenableMethod);
    }

    return completedFuture(getStageResult(upstreamResult.getContext(), CANCELLATION_IGNORED));
  }

  private StageExecutionResult tryExecuteStage(StageExecutionResult rs) {
    var context = rs.getContext();
    var flowId = context.flowId();
    try {
      var contextWrapper = createContextWrapper(context);
      stage.execute(contextWrapper);
      log.debug("[{}] Stage '{}' executed with status: {}", flowId, stageId, SUCCESS);
      return getStageResult(contextWrapper, SUCCESS);
    } catch (Exception exception) {
      log.debug("[{}] Stage '{}' executed with status: {}", flowId, stageId, FAILED, exception);
      return getStageResult(context, FAILED, exception);
    }
  }

  private boolean shouldRecover(StageExecutionResult result) {
    return result.isFailed() && isRecoverable;
  }

  private StageExecutionResult tryRecoverStage(StageExecutionResult rs) {
    var stageContext = rs.getContext();
    var flowId = stageContext.flowId();

    try {
      log.debug("[{}] Recovering stage '{}'", flowId, stageId);
      var contextWrapper = createContextWrapper(stageContext);
      stage.recover(contextWrapper);
      log.debug("[{}] Stage '{}' is recovered", flowId, stageId);
      return getStageResult(contextWrapper, RECOVERED);
    } catch (Exception exception) {
      log.debug("[{}] Stage '{}' recovery failed", flowId, stageId);
      return getStageResult(stageContext, FAILED, exception);
    }
  }

  private StageExecutionResult tryCancelStage(StageExecutionResult result) {
    var context = result.getContext();
    T contextWrapper = null;
    try {
      contextWrapper = createContextWrapper(context);
      stage.cancel(contextWrapper);
      log.debug("[{}] Stage '{}' is cancelled", result.getFlowId(), stageId);
      return getStageResult(contextWrapper, CANCELLED, result.getError());
    } catch (Exception exception) {
      log.debug("[{}] Stage '{}' cancellation failed", result.getFlowId(), stageId, exception);
      return getStageResult(contextWrapper != null ? contextWrapper : context, CANCELLATION_FAILED, exception);
    }
  }

  private StageExecutionResult applyTerminalListenableMethod(StageExecutionResult rs) {
    return rs.isSucceed()
      ? applyListenableMethod(rs, Stage::onSuccess)
      : applyListenableMethod(rs, (listenable, ctx) -> listenable.onError(ctx, rs.getError()));
  }

  private StageExecutionResult applyTerminalCancellationListenableMethod(StageExecutionResult rs) {
    return rs.getStatus() == CANCELLED
      ? applyListenableMethod(rs, Stage::onCancel)
      : applyListenableMethod(rs, (listenable, ctx) -> listenable.onCancelError(ctx, rs.getError()));
  }

  private StageExecutionResult applyListenableMethod(StageExecutionResult ser, BiConsumer<Stage<T>, T> c) {
    if (!isListenable) {
      return ser;
    }

    var flowId = ser.getFlowId();
    T contextWrapper;
    var stageContext = ser.getContext();

    try {
      contextWrapper = createContextWrapper(stageContext);
    } catch (Exception e) {
      log.warn("[{}] Failed to create a context wrapper for listenable method in stage '{}'", flowId, stageId, e);
      return getStageResult(stageContext, ser.getStatus(), ser.getError());
    }

    try {
      c.accept(stage, contextWrapper);
    } catch (Exception e) {
      log.warn("[{}] Listenable method failed in stage '{}'", flowId, stageId, e);
    }

    var listenableStageContext = contextWrapper != null ? contextWrapper : stageContext;
    return getStageResult(listenableStageContext, ser.getStatus(), ser.getError());
  }

  private Constructor<T> getStageContextWrapperConstructor() {
    var resolvableType = ResolvableType.forClass(stage.getClass()).as(Stage.class);
    var resolvedGenericClass = resolvableType.getGeneric(0).resolve();
    return getConstructorForGenericInterface(resolvedGenericClass);
  }

  private Constructor<T> getConstructorForGenericInterface(Class<?> resolvedClass) {
    try {
      if (AbstractStageContextWrapper.class.isAssignableFrom(resolvedClass)) {
        //noinspection unchecked
        return (Constructor<T>) resolvedClass.getDeclaredConstructor(StageContext.class);
      }
      return null;
    } catch (Exception e) {
      log.warn("Failed to find a constructor for a wrapper object in '{}'", stageId, e);
      return null;
    }
  }

  private T createContextWrapper(StageContext context)
    throws InvocationTargetException, InstantiationException, IllegalAccessException {
    if (stageContextWrapperConstructor == null) {
      //noinspection unchecked
      return (T) StageContext.copy(context);
    }

    return stageContextWrapperConstructor.newInstance(StageContext.copy(context));
  }

  private boolean isAnyDefaultMethodOverwritten(Stage<T> stage, Class<? extends Annotation> annotationClass) {
    var stageClass = resolveStageClass(stage);
    return Arrays.stream(Stage.class.getMethods())
      .filter(Method::isDefault)
      .filter(method -> method.getAnnotation(annotationClass) != null)
      .anyMatch(defaultMethod -> hasOverwrittenDefaultMethod(stageClass, defaultMethod));
  }

  private static <T extends StageContext> Class<?> resolveStageClass(Stage<T> stage) {
    Class<?> stageClass = stage.getClass();
    return Enhancer.isEnhanced(stageClass) ? stageClass.getSuperclass() : stageClass;
  }

  private static boolean hasOverwrittenDefaultMethod(Class<?> stageClass, Method defaultMethod) {
    var matchingMethod = getMatchingMethod(stageClass, defaultMethod.getName(), defaultMethod.getParameterTypes());
    return matchingMethod != null
      && Objects.equals(matchingMethod.getReturnType(), defaultMethod.getReturnType())
      && hasSameParameterTypes(defaultMethod, matchingMethod)
      && !Objects.equals(matchingMethod.getDeclaringClass(), Stage.class);
  }

  private static boolean hasSameParameterTypes(Method method1, Method method2) {
    for (int i = 0; i < method1.getParameterTypes().length; i++) {
      if (!method1.getParameterTypes()[i].equals(method2.getParameterTypes()[i])) {
        return false;
      }
    }

    return true;
  }

  private StageExecutionResult getStageResult(StageContext context, ExecutionStatus status) {
    return getStageResult(context, status, null);
  }

  private StageExecutionResult getStageResult(StageContext context, ExecutionStatus status, Exception error) {
    return StageExecutionResult.builder()
      .stageName(stageId)
      .stageType(stageType)
      .context(context)
      .status(status)
      .error(error)
      .build();
  }
}

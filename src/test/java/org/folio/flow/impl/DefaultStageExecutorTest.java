package org.folio.flow.impl;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_FAILED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.folio.flow.utils.FlowTestUtils.mockStageNames;
import static org.folio.flow.utils.FlowTestUtils.stageExecutionResult;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.folio.flow.api.AbstractStageContextWrapper;
import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;
import org.folio.flow.api.models.TestStageContextWrapper;
import org.folio.flow.model.StageExecutionResult;
import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;

@UnitTest
@ExtendWith(MockitoExtension.class)
class DefaultStageExecutorTest {

  @Mock private InvalidSimpleStage invalidSimpleStage;
  @Mock private InvalidSimpleStage2 invalidSimpleStage2;
  @Mock private FalseCancellableStage falseCancellableStage;
  @Mock private InvalidListenableStage invalidListenableStage;
  @Mock private InvalidCancellableStage invalidCancellableStage;
  @Mock private CancellableStageWithInheritance cancellableStageWithInheritance;

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(invalidSimpleStage, invalidSimpleStage2,
      falseCancellableStage, invalidListenableStage, invalidCancellableStage);
  }

  @Test
  void execute_negative_contextPassed() throws Exception {
    var contextAttributeVal = new AtomicReference<Object>();
    var stageExecutor = new DefaultStageExecutor<>(new Stage<StageContext>() {
      @Override
      public void execute(StageContext context) {
        context.put("customkey", "customval");
        throw new RuntimeException("Testing");
      }

      public void onError(StageContext context, Exception exception) {
        contextAttributeVal.set(context.get("customkey"));
      }
    });
    var stageContext = stageContext();
    var upstreamStageResult = stageExecutionResult("test", stageContext, SUCCESS);

    stageExecutor.execute(upstreamStageResult, newSingleThreadExecutor());
    assertThat(contextAttributeVal.get()).isEqualTo("customval");
  }

  @Test
  void execute_negative_wrapperInitializationError() throws Exception {
    mockStageNames(invalidSimpleStage);
    var stageExecutor = new DefaultStageExecutor<>(invalidSimpleStage);

    assertThat(stageExecutor.isCancellable()).isFalse();
    assertThat(stageExecutor.isRecoverable()).isFalse();
    assertThat(stageExecutor.isListenable()).isFalse();

    var stageContext = stageContext();
    var upstreamStageResult = stageExecutionResult("test", stageContext, SUCCESS);

    var executeResult = stageExecutor.execute(upstreamStageResult, newSingleThreadExecutor());
    var result = executeResult.get(500, TimeUnit.MILLISECONDS);

    assertThat(result).satisfies(rs -> {
      assertThat(rs.getStatus()).isEqualTo(FAILED);
      assertThat(rs.getStageName()).isEqualTo("invalidSimpleStage");
      assertThat(rs.getError()).isInstanceOf(InvocationTargetException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasRootCauseMessage("Initialization error");
    });
  }

  @Test
  void execute_negative_failedToFindConstructorForWrapperObject() throws Exception {
    mockStageNames(invalidSimpleStage2);
    var stageExecutor = new DefaultStageExecutor<>(invalidSimpleStage2);

    assertThat(stageExecutor.isCancellable()).isFalse();
    assertThat(stageExecutor.isRecoverable()).isFalse();
    assertThat(stageExecutor.isListenable()).isFalse();

    var stageContext = stageContext();
    var upstreamStageResult = stageExecutionResult("test", stageContext, SUCCESS);

    var executeResult = stageExecutor.execute(upstreamStageResult, newSingleThreadExecutor());
    var result = executeResult.get(500, TimeUnit.MILLISECONDS);

    assertThat(result).satisfies(rs -> {
      assertThat(rs.getStatus()).isEqualTo(FAILED);
      assertThat(rs.getStageName()).isEqualTo("invalidSimpleStage2");
      assertThat(rs.getError()).isInstanceOf(ClassCastException.class);
    });
  }

  @Test
  void execute_negative_wrapperInitializationErrorOnListenableMethod() throws Exception {
    mockStageNames(invalidListenableStage);
    var stageExecutor = new DefaultStageExecutor<>(invalidListenableStage);

    assertThat(stageExecutor.isCancellable()).isFalse();
    assertThat(stageExecutor.isRecoverable()).isFalse();
    assertThat(stageExecutor.isListenable()).isTrue();

    var stageContext = stageContext();
    var upstreamStageResult = StageExecutionResult.builder()
      .stageName("test")
      .context(stageContext)
      .status(SUCCESS)
      .build();

    var executeResult = stageExecutor.execute(upstreamStageResult, newSingleThreadExecutor());
    var result = executeResult.get(500, TimeUnit.MILLISECONDS);

    assertThat(result).satisfies(rs -> {
      assertThat(rs.getStatus()).isEqualTo(FAILED);
      assertThat(rs.getStageName()).isEqualTo("invalidListenableStage");
      assertThat(rs.getError()).isInstanceOf(InvocationTargetException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasRootCauseMessage("Initialization error");
    });
  }

  @Test
  void cancel_negative_wrapperInitializationError() throws Exception {
    mockStageNames(invalidCancellableStage);
    var stageExecutor = new DefaultStageExecutor<>(invalidCancellableStage);

    assertThat(stageExecutor.isCancellable()).isTrue();
    assertThat(stageExecutor.isRecoverable()).isFalse();
    assertThat(stageExecutor.isListenable()).isFalse();

    var stageContext = stageContext();
    var upstreamStageResult = StageExecutionResult.builder()
      .stageName("test")
      .context(stageContext)
      .status(SUCCESS)
      .build();

    var executeResult = stageExecutor.cancel(upstreamStageResult, newSingleThreadExecutor());
    var result = executeResult.get(500, TimeUnit.MILLISECONDS);

    assertThat(result).satisfies(rs -> {
      assertThat(rs.getStatus()).isEqualTo(CANCELLATION_FAILED);
      assertThat(rs.getStageName()).isEqualTo("invalidCancellableStage");
      assertThat(rs.getError()).isInstanceOf(InvocationTargetException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasRootCauseMessage("Initialization error");
    });

    assertThat(stageExecutor.shouldCancelIfFailed(stageContext)).isFalse();
  }

  @Test
  void execute_negative_stageWithFalseCancellableMethod() {
    mockStageNames(falseCancellableStage);
    var stageExecutor = new DefaultStageExecutor<>(falseCancellableStage);

    assertThat(stageExecutor.isCancellable()).isFalse();
    assertThat(stageExecutor.isRecoverable()).isFalse();
    assertThat(stageExecutor.isListenable()).isFalse();
  }

  @Test
  void execute_negative_stageWithInheritance() {
    mockStageNames(cancellableStageWithInheritance);
    var stageExecutor = new DefaultStageExecutor<>(cancellableStageWithInheritance);

    assertThat(stageExecutor.isCancellable()).isTrue();
    assertThat(stageExecutor.isRecoverable()).isFalse();
    assertThat(stageExecutor.isListenable()).isTrue();
  }

  @Test
  void execute_negative_cglibEnhancedClass() {
    Enhancer enhancer = new Enhancer();
    enhancer.setSuperclass(CancellableStageWithInheritance.class);
    enhancer.setCallback((MethodInterceptor) (obj, method, args, proxy) -> proxy.invokeSuper(obj, args));

    mockStageNames(cancellableStageWithInheritance);
    var stageExecutor = new DefaultStageExecutor<>(cancellableStageWithInheritance);

    assertThat(stageExecutor.isCancellable()).isTrue();
    assertThat(stageExecutor.isRecoverable()).isFalse();
    assertThat(stageExecutor.isListenable()).isTrue();
  }

  private static StageContext stageContext() {
    return StageContext.of("test-flow-id", emptyMap(), emptyMap());
  }

  public static class InvalidSimpleStage implements Stage<InvalidWrapper> {

    @Override
    public void execute(InvalidWrapper context) {
      // no operations here, used by unit test
    }
  }

  public static class CancellableStageWithInheritance extends AbstractListenableStage {

    @Override
    public void execute(TestStageContextWrapper context) {
      // no operations here, used by unit test
    }
  }

  public abstract static class AbstractListenableStage implements Stage<TestStageContextWrapper> {

    @Override
    public void cancel(TestStageContextWrapper context) {
      // no operations here, used by unit test
    }

    @Override
    public void onStart(TestStageContextWrapper context) {
      // no operations here, used by unit test
    }

    @Override
    public void onSuccess(TestStageContextWrapper context) {
      // no operations here, used by unit test
    }
  }

  public static class InvalidSimpleStage2 implements Stage<InvalidWrapper2> {

    @Override
    public void execute(InvalidWrapper2 context) {
      // no operations here, used by unit test
    }
  }

  public static class InvalidCancellableStage implements Stage<InvalidWrapper> {

    @Override
    public void execute(InvalidWrapper context) {
      // no operations here, used by unit test
    }

    @Override
    public void cancel(InvalidWrapper context) {
      // no operations here, used by unit test
    }
  }

  public static class InvalidListenableStage implements Stage<InvalidWrapper> {

    @Override
    public void execute(InvalidWrapper context) {
      // no operations here, used by unit test
    }

    @Override
    public void onStart(InvalidWrapper context) {
      // no operations here, used by unit test
    }
  }

  public static class FalseCancellableStage implements Stage<StageContext> {

    @Override
    public void execute(StageContext context) {}

    @SuppressWarnings("unused")
    public void cancel(StageContext context, String value) {
      // no operations here, used by unit test
    }

    @SuppressWarnings("unused")
    public void cancel(Object value) {
      // no operations here, used by unit test
    }
  }

  public static class InvalidWrapper extends AbstractStageContextWrapper {

    InvalidWrapper(StageContext stageContext) {
      super(stageContext);
      throw new RuntimeException("Initialization error");
    }
  }

  public static class InvalidWrapper2 extends AbstractStageContextWrapper {

    @SuppressWarnings("unused")
    InvalidWrapper2(StageContext stageContext, String additionalObject) {
      super(stageContext);
    }
  }
}

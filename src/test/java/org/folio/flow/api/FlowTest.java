package org.folio.flow.api;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.awaitility.Awaitility.waitAtMost;
import static org.awaitility.Durations.FIVE_HUNDRED_MILLISECONDS;
import static org.folio.flow.api.Flow.builder;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_FAILED;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.SKIPPED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.folio.flow.model.FlowExecutionStrategy.IGNORE_ON_ERROR;
import static org.folio.flow.utils.FlowTestUtils.PARAMETERIZED_TEST_NAME;
import static org.folio.flow.utils.FlowTestUtils.SINGLE_THREAD_FLOW_ENGINE;
import static org.folio.flow.utils.FlowTestUtils.executeFlow;
import static org.folio.flow.utils.FlowTestUtils.flowForStageSequence;
import static org.folio.flow.utils.FlowTestUtils.mockStageNames;
import static org.folio.flow.utils.FlowTestUtils.stageContext;
import static org.folio.flow.utils.FlowTestUtils.stageResult;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.folio.flow.api.models.CancellableTestStage;
import org.folio.flow.api.models.RecoverableAndCancellableTestStage;
import org.folio.flow.api.models.RecoverableTestStage;
import org.folio.flow.exception.FlowCancellationException;
import org.folio.flow.exception.FlowCancelledException;
import org.folio.flow.exception.FlowExecutionException;
import org.folio.flow.impl.StageExecutor;
import org.folio.flow.model.StageExecutionResult;
import org.folio.flow.model.StageResult;
import org.folio.flow.support.UnitTest;
import org.folio.flow.utils.FlowTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class FlowTest {

  @Mock private Stage simpleStage;
  @Mock private RecoverableTestStage recoverableStage;
  @Mock private CancellableTestStage cancellableStage;
  @Mock private CancellableTestStage cancellableStage1;
  @Mock private CancellableTestStage cancellableStage2;
  @Mock private RecoverableAndCancellableTestStage rcStage;

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(simpleStage, recoverableStage,
      cancellableStage, cancellableStage1, cancellableStage2, rcStage);
  }

  @Nested
  @DisplayName("common test cases")
  class CommonTestCases {

    @Test
    void builder_negative_idIsNull() {
      var builder = builder();
      assertThatThrownBy(() -> builder.id(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Flow id must not be null");
    }

    @Test
    void builder_negative_stageIsNull() {
      var builder = builder();
      assertThatThrownBy(() -> builder.stage((Stage) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Stage must not be null");
    }

    @Test
    void builder_negative_stageExecutorIsNull() {
      var builder = builder();
      assertThatThrownBy(() -> builder.stage((StageExecutor) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Stage must not be null");
    }

    @Test
    void execute_negative_flowCannotBeExecutedDirectly() {
      mockStageNames(simpleStage);
      var flow = flowForStageSequence(simpleStage);
      var stageContext = StageContext.of(flow.getId(), emptyMap(), emptyMap());
      assertThatThrownBy(() -> flow.execute(stageContext))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("method execute(StageContext context) is not supported by Flow");
    }

    @Test
    void execute_positive_customStageExecutorCanBeUsed() {
      var customStageExecutor = mock(StageExecutor.class);
      var stageExecutorName = "customStageExecutor";
      var stageContext = StageContext.of("flow-id", emptyMap(), emptyMap());
      var expectedStageResult = StageExecutionResult.stageResult(stageExecutorName, stageContext, SUCCESS);

      when(customStageExecutor.execute(any(), any())).thenReturn(completedFuture(expectedStageResult));

      var flow = builder().stage(customStageExecutor).stage(customStageExecutor).build();
      executeFlow(flow, SINGLE_THREAD_FLOW_ENGINE);

      verify(customStageExecutor, times(2)).execute(any(), any(Executor.class));
      verifyNoMoreInteractions(customStageExecutor);
    }

    @Test
    void execute_negative_customStageExecutorCanBeUsedAsFlowErrorStage() {
      var customStageExecutor = mock(StageExecutor.class);
      var stageExecutorName = "customStageExecutor";
      var stageContext = StageContext.of("flow-id", emptyMap(), emptyMap());
      var expectedStageResult = StageExecutionResult.stageResult(stageExecutorName, stageContext, SUCCESS);

      var exception = new RuntimeException("error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(simpleStage);

      when(customStageExecutor.execute(any(), any())).thenReturn(completedFuture(expectedStageResult));

      var flow = builder()
        .id("flow-id")
        .executionStrategy(IGNORE_ON_ERROR)
        .stage(simpleStage)
        .onFlowError(customStageExecutor)
        .build();

      assertThatThrownBy(() -> executeFlow(flow, SINGLE_THREAD_FLOW_ENGINE))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, stageExecutorName, SUCCESS));

      verify(customStageExecutor).execute(any(), any(Executor.class));
      verifyNoMoreInteractions(customStageExecutor);
    }

    @Test
    void execute_negative_customStageExecutorCanBeUsedAsFlowCancellation() {
      var customStageExecutor = mock(StageExecutor.class);
      var stageExecutorName = "customStageExecutor";
      var stageContext = StageContext.of("flow-id", emptyMap(), emptyMap());
      var expectedStageResult = StageExecutionResult.stageResult(stageExecutorName, stageContext, SUCCESS);

      var exception = new RuntimeException("error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(simpleStage);

      when(customStageExecutor.execute(any(), any())).thenReturn(completedFuture(expectedStageResult));

      var flow = builder()
        .id("flow-id")
        .stage(simpleStage)
        .onFlowCancellation(customStageExecutor)
        .build();

      assertThatThrownBy(() -> executeFlow(flow, SINGLE_THREAD_FLOW_ENGINE))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, stageExecutorName, SUCCESS));

      verify(customStageExecutor).execute(any(), any(Executor.class));
      verifyNoMoreInteractions(customStageExecutor);
    }

    @Test
    void execute_positive_flowParameter() {
      mockStageNames(simpleStage);

      var flow = builder()
        .stage(simpleStage)
        .flowParameter("k1", "v1")
        .flowParameter("k2", "v2")
        .build();

      executeFlow(flow, SINGLE_THREAD_FLOW_ENGINE);

      var expectedFlowParameters = Map.of("k1", "v1", "k2", "v2");
      verify(simpleStage).execute(StageContext.of(flow.getId(), expectedFlowParameters, emptyMap()));
    }

    @Test
    void execute_positive_flowParameters() {
      mockStageNames(simpleStage);

      var flow = builder()
        .stage(simpleStage)
        .flowParameters(Map.of("k1", "v1"))
        .flowParameters(Map.of("k2", "v2"))
        .build();

      executeFlow(flow, SINGLE_THREAD_FLOW_ENGINE);

      var expectedFlowParameters = Map.of("k1", "v1", "k2", "v2");
      verify(simpleStage).execute(stageContext(flow, expectedFlowParameters));
    }
  }

  @Nested
  @DisplayName("executionStrategy = default")
  class DefaultExecutionStrategy {

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive(FlowEngine flowEngine) {
      mockStageNames(simpleStage, cancellableStage);

      var mainFlowId = "test-flow";
      var subFlowId = mainFlowId + "/test-subflow";
      var subflow = flowForStageSequence(subFlowId, simpleStage, simpleStage, simpleStage);
      var flow = flowForStageSequence(mainFlowId, simpleStage, subflow, cancellableStage);

      executeFlow(flow, flowEngine);

      assertThat(flow.getId()).isEqualTo("test-flow");
      assertThat(subflow.getId()).isEqualTo("test-flow/test-subflow");

      verify(simpleStage).execute(stageContext(flow));
      verify(cancellableStage).execute(stageContext(flow));
      verify(simpleStage, times(3)).execute(stageContext(subflow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void executeAsync_positive(FlowEngine flowEngine) {
      var mainFlowId = "test-flow";
      var subFlowId = mainFlowId + "/test-subflow";
      mockStageNames(simpleStage, cancellableStage);

      var subflow = flowForStageSequence(subFlowId, simpleStage, simpleStage, simpleStage);
      var flow = flowForStageSequence(mainFlowId, simpleStage, subflow, cancellableStage);

      var future = flowEngine.executeAsync(flow);
      waitAtMost(FIVE_HUNDRED_MILLISECONDS).untilAsserted(() -> assertThat(future.isDone()).isTrue());

      assertThat(flow.getId()).isEqualTo(mainFlowId);
      assertThat(subflow.getId()).isEqualTo(subFlowId);

      verify(simpleStage).execute(stageContext(flow));
      verify(simpleStage, times(3)).execute(stageContext(subflow));
      verify(cancellableStage).execute(stageContext(flow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowOfSingleStageCancelled(FlowEngine flowEngine) {
      mockStageNames(simpleStage, cancellableStage);
      var exception = new RuntimeException("stage error");
      doThrow(exception).when(cancellableStage).execute(any());
      when(cancellableStage.shouldCancelIfFailed(any())).thenReturn(false);

      var subFlow = Flow.builder().id("main/sub").stage(cancellableStage).onFlowCancellation(simpleStage).build();
      var flow = Flow.builder().id("main").stage(subFlow).onFlowCancellation(simpleStage).build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, subFlow)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, subFlow, CANCELLED, exception, List.of(
            stageResult(subFlow, cancellableStage, FAILED, exception),
            stageResult(subFlow, simpleStage, SUCCESS))),
          stageResult(flow, simpleStage, SUCCESS));

      verify(simpleStage).execute(stageContext(flow));
      verify(simpleStage).execute(stageContext(subFlow));
      verify(cancellableStage).execute(stageContext(subFlow));
      verify(cancellableStage).shouldCancelIfFailed(stageContext(subFlow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowOfSingleStageCancellationFailed(FlowEngine flowEngine) {
      var onSubFlowCancellationError = mock(Stage.class, "onSubFlowCancellationError");
      var onFlowCancellationError = mock(Stage.class, "onFlowCancellationError");
      mockStageNames(cancellableStage, onSubFlowCancellationError, onFlowCancellationError);

      var exception = new RuntimeException("stage error");
      var cancellationException = new RuntimeException("cancellation error");
      doThrow(exception).when(cancellableStage).execute(any());
      doThrow(cancellationException).when(cancellableStage).cancel(any());
      when(cancellableStage.shouldCancelIfFailed(any())).thenReturn(true);

      var subFlow = Flow.builder()
        .id("main/sub")
        .stage(cancellableStage)
        .onFlowCancellationError(onSubFlowCancellationError)
        .build();

      var flow = Flow.builder().id("main").stage(subFlow).onFlowCancellationError(onFlowCancellationError).build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancellationException.class)
        .hasMessage("Failed to cancel flow %s. Stage '%s' failed and [%s] not cancelled", flow, subFlow, subFlow)
        .hasCause(cancellationException)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, subFlow, CANCELLATION_FAILED, exception, List.of(
            stageResult(subFlow, cancellableStage, CANCELLATION_FAILED, exception),
            stageResult(subFlow, onSubFlowCancellationError, SUCCESS))),
          stageResult(flow, onFlowCancellationError, SUCCESS));

      verify(onFlowCancellationError).execute(stageContext(flow));
      var expectedSubflowContext = stageContext(subFlow);
      verify(cancellableStage).execute(expectedSubflowContext);
      verify(cancellableStage).shouldCancelIfFailed(expectedSubflowContext);
      verify(onSubFlowCancellationError).execute(expectedSubflowContext);
      verifyNoMoreInteractions(onSubFlowCancellationError, onFlowCancellationError);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_finalStagesNotCalledOnCancellationFail(FlowEngine flowEngine) {
      mockStageNames(cancellableStage, simpleStage);

      var exception = new RuntimeException("stage error");
      var cancellationException = new RuntimeException("cancellation error");
      doThrow(exception).when(cancellableStage).execute(any());
      doThrow(cancellationException).when(cancellableStage).cancel(any());
      when(cancellableStage.shouldCancelIfFailed(any())).thenReturn(true);

      var subFlow = Flow.builder().id("main/sub").stage(cancellableStage).onFlowCancellation(simpleStage).build();
      var flow = Flow.builder().id("main").stage(subFlow).onFlowCancellation(simpleStage).build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancellationException.class)
        .hasMessage("Failed to cancel flow %s. Stage '%s' failed and [%s] not cancelled", flow, subFlow, subFlow)
        .hasCause(cancellationException)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, subFlow, CANCELLATION_FAILED, exception, List.of(
            stageResult(subFlow, cancellableStage, CANCELLATION_FAILED, exception))));

      var expectedSubflowContext = stageContext(subFlow);
      verify(cancellableStage).execute(expectedSubflowContext);
      verify(cancellableStage).shouldCancelIfFailed(expectedSubflowContext);
      verify(simpleStage, never()).execute(any(StageContext.class));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_complexFlowCancelledOnSimpleStage(FlowEngine flowEngine) {
      mockStageNames(simpleStage, cancellableStage, cancellableStage1, cancellableStage2);
      var exception = new RuntimeException("stage error");
      doThrow(exception).when(simpleStage).execute(any());

      var subFlow = flowForStageSequence("main/sub", cancellableStage1, cancellableStage2);
      var flow = flowForStageSequence("main", cancellableStage, subFlow, simpleStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, CANCELLED),
          stageResult(flow, subFlow, CANCELLED, List.of(
            stageResult(subFlow, cancellableStage1, CANCELLED),
            stageResult(subFlow, cancellableStage2, CANCELLED))),
          stageResult(flow, simpleStage, FAILED, exception));

      var expectedStageContext = stageContext(flow);
      verify(simpleStage).execute(expectedStageContext);
      verify(cancellableStage).execute(expectedStageContext);
      verify(cancellableStage).cancel(expectedStageContext);

      var expectedSubFlowContext = stageContext(subFlow);
      verify(cancellableStage1).execute(expectedSubFlowContext);
      verify(cancellableStage1).cancel(expectedSubFlowContext);
      verify(cancellableStage2).execute(expectedSubFlowContext);
      verify(cancellableStage2).cancel(expectedSubFlowContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_flowCancellationAtFailOnFirstStage(FlowEngine flowEngine) {
      var exception = new RuntimeException("stage error");
      doThrow(exception).when(cancellableStage).execute(any());
      mockStageNames(simpleStage, cancellableStage, cancellableStage1, cancellableStage2);
      when(cancellableStage.shouldCancelIfFailed(any())).thenReturn(false);

      var subFlow = flowForStageSequence("main/sub", cancellableStage1, cancellableStage2);
      var flow = flowForStageSequence("main", cancellableStage, subFlow, simpleStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, cancellableStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, FAILED),
          stageResult(flow, subFlow, SKIPPED, List.of(
            stageResult(subFlow, cancellableStage1, SKIPPED),
            stageResult(subFlow, cancellableStage2, SKIPPED))),
          stageResult(flow, simpleStage, SKIPPED, exception));

      var expectedStageContext = stageContext(flow);
      verify(cancellableStage).execute(expectedStageContext);
      verify(cancellableStage).shouldCancelIfFailed(expectedStageContext);
      verify(simpleStage, never()).execute(any());
      verify(cancellableStage1, never()).execute(any());
      verify(cancellableStage2, never()).execute(any());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_flowCanBeCancelledForSubflowException(FlowEngine flowEngine) {
      var exception = new RuntimeException("stage error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(cancellableStage, rcStage, cancellableStage1, cancellableStage2, simpleStage);

      var subFlow = flowForStageSequence("main/sub", cancellableStage1, cancellableStage2, simpleStage);
      var flow = flowForStageSequence("main", cancellableStage, subFlow, rcStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, subFlow)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, CANCELLED),
          stageResult(flow, subFlow, CANCELLED, List.of(
            stageResult(subFlow, cancellableStage1, CANCELLED),
            stageResult(subFlow, cancellableStage2, CANCELLED),
            stageResult(subFlow, simpleStage, FAILED, exception))),
          stageResult(flow, rcStage, SKIPPED));

      var mainFlowExpectedContext = stageContext(flow);
      verify(cancellableStage).execute(mainFlowExpectedContext);
      verify(cancellableStage).cancel(mainFlowExpectedContext);

      var subFlowExpectedContext = stageContext(subFlow);
      verify(simpleStage).execute(subFlowExpectedContext);
      verify(cancellableStage1).execute(subFlowExpectedContext);
      verify(cancellableStage1).cancel(subFlowExpectedContext);
      verify(cancellableStage2).execute(subFlowExpectedContext);
      verify(cancellableStage2).cancel(subFlowExpectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_onFlowSkipStageIsCalled(FlowEngine flowEngine) {
      var skipStage1 = mock(Stage.class, "skipStage1");
      mockStageNames(skipStage1, simpleStage, cancellableStage1);

      var subFlow1 = Flow.builder().id("main/sub1").stage(cancellableStage1).onFlowSkip(skipStage1).build();

      var flow = flowForStageSequence("main", simpleStage, subFlow1);

      var exception = new RuntimeException("stage error");
      doThrow(exception).when(simpleStage).execute(any());

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, subFlow1, SKIPPED, List.of(
            stageResult(subFlow1, cancellableStage1, SKIPPED),
            stageResult(subFlow1, skipStage1, SUCCESS))));

      verify(simpleStage).execute(stageContext(flow));
      verify(skipStage1).execute(stageContext(subFlow1));
    }
  }

  @Nested
  @DisplayName("executionStrategy = IGNORE_ON_ERROR")
  class IgnoreOnErrorExecutionStrategy {

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowIsFailedAtLastStage(FlowEngine flowEngine) {
      var exception = new RuntimeException("Stage error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(cancellableStage, cancellableStage1, cancellableStage2, simpleStage);

      var subFlow = flowForStageSequence("main/sub", IGNORE_ON_ERROR, cancellableStage1, cancellableStage2);
      var flow = flowForStageSequence("main", IGNORE_ON_ERROR, cancellableStage, subFlow, simpleStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, SUCCESS),
          stageResult(flow, subFlow, SUCCESS, List.of(
            stageResult(subFlow, cancellableStage1, SUCCESS),
            stageResult(subFlow, cancellableStage2, SUCCESS))),
          stageResult(flow, simpleStage, FAILED, exception));

      verify(cancellableStage).execute(stageContext(flow));
      verify(cancellableStage1).execute(stageContext(subFlow));
      verify(cancellableStage2).execute(stageContext(subFlow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowIsFailedAtFirstStage(FlowEngine flowEngine) {
      var exception = new RuntimeException("Stage error");
      doThrow(exception).when(cancellableStage).execute(any());
      mockStageNames(cancellableStage, cancellableStage1, cancellableStage2, simpleStage);

      var subFlow = flowForStageSequence("main/sub", IGNORE_ON_ERROR, cancellableStage1, cancellableStage2);
      var flow = flowForStageSequence("main", IGNORE_ON_ERROR, cancellableStage, subFlow, simpleStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, cancellableStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, FAILED, exception),
          stageResult(flow, subFlow, SKIPPED, List.of(
            stageResult(subFlow, cancellableStage1, SKIPPED),
            stageResult(subFlow, cancellableStage2, SKIPPED))),
          stageResult(flow, simpleStage, SKIPPED, exception));

      verify(cancellableStage).execute(stageContext(flow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowIsFailedAtSubflowStage(FlowEngine flowEngine) {
      var exception = new RuntimeException("Stage error");
      doThrow(exception).when(cancellableStage2).execute(any());
      mockStageNames(cancellableStage, cancellableStage1, cancellableStage2, simpleStage);

      var subFlow = flowForStageSequence("main/sub", IGNORE_ON_ERROR, cancellableStage1, cancellableStage2);
      var flow = flowForStageSequence("main", IGNORE_ON_ERROR, cancellableStage, subFlow, simpleStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, subFlow)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, SUCCESS),
          stageResult(flow, subFlow, FAILED, exception, List.of(
            stageResult(subFlow, cancellableStage1, SUCCESS),
            stageResult(subFlow, cancellableStage2, FAILED, exception))),
          stageResult(flow, simpleStage, SKIPPED));

      verify(cancellableStage).execute(stageContext(flow));
      verify(cancellableStage1).execute(stageContext(subFlow));
      verify(cancellableStage2).execute(stageContext(subFlow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_flowOfSingleStageFailed(FlowEngine flowEngine) {
      mockStageNames(simpleStage, cancellableStage);
      var exception = new RuntimeException("stage error");
      doThrow(exception).when(cancellableStage).execute(any());

      var subFlow = builder()
        .id("main/sub")
        .stage(cancellableStage)
        .onFlowError(simpleStage)
        .executionStrategy(IGNORE_ON_ERROR)
        .build();

      var flow = builder()
        .id("main")
        .stage(subFlow)
        .onFlowError(simpleStage)
        .executionStrategy(IGNORE_ON_ERROR)
        .build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, subFlow)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, subFlow, FAILED, exception, List.of(
            stageResult(subFlow, cancellableStage, FAILED, exception),
            stageResult(subFlow, simpleStage, SUCCESS, exception))),
          stageResult(flow, simpleStage, SUCCESS));

      verify(simpleStage).execute(stageContext(flow));
      verify(simpleStage).execute(stageContext(subFlow));
      verify(cancellableStage).execute(stageContext(subFlow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_onFlowSkipStageIsCalled(FlowEngine flowEngine) {
      var skipStage1 = mock(Stage.class, "skipStage1");
      mockStageNames(skipStage1, simpleStage, cancellableStage1);

      var subFlow1 = Flow.builder().id("main/sub1").stage(cancellableStage1).onFlowSkip(skipStage1).build();
      var flow = flowForStageSequence("main", IGNORE_ON_ERROR, simpleStage, subFlow1);

      var exception = new RuntimeException("stage error");
      doThrow(exception).when(simpleStage).execute(any());

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, subFlow1, SKIPPED, List.of(
            stageResult(subFlow1, cancellableStage1, SKIPPED),
            stageResult(subFlow1, skipStage1, SUCCESS))));

      verify(simpleStage).execute(stageContext(flow));
      verify(skipStage1).execute(stageContext(subFlow1));
    }
  }
}

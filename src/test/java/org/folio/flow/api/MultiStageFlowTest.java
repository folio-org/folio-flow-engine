package org.folio.flow.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.folio.flow.api.Flow.builder;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_FAILED;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_IGNORED;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.RECOVERED;
import static org.folio.flow.model.ExecutionStatus.SKIPPED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.folio.flow.model.FlowExecutionStrategy.IGNORE_ON_ERROR;
import static org.folio.flow.utils.FlowTestUtils.PARAMETERIZED_TEST_NAME;
import static org.folio.flow.utils.FlowTestUtils.executeFlow;
import static org.folio.flow.utils.FlowTestUtils.flowForStageSequence;
import static org.folio.flow.utils.FlowTestUtils.mockStageNames;
import static org.folio.flow.utils.FlowTestUtils.stageContext;
import static org.folio.flow.utils.FlowTestUtils.stageResult;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.folio.flow.api.models.CancellableTestStage;
import org.folio.flow.api.models.RecoverableAndCancellableTestStage;
import org.folio.flow.api.models.RecoverableTestStage;
import org.folio.flow.exception.FlowCancellationException;
import org.folio.flow.exception.FlowCancelledException;
import org.folio.flow.exception.FlowExecutionException;
import org.folio.flow.model.StageResult;
import org.folio.flow.support.UnitTest;
import org.folio.flow.utils.FlowTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
public class MultiStageFlowTest {

  @Mock private Stage<StageContext> simpleStage;
  @Mock private Stage<StageContext> onFlowCancellationError;
  @Mock private CancellableTestStage cancellableStage;
  @Mock private RecoverableTestStage recoverableStage;
  @Mock private RecoverableAndCancellableTestStage rcStage;

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(simpleStage, onFlowCancellationError, recoverableStage, cancellableStage, rcStage);
  }

  @Nested
  @DisplayName("executionStrategy = default")
  class DefaultExecutionStrategy {

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive(FlowEngine flowEngine) {
      mockStageNames(simpleStage, recoverableStage, cancellableStage, rcStage);
      var flow = flowForStageSequence(simpleStage, recoverableStage, cancellableStage, rcStage);
      executeFlow(flow, flowEngine);

      var expectedContext = stageContext(flow);

      verify(simpleStage).execute(expectedContext);
      verify(recoverableStage).execute(expectedContext);
      verify(cancellableStage).execute(expectedContext);
      verify(rcStage).execute(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_taskIsRecovered(FlowEngine flowEngine) {
      doThrow(RuntimeException.class).when(rcStage).execute(any());
      mockStageNames(simpleStage, recoverableStage, cancellableStage, rcStage);

      var flow = flowForStageSequence(simpleStage, recoverableStage, cancellableStage, rcStage);
      executeFlow(flow, flowEngine);

      var expectedContext = stageContext(flow);

      verify(simpleStage).execute(expectedContext);
      verify(recoverableStage).execute(expectedContext);
      verify(cancellableStage).execute(expectedContext);
      verify(rcStage).execute(expectedContext);
      verify(rcStage).recover(expectedContext);
      assertThat(flowEngine.getFlowStatus(flow)).isEqualTo(SUCCESS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_onFlowErrorStageIsNotExecuted(FlowEngine flowEngine) {
      mockStageNames(simpleStage, recoverableStage);
      var flow = Flow.builder().stage(simpleStage).onFlowError(recoverableStage).build();
      executeFlow(flow, flowEngine);

      var expectedContext = stageContext(flow);

      verify(simpleStage).execute(expectedContext);
      verify(recoverableStage, never()).execute(any(StageContext.class));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowIsCancelledFromErrorAtFirstStage(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(simpleStage, recoverableStage, cancellableStage, rcStage);

      var flow = flowForStageSequence(simpleStage, recoverableStage, cancellableStage, rcStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, recoverableStage, SKIPPED),
          stageResult(flow, cancellableStage, SKIPPED),
          stageResult(flow, rcStage, SKIPPED));

      var expectedContext = stageContext(flow);
      verify(simpleStage).execute(expectedContext);
      verify(recoverableStage, never()).execute(expectedContext);
      verify(cancellableStage, never()).execute(expectedContext);
      verify(rcStage, never()).execute(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowIsCancelledFromErrorAtSecondStage(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(recoverableStage).execute(any());
      doThrow(exception).when(recoverableStage).recover(any());
      mockStageNames(simpleStage, recoverableStage, cancellableStage, rcStage);

      var flow = flowForStageSequence(simpleStage, recoverableStage, cancellableStage, rcStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, recoverableStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, CANCELLATION_IGNORED),
          stageResult(flow, recoverableStage, FAILED, exception),
          stageResult(flow, cancellableStage, SKIPPED),
          stageResult(flow, rcStage, SKIPPED));

      var expectedContext = stageContext(flow);
      verify(simpleStage).execute(expectedContext);
      verify(recoverableStage).execute(expectedContext);
      verify(recoverableStage).recover(expectedContext);
      verify(cancellableStage, never()).execute(expectedContext);
      verify(rcStage, never()).execute(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowIsCancelledFromErrorAtLastStage(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(rcStage).execute(any());
      doThrow(exception).when(rcStage).recover(any());
      mockStageNames(simpleStage, recoverableStage, cancellableStage, rcStage);
      when(rcStage.shouldCancelIfFailed(any())).thenReturn(false);

      var flow = flowForStageSequence(simpleStage, recoverableStage, cancellableStage, rcStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, rcStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, CANCELLATION_IGNORED),
          stageResult(flow, recoverableStage, CANCELLATION_IGNORED),
          stageResult(flow, cancellableStage, CANCELLED),
          stageResult(flow, rcStage, FAILED, exception));

      var expectedContext = stageContext(flow);
      verify(simpleStage).execute(expectedContext);
      verify(recoverableStage).execute(expectedContext);
      verify(cancellableStage).execute(expectedContext);
      verify(cancellableStage).cancel(expectedContext);
      verify(rcStage).execute(expectedContext);
      verify(rcStage).recover(expectedContext);
      verify(rcStage).shouldCancelIfFailed(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowOfTwoStagesIsCancelled(FlowEngine flowEngine) {
      mockStageNames(simpleStage, cancellableStage);

      var exception = new RuntimeException("stage error");
      doThrow(exception).when(simpleStage).execute(any());

      var flow = flowForStageSequence(cancellableStage, simpleStage);
      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, CANCELLED),
          stageResult(flow, simpleStage, FAILED, exception));

      var expectedContext = stageContext(flow);
      verify(cancellableStage).execute(expectedContext);
      verify(cancellableStage).cancel(expectedContext);
      verify(simpleStage).execute(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowCancellationStageFailed(FlowEngine flowEngine) {
      mockStageNames(cancellableStage, rcStage);

      var exception = new RuntimeException("error");
      doThrow(exception).when(rcStage).execute(any());
      doThrow(exception).when(rcStage).recover(any());
      doThrow(exception).when(cancellableStage).cancel(any());
      when(rcStage.shouldCancelIfFailed(any())).thenReturn(false);

      var flow = flowForStageSequence(cancellableStage, rcStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancellationException.class)
        .hasMessage("Failed to cancel flow %s. Stage '%s' failed and [%s] not cancelled",
          flow, rcStage, cancellableStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, CANCELLATION_FAILED, exception),
          stageResult(flow, rcStage, FAILED, exception));

      var expectedContext = stageContext(flow);
      verify(cancellableStage).execute(expectedContext);
      verify(rcStage).execute(expectedContext);
      verify(rcStage).recover(expectedContext);
      verify(rcStage).shouldCancelIfFailed(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowCancellationStageFailedWithTerminalStages(FlowEngine flowEngine) {
      mockStageNames(simpleStage, onFlowCancellationError, cancellableStage, rcStage);

      var exception = new RuntimeException("error");
      doThrow(exception).when(cancellableStage).execute(any());
      doThrow(exception).when(cancellableStage).cancel(any());
      when(cancellableStage.shouldCancelIfFailed(any())).thenReturn(true);

      var flow = Flow.builder()
        .stage(rcStage)
        .stage(cancellableStage)
        .onFlowCancellation(simpleStage)
        .onFlowCancellationError(onFlowCancellationError)
        .build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancellationException.class)
        .hasMessage("Failed to cancel flow %s. Stage '%s' failed and [%s] not cancelled",
          flow, cancellableStage, cancellableStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, rcStage, CANCELLED, exception),
          stageResult(flow, cancellableStage, CANCELLATION_FAILED, exception),
          stageResult(flow, onFlowCancellationError, SUCCESS));

      var expectedContext = stageContext(flow);
      verify(cancellableStage).execute(expectedContext);
      verify(cancellableStage).cancel(expectedContext);
      verify(cancellableStage).shouldCancelIfFailed(expectedContext);
      verify(onFlowCancellationError).execute(expectedContext);
      verify(rcStage).execute(expectedContext);
      verify(rcStage).cancel(expectedContext);
      verify(simpleStage, never()).execute(any(StageContext.class));
      verifyNoMoreInteractions(onFlowCancellationError);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowOnCancellationStageCalled(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(rcStage).execute(any());
      doThrow(exception).when(rcStage).recover(any());
      when(rcStage.shouldCancelIfFailed(any())).thenReturn(false);
      mockStageNames(simpleStage, cancellableStage, rcStage);

      var flow = builder()
        .stage(cancellableStage)
        .stage(rcStage)
        .onFlowCancellation(simpleStage)
        .build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, rcStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, CANCELLED),
          stageResult(flow, rcStage, FAILED, exception),
          stageResult(flow, simpleStage, SUCCESS));

      var expectedContext = stageContext(flow);
      verify(cancellableStage).execute(expectedContext);
      verify(cancellableStage).cancel(expectedContext);
      verify(rcStage).execute(expectedContext);
      verify(rcStage).recover(expectedContext);
      verify(simpleStage).execute(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_flowOnCancellationStageIsNotCalledWhenCancellationFailed(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(rcStage).execute(any());
      doThrow(exception).when(rcStage).recover(any());
      mockStageNames(simpleStage, cancellableStage, rcStage, onFlowCancellationError);
      when(rcStage.shouldCancelIfFailed(any())).thenReturn(false);

      var cancellationException = new RuntimeException("cancellation error");
      doThrow(cancellationException).when(cancellableStage).cancel(any());

      var flow = builder()
        .stage(cancellableStage)
        .stage(rcStage)
        .onFlowCancellation(simpleStage)
        .onFlowCancellationError(onFlowCancellationError)
        .build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancellationException.class)
        .hasMessage("Failed to cancel flow %s. Stage '%s' failed and [%s] not cancelled",
          flow, rcStage, cancellableStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, CANCELLATION_FAILED, cancellationException),
          stageResult(flow, rcStage, FAILED, exception),
          stageResult(flow, onFlowCancellationError, SUCCESS));

      var expectedContext = stageContext(flow);
      verify(cancellableStage).execute(expectedContext);
      verify(rcStage).execute(expectedContext);
      verify(rcStage).recover(expectedContext);
      verify(rcStage).shouldCancelIfFailed(expectedContext);
      verify(simpleStage, never()).execute(expectedContext);
      verify(onFlowCancellationError).execute(expectedContext);

      verifyNoMoreInteractions(onFlowCancellationError);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_failedCancellableStageCanBeCancelled(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(rcStage).execute(any());
      doThrow(exception).when(rcStage).recover(any());
      mockStageNames(simpleStage, cancellableStage, rcStage);
      when(rcStage.shouldCancelIfFailed(any())).thenReturn(true);

      var flow = builder().stage(cancellableStage).stage(rcStage).onFlowCancellation(simpleStage).build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, rcStage, cancellableStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, CANCELLED),
          stageResult(flow, rcStage, CANCELLED, exception),
          stageResult(flow, simpleStage, SUCCESS));

      var expectedContext = stageContext(flow);
      verify(cancellableStage).execute(expectedContext);
      verify(cancellableStage).cancel(expectedContext);
      verify(rcStage).execute(expectedContext);
      verify(rcStage).recover(expectedContext);
      verify(rcStage).cancel(expectedContext);
      verify(rcStage).shouldCancelIfFailed(expectedContext);
      verify(simpleStage).execute(expectedContext);
    }
  }

  @Nested
  @DisplayName("executionStrategy = IGNORE_ON_ERROR")
  class IgnoreOnErrorExecutionStrategy {

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_twoStages(FlowEngine flowEngine) {
      var exception = new RuntimeException("Stage error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(simpleStage, cancellableStage);

      var flow = flowForStageSequence(IGNORE_ON_ERROR, cancellableStage, simpleStage);
      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, SUCCESS),
          stageResult(flow, simpleStage, FAILED, exception));

      var expectedContext = stageContext(flow);
      verify(cancellableStage).execute(expectedContext);
      verify(simpleStage).execute(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_threeStages(FlowEngine flowEngine) {
      var exception = new RuntimeException("Stage error");
      doThrow(exception).when(simpleStage).execute(any());
      doThrow(exception).when(recoverableStage).execute(any());
      mockStageNames(simpleStage, recoverableStage, cancellableStage);

      var flow = flowForStageSequence(IGNORE_ON_ERROR, cancellableStage, recoverableStage, simpleStage);
      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, SUCCESS),
          stageResult(flow, recoverableStage, RECOVERED),
          stageResult(flow, simpleStage, FAILED, exception));

      var expectedContext = stageContext(flow);
      verify(cancellableStage).execute(expectedContext);
      verify(recoverableStage).execute(expectedContext);
      verify(recoverableStage).recover(expectedContext);
      verify(simpleStage).execute(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_onFlowErrorStageCalled(FlowEngine flowEngine) {
      mockStageNames(simpleStage, rcStage);
      var exception = new RuntimeException("Stage error");
      doThrow(exception).when(simpleStage).execute(any());

      var flow = builder()
        .stage(simpleStage)
        .executionStrategy(IGNORE_ON_ERROR)
        .onFlowError(rcStage)
        .build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, rcStage, SUCCESS));

      verify(simpleStage).execute(stageContext(flow));
      verify(rcStage).execute(stageContext(flow));
    }
  }
}

package org.folio.flow.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.awaitility.Awaitility.waitAtMost;
import static org.awaitility.Durations.FIVE_HUNDRED_MILLISECONDS;
import static org.folio.flow.api.Flow.builder;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
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
import org.folio.flow.api.models.CustomSimpleStage;
import org.folio.flow.api.models.RecoverableAndCancellableTestStage;
import org.folio.flow.api.models.RecoverableTestStage;
import org.folio.flow.api.models.SimpleNonGenericStage;
import org.folio.flow.api.models.SimpleStage;
import org.folio.flow.api.models.StageWithInheritance;
import org.folio.flow.api.models.TestStageContextWrapper;
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
public class SingleStageFlowTest {

  @Mock private SimpleStage simpleStage;
  @Mock private SimpleStage onFlowErrorStage;
  @Mock private SimpleStage onFlowCancelledStage;
  @Mock private CancellableTestStage cancellableStage;
  @Mock private RecoverableTestStage recoverableStage;
  @Mock private RecoverableAndCancellableTestStage rcStage;

  @Mock private CustomSimpleStage customSimpleStage;
  @Mock private SimpleNonGenericStage simpleNonGenericStage;
  @Mock private StageWithInheritance customStageWithInheritance;

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(simpleStage, recoverableStage, cancellableStage, rcStage,
      simpleNonGenericStage, customStageWithInheritance);
  }

  @Nested
  @DisplayName("executionStrategy = default")
  class DefaultExecutionStrategy {

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive(FlowEngine flowEngine) {
      mockStageNames(simpleStage);
      var flow = flowForStageSequence(simpleStage);

      executeFlow(flow, flowEngine);

      verify(simpleStage).execute(stageContext(flow));
      assertThat(flowEngine.getFlowStatus(flow)).isEqualTo(SUCCESS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_lambdaStage(FlowEngine flowEngine) {
      var stage = (Stage<StageContext>) ctx -> {};
      var flow = flowForStageSequence(stage);

      executeFlow(flow, flowEngine);

      assertThat(flowEngine.getFlowStatus(flow)).isEqualTo(SUCCESS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_simpleNonGenericStage(FlowEngine flowEngine) {
      mockStageNames(simpleNonGenericStage);
      var flow = flowForStageSequence(simpleNonGenericStage);

      executeFlow(flow, flowEngine);

      verify(simpleNonGenericStage).execute(stageContext(flow));
      assertThat(flowEngine.getFlowStatus(flow)).isEqualTo(SUCCESS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_customSingleStage(FlowEngine flowEngine) {
      mockStageNames(customSimpleStage);
      var flow = flowForStageSequence(customSimpleStage);

      executeFlow(flow, flowEngine);

      verify(customSimpleStage).execute(new TestStageContextWrapper(stageContext(flow)));
      assertThat(flowEngine.getFlowStatus(flow)).isEqualTo(SUCCESS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_customSingleStageWithInheritance(FlowEngine flowEngine) {
      mockStageNames(customStageWithInheritance);
      var flow = flowForStageSequence(customStageWithInheritance);

      executeFlow(flow, flowEngine);

      verify(customStageWithInheritance).execute(new TestStageContextWrapper(stageContext(flow)));
      assertThat(flowEngine.getFlowStatus(flow)).isEqualTo(SUCCESS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void executeAsync_positive(FlowEngine flowEngine) {
      mockStageNames(simpleStage);

      var flow = flowForStageSequence(simpleStage);
      var future = flowEngine.executeAsync(flow);

      waitAtMost(FIVE_HUNDRED_MILLISECONDS).untilAsserted(() -> assertThat(future.isDone()).isTrue());
      verify(simpleStage).execute(stageContext(flow));
      assertThat(flowEngine.getFlowStatus(flow)).isEqualTo(SUCCESS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_stageIsRecovered(FlowEngine flowEngine) {
      var exception = new RuntimeException("Stage error");
      mockStageNames(recoverableStage);
      doThrow(exception).when(recoverableStage).execute(any());

      var flow = flowForStageSequence(recoverableStage);
      executeFlow(flow, flowEngine);

      var expectedStageContext = stageContext(flow);
      verify(recoverableStage).execute(expectedStageContext);
      verify(recoverableStage).recover(expectedStageContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_simpleStageFailed(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(simpleStage);

      var flow = flowForStageSequence(simpleStage);
      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(stageResult(flow, simpleStage, FAILED, exception));

      var expectedContext = stageContext(flow);
      verify(simpleStage).execute(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_simpleStageFailedWithFinalStage(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(simpleStage, onFlowErrorStage, onFlowCancelledStage);

      var flow = builder()
        .stage(simpleStage)
        .onFlowError(onFlowErrorStage)
        .onFlowCancellation(onFlowCancelledStage)
        .build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, onFlowCancelledStage, SUCCESS)
        );

      var expectedContext = stageContext(flow);
      verify(simpleStage).execute(expectedContext);
      verify(onFlowCancelledStage).execute(expectedContext);
      verify(onFlowErrorStage, never()).execute(any());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_simpleAndFinalStagesFailed(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(simpleStage).execute(any());
      doThrow(exception).when(onFlowCancelledStage).execute(any());
      mockStageNames(simpleStage, onFlowErrorStage, onFlowCancelledStage);

      var flow = builder()
        .stage(simpleStage)
        .onFlowError(onFlowErrorStage)
        .onFlowCancellation(onFlowCancelledStage)
        .build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancellationException.class)
        .hasMessage("Failed to cancel flow %s, stage '%s' failed", flow, onFlowCancelledStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, onFlowCancelledStage, FAILED, exception));

      var expectedContext = stageContext(flow);
      verify(simpleStage).execute(expectedContext);
      verify(onFlowCancelledStage).execute(expectedContext);
      verify(onFlowErrorStage, never()).execute(any());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_recoverableStageFailed(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(rcStage).execute(any());
      doThrow(exception).when(rcStage).recover(any());
      mockStageNames(rcStage);
      when(rcStage.shouldCancelIfFailed(any())).thenReturn(false);

      var flow = flowForStageSequence(rcStage);
      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, rcStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(stageResult(flow, rcStage, FAILED, exception));

      var expectedContext = stageContext(flow);
      verify(rcStage).execute(expectedContext);
      verify(rcStage).recover(expectedContext);
      verify(rcStage).shouldCancelIfFailed(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_cancellableStageFailed(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(cancellableStage).execute(any());
      when(cancellableStage.shouldCancelIfFailed(any())).thenReturn(false);
      mockStageNames(cancellableStage);

      var flow = flowForStageSequence(cancellableStage);
      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, cancellableStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(stageResult(flow, cancellableStage, FAILED));

      var expectedContext = stageContext(flow);
      verify(cancellableStage).execute(expectedContext);
      verify(cancellableStage).shouldCancelIfFailed(expectedContext);
      verify(cancellableStage, never()).cancel(expectedContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_cancellableStageFailedAndThenCancelled(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(cancellableStage).execute(any());
      when(cancellableStage.shouldCancelIfFailed(any())).thenReturn(true);
      mockStageNames(cancellableStage);

      var flow = flowForStageSequence(cancellableStage);
      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, cancellableStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(stageResult(flow, cancellableStage, CANCELLED));

      var expectedContext = stageContext(flow);
      verify(cancellableStage).execute(expectedContext);
      verify(cancellableStage).cancel(expectedContext);
      verify(cancellableStage).shouldCancelIfFailed(expectedContext);
    }
  }

  @Nested
  @DisplayName("executionStrategy = IGNORE_ON_ERROR")
  class IgnoreOnErrorExecutionStrategy {

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_stageCanBeRecovered(FlowEngine flowEngine) {
      var exception = new RuntimeException("Stage error");
      doThrow(exception).when(recoverableStage).execute(any());
      mockStageNames(recoverableStage);

      var flow = flowForStageSequence(IGNORE_ON_ERROR, recoverableStage);
      executeFlow(flow, flowEngine);

      var expectedStageContext = stageContext(flow);
      verify(recoverableStage).execute(expectedStageContext);
      verify(recoverableStage).recover(expectedStageContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_simpleStageFailed(FlowEngine flowEngine) {
      var exception = new RuntimeException("Stage error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(simpleStage);

      var flow = flowForStageSequence(IGNORE_ON_ERROR, simpleStage);
      var expectedStageContext = stageContext(flow);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(stageResult(flow, "simpleStage", FAILED, exception));

      verify(simpleStage).execute(expectedStageContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_simpleStageFailedWithFinalStage(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(simpleStage, onFlowErrorStage, onFlowCancelledStage);

      var flow = builder()
        .stage(simpleStage)
        .executionStrategy(IGNORE_ON_ERROR)
        .onFlowError(onFlowErrorStage)
        .onFlowCancellation(onFlowCancelledStage)
        .build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, onFlowErrorStage, SUCCESS));

      var expectedContext = stageContext(flow);
      verify(simpleStage).execute(expectedContext);
      verify(onFlowErrorStage).execute(expectedContext);
      verify(onFlowCancelledStage, never()).execute(any());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_simpleAndFinalStagesFailed(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      var onFlowErrorStageException = new RuntimeException("Final stage execution error");
      doThrow(exception).when(simpleStage).execute(any());
      doThrow(onFlowErrorStageException).when(onFlowErrorStage).execute(any());
      mockStageNames(simpleStage, onFlowErrorStage, onFlowCancelledStage);

      var flow = builder()
        .stage(simpleStage)
        .executionStrategy(IGNORE_ON_ERROR)
        .onFlowError(onFlowErrorStage)
        .onFlowCancellation(onFlowCancelledStage)
        .build();

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, onFlowErrorStage)
        .hasCause(onFlowErrorStageException)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, onFlowErrorStage, FAILED, onFlowErrorStageException));

      var expectedContext = stageContext(flow);
      verify(simpleStage).execute(expectedContext);
      verify(onFlowErrorStage).execute(expectedContext);
      verify(onFlowCancelledStage, never()).execute(any());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_recoverableStageFailed(FlowEngine flowEngine) {
      var exception = new RuntimeException("Stage error");
      doThrow(exception).when(recoverableStage).execute(any());
      doThrow(exception).when(recoverableStage).recover(any());
      mockStageNames(recoverableStage);

      var flow = flowForStageSequence(IGNORE_ON_ERROR, recoverableStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, recoverableStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(stageResult(flow, recoverableStage, FAILED));

      var expectedStageContext = stageContext(flow);
      verify(recoverableStage).execute(expectedStageContext);
      verify(recoverableStage).recover(expectedStageContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_cancellableStageFailed(FlowEngine flowEngine) {
      var exception = new RuntimeException("Stage error");
      doThrow(exception).when(cancellableStage).execute(any());
      mockStageNames(cancellableStage);

      var flow = flowForStageSequence(IGNORE_ON_ERROR, cancellableStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, cancellableStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(stageResult(flow, cancellableStage, FAILED));

      var expectedStageContext = stageContext(flow);
      verify(cancellableStage).execute(expectedStageContext);
      verify(cancellableStage, never()).cancel(expectedStageContext);
    }
  }
}

package org.folio.flow.api;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_FAILED;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.utils.FlowTestUtils.PARAMETERIZED_TEST_NAME;
import static org.folio.flow.utils.FlowTestUtils.executeFlow;
import static org.folio.flow.utils.FlowTestUtils.flowForStageSequence;
import static org.folio.flow.utils.FlowTestUtils.mockStageNames;
import static org.folio.flow.utils.FlowTestUtils.stageContext;
import static org.folio.flow.utils.FlowTestUtils.stageResult;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.folio.flow.api.models.TestListenableStage;
import org.folio.flow.exception.FlowCancellationException;
import org.folio.flow.exception.FlowCancelledException;
import org.folio.flow.model.StageResult;
import org.folio.flow.support.UnitTest;
import org.folio.flow.utils.FlowTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class ListenableStageTest {

  @Mock private TestListenableStage listenableStage;

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(listenableStage);
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_positive(FlowEngine flowEngine) {
    mockStageNames(listenableStage);

    var flow = flowForStageSequence(listenableStage);
    executeFlow(flow, flowEngine);

    var expectedContext = stageContext(flow);
    verify(listenableStage).execute(expectedContext);
    verify(listenableStage).onStart(expectedContext);
    verify(listenableStage).onSuccess(expectedContext);
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_negative_listenableStageFailedAndCancelled(FlowEngine flowEngine) {
    mockStageNames(listenableStage);
    var exception = new RuntimeException("Stage error");
    doThrow(exception).when(listenableStage).execute(any());
    when(listenableStage.shouldCancelIfFailed(any())).thenReturn(true);

    var flow = flowForStageSequence(listenableStage);
    assertThatThrownBy(() -> executeFlow(flow, flowEngine))
      .isInstanceOf(FlowCancelledException.class)
      .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, listenableStage)
      .extracting(FlowTestUtils::stageResults, list(StageResult.class))
      .containsExactly(stageResult(flow, listenableStage, CANCELLED));

    var expectedContext = stageContext(flow);

    verify(listenableStage).onStart(expectedContext);
    verify(listenableStage).execute(expectedContext);
    verify(listenableStage).onError(expectedContext, exception);
    verify(listenableStage).cancel(expectedContext);
    verify(listenableStage).onCancel(expectedContext);
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_negative_listenableStageFailedAndNotCancelled(FlowEngine flowEngine) {
    mockStageNames(listenableStage);
    var executionException = new RuntimeException("Stage error");
    var cancellationException = new RuntimeException("cancellation error");
    doThrow(executionException).when(listenableStage).execute(any());
    doThrow(cancellationException).when(listenableStage).cancel(any());
    when(listenableStage.shouldCancelIfFailed(any())).thenReturn(true);

    var flow = flowForStageSequence(listenableStage);
    assertThatThrownBy(() -> executeFlow(flow, flowEngine))
      .isInstanceOf(FlowCancellationException.class)
      .hasMessage("Failed to cancel flow %s. Stage '%2$s' failed and [%2$s] not cancelled", flow, listenableStage)
      .extracting(FlowTestUtils::stageResults, list(StageResult.class))
      .containsExactly(stageResult(flow, listenableStage, CANCELLATION_FAILED));

    var expectedContext = stageContext(flow);

    verify(listenableStage).onStart(expectedContext);
    verify(listenableStage).execute(expectedContext);
    verify(listenableStage).onError(expectedContext, executionException);
    verify(listenableStage).cancel(expectedContext);
    verify(listenableStage).onCancelError(expectedContext, cancellationException);
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_positive_listenableMethodException(FlowEngine flowEngine) {
    mockStageNames(listenableStage);
    var exception = new RuntimeException("Stage error");
    doThrow(exception).when(listenableStage).onStart(any());

    var flow = flowForStageSequence(listenableStage);
    executeFlow(flow, flowEngine);

    var expectedContext = stageContext(flow);
    verify(listenableStage).execute(expectedContext);
    verify(listenableStage).onStart(expectedContext);
    verify(listenableStage).onSuccess(expectedContext);
  }
}

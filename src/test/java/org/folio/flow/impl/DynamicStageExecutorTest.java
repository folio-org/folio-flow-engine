package org.folio.flow.impl;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.SKIPPED;
import static org.folio.flow.model.FlowExecutionStrategy.IGNORE_ON_ERROR;
import static org.folio.flow.utils.FlowTestUtils.PARAMETERIZED_TEST_NAME;
import static org.folio.flow.utils.FlowTestUtils.executeFlow;
import static org.folio.flow.utils.FlowTestUtils.flowForStageSequence;
import static org.folio.flow.utils.FlowTestUtils.mockStageNames;
import static org.folio.flow.utils.FlowTestUtils.stageContext;
import static org.folio.flow.utils.FlowTestUtils.stageResult;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import org.folio.flow.api.DynamicStage;
import org.folio.flow.api.FlowEngine;
import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;
import org.folio.flow.api.models.CancellableTestStage;
import org.folio.flow.exception.FlowCancelledException;
import org.folio.flow.exception.FlowExecutionException;
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
class DynamicStageExecutorTest {

  @Mock private Stage<StageContext> simpleStage;
  @Mock private Stage<StageContext> simpleStage2;
  @Mock private CancellableTestStage cancellableStage;

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(simpleStage, simpleStage2);
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_positive_simpleStage(FlowEngine flowEngine) {
    mockStageNames(simpleStage);
    var flow = flowForStageSequence("main", DynamicStage.of("dynamicStage", ctx -> simpleStage));

    executeFlow(flow, flowEngine);

    verify(simpleStage).execute(stageContext(flow));
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_positive_flowStage(FlowEngine flowEngine) {
    mockStageNames(simpleStage);
    var dynamicStage = DynamicStage.of("dynamicStage", ctx -> flowForStageSequence("main/f1", simpleStage));
    var flow = flowForStageSequence("main", dynamicStage);

    executeFlow(flow, flowEngine);

    verify(simpleStage).execute(StageContext.of("main/f1", emptyMap(), emptyMap()));
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_positive_nullStage(FlowEngine flowEngine) {
    var dynamicStage = DynamicStage.of("dynamicStage", ctx -> null);
    var flow = flowForStageSequence("main", dynamicStage);
    executeFlow(flow, flowEngine);
    verifyNoInteractions(simpleStage);
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_negative_failedToCreatedDynamicStage(FlowEngine flowEngine) {
    var dynamicStage = DynamicStage.of("dynamicStage", ctx -> {
      throw new RuntimeException("Error");
    });

    var flow = flowForStageSequence("main", dynamicStage);

    var cause = new RuntimeException("Failed to create a dynamic stage: dynamicStage");
    assertThatThrownBy(() -> executeFlow(flow, flowEngine))
      .isInstanceOf(FlowCancelledException.class)
      .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, dynamicStage)
      .hasCause(cause)
      .extracting(FlowTestUtils::stageResults, list(StageResult.class))
      .containsExactly(stageResult(flow, dynamicStage, CANCELLED, cause));
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_negative_failedToCreatedDynamicStageWithIgnoreErrors(FlowEngine flowEngine) {
    var dynamicStage = DynamicStage.of("dynamicStage", ctx -> {
      throw new RuntimeException("Error");
    });

    var flow = flowForStageSequence("main", IGNORE_ON_ERROR, dynamicStage);

    var cause = new RuntimeException("Failed to create a dynamic stage: dynamicStage");
    assertThatThrownBy(() -> executeFlow(flow, flowEngine))
      .isInstanceOf(FlowExecutionException.class)
      .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, dynamicStage)
      .hasCause(cause)
      .extracting(FlowTestUtils::stageResults, list(StageResult.class))
      .containsExactly(stageResult(flow, dynamicStage, FAILED, cause));
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_negative_canBeSkipped(FlowEngine flowEngine) {
    mockStageNames(simpleStage);
    var exception = new RuntimeException("stage error");
    doThrow(exception).when(simpleStage).execute(any());

    var dynamicStage = DynamicStage.of("dynamicStage", ctx -> simpleStage2);
    var flow = flowForStageSequence("main", simpleStage, dynamicStage);

    assertThatThrownBy(() -> executeFlow(flow, flowEngine))
      .isInstanceOf(FlowCancelledException.class)
      .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, simpleStage)
      .hasCause(exception)
      .extracting(FlowTestUtils::stageResults, list(StageResult.class))
      .containsExactly(
        stageResult(flow, simpleStage, FAILED, exception),
        stageResult(flow, dynamicStage, SKIPPED));

    verify(simpleStage).execute(stageContext(flow));
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_negative_canBeCancelled(FlowEngine flowEngine) {
    mockStageNames(simpleStage, cancellableStage);
    var exception = new RuntimeException("stage error");
    doThrow(exception).when(simpleStage).execute(any());

    var dynamicStage = DynamicStage.of("dynamicStage", ctx -> cancellableStage);
    var flow = flowForStageSequence("main", dynamicStage, simpleStage);

    assertThatThrownBy(() -> executeFlow(flow, flowEngine))
      .isInstanceOf(FlowCancelledException.class)
      .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, simpleStage)
      .hasCause(exception)
      .extracting(FlowTestUtils::stageResults, list(StageResult.class))
      .containsExactly(
        stageResult(flow, dynamicStage, CANCELLED, List.of(
          stageResult(flow, cancellableStage, CANCELLED))),
        stageResult(flow, simpleStage, FAILED, exception));

    verify(simpleStage).execute(stageContext(flow));
  }
}

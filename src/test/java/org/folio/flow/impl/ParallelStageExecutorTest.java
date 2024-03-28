package org.folio.flow.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.folio.flow.api.ParallelStage.parallelStageBuilder;
import static org.folio.flow.model.ExecutionStatus.CANCELLATION_IGNORED;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import org.folio.flow.api.FlowEngine;
import org.folio.flow.api.ParallelStage;
import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;
import org.folio.flow.api.models.CancellableTestStage;
import org.folio.flow.api.models.RecoverableAndCancellableTestStage;
import org.folio.flow.api.models.RecoverableTestStage;
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
public class ParallelStageExecutorTest {

  @Mock private Stage<StageContext> simpleStage;
  @Mock private Stage<StageContext> simpleStage1;
  @Mock private Stage<StageContext> simpleStage2;
  @Mock private RecoverableTestStage recoverableStage;
  @Mock private CancellableTestStage cancellableStage;
  @Mock private CancellableTestStage cancellableStage1;
  @Mock private CancellableTestStage cancellableStage2;
  @Mock private RecoverableAndCancellableTestStage rcStage;

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(simpleStage, recoverableStage, cancellableStage, rcStage);
  }

  @Nested
  @DisplayName("executionStrategy = default")
  class DefaultExecutionStrategy {

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_parallelExecutingStage(FlowEngine flowEngine) {
      mockStageNames(simpleStage, recoverableStage, cancellableStage);
      var flow = FlowTestUtils.flowForStageSequence(ParallelStage.of(simpleStage, recoverableStage, cancellableStage));

      executeFlow(flow, flowEngine);

      verify(simpleStage).execute(stageContext(flow));
      verify(recoverableStage).execute(stageContext(flow));
      verify(cancellableStage).execute(stageContext(flow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_parallelStageByBuilder(FlowEngine flowEngine) {
      mockStageNames(simpleStage, simpleStage2);
      var parallelStage = parallelStageBuilder()
        .id("customId")
        .stage(simpleStage)
        .stage(simpleStage2)
        .build();

      var flow = flowForStageSequence(parallelStage);
      executeFlow(flow, flowEngine);

      verify(simpleStage).execute(stageContext(flow));
      verify(simpleStage2).execute(stageContext(flow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_positive_parallelStageBuilderWithListOfStages(FlowEngine flowEngine) {
      mockStageNames(simpleStage1, simpleStage2);
      var parallelStage = parallelStageBuilder()
        .stages(List.of(simpleStage1, simpleStage2))
        .build();

      var flow = flowForStageSequence(parallelStage);
      executeFlow(flow, flowEngine);

      assertThat(parallelStage.getId()).startsWith("par-stage").hasSize(16);
      verify(simpleStage1).execute(stageContext(flow));
      verify(simpleStage2).execute(stageContext(flow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_parallelStageCanBeCancelled(FlowEngine flowEngine) {
      var exception = new RuntimeException("simpleStage failed");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(simpleStage, simpleStage1, recoverableStage, cancellableStage);

      var parallelStage = ParallelStage.of("par", simpleStage1, recoverableStage, cancellableStage);
      var flow = flowForStageSequence(parallelStage, simpleStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, parallelStage, CANCELLED, List.of(
            stageResult(flow, simpleStage1, CANCELLATION_IGNORED),
            stageResult(flow, recoverableStage, CANCELLATION_IGNORED),
            stageResult(flow, cancellableStage, CANCELLED))),
          stageResult(flow, simpleStage, FAILED, exception));

      var expectedStageContext = stageContext(flow);
      verify(simpleStage).execute(expectedStageContext);
      verify(simpleStage1).execute(expectedStageContext);
      verify(recoverableStage).execute(expectedStageContext);
      verify(cancellableStage).execute(expectedStageContext);
      verify(cancellableStage).cancel(expectedStageContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_allStagesFailed(FlowEngine flowEngine) {
      var exception1 = new RuntimeException("simpleStage failed");
      var exception2 = new RuntimeException("recoverableStage failed");
      var exception3 = new RuntimeException("cancellableStage failed");
      doThrow(exception1).when(simpleStage).execute(any());
      doThrow(exception2).when(recoverableStage).execute(any());
      doThrow(exception2).when(recoverableStage).recover(any());
      doThrow(exception3).when(cancellableStage).execute(any());
      when(cancellableStage.shouldCancelIfFailed(any())).thenReturn(false);
      mockStageNames(simpleStage, recoverableStage, cancellableStage);

      var parallelStage = ParallelStage.of("par", simpleStage, recoverableStage, cancellableStage);
      var flow = flowForStageSequence(parallelStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, parallelStage)
        .hasCause(exception1)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, parallelStage, CANCELLED, exception1, List.of(
            stageResult(flow, simpleStage, FAILED, exception1),
            stageResult(flow, recoverableStage, FAILED, exception2),
            stageResult(flow, cancellableStage, FAILED, exception3))));

      var expectedStageContext = stageContext(flow);
      verify(simpleStage).execute(expectedStageContext);
      verify(recoverableStage).execute(expectedStageContext);
      verify(cancellableStage).execute(expectedStageContext);
      verify(cancellableStage).shouldCancelIfFailed(expectedStageContext);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_allStagesFailedAndIgnored(FlowEngine flowEngine) {
      var exception1 = new RuntimeException("simpleStage1 failed");
      var exception2 = new RuntimeException("simpleStage2 failed");
      doThrow(exception1).when(simpleStage1).execute(any());
      doThrow(exception2).when(simpleStage2).execute(any());
      mockStageNames(simpleStage, simpleStage1, simpleStage2, cancellableStage);

      var parallelStage = parallelStageBuilder()
        .stage(simpleStage1)
        .stage(simpleStage2)
        .shouldCancelIfFailed(false)
        .build();

      var flow = flowForStageSequence(cancellableStage, parallelStage, simpleStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowCancelledException.class)
        .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, parallelStage)
        .hasCause(exception1)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, cancellableStage, CANCELLED),
          stageResult(flow, parallelStage, FAILED, exception1, List.of(
            stageResult(flow, simpleStage1, FAILED, exception1),
            stageResult(flow, simpleStage2, FAILED, exception2))),
          stageResult(flow, simpleStage, SKIPPED));

      var expectedStageContext = stageContext(flow);
      verify(cancellableStage).execute(expectedStageContext);
      verify(cancellableStage).cancel(expectedStageContext);
      verify(simpleStage1).execute(expectedStageContext);
      verify(simpleStage2).execute(expectedStageContext);
    }
  }

  @Nested
  @DisplayName("executionStrategy = IGNORE_ON_ERROR")
  class IgnoreOnErrorExecutionStrategy {

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_oneStageIsFailed(FlowEngine flowEngine) {
      var exception = new RuntimeException("Stage failed");
      mockStageNames(simpleStage, recoverableStage, cancellableStage);
      doThrow(exception).when(simpleStage).execute(any());

      var parallelStage = ParallelStage.of("par", simpleStage, recoverableStage, cancellableStage);
      var flow = flowForStageSequence(IGNORE_ON_ERROR, parallelStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, parallelStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, parallelStage.getId(), FAILED, exception, List.of(
            stageResult(flow, simpleStage, FAILED, exception),
            stageResult(flow, recoverableStage, SUCCESS),
            stageResult(flow, cancellableStage, SUCCESS))));

      verify(simpleStage).execute(stageContext(flow));
      verify(recoverableStage).execute(stageContext(flow));
      verify(cancellableStage).execute(stageContext(flow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_allStagesFailed(FlowEngine flowEngine) {
      var exception1 = new RuntimeException("simpleStage failed");
      var exception2 = new RuntimeException("recoverableStage failed");
      var exception3 = new RuntimeException("cancellableStage failed");
      doThrow(exception1).when(simpleStage).execute(any());
      doThrow(exception2).when(recoverableStage).execute(any());
      doThrow(exception2).when(recoverableStage).recover(any());
      doThrow(exception3).when(cancellableStage).execute(any());
      mockStageNames(simpleStage, recoverableStage, cancellableStage);

      var parallelStage = ParallelStage.of("par", simpleStage, recoverableStage, cancellableStage);
      var flow = flowForStageSequence(IGNORE_ON_ERROR, parallelStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, parallelStage)
        .hasCause(exception1)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, parallelStage, FAILED, exception1, List.of(
            stageResult(flow, simpleStage, FAILED, exception1),
            stageResult(flow, recoverableStage, FAILED, exception2),
            stageResult(flow, cancellableStage, FAILED, exception3))));

      verify(simpleStage).execute(stageContext(flow));
      verify(recoverableStage).execute(stageContext(flow));
      verify(cancellableStage).execute(stageContext(flow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_oneOfTheFlowsInTheParallelFailed(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(simpleStage1).execute(any());
      mockStageNames(simpleStage, simpleStage1, simpleStage2, cancellableStage, cancellableStage1, cancellableStage2);

      var parFlow1 = flowForStageSequence("main/f1", IGNORE_ON_ERROR, simpleStage, cancellableStage);
      var parFlow2 = flowForStageSequence("main/f2", IGNORE_ON_ERROR, simpleStage1, cancellableStage1);
      var parFlow3 = flowForStageSequence("main/f3", IGNORE_ON_ERROR, simpleStage2, cancellableStage2);
      var parallelStage = ParallelStage.of("par-stage", parFlow1, parFlow2, parFlow3);
      var flow = flowForStageSequence("main", IGNORE_ON_ERROR, parallelStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, parallelStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, parallelStage, FAILED, exception, List.of(
            stageResult(flow, parFlow1, SUCCESS, List.of(
              stageResult(parFlow1, simpleStage, SUCCESS),
              stageResult(parFlow1, cancellableStage, SUCCESS))),

            stageResult(flow, parFlow2, FAILED, List.of(
              stageResult(parFlow2, simpleStage1, FAILED, exception),
              stageResult(parFlow2, cancellableStage1, SKIPPED))),

            stageResult(flow, parFlow3.getId(), SUCCESS, List.of(
              stageResult(parFlow3, simpleStage2, SUCCESS),
              stageResult(parFlow3, cancellableStage2, SUCCESS)))
          )));

      verify(simpleStage).execute(stageContext(parFlow1));
      verify(simpleStage1).execute(stageContext(parFlow2));
      verify(simpleStage2).execute(stageContext(parFlow3));
      verify(cancellableStage).execute(stageContext(parFlow1));
      verify(cancellableStage2).execute(stageContext(parFlow3));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_parallelStageSkippedOnFailureSimpleCase(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(simpleStage, simpleStage1, simpleStage2);

      var parallelStage = ParallelStage.of("par-stage", simpleStage1, simpleStage2);
      var flow = flowForStageSequence("main", IGNORE_ON_ERROR, simpleStage, parallelStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, parallelStage, SKIPPED, List.of(
            stageResult(flow, simpleStage1, SKIPPED),
            stageResult(flow, simpleStage2, SKIPPED))));

      verify(simpleStage).execute(stageContext(flow));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
    void execute_negative_parallelStageSkippedOnFailureComplexCase(FlowEngine flowEngine) {
      var exception = new RuntimeException("error");
      doThrow(exception).when(simpleStage).execute(any());
      mockStageNames(simpleStage, simpleStage1, simpleStage2, cancellableStage);

      var parFlow1 = flowForStageSequence("main/f1", IGNORE_ON_ERROR, simpleStage1);
      var parFlow2 = flowForStageSequence("main/f2", IGNORE_ON_ERROR, simpleStage2);
      var parallelStage = ParallelStage.of("par-stage", parFlow1, parFlow2);
      var flow = flowForStageSequence("main", IGNORE_ON_ERROR, simpleStage, parallelStage, cancellableStage);

      assertThatThrownBy(() -> executeFlow(flow, flowEngine))
        .isInstanceOf(FlowExecutionException.class)
        .hasMessage("Failed to execute flow %s, stage '%s' failed", flow, simpleStage)
        .hasCause(exception)
        .extracting(FlowTestUtils::stageResults, list(StageResult.class))
        .containsExactly(
          stageResult(flow, simpleStage, FAILED, exception),
          stageResult(flow, parallelStage, SKIPPED, List.of(
            stageResult(flow, parFlow1, SKIPPED, List.of(stageResult(parFlow1, simpleStage1, SKIPPED))),
            stageResult(flow, parFlow2, SKIPPED, List.of(stageResult(parFlow2, simpleStage2, SKIPPED)))
          )),
          stageResult(flow, cancellableStage, SKIPPED, exception));

      verify(simpleStage).execute(stageContext(flow));
    }
  }
}

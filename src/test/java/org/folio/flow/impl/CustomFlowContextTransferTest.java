package org.folio.flow.impl;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.folio.flow.api.Flow.builder;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.utils.FlowTestUtils.PARAMETERIZED_TEST_NAME;
import static org.folio.flow.utils.FlowTestUtils.executeFlow;
import static org.folio.flow.utils.FlowTestUtils.mockStageNames;
import static org.folio.flow.utils.FlowTestUtils.removeParameterFromContext;
import static org.folio.flow.utils.FlowTestUtils.setParameterInContext;
import static org.folio.flow.utils.FlowTestUtils.stageResult;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.folio.flow.api.Flow;
import org.folio.flow.api.FlowEngine;
import org.folio.flow.api.ParallelStage;
import org.folio.flow.api.StageContext;
import org.folio.flow.api.models.StageWithInheritance;
import org.folio.flow.api.models.TestStageContextWrapper;
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
public class CustomFlowContextTransferTest {

  @Mock private StageWithInheritance stage1;
  @Mock private StageWithInheritance stage2;
  @Mock private StageWithInheritance stage3;
  @Mock private StageWithInheritance stage4;

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(stage1, stage2, stage3, stage4);
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_positive_contextData(FlowEngine flowEngine) {
    doAnswer(inv -> setParameterInContext(inv, "p", "v1")).when(stage1).execute(any());
    doAnswer(inv -> setParameterInContext(inv, "c", "w1")).when(stage1).cancel(any());
    doAnswer(inv -> setParameterInContext(inv, "p", "v2")).when(stage2).execute(any());
    doAnswer(inv -> setParameterInContext(inv, "c", "w2")).when(stage2).cancel(any());
    doAnswer(inv -> removeParameterFromContext(inv, "p")).when(stage3).execute(any());

    doAnswer(inv -> {
      setParameterInContext(inv, "c", "w3");
      setParameterInContext(inv, "p", "v4");
      return null;
    }).when(stage3).cancel(any());

    var exception = new RuntimeException("stage error");
    doThrow(exception).when(stage4).execute(any());
    doThrow(exception).when(stage4).recover(any());
    when(stage4.shouldCancelIfFailed(any())).thenReturn(false);
    mockStageNames(stage1, stage2, stage3, stage4);

    var flowParams = Map.of("f1", "v1", "f2", "v2");
    var flow = builder()
      .flowParameters(flowParams)
      .stage(stage1)
      .stage(stage2)
      .stage(stage3)
      .stage(stage4)
      .build();

    assertThatThrownBy(() -> executeFlow(flow, flowEngine))
      .isInstanceOf(FlowCancelledException.class)
      .hasMessage("Flow %s is cancelled, stage '%s' failed", flow, stage4)
      .extracting(FlowTestUtils::stageResults, list(StageResult.class))
      .containsExactly(
        stageResult(flow, stage1, CANCELLED),
        stageResult(flow, stage2, CANCELLED),
        stageResult(flow, stage3, CANCELLED),
        stageResult(flow, stage4, FAILED, exception));

    var flowId = flow.getId();
    verify(stage1).execute(wrapper(flowId, flowParams, Map.of("p", "v1")));
    verify(stage1).cancel(wrapper(flowId, flowParams, Map.of("p", "v4", "c", "w1")));

    verify(stage2).execute(wrapper(flowId, flowParams, Map.of("p", "v2")));
    verify(stage2).cancel(wrapper(flowId, flowParams, Map.of("p", "v4", "c", "w2")));

    verify(stage3).execute(wrapper(flowId, flowParams, emptyMap()));
    verify(stage3).cancel(wrapper(flowId, flowParams, Map.of("p", "v4", "c", "w3")));

    verify(stage4).execute(wrapper(flowId, flowParams, emptyMap()));
    verify(stage4).recover(wrapper(flowId, flowParams, emptyMap()));
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_positive_cancellationFlowParametersCheck(FlowEngine flowEngine) {
    mockStageNames(stage1, stage2, stage3, stage4);
    var mainFlowParams = Map.of("mf", "main-value");
    var subflow1Params = Map.of("sf1", "value1", "mf", "overwritten");
    var subflow2Params = Map.of("sf2", "test");
    var exception = new RuntimeException("stage error");
    doThrow(exception).when(stage1).execute(any());
    doThrow(exception).when(stage2).execute(any());
    doThrow(exception).when(stage3).execute(any());
    doThrow(exception).when(stage4).execute(any());
    doThrow(exception).when(stage4).recover(any());
    when(stage4.shouldCancelIfFailed(any())).thenReturn(true);

    var subflow1 = Flow.builder().id("main/f1").stage(stage2).flowParameters(subflow1Params).build();
    var subflow2 = Flow.builder().id("main/f2").stage(stage3).flowParameters(subflow2Params).build();
    var mainFlow = Flow.builder().id("main").stage(stage1).stage(subflow1).stage(subflow2).stage(stage4)
      .flowParameters(mainFlowParams).build();

    assertThatThrownBy(() -> executeFlow(mainFlow, flowEngine))
      .isInstanceOf(FlowCancelledException.class)
      .hasMessage("Flow %s is cancelled, stage '%s' failed", mainFlow, stage4)
      .extracting(FlowTestUtils::stageResults, list(StageResult.class))
      .containsExactly(
        stageResult(mainFlow, stage1, CANCELLED),
        stageResult(mainFlow, subflow1, CANCELLED, List.of(stageResult(subflow1, stage2, CANCELLED))),
        stageResult(mainFlow, subflow2, CANCELLED, List.of(stageResult(subflow2, stage3, CANCELLED))),
        stageResult(mainFlow, stage4, CANCELLED, exception));

    var mainFlowExpectedContext = wrapper(mainFlow.getId(), mainFlowParams, emptyMap());
    verify(stage1).execute(mainFlowExpectedContext);
    verify(stage1).recover(mainFlowExpectedContext);
    verify(stage1).cancel(mainFlowExpectedContext);

    var subflow1ExpectedParams = Map.of("sf1", "value1", "mf", "overwritten");
    var subflow1ExpectedContext = wrapper(subflow1.getId(), subflow1ExpectedParams, emptyMap());
    verify(stage2).execute(subflow1ExpectedContext);
    verify(stage2).recover(subflow1ExpectedContext);
    verify(stage2).cancel(subflow1ExpectedContext);

    var subflow2ExpectedParams = Map.of("mf", "main-value", "sf2", "test");
    var subflow2ExpectedContext = wrapper(subflow2.getId(), subflow2ExpectedParams, emptyMap());
    verify(stage3).execute(subflow2ExpectedContext);
    verify(stage3).recover(subflow2ExpectedContext);
    verify(stage3).cancel(subflow2ExpectedContext);

    verify(stage4).execute(mainFlowExpectedContext);
    verify(stage4).recover(mainFlowExpectedContext);
    verify(stage4).cancel(mainFlowExpectedContext);
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_positive_parallelFlowCheck(FlowEngine flowEngine) {
    mockStageNames(stage1, stage2, stage3, stage4);
    var exception = new RuntimeException();
    doThrow(exception).when(stage4).execute(any());
    doThrow(exception).when(stage4).recover(any());
    when(stage4.shouldCancelIfFailed(any())).thenReturn(false);

    mockStageNames(stage1, stage2, stage3, stage4);
    var mainFlowParams = Map.of("mf", "main-value");
    var subflow1Params = Map.of("sf1", "value1", "mf", "overwritten");
    var subflow2Params = Map.of("sf2", "test");
    var subflow3Params = Map.of("sf3", "test2", "mf", "test3");

    var subflow1 = Flow.builder().id("main/f1").stage(stage2).flowParameters(subflow1Params).build();
    var subflow2 = Flow.builder().id("main/f2").stage(stage3).flowParameters(subflow2Params).build();
    var subflow3 = Flow.builder().id("main/f3").stage(stage4).flowParameters(subflow3Params).build();
    var parallelStage = ParallelStage.of(subflow1, subflow2);

    var mainFlow = Flow.builder()
      .id("main")
      .stage(stage1)
      .stage(parallelStage)
      .stage(subflow3)
      .flowParameters(mainFlowParams)
      .build();

    assertThatThrownBy(() -> executeFlow(mainFlow, flowEngine))
      .isInstanceOf(FlowCancelledException.class)
      .hasMessage("Flow %s is cancelled, stage '%s' failed", mainFlow, subflow3)
      .hasCause(exception)
      .extracting(FlowTestUtils::stageResults, list(StageResult.class))
      .containsExactly(
        stageResult(mainFlow, stage1, CANCELLED),
        stageResult(mainFlow, parallelStage, CANCELLED, List.of(
          stageResult(mainFlow, subflow1, CANCELLED, List.of(stageResult(subflow1, stage2, CANCELLED))),
          stageResult(mainFlow, subflow2, CANCELLED, List.of(stageResult(subflow2, stage3, CANCELLED))))),
        stageResult(mainFlow, subflow3, CANCELLED, exception, List.of(stageResult(subflow3, stage4, FAILED))));

    var mainFlowExpectedContext = wrapper(mainFlow.getId(), mainFlowParams, emptyMap());
    verify(stage1).execute(mainFlowExpectedContext);
    verify(stage1).cancel(mainFlowExpectedContext);

    var subflow1ExpectedContext = wrapper(subflow1.getId(), subflow1Params, emptyMap());
    verify(stage2).execute(subflow1ExpectedContext);
    verify(stage2).cancel(subflow1ExpectedContext);

    var subflow2ExpectedParams = Map.of("mf", "main-value", "sf2", "test");
    var subflow2ExpectedContext = wrapper(subflow2.getId(), subflow2ExpectedParams, emptyMap());
    verify(stage3).execute(subflow2ExpectedContext);
    verify(stage3).cancel(subflow2ExpectedContext);

    var subflow3ExpectedContext = wrapper(subflow3.getId(), subflow3Params, emptyMap());
    verify(stage4).execute(subflow3ExpectedContext);
    verify(stage4).recover(subflow3ExpectedContext);
  }

  private static TestStageContextWrapper wrapper(Object flowId, Map<?, ?> flowParameters, Map<?, ?> data) {
    return new TestStageContextWrapper(StageContext.of(flowId, flowParameters, data));
  }
}

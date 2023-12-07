package org.folio.flow.api;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.folio.flow.api.ParallelStage.parallelStageBuilder;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.folio.flow.utils.FlowTestUtils.SINGLE_THREAD_FLOW_ENGINE;
import static org.folio.flow.utils.FlowTestUtils.executeFlow;
import static org.folio.flow.utils.FlowTestUtils.flowForStageSequence;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;
import org.folio.flow.impl.StageExecutor;
import org.folio.flow.model.StageExecutionResult;
import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class ParallelStageTest {

  @Test
  void builder_negative_idIsNull() {
    var builder = parallelStageBuilder();
    assertThatThrownBy(() -> builder.id(null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Parallel stage id must not be null");
  }

  @Test
  void builder_negative_stageIsNull() {
    var builder = parallelStageBuilder();
    assertThatThrownBy(() -> builder.stage((Stage) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Stage must not be null");
  }

  @Test
  void builder_negative_stageExecutorIsNull() {
    var builder = parallelStageBuilder();
    assertThatThrownBy(() -> builder.stage((StageExecutor) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Stage must not be null");
  }

  @Test
  void builder_negative_listOfStagesIsNull() {
    var builder = parallelStageBuilder();
    assertThatThrownBy(() -> builder.stages(null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Stages must not be null");
  }

  @Test
  void execute_positive_customStageExecutorCanBeUsed() {
    var customStageExecutor = mock(StageExecutor.class);
    var stageExecutorName = "customStageExecutor";
    var flowId = "main";
    var stageContext = StageContext.of(flowId, emptyMap(), emptyMap());
    var expectedStageResult = StageExecutionResult.stageResult(stageExecutorName, stageContext, SUCCESS);

    when(customStageExecutor.getStageId()).thenReturn(stageExecutorName);
    when(customStageExecutor.execute(any(), any())).thenReturn(completedFuture(expectedStageResult));

    var parallelStage = parallelStageBuilder("par-id").stage(customStageExecutor).build();
    var flow = flowForStageSequence(flowId, parallelStage);
    executeFlow(flow, SINGLE_THREAD_FLOW_ENGINE);

    assertThat(parallelStage.getId()).isEqualTo("par-id");
    verify(customStageExecutor).execute(any(), any(Executor.class));
    verifyNoMoreInteractions(customStageExecutor);
  }

  @Test
  void execute_negative_directCall() {
    var parallelStage = parallelStageBuilder().build();
    var context = StageContext.of("flow-id", emptyMap(), emptyMap());
    assertThatThrownBy(() -> parallelStage.execute(context))
      .isInstanceOf(UnsupportedOperationException.class)
      .hasMessage("method execute(StageContext context) is not supported for ParallelStage");
  }
}

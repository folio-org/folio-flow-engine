package org.folio.flow.model;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.mockito.Mockito.when;

import java.util.List;
import org.folio.flow.api.StageContext;
import org.folio.flow.impl.FlowExecutor;
import org.folio.flow.impl.StageExecutor;
import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class StageResultHolderTest {

  @Mock private StageExecutor stageExecutor;
  @Mock private FlowExecutor flowExecutor;

  @Test
  void isFlow_positive_regularStage() {
    var result = StageExecutionResult.builder()
      .stageName("test-stage")
      .context(StageContext.of("flow-id", emptyMap(), emptyMap()))
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var holder = new StageResultHolder(result, stageExecutor, false);

    assertThat(holder.isFlow()).isFalse();
  }

  @Test
  void isFlow_positive_flowStage() {
    var result = StageExecutionResult.builder()
      .stageName("test-flow")
      .context(StageContext.of("flow-id", emptyMap(), emptyMap()))
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var holder = new StageResultHolder(result, flowExecutor, false);

    assertThat(holder.isFlow()).isTrue();
  }

  @Test
  void getStageId_positive() {
    var result = StageExecutionResult.builder()
      .stageName("test-stage")
      .context(StageContext.of("flow-id", emptyMap(), emptyMap()))
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    when(stageExecutor.getStageId()).thenReturn("my-stage-id");

    var holder = new StageResultHolder(result, stageExecutor, false);

    assertThat(holder.getStageId()).isEqualTo("my-stage-id");
  }

  @Test
  void getError_positive_withError() {
    var error = new RuntimeException("test error");
    var result = StageExecutionResult.builder()
      .stageName("test-stage")
      .context(StageContext.of("flow-id", emptyMap(), emptyMap()))
      .status(FAILED)
      .error(error)
      .executedStages(List.of())
      .build();

    var holder = new StageResultHolder(result, stageExecutor, false);

    assertThat(holder.getError()).isEqualTo(error);
  }

  @Test
  void getError_positive_withoutError() {
    var result = StageExecutionResult.builder()
      .stageName("test-stage")
      .context(StageContext.of("flow-id", emptyMap(), emptyMap()))
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var holder = new StageResultHolder(result, stageExecutor, false);

    assertThat(holder.getError()).isNull();
  }

  @Test
  void getStatus_positive() {
    var result = StageExecutionResult.builder()
      .stageName("test-stage")
      .context(StageContext.of("flow-id", emptyMap(), emptyMap()))
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var holder = new StageResultHolder(result, stageExecutor, false);

    assertThat(holder.getStatus()).isEqualTo(SUCCESS);
  }

  @Test
  void getResult_positive() {
    var result = StageExecutionResult.builder()
      .stageName("test-stage")
      .context(StageContext.of("flow-id", emptyMap(), emptyMap()))
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var holder = new StageResultHolder(result, stageExecutor, false);

    assertThat(holder.getResult()).isEqualTo(result);
  }

  @Test
  void getStage_positive() {
    var result = StageExecutionResult.builder()
      .stageName("test-stage")
      .context(StageContext.of("flow-id", emptyMap(), emptyMap()))
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var holder = new StageResultHolder(result, stageExecutor, false);

    assertThat(holder.getStage()).isEqualTo(stageExecutor);
  }

  @Test
  void isCancel_positive() {
    var result = StageExecutionResult.builder()
      .stageName("test-stage")
      .context(StageContext.of("flow-id", emptyMap(), emptyMap()))
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var holderWithCancel = new StageResultHolder(result, stageExecutor, true);
    var holderWithoutCancel = new StageResultHolder(result, stageExecutor, false);

    assertThat(holderWithCancel.isCancel()).isTrue();
    assertThat(holderWithoutCancel.isCancel()).isFalse();
  }
}

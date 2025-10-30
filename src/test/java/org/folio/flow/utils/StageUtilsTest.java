package org.folio.flow.utils;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.SKIPPED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import org.folio.flow.api.StageContext;
import org.folio.flow.impl.StageExecutor;
import org.folio.flow.model.StageExecutionResult;
import org.folio.flow.model.StageResultHolder;
import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class StageUtilsTest {

  @Mock private StageExecutor stageExecutor;

  @Test
  void cancelStageAsync_positive_successfulStage() {
    var flowId = "test-flow";
    var context = StageContext.of("original-flow", emptyMap(), emptyMap());
    var upstreamResult = StageExecutionResult.builder()
      .stageName("upstream")
      .context(context)
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var stageResult = StageExecutionResult.builder()
      .stageName("stage")
      .context(context)
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var cancelResult = StageExecutionResult.builder()
      .stageName("stage")
      .context(context.withFlowId(flowId))
      .status(CANCELLED)
      .executedStages(List.of())
      .build();

    var holder = new StageResultHolder(stageResult, stageExecutor, false);

    when(stageExecutor.cancel(any(), any())).thenReturn(CompletableFuture.completedFuture(cancelResult));

    var result = StageUtils.cancelStageAsync(flowId, holder, upstreamResult, ForkJoinPool.commonPool());

    assertThat(result).isCompleted();
    assertThat(result.join().getStatus()).isEqualTo(CANCELLED);
    assertThat(result.join().getContext().flowId()).isEqualTo(flowId);
    verify(stageExecutor).cancel(any(), any());
  }

  @Test
  void cancelStageAsync_positive_failedStageShouldCancel() {
    var flowId = "test-flow";
    var context = StageContext.of("original-flow", emptyMap(), emptyMap());
    var error = new RuntimeException("stage error");
    var upstreamResult = StageExecutionResult.builder()
      .stageName("upstream")
      .context(context)
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var stageResult = StageExecutionResult.builder()
      .stageName("stage")
      .context(context)
      .status(FAILED)
      .error(error)
      .executedStages(List.of())
      .build();

    var cancelResult = StageExecutionResult.builder()
      .stageName("stage")
      .context(context.withFlowId(flowId))
      .status(CANCELLED)
      .executedStages(List.of())
      .build();

    var holder = new StageResultHolder(stageResult, stageExecutor, false);

    when(stageExecutor.shouldCancelIfFailed(any())).thenReturn(true);
    when(stageExecutor.cancel(any(), any())).thenReturn(CompletableFuture.completedFuture(cancelResult));

    var result = StageUtils.cancelStageAsync(flowId, holder, upstreamResult, ForkJoinPool.commonPool());

    assertThat(result).isCompleted();
    verify(stageExecutor).shouldCancelIfFailed(any());
    verify(stageExecutor).cancel(any(), any());
  }

  @Test
  void cancelStageAsync_positive_failedStageShouldNotCancel() {
    var flowId = "test-flow";
    var context = StageContext.of("original-flow", emptyMap(), emptyMap());
    var error = new RuntimeException("stage error");
    var upstreamResult = StageExecutionResult.builder()
      .stageName("upstream")
      .context(context)
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var stageResult = StageExecutionResult.builder()
      .stageName("stage")
      .context(context)
      .status(FAILED)
      .error(error)
      .executedStages(List.of())
      .build();

    var holder = new StageResultHolder(stageResult, stageExecutor, false);

    when(stageExecutor.shouldCancelIfFailed(any())).thenReturn(false);
    when(stageExecutor.getStageId()).thenReturn("stage");

    var result = StageUtils.cancelStageAsync(flowId, holder, upstreamResult, ForkJoinPool.commonPool());

    assertThat(result).isCompleted();
    assertThat(result.join().getStatus()).isEqualTo(FAILED);
    assertThat(result.join().getError()).isEqualTo(error);
    verify(stageExecutor).shouldCancelIfFailed(any());
    verify(stageExecutor, never()).cancel(any(), any());
  }

  @Test
  void cancelStageAsync_positive_skippedStage() {
    var flowId = "test-flow";
    var context = StageContext.of("original-flow", emptyMap(), emptyMap());
    var upstreamResult = StageExecutionResult.builder()
      .stageName("upstream")
      .context(context)
      .status(SUCCESS)
      .executedStages(List.of())
      .build();

    var stageResult = StageExecutionResult.builder()
      .stageName("stage")
      .context(context)
      .status(SKIPPED)
      .executedStages(List.of())
      .build();

    var holder = new StageResultHolder(stageResult, stageExecutor, false);

    when(stageExecutor.getStageId()).thenReturn("stage");

    var result = StageUtils.cancelStageAsync(flowId, holder, upstreamResult, ForkJoinPool.commonPool());

    assertThat(result).isCompleted();
    assertThat(result.join().getStatus()).isEqualTo(SKIPPED);
    verify(stageExecutor, never()).cancel(any(), any());
  }
}

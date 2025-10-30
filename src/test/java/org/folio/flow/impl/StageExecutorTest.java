package org.folio.flow.impl;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.folio.flow.api.StageContext;
import org.folio.flow.model.StageExecutionResult;
import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.Test;

@UnitTest
class StageExecutorTest {

  @Test
  void getStageType_positive_defaultImplementation() {
    var executor = new TestStageExecutor();
    assertThat(executor.getStageType()).isEqualTo("Stage");
  }

  @Test
  void getStageId_positive_defaultImplementation() {
    var executor = new TestStageExecutor();
    assertThat(executor.getStageId()).isEqualTo(executor.toString());
  }

  @Test
  void shouldCancelIfFailed_positive_defaultImplementation() {
    var executor = new TestStageExecutor();
    var context = StageContext.of("flow-id", emptyMap(), emptyMap());
    assertThat(executor.shouldCancelIfFailed(context)).isTrue();
  }

  private static final class TestStageExecutor implements StageExecutor {

    @Override
    public CompletableFuture<StageExecutionResult> execute(
      StageExecutionResult upstreamResult, Executor executor) {
      return CompletableFuture.completedFuture(upstreamResult);
    }

    @Override
    public CompletableFuture<StageExecutionResult> skip(
      StageExecutionResult upstreamResult, Executor executor) {
      return CompletableFuture.completedFuture(upstreamResult);
    }

    @Override
    public CompletableFuture<StageExecutionResult> cancel(
      StageExecutionResult upstreamResult, Executor executor) {
      return CompletableFuture.completedFuture(upstreamResult);
    }
  }
}

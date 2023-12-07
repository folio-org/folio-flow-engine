package org.folio.flow.impl;

import static java.lang.Thread.currentThread;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.awaitility.Awaitility.waitAtMost;
import static org.awaitility.Durations.FIVE_HUNDRED_MILLISECONDS;
import static org.awaitility.Durations.ONE_HUNDRED_MILLISECONDS;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.folio.flow.model.ExecutionStatus.UNKNOWN;
import static org.folio.flow.model.FlowExecutionStrategy.IGNORE_ON_ERROR;
import static org.folio.flow.utils.FlowTestUtils.FJP_FLOW_ENGINE;
import static org.folio.flow.utils.FlowTestUtils.PARAMETERIZED_TEST_NAME;
import static org.folio.flow.utils.FlowTestUtils.awaitFor;
import static org.folio.flow.utils.FlowTestUtils.executeFlow;
import static org.folio.flow.utils.FlowTestUtils.flowForStageSequence;
import static org.folio.flow.utils.FlowTestUtils.mockStageNames;
import static org.folio.flow.utils.FlowTestUtils.stageContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.folio.flow.api.Flow;
import org.folio.flow.api.FlowEngine;
import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;
import org.folio.flow.api.models.CancellableTestStage;
import org.folio.flow.exception.FlowExecutionException;
import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class FlowEngineImplTest {

  @Mock private Stage simpleStage;
  @Mock private CancellableTestStage cancellableStage;

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(simpleStage, cancellableStage);
  }

  @Test
  void builder_nullExecutor() {
    var builder = FlowEngine.builder();
    assertThatThrownBy(() -> builder.executor(null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Flow engine executor must not be null");
  }

  @Test
  void builder_nullName() {
    var builder = FlowEngine.builder();
    assertThatThrownBy(() -> builder.name(null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Flow engine name must not be null");
  }

  @Test
  void builder_invalidLastExecutionCacheSize() {
    var builder = FlowEngine.builder();
    assertThatThrownBy(() -> builder.lastExecutionsStatusCacheSize(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("lastExecutionsStatusCacheSize must be more than 0");
  }

  @Test
  void builder_nullExecutionTimeout() {
    var builder = FlowEngine.builder();
    assertThatThrownBy(() -> builder.executionTimeout(null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("executionTimeout must be not null");
  }

  @Test
  void builder_nullReportCreator() {
    var builder = FlowEngine.builder();
    assertThatThrownBy(() -> builder.stageReportProvider(null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("stageReportProvider must not be null");
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  void execute_negative_emptyFlow(FlowEngine flowEngine) {
    var flow = Flow.builder().build();
    executeFlow(flow, flowEngine);
    assertThat(flowEngine.getFlowStatus(flow)).isEqualTo(SUCCESS);
  }

  @Test
  void execute_positive_interruptedException() {
    var countDownLatch = new CountDownLatch(1);
    var testStage = new Stage() {
      @Override
      public void execute(StageContext context) {
        try {
          countDownLatch.await();
        } catch (InterruptedException e) {
          currentThread().interrupt();
        }
      }
    };

    var simpleStageSpy = spy(testStage);
    doCallRealMethod().when(simpleStageSpy).execute(any());
    var flow = flowForStageSequence(IGNORE_ON_ERROR, simpleStageSpy);

    var flowEngine = FJP_FLOW_ENGINE;
    var flowExecutor = new Thread(() -> flowEngine.execute(flow));
    flowExecutor.start();
    flowExecutor.interrupt();

    waitAtMost(FIVE_HUNDRED_MILLISECONDS).untilAsserted(() -> {
      assertThat(flowEngine.getFlowStatus(flow)).isEqualTo(FAILED);
      verify(simpleStageSpy).execute(stageContext(flow));
    });

    countDownLatch.countDown();
  }

  @Test
  void execute_positive_manyFlowEngines() {
    var flowEngine = FlowEngine.builder()
      .printFlowResult(false)
      .executionTimeout(FIVE_HUNDRED_MILLISECONDS)
      .executor(Executors.newSingleThreadExecutor())
      .lastExecutionsStatusCacheSize(10)
      .build();

    mockStageNames(simpleStage);
    var executedFlows = new ArrayList<Flow>();

    for (int i = 0; i < 15; i++) {
      var flow = Flow.builder().stage(simpleStage).build();
      flowEngine.execute(flow);
      executedFlows.add(flow);
      verify(simpleStage).execute(stageContext(flow));
    }

    for (int i = 0; i < 5; i++) {
      assertThat(flowEngine.getFlowStatus(executedFlows.get(i))).isEqualTo(UNKNOWN);
    }

    for (int i = 6; i < 15; i++) {
      assertThat(flowEngine.getFlowStatus(executedFlows.get(i))).isEqualTo(SUCCESS);
    }
  }

  @Test
  void execute_positive_timeoutException() {
    var testStage = new Stage() {
      @Override
      public void execute(StageContext context) {
        awaitFor(FIVE_HUNDRED_MILLISECONDS);
      }
    };

    var flow = flowForStageSequence(IGNORE_ON_ERROR, testStage);

    assertThatThrownBy(() -> FJP_FLOW_ENGINE.execute(flow, ONE_HUNDRED_MILLISECONDS))
      .isInstanceOf(FlowExecutionException.class)
      .hasMessage("Failed to execute flow %s", flow)
      .hasCauseInstanceOf(TimeoutException.class);

    waitAtMost(FIVE_HUNDRED_MILLISECONDS).untilAsserted(() ->
      assertThat(FJP_FLOW_ENGINE.getFlowStatus(flow)).isEqualTo(FAILED));
  }

  @Test
  void execute_positive_flowIsExecuting() {
    var flow = flowForStageSequence(IGNORE_ON_ERROR, new AwaitingStage());

    var executor = Executors.newSingleThreadExecutor();
    var flowEngine = FlowEngine.builder().executor(executor).build();
    flowEngine.executeAsync(flow);

    assertThatThrownBy(() -> flowEngine.execute(flow))
      .isInstanceOf(FlowExecutionException.class)
      .hasMessage("Failed to execute flow: %s", flow)
      .hasCauseInstanceOf(IllegalStateException.class)
      .hasRootCauseMessage("Flow %s already executing", flow.getId());

    waitAtMost(FIVE_HUNDRED_MILLISECONDS).untilAsserted(() ->
      assertThat(flowEngine.getFlowStatus(flow)).isEqualTo(SUCCESS));
  }

  private static final class AwaitingStage implements Stage {

    @Override
    public void execute(StageContext context) {
      awaitFor(Duration.ofMillis(250));
    }

    @Override
    public String getId() {
      return "awaiting stage";
    }
  }
}

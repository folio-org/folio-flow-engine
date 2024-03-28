package org.folio.flow.api;

import static java.lang.Thread.currentThread;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.awaitility.Awaitility.waitAtMost;
import static org.awaitility.Durations.FIVE_HUNDRED_MILLISECONDS;
import static org.awaitility.Durations.ONE_HUNDRED_MILLISECONDS;
import static org.awaitility.Durations.TWO_HUNDRED_MILLISECONDS;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;
import static org.folio.flow.model.FlowExecutionStrategy.IGNORE_ON_ERROR;
import static org.folio.flow.utils.FlowTestUtils.FJP_FLOW_ENGINE;
import static org.folio.flow.utils.FlowTestUtils.PARAMETERIZED_TEST_NAME;
import static org.folio.flow.utils.FlowTestUtils.SINGLE_THREAD_FLOW_ENGINE;
import static org.folio.flow.utils.FlowTestUtils.awaitFor;
import static org.folio.flow.utils.FlowTestUtils.executeFlow;
import static org.folio.flow.utils.FlowTestUtils.flowForStageSequence;
import static org.folio.flow.utils.FlowTestUtils.stageContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
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
class FlowCornerCasesTest {

  @Mock private Stage<StageContext> simpleStage;
  @Mock private CancellableTestStage cancellableStage;

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(simpleStage, cancellableStage);
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
    var testStage = new Stage<>() {
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

    var flowExecutor = new Thread(() -> FJP_FLOW_ENGINE.execute(flow));
    flowExecutor.start();
    flowExecutor.interrupt();

    waitAtMost(FIVE_HUNDRED_MILLISECONDS).untilAsserted(() -> {
      assertThat(FJP_FLOW_ENGINE.getFlowStatus(flow)).isEqualTo(FAILED);
      verify(simpleStageSpy).execute(stageContext(flow));
    });

    countDownLatch.countDown();
  }

  @Test
  void execute_positive_timeoutException() {
    var testStage = new Stage<>() {
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
    var flowEngine = SINGLE_THREAD_FLOW_ENGINE;
    flowEngine.executeAsync(flow);

    assertThatThrownBy(() -> flowEngine.execute(flow, TWO_HUNDRED_MILLISECONDS))
      .isInstanceOf(FlowExecutionException.class)
      .hasMessage("Failed to execute flow: %s", flow)
      .hasCauseInstanceOf(IllegalStateException.class)
      .hasRootCauseMessage("Flow %s already executing", flow.getId());

    waitAtMost(FIVE_HUNDRED_MILLISECONDS).untilAsserted(() ->
      assertThat(flowEngine.getFlowStatus(flow)).isEqualTo(SUCCESS));
  }

  private static final class AwaitingStage implements Stage<StageContext> {

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

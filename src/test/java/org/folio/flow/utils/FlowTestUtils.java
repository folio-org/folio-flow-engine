package org.folio.flow.utils;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Durations.FIVE_HUNDRED_MILLISECONDS;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.folio.flow.utils.FlowUtils.FLOW_ENGINE_LOGGER_NAME;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.awaitility.Awaitility;
import org.folio.flow.api.Flow;
import org.folio.flow.api.FlowEngine;
import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;
import org.folio.flow.exception.StageExecutionException;
import org.folio.flow.model.ExecutionStatus;
import org.folio.flow.model.FlowExecutionStrategy;
import org.folio.flow.model.StageResult;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.invocation.InvocationOnMock;

@Log4j2(topic = FLOW_ENGINE_LOGGER_NAME)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FlowTestUtils {

  public static final String PARAMETERIZED_TEST_NAME = "[{index}] flowEngine = {0}";

  public static final FlowEngine FJP_FLOW_ENGINE = FlowEngine.builder()
    .printFlowResult(false)
    .executionTimeout(FIVE_SECONDS)
    .name("default-flow-engine")
    .lastExecutionsStatusCacheSize(10)
    .build();

  public static final FlowEngine SINGLE_THREAD_FLOW_ENGINE = FlowEngine.builder()
    .printFlowResult(true)
    .executor(newSingleThreadExecutor())
    .executionTimeout(FIVE_SECONDS)
    .name("single-thread-flow-engine")
    .stageReportProvider(StageReportProvider.builder()
      .template("[${flowId}] -> [${stage}] is ${statusName}")
      .errorTemplate("[${flowId}] -> [${stage}] is ${statusName} with [${errorType}]: ${shortErrorMessage}")
      .build())
    .lastExecutionsStatusCacheSize(10)
    .build();

  public static Flow flowForStageSequence(Stage... stages) {
    var flowBuilder = Flow.builder();
    for (var stage : stages) {
      flowBuilder.stage(stage);
    }

    return flowBuilder.build();
  }

  public static Flow flowForStageSequence(FlowExecutionStrategy strategy, Stage... stages) {
    var flowBuilder = Flow.builder().executionStrategy(strategy);
    for (var stage : stages) {
      flowBuilder.stage(stage);
    }

    return flowBuilder.build();
  }

  public static Flow flowForStageSequence(String id, Stage... stages) {
    var flowBuilder = Flow.builder().id(id);
    for (var stage : stages) {
      flowBuilder.stage(stage);
    }

    return flowBuilder.build();
  }

  public static Flow flowForStageSequence(String id, FlowExecutionStrategy strategy, Stage... stages) {
    var flowBuilder = Flow.builder().id(id).executionStrategy(strategy);

    for (var stage : stages) {
      flowBuilder.stage(stage);
    }

    return flowBuilder.build();
  }

  public static Stream<Arguments> flowEnginesDataSource() {
    return Stream.of(
      arguments(FJP_FLOW_ENGINE),
      arguments(SINGLE_THREAD_FLOW_ENGINE)
    );
  }

  public static StageContext stageContext(Stage root) {
    return StageContext.of(root.getId(), emptyMap(), emptyMap());
  }

  public static StageContext stageContext(Stage root, Map<?, ?> flowParameters) {
    return StageContext.of(root.getId(), flowParameters, emptyMap());
  }

  public static StageResult stageResult(Stage root, String name, ExecutionStatus status) {
    return stageResult(root, name, status, null, emptyList());
  }

  public static StageResult stageResult(Stage root, Stage stage, ExecutionStatus status) {
    return stageResult(root, stage.getId(), status, null, emptyList());
  }

  public static StageResult stageResult(Stage root, String name, ExecutionStatus status, Exception error) {
    return stageResult(root, name, status, error, emptyList());
  }

  public static StageResult stageResult(Stage root, Stage stage, ExecutionStatus status, Exception error) {
    return stageResult(root, stage.getId(), status, error, emptyList());
  }

  public static StageResult stageResult(Stage root, String name, ExecutionStatus status, List<StageResult> results) {
    return stageResult(root, name, status, null, results);
  }

  public static StageResult stageResult(Stage root, Stage stage, ExecutionStatus status, List<StageResult> results) {
    return stageResult(root, stage.getId(), status, null, results);
  }

  public static StageResult stageResult(Stage root, Stage stage,
    ExecutionStatus status, Exception error, List<StageResult> results) {
    return stageResult(root, stage.getId(), status, error, results);
  }

  public static StageResult stageResult(Stage root, String name,
    ExecutionStatus status, Exception error, List<StageResult> subResults) {
    return StageResult.builder()
      .flowId(root.getId())
      .stageName(name)
      .status(status)
      .error(error)
      .subStageResults(subResults)
      .build();
  }

  @SneakyThrows
  public static List<StageResult> stageResults(Throwable error) {
    return ((StageExecutionException) error).getStageResults();
  }

  public static void executeFlow(Flow flow, FlowEngine flowEngine) {
    flowEngine.execute(flow, FIVE_HUNDRED_MILLISECONDS);
  }

  public static Object setParameterInContext(InvocationOnMock invocation, Object key, Object value) {
    var stageContext = invocation.<StageContext>getArgument(0);
    stageContext.put(key, value);
    return null;
  }

  public static void mockStageNames(Stage... stages) {
    for (var stage : stages) {
      var stringValue = stage.toString();
      when(stage.getId()).thenReturn(stringValue);
    }
  }

  /**
   * Sonar friendly Thread.sleep(millis) implementation
   *
   * @param duration - duration to await.
   */
  @SuppressWarnings("SameParameterValue")
  public static void awaitFor(Duration duration) {
    var sampleResult = Optional.of(1);
    Awaitility.await()
      .pollInSameThread()
      .atMost(duration.plus(Duration.ofMillis(250)))
      .pollDelay(duration)
      .untilAsserted(() -> assertThat(sampleResult).isPresent());
  }
}

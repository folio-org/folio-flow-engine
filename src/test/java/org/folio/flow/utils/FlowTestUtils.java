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
import org.folio.flow.api.DynamicStage;
import org.folio.flow.api.Flow;
import org.folio.flow.api.FlowEngine;
import org.folio.flow.api.ParallelStage;
import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;
import org.folio.flow.exception.StageExecutionException;
import org.folio.flow.model.ExecutionStatus;
import org.folio.flow.model.FlowExecutionStrategy;
import org.folio.flow.model.StageExecutionResult;
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
      .template("[${flowId}] -> [${stageId}] is ${statusName}")
      .errorTemplate("[${flowId}] -> [${stageId}] is ${statusName} with [${errorType}]: ${shortErrorMessage}")
      .build())
    .lastExecutionsStatusCacheSize(10)
    .build();

  @SafeVarargs
  public static Flow flowForStageSequence(Stage<? extends StageContext>... stages) {
    var flowBuilder = Flow.builder();
    for (var stage : stages) {
      flowBuilder.stage(stage);
    }

    return flowBuilder.build();
  }

  @SafeVarargs
  public static Flow flowForStageSequence(FlowExecutionStrategy strategy, Stage<? extends StageContext>... stages) {
    var flowBuilder = Flow.builder().executionStrategy(strategy);
    for (var stage : stages) {
      flowBuilder.stage(stage);
    }

    return flowBuilder.build();
  }

  @SafeVarargs
  public static Flow flowForStageSequence(String id, Stage<StageContext>... stages) {
    var flowBuilder = Flow.builder().id(id);
    for (var stage : stages) {
      flowBuilder.stage(stage);
    }

    return flowBuilder.build();
  }

  @SafeVarargs
  public static Flow flowForStageSequence(String id, FlowExecutionStrategy strategy, Stage<StageContext>... stages) {
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

  public static StageContext stageContext(Stage<? extends StageContext> root) {
    return StageContext.of(root.getId(), emptyMap(), emptyMap());
  }

  public static StageContext stageContext(Stage<? extends StageContext> root, Map<?, ?> flowParameters) {
    return StageContext.of(root.getId(), flowParameters, emptyMap());
  }

  public static StageResult stageResult(Stage<? extends StageContext> root, String name, ExecutionStatus status) {
    return StageResult.builder()
      .flowId(root.getId())
      .stageId(name)
      .status(status)
      .build();
  }

  public static StageResult stageResult(Stage<? extends StageContext> root,
    Stage<? extends StageContext> stage, ExecutionStatus status) {
    return stageResult(root, stage, status, null, emptyList());
  }

  public static StageResult stageResult(Stage<? extends StageContext> root, Stage<? extends StageContext> stage,
    ExecutionStatus status, Exception error) {
    return stageResult(root, stage, status, error, emptyList());
  }

  public static StageResult stageResult(Stage<? extends StageContext> root, Stage<? extends StageContext> stage,
    ExecutionStatus status, List<StageResult> results) {
    return stageResult(root, stage, status, null, results);
  }

  public static StageResult stageResult(Stage<? extends StageContext> root, Stage<? extends StageContext> stage,
    ExecutionStatus status, Exception error, List<StageResult> subResults) {
    return StageResult.builder()
      .flowId(root.getId())
      .stageId(stage.getId())
      .stageType(resolveStageType(stage))
      .status(status)
      .error(error)
      .subStageResults(subResults)
      .build();
  }

  @SneakyThrows
  public static List<StageResult> stageResults(Throwable error) {
    return ((StageExecutionException) error).getStageResults();
  }

  public static StageExecutionResult stageExecutionResult(String name, StageContext context, ExecutionStatus status) {
    return StageExecutionResult.builder()
      .stageName(name)
      .status(status)
      .context(context)
      .build();
  }

  public static void executeFlow(Flow flow, FlowEngine flowEngine) {
    flowEngine.execute(flow, FIVE_HUNDRED_MILLISECONDS);
  }

  public static Object setParameterInContext(InvocationOnMock invocation, Object key, Object value) {
    var stageContext = invocation.<StageContext>getArgument(0);
    stageContext.put(key, value);
    return null;
  }

  public static Object removeParameterFromContext(InvocationOnMock invocation, Object key) {
    var stageContext = invocation.<StageContext>getArgument(0);
    stageContext.remove(key);
    return null;
  }

  @SafeVarargs
  public static void mockStageNames(Stage<? extends StageContext>... stages) {
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

  private static String resolveStageType(Stage<?> stage) {
    if (stage instanceof Flow) {
      return "Flow";
    }

    if (stage instanceof ParallelStage) {
      return "ParallelStage";
    }

    if (stage instanceof DynamicStage) {
      return "DynamicStage";
    }

    return "Stage";
  }
}

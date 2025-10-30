package org.folio.flow.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.flow.api.ParallelStage.parallelStageBuilder;
import static org.folio.flow.utils.FlowTestUtils.PARAMETERIZED_TEST_NAME;
import static org.folio.flow.utils.FlowTestUtils.executeFlow;
import static org.folio.flow.utils.FlowTestUtils.flowForStageSequence;
import static org.folio.flow.utils.FlowTestUtils.mockStageNames;
import static org.folio.flow.utils.FlowTestUtils.stageContext;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.folio.flow.api.Flow;
import org.folio.flow.api.FlowEngine;
import org.folio.flow.api.ParallelStage;
import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;
import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class ParallelStageCustomExecutorTest {

  @Mock
  private Stage<StageContext> simpleStage1;
  @Mock
  private Stage<StageContext> simpleStage2;
  @Mock
  private Stage<StageContext> simpleStage3;

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(simpleStage1, simpleStage2, simpleStage3);
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  @DisplayName("execute_positive_customExecutorUsedForParallelStage")
  void execute_positive_customExecutorUsedForParallelStage(FlowEngine flowEngine) {
    var customExecutorInvocationCount = new AtomicInteger(0);
    var customExecutor = createCustomExecutor(customExecutorInvocationCount);

    mockStageNames(simpleStage1, simpleStage2, simpleStage3);
    var parallelStage = ParallelStage.of("custom-executor-stage",
      List.of(simpleStage1, simpleStage2, simpleStage3), customExecutor);
    var flow = flowForStageSequence(parallelStage);

    executeFlow(flow, flowEngine);

    verify(simpleStage1).execute(stageContext(flow));
    verify(simpleStage2).execute(stageContext(flow));
    verify(simpleStage3).execute(stageContext(flow));

    assertThat(customExecutorInvocationCount.get())
      .as("Custom executor should be invoked for parallel execution")
      .isGreaterThan(0);
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  @DisplayName("execute_positive_customExecutorUsedWithBuilder")
  void execute_positive_customExecutorUsedWithBuilder(FlowEngine flowEngine) {
    var customExecutorInvocationCount = new AtomicInteger(0);
    var customExecutor = createCustomExecutor(customExecutorInvocationCount);

    mockStageNames(simpleStage1, simpleStage2);
    var parallelStage = parallelStageBuilder()
      .id("builder-stage")
      .stage(simpleStage1)
      .stage(simpleStage2)
      .executor(customExecutor)
      .build();

    var flow = flowForStageSequence(parallelStage);
    executeFlow(flow, flowEngine);

    verify(simpleStage1).execute(stageContext(flow));
    verify(simpleStage2).execute(stageContext(flow));

    assertThat(customExecutorInvocationCount.get())
      .as("Custom executor should be invoked")
      .isGreaterThan(0);
  }

  @Test
  @DisplayName("execute_positive_customExecutorWithDifferentThreadPool")
  void execute_positive_customExecutorWithDifferentThreadPool() {
    var defaultThreadNames = ConcurrentHashMap.<String>newKeySet();
    var customThreadNames = ConcurrentHashMap.<String>newKeySet();

    var defaultStage1 = createThreadCapturingStage(defaultThreadNames);
    var defaultStage2 = createThreadCapturingStage(defaultThreadNames);

    var customStage1 = createThreadCapturingStage(customThreadNames);
    var customStage2 = createThreadCapturingStage(customThreadNames);

    var customExecutor = Executors.newFixedThreadPool(2, r -> {
      var thread = new Thread(r);
      thread.setName("custom-executor-thread-" + thread.getName());
      return thread;
    });

    try {
      var defaultParallelStage = ParallelStage.of("default-stage",
        List.of(defaultStage1, defaultStage2));
      var customParallelStage = ParallelStage.of("custom-stage",
        List.of(customStage1, customStage2), customExecutor);

      var flow = flowForStageSequence(defaultParallelStage, customParallelStage);

      var flowEngine = FlowEngine.builder()
        .printFlowResult(false)
        .name("test-flow-engine")
        .build();

      executeFlow(flow, flowEngine);

      assertThat(customThreadNames)
        .as("Custom executor should use different threads")
        .isNotEmpty()
        .allMatch(name -> name.contains("custom-executor-thread"));

      assertThat(defaultThreadNames)
        .as("Default executor should use different threads")
        .isNotEmpty()
        .noneMatch(name -> name.contains("custom-executor-thread"));
    } finally {
      if (customExecutor instanceof java.util.concurrent.ExecutorService) {
        ((java.util.concurrent.ExecutorService) customExecutor).shutdown();
      }
    }
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  @DisplayName("execute_positive_noCustomExecutorUsesDefault")
  void execute_positive_noCustomExecutorUsesDefault(FlowEngine flowEngine) {
    mockStageNames(simpleStage1, simpleStage2);
    var parallelStage = ParallelStage.of("default-executor-stage",
      List.of(simpleStage1, simpleStage2));

    var flow = flowForStageSequence(parallelStage);
    executeFlow(flow, flowEngine);

    verify(simpleStage1).execute(stageContext(flow));
    verify(simpleStage2).execute(stageContext(flow));

    assertThat(parallelStage.getCustomExecutor())
      .as("No custom executor should be set")
      .isNull();
  }

  @Test
  @DisplayName("execute_positive_customExecutorControlsConcurrency")
  void execute_positive_customExecutorControlsConcurrency() {
    var maxConcurrency = new AtomicInteger(0);
    var currentConcurrency = new AtomicInteger(0);
    var executionCounts = new AtomicInteger(0);

    var stage1 = createConcurrencyTestStage(currentConcurrency, maxConcurrency, executionCounts);
    var stage2 = createConcurrencyTestStage(currentConcurrency, maxConcurrency, executionCounts);
    var stage3 = createConcurrencyTestStage(currentConcurrency, maxConcurrency, executionCounts);
    var stage4 = createConcurrencyTestStage(currentConcurrency, maxConcurrency, executionCounts);

    var customExecutor = Executors.newFixedThreadPool(2);

    try {
      var parallelStage = ParallelStage.of("concurrency-test",
        List.of(stage1, stage2, stage3, stage4), customExecutor);

      var flow = Flow.builder().stage(parallelStage).build();

      var flowEngine = FlowEngine.builder()
        .printFlowResult(false)
        .name("concurrency-test-engine")
        .build();

      executeFlow(flow, flowEngine);

      assertThat(executionCounts.get())
        .as("All stages should execute")
        .isEqualTo(4);

      assertThat(maxConcurrency.get())
        .as("Max concurrency should be limited by custom executor")
        .isLessThanOrEqualTo(2);
    } finally {
      if (customExecutor instanceof java.util.concurrent.ExecutorService) {
        ((java.util.concurrent.ExecutorService) customExecutor).shutdown();
      }
    }
  }

  @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
  @MethodSource("org.folio.flow.utils.FlowTestUtils#flowEnginesDataSource")
  @DisplayName("execute_positive_nestedParallelStagesWithDifferentExecutors")
  void execute_positive_nestedParallelStagesWithDifferentExecutors(FlowEngine flowEngine) {
    var outerExecutorCount = new AtomicInteger(0);
    var innerExecutorCount = new AtomicInteger(0);

    var outerExecutor = createCustomExecutor(outerExecutorCount);
    var innerExecutor = createCustomExecutor(innerExecutorCount);

    mockStageNames(simpleStage1, simpleStage2, simpleStage3);

    var innerParallelStage = ParallelStage.of("inner-stage",
      List.of(simpleStage2, simpleStage3), innerExecutor);

    var outerParallelStage = ParallelStage.of("outer-stage",
      List.of(simpleStage1, innerParallelStage), outerExecutor);

    var flow = flowForStageSequence(outerParallelStage);
    executeFlow(flow, flowEngine);

    verify(simpleStage1).execute(stageContext(flow));
    verify(simpleStage2).execute(stageContext(flow));
    verify(simpleStage3).execute(stageContext(flow));

    assertThat(outerExecutorCount.get())
      .as("Outer executor should be invoked")
      .isGreaterThan(0);

    assertThat(innerExecutorCount.get())
      .as("Inner executor should be invoked")
      .isGreaterThan(0);
  }

  private Executor createCustomExecutor(AtomicInteger invocationCount) {
    return command -> {
      invocationCount.incrementAndGet();
      command.run();
    };
  }

  private Stage<StageContext> createThreadCapturingStage(Set<String> threadNames) {
    return context -> {
      threadNames.add(Thread.currentThread().getName());
    };
  }

  private Stage<StageContext> createConcurrencyTestStage(
    AtomicInteger currentConcurrency, AtomicInteger maxConcurrency, AtomicInteger executionCounts) {
    return context -> {
      var current = currentConcurrency.incrementAndGet();
      executionCounts.incrementAndGet();

      maxConcurrency.updateAndGet(max -> Math.max(max, current));
      currentConcurrency.decrementAndGet();
    };
  }
}

package org.folio.flow.api;

import static org.folio.flow.utils.FlowUtils.defaultIfNull;
import static org.folio.flow.utils.FlowUtils.generateRandomId;
import static org.folio.flow.utils.FlowUtils.requireNonNull;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import org.folio.flow.exception.FlowCancelledException;
import org.folio.flow.impl.FlowEngineImpl;
import org.folio.flow.model.ExecutionStatus;
import org.folio.flow.utils.StageReportProvider;

public interface FlowEngine {

  /**
   * Executes flow using default executor, default timeout, and timeunit parameters.
   *
   * @throws FlowCancelledException if flow has been cancelled due to error in stage
   * @throws org.folio.flow.exception.FlowExecutionException if flow has been failed
   */
  void execute(Flow flow);

  /**
   * Executes flow using default executor.
   *
   * @param customDuration - the maximum time to wait as {@link Duration} object
   * @throws FlowCancelledException if flow has been cancelled due to error in stage
   * @throws org.folio.flow.exception.FlowExecutionException if flow has been failed
   */
  void execute(Flow flow, Duration customDuration);

  /**
   * Executes asynchronously flow using default executor.
   *
   * @throws FlowCancelledException if flow has been cancelled due to error in stage
   * @throws org.folio.flow.exception.FlowExecutionException if flow has been failed
   */
  Future<Void> executeAsync(Flow flow);

  /**
   * Retrieves last flow status.
   *
   * @param flow - flow to check
   * @return flow execution status as {@link ExecutionStatus}, can be unknown if {@link ExecutionStatus#UNKNOWN} if flow
   *   engine execution query does not contain information about this flow
   */
  ExecutionStatus getFlowStatus(Flow flow);

  /**
   * Returns a new builder for {@link FlowEngine} object.
   *
   * @return {@link FlowEngineBuilder} object
   */
  static FlowEngineBuilder builder() {
    return new FlowEngineBuilder();
  }

  /**
   * Builder for {@link FlowEngine} object.
   */
  class FlowEngineBuilder {

    private static final Duration DEFAULT_AWAIT_DURATION = Duration.ofMinutes(5);

    private String name;
    private Executor executor;
    private Duration executionTimeout;
    private int lastExecutionsStatusCacheSize = 50;
    private StageReportProvider stageReportProvider;
    private boolean printFlowResult = false;

    /**
     * Sets id field and returns {@link FlowEngineBuilder}.
     *
     * @return modified {@link FlowEngineBuilder} value
     */
    public FlowEngineBuilder name(String name) {
      this.name = requireNonNull(name, "Flow engine name must not be null");
      return this;
    }

    /**
     * Sets cacheSize field and returns {@link FlowEngineBuilder}.
     *
     * @return modified {@link FlowEngineBuilder} value
     */
    public FlowEngineBuilder lastExecutionsStatusCacheSize(int lastExecutionsStatusCacheSize) {
      if (lastExecutionsStatusCacheSize < 0) {
        throw new IllegalArgumentException("lastExecutionsStatusCacheSize must be more than 0");
      }

      this.lastExecutionsStatusCacheSize = lastExecutionsStatusCacheSize;
      return this;
    }

    /**
     * Sets executor field and returns {@link FlowEngineBuilder}.
     *
     * @return modified {@link FlowEngineBuilder} value
     */
    public FlowEngineBuilder executor(Executor executor) {
      this.executor = requireNonNull(executor, "Flow engine executor must not be null");
      return this;
    }

    /**
     * Sets executionTimeout field and returns {@link FlowEngineBuilder}.
     *
     * @return modified {@link FlowEngineBuilder} value
     */
    public FlowEngineBuilder executionTimeout(Duration executionTimeout) {
      this.executionTimeout = requireNonNull(executionTimeout, "executionTimeout must be not null");
      return this;
    }

    /**
     * Sets printFlowResult field and returns {@link FlowEngineBuilder}.
     *
     * @return modified {@link FlowEngineBuilder} value
     */
    public FlowEngineBuilder printFlowResult(boolean printFlowResult) {
      this.printFlowResult = printFlowResult;
      return this;
    }

    /**
     * Sets stageReportProvider field and returns {@link FlowEngineBuilder}.
     *
     * @return modified {@link FlowEngineBuilder} value
     */
    public FlowEngineBuilder stageReportProvider(StageReportProvider stageReportProvider) {
      this.stageReportProvider = requireNonNull(stageReportProvider, "stageReportProvider must not be null");
      return this;
    }

    public FlowEngine build() {
      return new FlowEngineImpl(
        defaultIfNull(this.name, () -> "flow-engine-" + generateRandomId()),
        this.lastExecutionsStatusCacheSize,
        defaultIfNull(this.executor, ForkJoinPool::commonPool),
        defaultIfNull(this.executionTimeout, () -> DEFAULT_AWAIT_DURATION),
        this.printFlowResult,
        defaultIfNull(stageReportProvider, () -> StageReportProvider.builder().build())
      );
    }
  }
}

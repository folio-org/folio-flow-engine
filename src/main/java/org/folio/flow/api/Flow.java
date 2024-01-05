package org.folio.flow.api;

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.folio.flow.model.FlowExecutionStrategy.CANCEL_ON_ERROR;
import static org.folio.flow.utils.CollectionUtils.emptyIfNull;
import static org.folio.flow.utils.FlowUtils.generateRandomId;
import static org.folio.flow.utils.FlowUtils.getStageExecutor;
import static org.folio.flow.utils.FlowUtils.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.folio.flow.impl.StageExecutor;
import org.folio.flow.model.FlowExecutionStrategy;
import org.folio.flow.utils.FlowUtils;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class Flow implements Stage {

  /**
   * Flow identifier.
   */
  @EqualsAndHashCode.Include
  private final String id;

  /**
   * Flow parameters.
   */
  @EqualsAndHashCode.Include
  private final Map<Object, Object> flowParameters;

  /**
   * Flow execution strategy.
   */
  @EqualsAndHashCode.Include
  private final FlowExecutionStrategy flowExecutionStrategy;

  /**
   * List with executable stages.
   */
  private final List<StageExecutor> stages;

  /**
   * Stage to be executed if flow is ignored.
   */
  private final StageExecutor onFlowSkipFinalStage;

  /**
   * Stage to be executed if flow is finished with {@link org.folio.flow.exception.FlowExecutionException}.
   */
  private final StageExecutor onFlowErrorFinalStage;

  /**
   * Stage to be executed if flow is finished with {@link org.folio.flow.exception.FlowCancelledException}.
   */
  private final StageExecutor onFlowCancellationFinalStage;

  /**
   * Stage to be executed if flow is finished with {@link org.folio.flow.exception.FlowCancellationException}.
   */
  private final StageExecutor onFlowCancellationErrorFinalStage;

  /**
   * Creates an {@link FlowBuilder} object.
   *
   * @return created {@link FlowBuilder} object
   */
  public static FlowBuilder builder() {
    return new FlowBuilder();
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public String toString() {
    return this.id;
  }

  @Override
  public void execute(StageContext context) {
    throw new UnsupportedOperationException("method execute(StageContext context) is not supported by Flow");
  }

  /**
   * Builder for {@link Flow} object.
   */
  @Data
  public static class FlowBuilder {

    private String id;
    private Map<Object, Object> flowParameters;
    private FlowExecutionStrategy flowExecutionStrategy;

    private List<StageExecutor> stageExecutors;

    private StageExecutor onFlowSkipFinalStage;
    private StageExecutor onFlowErrorFinalStage;
    private StageExecutor onFlowCancellationFinalStage;
    private StageExecutor onFlowCancellationErrorFinalStage;

    /**
     * Sets the id for flow. If it is not specified, it will be generated.
     *
     * @param id - flow id or name as {@link String}.
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder id(Object id) {
      FlowUtils.requireNonNull(id, "Flow id must not be null");
      this.id = id.toString();
      return this;
    }

    /**
     * Adds {@link Stage} to the {@link FlowBuilder} object.
     *
     * @param stage - {@link Stage} object
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder stage(Stage stage) {
      requireNonNull(stage, "Stage must not be null");
      if (stageExecutors == null) {
        this.stageExecutors = new ArrayList<>();
      }

      this.stageExecutors.add(getStageExecutor(stage));
      return this;
    }

    /**
     * Adds {@link StageExecutor} to the {@link FlowBuilder} object.
     *
     * @param stage - {@link StageExecutor} object
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder stage(StageExecutor stage) {
      requireNonNull(stage, "Stage must not be null");
      if (stageExecutors == null) {
        this.stageExecutors = new ArrayList<>();
      }

      this.stageExecutors.add(stage);
      return this;
    }

    /**
     * Adds a flow parameter to the {@link FlowBuilder} object.
     *
     * @param key - flow parameter key
     * @param value - flow parameter value
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder flowParameter(Object key, Object value) {
      if (this.flowParameters == null) {
        this.flowParameters = new HashMap<>();
      }

      this.flowParameters.put(key, value);
      return this;
    }

    /**
     * Adds a flow parameters map to the {@link FlowBuilder} object.
     *
     * @param flowParameters - map with flow parameters
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder flowParameters(Map<?, ?> flowParameters) {
      FlowUtils.requireNonNull(flowParameters, "Flow parameters must not be null");
      if (this.flowParameters == null) {
        this.flowParameters = new HashMap<>();
      }

      this.flowParameters.putAll(flowParameters);
      return this;
    }

    /**
     * Sets {@link FlowExecutionStrategy} for the {@link FlowBuilder} object.
     *
     * @param flowExecutionStrategy - the execution strategy type.
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder executionStrategy(FlowExecutionStrategy flowExecutionStrategy) {
      this.flowExecutionStrategy = requireNonNull(flowExecutionStrategy, "Flow execution strategy cannot be null");
      return this;
    }

    /**
     * Adds a {@link Stage} that will be executed only if flow finished with error as a last stage.
     *
     * @param stage - a {@link Stage} to execute if flow finished with error
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder onFlowError(Stage stage) {
      requireNonNull(stage, "onFlowError stage must not be null");
      this.onFlowErrorFinalStage = getStageExecutor(stage);
      return this;
    }

    /**
     * Adds a {@link StageExecutor} that will be executed only if flow finished with error as a final stage.
     *
     * @param stageExecutor - a {@link StageExecutor} to execute if flow finished with error
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder onFlowError(StageExecutor stageExecutor) {
      this.onFlowErrorFinalStage = requireNonNull(stageExecutor, "onFlowError stage must not be null");
      return this;
    }

    /**
     * Adds a {@link Stage} that will be executed only if flow finished with error and cancelled.
     *
     * @param stage - a {@link Stage} to execute if flow cancelled
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder onFlowCancellation(Stage stage) {
      requireNonNull(stage, "onFlowCancellation stage must not be null");
      this.onFlowCancellationFinalStage = getStageExecutor(stage);
      return this;
    }

    /**
     * Adds a {@link StageExecutor} that will be executed only if flow finished with error and cancelled.
     *
     * @param stageExecutor - a {@link StageExecutor} to execute if flow cancelled
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder onFlowCancellation(StageExecutor stageExecutor) {
      requireNonNull(stageExecutor, "onFlowCancellation stage must not be null");
      this.onFlowCancellationFinalStage = stageExecutor;
      return this;
    }

    /**
     * Adds a {@link Stage} that will be executed only if flow failed to cancel.
     *
     * @param stage - a {@link Stage} to execute if flow is failed to cancel
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder onFlowCancellationError(Stage stage) {
      requireNonNull(stage, "onFlowCancellationError stage must not be null");
      this.onFlowCancellationErrorFinalStage = getStageExecutor(stage);
      return this;
    }

    /**
     * Adds a {@link Stage} that will be executed only if flow failed to cancel.
     *
     * @param stage - a {@link Stage} to execute if flow is failed to cancel
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder onFlowCancellationError(StageExecutor stage) {
      this.onFlowCancellationErrorFinalStage = requireNonNull(stage, "onFlowCancellationError stage must not be null");
      return this;
    }

    /**
     * Adds a {@link Stage} that will be executed only if flow is skipped.
     *
     * @param stage - a {@link Stage} to execute if flow is skipped
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder onFlowSkip(Stage stage) {
      requireNonNull(stage, "onFlowSkipError stage must not be null");
      this.onFlowSkipFinalStage = getStageExecutor(stage);
      return this;
    }

    /**
     * Adds a {@link Stage} that will be executed only if flow is skipped.
     *
     * @param stage - a {@link Stage} to execute if flow is skipped
     * @return reference to the current {@link FlowBuilder} object
     */
    public FlowBuilder onFlowSkip(StageExecutor stage) {
      this.onFlowSkipFinalStage = requireNonNull(stage, "onFlowSkip stage must not be null");
      return this;
    }

    /**
     * Creates a {@link Flow} object from builder.
     *
     * @return immutable {@link Flow} object
     */
    public Flow build() {
      return new Flow(
        defaultIfNull(this.id, "flow-" + generateRandomId()),
        Map.copyOf(defaultIfNull(this.flowParameters, emptyMap())),
        defaultIfNull(this.flowExecutionStrategy, CANCEL_ON_ERROR),
        List.copyOf(emptyIfNull(this.stageExecutors)),
        onFlowSkipFinalStage,
        onFlowErrorFinalStage,
        onFlowCancellationFinalStage,
        onFlowCancellationErrorFinalStage);
    }
  }
}

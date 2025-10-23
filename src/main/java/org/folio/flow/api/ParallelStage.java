package org.folio.flow.api;

import static java.util.stream.Collectors.toList;
import static org.folio.flow.utils.CollectionUtils.emptyIfNull;
import static org.folio.flow.utils.FlowUtils.generateRandomId;
import static org.folio.flow.utils.FlowUtils.getStageExecutor;
import static org.folio.flow.utils.FlowUtils.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.folio.flow.impl.StageExecutor;
import org.folio.flow.utils.FlowUtils;

@Data
@RequiredArgsConstructor
@SuppressWarnings("ClassCanBeRecord")
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public final class ParallelStage implements Stage<StageContext> {

  /**
   * Parallel stage identifier.
   */
  private final String id;

  /**
   * List of {@link StageExecutor} objects to be executed in parallel.
   */
  private final List<StageExecutor> stages;

  /**
   * Defines if stage must be cancelled even it's failed.
   */
  private final boolean shouldCancelIfFailed;

  /**
   * Optional custom executor for this parallel stage.
   */
  private final Executor customExecutor;

  /**
   * Creates {@link ParallelStage} object from an arbitrary number of {@link Stage} objects.
   *
   * @param stages - array of {@link Stage} objects as varargs.
   * @return created {@link ParallelStage} object
   */
  @SafeVarargs
  public static ParallelStage of(Stage<? extends StageContext>... stages) {
    return ParallelStage.of(null, List.of(stages));
  }

  /**
   * Creates {@link ParallelStage} object from an arbitrary number of {@link Stage} objects.
   *
   * @param id - stage identifier
   * @param stages - array of {@link Stage} objects as varargs.
   * @return created {@link ParallelStage} object
   */
  @SafeVarargs
  public static ParallelStage of(String id, Stage<? extends StageContext>... stages) {
    return ParallelStage.of(getParallelStageId(id), List.of(stages));
  }

  /**
   * Creates {@link ParallelStage} object from a list of {@link Stage} objects.
   *
   * @param stages - {@link List} with {@link Stage} objects
   * @return created {@link ParallelStage} object
   */
  public static ParallelStage of(List<? extends Stage<? extends StageContext>> stages) {
    return ParallelStage.of(null, stages);
  }

  /**
   * Creates {@link ParallelStage} object from a list of {@link Stage} objects.
   *
   * @param id - stage identifier
   * @param stages - {@link List} with {@link Stage} objects
   * @return created {@link ParallelStage} object
   */
  public static ParallelStage of(String id, List<? extends Stage<? extends StageContext>> stages) {
    var stageExecutors = stages.stream()
      .map(FlowUtils::getStageExecutor)
      .toList();

    return new ParallelStage(getParallelStageId(id), stageExecutors, true, null);
  }

  /**
   * Creates {@link ParallelStage} object from a list of {@link Stage} objects with a custom executor.
   *
   * @param id - stage identifier
   * @param stages - {@link List} with {@link Stage} objects
   * @param customExecutor - custom {@link Executor} for parallel execution
   * @return created {@link ParallelStage} object
   */
  public static ParallelStage of(String id, List<? extends Stage<? extends StageContext>> stages,
    Executor customExecutor) {
    var stageExecutors = stages.stream()
      .map(FlowUtils::getStageExecutor)
      .toList();

    return new ParallelStage(getParallelStageId(id), stageExecutors, true, customExecutor);
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public String toString() {
    return this.id;
  }

  private static String getParallelStageId(String id) {
    return StringUtils.isNotBlank(id) ? id : "par-stage-" + generateRandomId();
  }

  @Override
  public void execute(StageContext context) {
    throw new UnsupportedOperationException("method execute(StageContext context) is not supported for ParallelStage");
  }

  /**
   * Creates an {@link ParallelStageBuilder} object.
   *
   * @return created {@link ParallelStageBuilder} object
   */
  public static ParallelStageBuilder parallelStageBuilder() {
    return new ParallelStageBuilder();
  }

  /**
   * Creates an {@link ParallelStageBuilder} object with identifier.
   *
   * @return created {@link ParallelStageBuilder} object
   */
  public static ParallelStageBuilder parallelStageBuilder(String id) {
    return new ParallelStageBuilder().id(id);
  }

  /**
   * Builder for {@link Flow} object.
   */
  @Data
  public static class ParallelStageBuilder {

    private String id;
    private List<StageExecutor> stageExecutors;
    private boolean shouldCancelIfFailed = true;
    private Executor customExecutor;

    /**
     * Sets the id for flow. If it is not specified, it will be generated.
     *
     * @param id - flow id or name as {@link String}.
     * @return reference to the current {@link ParallelStageBuilder} object
     */
    public ParallelStageBuilder id(Object id) {
      FlowUtils.requireNonNull(id, "Parallel stage id must not be null");
      this.id = id.toString();
      return this;
    }

    /**
     * Adds {@link Stage} to the {@link ParallelStageBuilder} object.
     *
     * @param stage - {@link Stage} object
     * @return reference to the current {@link ParallelStageBuilder} object
     */
    public ParallelStageBuilder stage(Stage<? extends StageContext> stage) {
      requireNonNull(stage, "Stage must not be null");
      if (stageExecutors == null) {
        this.stageExecutors = new ArrayList<>();
      }

      this.stageExecutors.add(getStageExecutor(stage));
      return this;
    }

    /**
     * Adds {@link StageExecutor} to the {@link ParallelStageBuilder} object.
     *
     * @param stage - {@link StageExecutor} object
     * @return reference to the current {@link ParallelStageBuilder} object
     */
    public ParallelStageBuilder stage(StageExecutor stage) {
      requireNonNull(stage, "Stage must not be null");
      if (stageExecutors == null) {
        this.stageExecutors = new ArrayList<>();
      }

      this.stageExecutors.add(stage);
      return this;
    }

    /**
     * Adds {@link Stage} to the {@link ParallelStageBuilder} object.
     *
     * @param stage - {@link Stage} object
     * @return reference to the current {@link ParallelStageBuilder} object
     */
    public ParallelStageBuilder stages(List<Stage<? extends StageContext>> stage) {
      this.stageExecutors = requireNonNull(stage, "Stages must not be null").stream()
        .map(FlowUtils::getStageExecutor)
        .collect(toList());

      return this;
    }

    public ParallelStageBuilder shouldCancelIfFailed(boolean shouldCancelIfFailed) {
      this.shouldCancelIfFailed = shouldCancelIfFailed;
      return this;
    }

    /**
     * Sets a custom executor for parallel execution.
     *
     * @param customExecutor - custom {@link Executor} for parallel stage execution
     * @return reference to the current {@link ParallelStageBuilder} object
     */
    public ParallelStageBuilder executor(Executor customExecutor) {
      this.customExecutor = customExecutor;
      return this;
    }

    /**
     * Creates a {@link Flow} object from builder.
     *
     * @return immutable {@link Flow} object
     */
    public ParallelStage build() {
      return new ParallelStage(
        getParallelStageId(this.id),
        List.copyOf(emptyIfNull(this.stageExecutors)),
        shouldCancelIfFailed,
        customExecutor);
    }
  }
}

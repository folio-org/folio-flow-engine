package org.folio.flow.api;

import static org.folio.flow.utils.FlowUtils.generateRandomId;

import java.util.function.Function;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public final class DynamicStage implements Stage {

  private final String name;
  private final Function<StageContext, Stage> stageProvider;

  /**
   * Creates dynamic stage from {@link StageContext} using stageProvider function.
   *
   * @param stageProvider - stage provider as {@link Function}
   * @return {@link DynamicStage} object
   */
  public static DynamicStage of(Function<StageContext, Stage> stageProvider) {
    return new DynamicStage("dynamic-stage-" + generateRandomId(), stageProvider);
  }

  /**
   * Creates dynamic stage from {@link StageContext} using stageProvider function.
   *
   * @param stageProvider - stage provider as {@link Function}
   * @return {@link DynamicStage} object
   */
  public static DynamicStage of(String name, Function<StageContext, Stage> stageProvider) {
    return new DynamicStage(name, stageProvider);
  }

  @Override
  public void execute(StageContext context) {
    throw new UnsupportedOperationException("method execute(StageContext context) is not supported for DynamicStage");
  }

  @Override
  public String toString() {
    return this.name;
  }
}

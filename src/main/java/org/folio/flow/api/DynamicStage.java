package org.folio.flow.api;

import static org.folio.flow.utils.FlowUtils.generateRandomId;

import java.util.function.Function;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
@SuppressWarnings("ClassCanBeRecord")
public final class DynamicStage implements Stage<StageContext> {

  private final String id;
  private final Function<StageContext, Stage<? extends StageContext>> stageProvider;

  /**
   * Creates dynamic stage from {@link StageContext} using stageProvider function.
   *
   * @param provider - stage provider as {@link Function}
   * @return {@link DynamicStage} object
   */
  public static DynamicStage of(Function<StageContext, Stage<? extends StageContext>> provider) {
    return new DynamicStage("dynamic-stage-" + generateRandomId(), provider);
  }

  /**
   * Creates dynamic stage from {@link StageContext} using stageProvider function.
   *
   * @param stageProvider - stage provider as {@link Function}
   * @return {@link DynamicStage} object
   */
  public static DynamicStage of(String id, Function<StageContext, Stage<? extends StageContext>> stageProvider) {
    return new DynamicStage(id, stageProvider);
  }

  @Override
  public void execute(StageContext context) {
    throw new UnsupportedOperationException("method execute(StageContext context) is not supported for DynamicStage");
  }

  @Override
  public String toString() {
    return this.id;
  }
}

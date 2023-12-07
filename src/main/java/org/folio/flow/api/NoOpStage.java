package org.folio.flow.api;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NoOpStage implements Stage {

  /**
   * {@inheritDoc}
   *
   * <p>This stage is intended to be a placeholder for optional set of stages to simplify flow creating</p>
   */
  @Override
  public void execute(StageContext context) {
    // this stage is intended to be a placeholder for optional stages
  }

  /**
   * Provides singleton instance for {@link NoOpStage}.
   *
   * @return {@link NoOpStage} singleton instance
   */
  public static NoOpStage getInstance() {
    return SingletonHolder.NO_OP_STAGE_INSTANCE;
  }

  /**
   * Provides singleton instance for {@link NoOpStage}.
   *
   * @return {@link NoOpStage} singleton instance
   */
  public static NoOpStage noOpStage() {
    return SingletonHolder.NO_OP_STAGE_INSTANCE;
  }

  @Override
  public String getId() {
    return "no-op-stage";
  }

  @Override
  public String toString() {
    return "no-op-stage";
  }

  /**
   * On demand holder for singleton, for lazy initialization.
   */
  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  static class SingletonHolder {

    /**
     * Singleton instance holder.
     */
    public static final NoOpStage NO_OP_STAGE_INSTANCE = new NoOpStage();
  }
}

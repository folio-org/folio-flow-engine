package org.folio.flow.api;

public interface Cancellable {

  /**
   * Provides an ability to cancel a stage if flow is being aborted.
   *
   * @param context - {@link StageContext} object with {@link Flow} parameters, etc.
   */
  void cancel(StageContext context);

  /**
   * Defines if stage must be cancelled even it's failed.
   *
   * @param context - {@link StageContext} object with {@link Flow} parameters, etc.
   * @return true if stage must be cancelled if failed, false - otherwise
   */
  default boolean shouldCancelIfFailed(StageContext context) {
    return false;
  }
}

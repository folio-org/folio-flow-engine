package org.folio.flow.api;

public interface Listenable {

  /**
   * Executes before calling {@link Stage#execute(StageContext)} method.
   *
   * @param context - {@link StageContext} object with {@link Flow} parameters, etc.
   */
  default void onStart(StageContext context) {}

  /**
   * Executes after calling {@link Stage#execute(StageContext)} method (on success method exit or after success recovery
   * operation).
   *
   * @param context - {@link StageContext} object with {@link Flow} parameters, etc.
   * @see Recoverable
   */
  default void onSuccess(StageContext context) {}

  /**
   * Executes after calling {@link Cancellable#cancel(StageContext)} method.
   *
   * @param context - {@link StageContext} object with {@link Flow} parameters, etc.
   */
  default void onCancel(StageContext context) {}

  /**
   * Executes after calling {@link Cancellable#cancel(StageContext)} method (if exception was thrown).
   *
   * @param context - {@link StageContext} object with {@link Flow} parameters, etc.
   */
  default void onCancelError(StageContext context, Exception exception) {}

  /**
   * Executes after calling {@link Stage#execute(StageContext)} method (if exception was thrown).
   *
   * @param context - {@link StageContext} object with {@link Flow} parameters, etc.
   */
  default void onError(StageContext context, Exception exception) {}
}

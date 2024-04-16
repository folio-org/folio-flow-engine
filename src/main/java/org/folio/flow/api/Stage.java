package org.folio.flow.api;

import org.folio.flow.utils.types.Cancellable;
import org.folio.flow.utils.types.Listenable;
import org.folio.flow.utils.types.Recoverable;

@FunctionalInterface
public interface Stage<T extends StageContext> {

  /**
   * Executes an operation in the flow.
   *
   * @param context - {@link StageContext} object with {@link Flow} parameters, etc.
   */
  void execute(T context);

  /**
   * Returns stage name.
   *
   * @return stage name as {@link String} object
   */
  default String getId() {
    return toString();
  }

  /**
   * Provides the ability to recover {@link Stage} after fail.
   *
   * @param context - {@link T} object with {@link Flow} parameters, etc.
   */
  @Recoverable
  default void recover(T context) {}

  /**
   * Provides an ability to cancel a stage if flow is being aborted.
   *
   * @param context - {@link T} object with {@link Flow} parameters, etc.
   */
  @Cancellable
  default void cancel(T context) {}

  /**
   * Defines if stage must be cancelled even it's failed.
   *
   * <p>This method is called only if {@link Stage#cancel(T)} is implemented</p>
   *
   * @param context - {@link T} object with {@link Flow} parameters, etc.
   * @return true if stage must be cancelled if failed, false - otherwise
   */
  @SuppressWarnings("unused")
  default boolean shouldCancelIfFailed(T context) {
    return false;
  }

  /**
   * Executes before calling {@link Stage#execute(T)} method.
   *
   * @param context - {@link T} object with {@link Flow} parameters, etc.
   */
  @Listenable
  default void onStart(T context) {}

  /**
   * Executes after calling {@link Stage#execute(T)} method (on success method exit or after success recovery
   * operation).
   *
   * @param context - {@link T} object with {@link Flow} parameters, etc.
   * @see org.folio.flow.api.Stage#recover(T)
   */
  @Listenable
  default void onSuccess(T context) {}

  /**
   * Executes after calling {@link org.folio.flow.api.Stage#cancel(T)} method.
   *
   * @param context - {@link T} object with {@link Flow} parameters, etc.
   */
  @Listenable
  default void onCancel(T context) {}

  /**
   * Executes after calling {@link org.folio.flow.api.Stage#cancel(T)} method (if exception was thrown).
   *
   * @param context - {@link T} object with {@link Flow} parameters, etc.
   */
  @Listenable
  default void onCancelError(T context, Exception exception) {}

  /**
   * Executes after calling {@link Stage#execute(T)} method (if exception was thrown).
   *
   * @param context - {@link T} object with {@link Flow} parameters, etc.
   */
  @Listenable
  default void onError(T context, Exception exception) {}
}

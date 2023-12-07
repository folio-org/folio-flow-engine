package org.folio.flow.api;

public interface Recoverable {

  /**
   * Provides the ability to recover {@link Stage} after fail.
   *
   * @param context - {@link StageContext} object with {@link Flow} parameters, etc.
   */
  void recover(StageContext context);
}

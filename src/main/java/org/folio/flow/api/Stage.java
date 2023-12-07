package org.folio.flow.api;

@FunctionalInterface
public interface Stage {

  /**
   * Executes an operation in the flow.
   *
   * @param context - {@link StageContext} object with {@link Flow} parameters, etc.
   */
  void execute(StageContext context);

  /**
   * Returns stage name.
   *
   * @return stage name as {@link String} object
   */
  default String getId() {
    return toString();
  }
}

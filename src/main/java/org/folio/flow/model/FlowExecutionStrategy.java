package org.folio.flow.model;

public enum FlowExecutionStrategy {

  /**
   * Sets the behaviour for {@link org.folio.flow.api.Flow} to cancel all executed tasks if exception occurred.
   */
  CANCEL_ON_ERROR,

  /**
   * Sets the behaviour for {@link org.folio.flow.api.Flow} to ignore cancellation process and exit with the latest
   * exception.
   */
  IGNORE_ON_ERROR
}

package org.folio.flow.api.models;

import org.folio.flow.api.Stage;

public class CustomSimpleStage implements Stage<TestStageContextWrapper> {

  @Override
  public void execute(TestStageContextWrapper context) {
    // do nothing, used as mock in unit tests
  }
}

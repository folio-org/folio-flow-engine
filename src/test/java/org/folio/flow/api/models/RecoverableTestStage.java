package org.folio.flow.api.models;

import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;

public class RecoverableTestStage implements Stage<StageContext> {

  @Override
  public void execute(StageContext context) {
    // do nothing, used as mock in unit tests
  }

  @Override
  public void recover(StageContext context) {
    Stage.super.recover(context);
  }
}

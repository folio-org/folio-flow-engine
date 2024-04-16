package org.folio.flow.api.models;

import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;

public class CancellableTestStage implements Stage<StageContext> {

  @Override
  public void execute(StageContext context) {
    // do nothing, used as mock in unit tests
  }

  @Override
  public void cancel(StageContext context) {
    Stage.super.cancel(context);
  }
}

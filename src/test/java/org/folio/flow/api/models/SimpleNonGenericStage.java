package org.folio.flow.api.models;

import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;

@SuppressWarnings("rawtypes")
public class SimpleNonGenericStage implements Stage {

  @Override
  public void execute(StageContext context) {
    // do nothing, used as mock in unit tests
  }
}

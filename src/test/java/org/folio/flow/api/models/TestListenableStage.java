package org.folio.flow.api.models;

import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;

public class TestListenableStage implements Stage<StageContext> {

  @Override
  public void execute(StageContext context) {
    // do nothing, used as mock in unit tests
  }

  @Override
  public void cancel(StageContext context) {
    Stage.super.cancel(context);
  }

  @Override
  public void onStart(StageContext context) {
    Stage.super.onStart(context);
  }

  @Override
  public void onSuccess(StageContext context) {
    Stage.super.onSuccess(context);
  }

  @Override
  public void onCancel(StageContext context) {
    Stage.super.onCancel(context);
  }

  @Override
  public void onCancelError(StageContext context, Exception exception) {
    Stage.super.onCancelError(context, exception);
  }

  @Override
  public void onError(StageContext context, Exception exception) {
    Stage.super.onError(context, exception);
  }
}

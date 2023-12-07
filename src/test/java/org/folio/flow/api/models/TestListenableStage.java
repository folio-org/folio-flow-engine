package org.folio.flow.api.models;

import org.folio.flow.api.Cancellable;
import org.folio.flow.api.Listenable;
import org.folio.flow.api.Stage;
import org.folio.flow.api.StageContext;

public interface TestListenableStage extends Stage, Cancellable, Listenable {

  @Override
  default void onStart(StageContext context) {
    Listenable.super.onStart(context);
  }

  @Override
  default void onSuccess(StageContext context) {
    Listenable.super.onSuccess(context);
  }

  @Override
  default void onCancel(StageContext context) {
    Listenable.super.onCancel(context);
  }

  @Override
  default void onCancelError(StageContext context, Exception exception) {
    Listenable.super.onCancelError(context, exception);
  }

  @Override
  default void onError(StageContext context, Exception exception) {
    Listenable.super.onError(context, exception);
  }
}

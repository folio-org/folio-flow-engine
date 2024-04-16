package org.folio.flow.api.models;

public class StageWithInheritance extends TestAbstractStage<TestStageContextWrapper> {

  @Override
  public void execute(TestStageContextWrapper context) {
    // do nothing, used as mock in unit tests
  }

  @Override
  public void recover(TestStageContextWrapper context) {
    // do nothing, used as mock in unit tests
  }

  @Override
  public void cancel(TestStageContextWrapper context) {
    // do nothing, used as mock in unit tests
  }
}

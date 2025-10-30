package org.folio.flow.api;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.Test;

@UnitTest
class StageTest {

  @Test
  void getId_positive_defaultImplementation() {
    var stage = new TestStage();
    assertThat(stage.getId()).isEqualTo(stage.toString());
  }

  @Test
  void recover_positive_defaultImplementation() {
    var stage = new TestStage();
    var context = StageContext.of("flow-id", emptyMap(), emptyMap());
    stage.recover(context);
  }

  @Test
  void cancel_positive_defaultImplementation() {
    var stage = new TestStage();
    var context = StageContext.of("flow-id", emptyMap(), emptyMap());
    stage.cancel(context);
  }

  @Test
  void shouldCancelIfFailed_positive_defaultImplementation() {
    var stage = new TestStage();
    var context = StageContext.of("flow-id", emptyMap(), emptyMap());
    assertThat(stage.shouldCancelIfFailed(context)).isFalse();
  }

  @Test
  void onStart_positive_defaultImplementation() {
    var stage = new TestStage();
    var context = StageContext.of("flow-id", emptyMap(), emptyMap());
    stage.onStart(context);
  }

  @Test
  void onSuccess_positive_defaultImplementation() {
    var stage = new TestStage();
    var context = StageContext.of("flow-id", emptyMap(), emptyMap());
    stage.onSuccess(context);
  }

  @Test
  void onCancel_positive_defaultImplementation() {
    var stage = new TestStage();
    var context = StageContext.of("flow-id", emptyMap(), emptyMap());
    stage.onCancel(context);
  }

  @Test
  void onCancelError_positive_defaultImplementation() {
    var stage = new TestStage();
    var context = StageContext.of("flow-id", emptyMap(), emptyMap());
    var exception = new RuntimeException("test error");
    stage.onCancelError(context, exception);
  }

  @Test
  void onError_positive_defaultImplementation() {
    var stage = new TestStage();
    var context = StageContext.of("flow-id", emptyMap(), emptyMap());
    var exception = new RuntimeException("test error");
    stage.onError(context, exception);
  }

  private static final class TestStage implements Stage<StageContext> {

    @Override
    public void execute(StageContext context) {
      // test implementation
    }
  }
}

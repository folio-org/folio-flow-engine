package org.folio.flow.api;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.Test;

@UnitTest
class DynamicStageTest {

  @Test
  void of_positive_withProviderOnly() {
    var stage = DynamicStage.of(ctx -> new TestStage());
    assertThat(stage.getId()).startsWith("dynamic-stage-");
    assertThat(stage.getStageProvider()).isNotNull();
  }

  @Test
  void of_positive_withIdAndProvider() {
    var stageId = "custom-dynamic-stage";
    var stage = DynamicStage.of(stageId, ctx -> new TestStage());
    assertThat(stage.getId()).isEqualTo(stageId);
    assertThat(stage.getStageProvider()).isNotNull();
  }

  @Test
  void execute_negative_unsupportedOperation() {
    var stage = DynamicStage.of(ctx -> new TestStage());
    var context = StageContext.of("flow-id", emptyMap(), emptyMap());
    
    assertThatThrownBy(() -> stage.execute(context))
      .isInstanceOf(UnsupportedOperationException.class)
      .hasMessage("method execute(StageContext context) is not supported for DynamicStage");
  }

  @Test
  void toString_positive() {
    var stageId = "test-dynamic-stage";
    var stage = DynamicStage.of(stageId, ctx -> new TestStage());
    assertThat(stage.toString()).hasToString(stageId);
  }

  @Test
  void getStageProvider_positive() {
    var provider = (Stage<? extends StageContext>) new TestStage();
    var stage = DynamicStage.of(ctx -> provider);
    assertThat(stage.getStageProvider().apply(null)).isEqualTo(provider);
  }

  private static final class TestStage implements Stage<StageContext> {

    @Override
    public void execute(StageContext context) {
      // test implementation
    }
  }
}

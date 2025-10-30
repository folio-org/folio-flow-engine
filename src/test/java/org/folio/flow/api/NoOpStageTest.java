package org.folio.flow.api;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.Test;

@UnitTest
class NoOpStageTest {

  @Test
  void getInstance_positive() {
    //noinspection EqualsWithItself
    assertThat(NoOpStage.getInstance()).isEqualTo(NoOpStage.getInstance());
  }

  @Test
  void execute_positive_firstFactoryMethod() {
    var stageContext = StageContext.of("flow-id", emptyMap(), emptyMap());
    NoOpStage.getInstance().execute(stageContext);
    assertThat(stageContext.data()).isEqualTo(emptyMap());
  }

  @Test
  void execute_positive_secondFactoryMethod() {
    var stageContext = StageContext.of("flow-id", emptyMap(), emptyMap());
    NoOpStage.noOpStage().execute(stageContext);
    assertThat(stageContext.data()).isEqualTo(emptyMap());
  }

  @Test
  void getId_positive() {
    assertThat(NoOpStage.getInstance().getId()).hasToString("no-op-stage");
  }

  @Test
  void toString_positive() {
    assertThat(NoOpStage.getInstance().toString()).hasToString("no-op-stage");
  }

  @Test
  void singletonPattern_positive() {
    var instance1 = NoOpStage.getInstance();
    var instance2 = NoOpStage.noOpStage();
    assertThat(instance1).isSameAs(instance2);
  }
}

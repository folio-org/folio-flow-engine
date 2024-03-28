package org.folio.flow.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.Test;

@UnitTest
class StageContextImplTest {

  @Test
  void contextParametersTest() {
    var stageContext = StageContext.of("flow-id", Map.of("key", "value"), new HashMap<>());

    var parameter = stageContext.<String>getFlowParameter("key");
    assertThat(parameter).isEqualTo("value");

    stageContext.put("customKey", 1234);

    assertThat(stageContext.<Integer>get("customKey")).isEqualTo(1234);
    assertThat(stageContext.get("customKey", Integer.class)).isEqualTo(1234);
    assertThat(stageContext.<Long>get("customKey2", 25L)).isEqualTo(25L);
    assertThat(stageContext.<Long>get("customKey2")).isNull();

    assertThatThrownBy(() -> {
      @SuppressWarnings("unused") var value = stageContext.<Integer>getFlowParameter("key");
    }).isInstanceOf(ClassCastException.class);

    assertThatThrownBy(() -> {
      @SuppressWarnings("unused") var value = stageContext.<String>get("customKey");
    }).isInstanceOf(ClassCastException.class);
  }

  @Test
  void contextWrapperParametersTest() {
    var stageContext = StageContext.of("flow-id", Map.of("key", "value"), new HashMap<>());
    var testWrapper = new AbstractStageContextWrapper(stageContext) {};

    assertThat(testWrapper.flowId()).isEqualTo("flow-id");
    assertThat(testWrapper.flowParameters()).isEqualTo(Map.of("key", "value"));
    assertThat(testWrapper.data()).isEmpty();

    var parameter = testWrapper.<String>getFlowParameter("key");
    assertThat(parameter).isEqualTo("value");

    testWrapper.put("customKey", 1234);

    assertThat(testWrapper.<Integer>get("customKey")).isEqualTo(1234);
    assertThat(testWrapper.get("customKey", Integer.class)).isEqualTo(1234);
    assertThat(testWrapper.<Long>get("customKey2", 25L)).isEqualTo(25L);
    assertThat(testWrapper.<Long>get("customKey2")).isNull();

    assertThatThrownBy(() -> {
      @SuppressWarnings("unused") var value = testWrapper.<Integer>getFlowParameter("key");
    }).isInstanceOf(ClassCastException.class);

    assertThatThrownBy(() -> {
      @SuppressWarnings("unused") var value = testWrapper.<String>get("customKey");
    }).isInstanceOf(ClassCastException.class);

    testWrapper.remove("customKey");
    assertThat(testWrapper.get("customKey", Integer.class)).isNull();

    var updatedStageContext = testWrapper.withFlowId("new-flow-id");
    assertThat(updatedStageContext.flowId()).isEqualTo("new-flow-id");
  }
}

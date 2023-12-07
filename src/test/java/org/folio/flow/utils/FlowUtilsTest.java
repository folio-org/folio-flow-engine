package org.folio.flow.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.Test;

@UnitTest
class FlowUtilsTest {

  @Test
  void generateRandomId_positive() {
    var result = FlowUtils.generateRandomId();
    assertThat(result).matches("[a-z0-9]{6}");
  }
}

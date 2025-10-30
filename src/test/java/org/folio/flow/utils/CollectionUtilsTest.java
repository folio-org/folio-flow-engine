package org.folio.flow.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.flow.utils.CollectionUtils.emptyIfNull;
import static org.folio.flow.utils.CollectionUtils.isEmpty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.Test;

@UnitTest
class CollectionUtilsTest {

  @Test
  void isEmpty_positive_nullCollection() {
    assertThat(isEmpty(null)).isTrue();
  }

  @Test
  void isEmpty_positive_emptyCollection() {
    assertThat(isEmpty(Collections.emptyList())).isTrue();
  }

  @Test
  void isEmpty_negative_nonEmptyCollection() {
    assertThat(isEmpty(List.of("item"))).isFalse();
  }

  @Test
  void emptyIfNull_positive_nullList() {
    List<String> result = emptyIfNull(null);
    assertThat(result).isEmpty();
  }

  @Test
  void emptyIfNull_positive_emptyList() {
    List<String> emptyList = Collections.emptyList();
    List<String> result = emptyIfNull(emptyList);
    assertThat(result).isSameAs(emptyList);
  }

  @Test
  void emptyIfNull_positive_nonEmptyList() {
    List<String> list = new ArrayList<>();
    list.add("item");
    List<String> result = emptyIfNull(list);
    assertThat(result).isSameAs(list).containsExactly("item");
  }
}

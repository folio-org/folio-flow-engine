package org.folio.flow.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CollectionUtils {

  public static boolean isEmpty(final Collection<?> coll) {
    return coll == null || coll.isEmpty();
  }

  public static <T> List<T> emptyIfNull(final List<T> list) {
    return list == null ? Collections.emptyList() : list;
  }
}

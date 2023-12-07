package org.folio.flow.utils;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ObjectUtils.isNotEmpty;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.folio.flow.api.DynamicStage;
import org.folio.flow.api.Flow;
import org.folio.flow.api.ParallelStage;
import org.folio.flow.api.Stage;
import org.folio.flow.impl.DefaultStageExecutor;
import org.folio.flow.impl.DynamicStageExecutor;
import org.folio.flow.impl.FlowExecutor;
import org.folio.flow.impl.ParallelStageExecutor;
import org.folio.flow.impl.StageExecutor;
import org.folio.flow.model.ExecutionStatus;
import org.folio.flow.model.StageResult;
import org.folio.flow.model.StageResultHolder;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FlowUtils {

  public static final String FLOW_ENGINE_LOGGER_NAME = "folio-flow-engine";

  /**
   * Checks that source object is not null.
   *
   * @param obj - value to check
   * @param message - error message
   * @param <T> - generic type for source value
   * @return source value if not null
   * @throws IllegalArgumentException if value is null
   */
  public static <T> T requireNonNull(T obj, String message) {
    if (obj == null) {
      throw new IllegalArgumentException(message);
    }

    return obj;
  }

  /**
   * Generates random string from digits and lowercase english-alphabet letters.
   *
   * @return generated identifier for flow logging.
   */
  public static String generateRandomId() {
    var randomStringLength = 6;
    var current = ThreadLocalRandom.current();
    return current.ints('0', (int) 'z' + 1)
      .filter(i -> i <= '9' || i >= 'a')
      .limit(randomStringLength)
      .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
      .toString();
  }

  public static Optional<StageResultHolder> getLastFailedStage(List<StageResultHolder> results) {
    var index = getLastFailedStageIndex(results, Set.of(FAILED));
    return getValueByIndex(results, index);
  }

  /**
   * Provides {@link StageExecutor} for {@link Stage} implementation.
   *
   * @param stage - stage as {@link Stage} object
   * @return {@link StageExecutor} object
   */
  public static StageExecutor getStageExecutor(Stage stage) {
    if (stage instanceof Flow flow) {
      return new FlowExecutor(flow);
    }

    if (stage instanceof ParallelStage parallelStage) {
      return new ParallelStageExecutor(parallelStage);
    }

    if (stage instanceof DynamicStage dynamicStage) {
      return new DynamicStageExecutor(dynamicStage);
    }

    return new DefaultStageExecutor(stage);
  }

  /**
   * Provides a last element of the {@link List} with {@link StageResultHolder} having failed status.
   *
   * @param results - a list with stage results to check
   * @param statuses - a set with failed statuses
   * @return {@link Optional} with last found failed {@link StageResultHolder}
   */
  public static Optional<StageResultHolder> getLastFailedStageWithAnyStatusOf(
    List<StageResultHolder> results, Set<ExecutionStatus> statuses) {
    var index = getLastFailedStageIndex(results, statuses);
    return getValueByIndex(results, index);
  }

  public static <T> Optional<T> getValueByIndex(List<T> list, int index) {
    if (index < 0 || index > list.size()) {
      return Optional.empty();
    }

    return Optional.of(list.get(index));
  }

  private static int getLastFailedStageIndex(List<StageResultHolder> results, Set<ExecutionStatus> executionStatuses) {
    for (int i = results.size() - 1; i >= 0; i--) {
      var current = results.get(i);
      var currentStatus = current.getStatus();
      if (executionStatuses.contains(currentStatus) || current.isFlow() && currentStatus == CANCELLED) {
        return i;
      }
    }

    return -1;
  }

  /**
   * Retrieves first element of the list as {@link Optional} object.
   *
   * @param list - list to retrieve first element
   * @param <T> - generic type for list element
   * @return {@link Optional} with first element of the list, it will be empty if source list is null or empty
   */
  public static <T> Optional<T> findFirstValue(List<T> list) {
    return isNotEmpty(list) ? Optional.of(list.get(0)) : Optional.empty();
  }

  /**
   * Converts a {@link List} with {@link StageResultHolder} object to the list with stage results.
   *
   * @param stageResultList - a {@link List} with {@link StageResultHolder}
   * @return {@link List} with {@link StageResult} objects
   */
  public static List<StageResult> convertToStageResults(List<StageResultHolder> stageResultList) {
    if (CollectionUtils.isEmpty(stageResultList)) {
      return emptyList();
    }

    return stageResultList.stream()
      .map(StageResultHolder::getResult)
      .map(StageResult::from)
      .collect(toList());
  }

  /**
   * Returns default value using {@link Supplier} if source value is null.
   *
   * @param value - value to check and return if not null
   * @param valueSupplier - default value {@link Supplier}.
   * @param <T> -generic type for source value
   * @return source value if not null, or provide a default value using supplier
   */
  public static <T> T defaultIfNull(T value, Supplier<T> valueSupplier) {
    return value == null ? valueSupplier.get() : value;
  }
}
